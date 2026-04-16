package main

import (
	"bytes"
	"fmt"
	"log"
	"math/rand"
	"os"
	"time"

	badger "github.com/dgraph-io/badger/v4"
)

// 内存真理账本
var truthMap = make(map[string]string)

func main() {
	rand.Seed(time.Now().UnixNano())
	dbPath := "./zzl_badger_data_debug"
	os.RemoveAll(dbPath)

	fmt.Println("=====================================================")
	fmt.Println("🚀 启动 HeatLSM 地狱级数据一致性测试 [Vlog 大 KV 破防版]")
	fmt.Println("=====================================================")

	// ==========================================
	// 1. 初始化数据库 (制造频繁 Flush 的恶劣环境)
	// ==========================================
	opt := badger.DefaultOptions(dbPath).
		WithMemTableSize(8 << 20). // 8MB，给大 KV 留点空间，避免 ErrTxnTooBig
		WithBaseTableSize(8 << 20).
		WithValueThreshold(4 << 10). // 💥 4KB 阈值！超过 4KB 的全部强制打入 Vlog！
		WithNumLevelZeroTables(5).   // 只要有1个L0表就触发合并
		WithNumMemtables(5).
		WithSyncWrites(false).
		WithLogger(nil)

	db, err := badger.Open(opt)
	if err != nil {
		log.Fatalf("❌ 数据库打开失败: %v", err)
	}

	// ==========================================
	// 2. 混沌写入阶段 (Chaos Writes)
	// ==========================================
	fmt.Println("\n>>> [阶段 1] 混沌写入与热点培养中 (强制大 KV 触发冷热 Vlog 分流)...")

	// 💥 缩小总次数防止 truthMap OOM，但依然保持极高密度的冲突
	const totalOps = 2000000
	const hotKeyCount = 3000     // 只有200个热点Key，疯狂覆写，产生大量 Vlog 垃圾
	const coldKeyCount = 1500000 // 两万个冷数据

	for i := 0; i < totalOps; i++ {
		isHot := rand.Intn(100) < 80 // 80% 的概率写热点数据

		var keyStr string
		if isHot {
			keyStr = fmt.Sprintf("hot_key_%04d", rand.Intn(hotKeyCount))
		} else {
			keyStr = fmt.Sprintf("cold_key_%08d", rand.Intn(coldKeyCount))
		}

		key := []byte(keyStr)

		// 10% 的概率进行删除测试
		if rand.Intn(100) < 10 {
			err = db.Update(func(txn *badger.Txn) error {
				return txn.Delete(key)
			})
			if err != nil {
				log.Fatalf("❌ 删除失败: %v", err)
			}
			delete(truthMap, keyStr) // 同步删除真理账本
		} else {
			// 💥 随机生成大 KV 或 小 KV
			payloadSize := 128 // 默认小 KV (进 LSM)
			if rand.Intn(100) < 70 {
				// 70% 的概率生成 8KB 大数据，绝对超过 4KB 的阈值，强制打入 Vlog！
				payloadSize = 8 * 1024
			}

			valStr := fmt.Sprintf("value_payload_version_%d_data_%s", i, stringsRepeat("A", payloadSize))

			err = db.Update(func(txn *badger.Txn) error {
				return txn.Set(key, []byte(valStr))
			})
			if err != nil {
				log.Fatalf("❌ 写入失败 (可能是大 KV 越界/溢出): %v", err)
			}
			truthMap[keyStr] = valStr // 同步更新真理账本
		}

		if i > 0 && i%100000 == 0 {
			fmt.Printf("   ... 已执行 %d 次操作，等待后台 Vlog/LSM 合并...\n", i)
			time.Sleep(1 * time.Second) // 故意停顿，给后台 compaction 制造竞态机会
		}
	}

	fmt.Printf("   ✅ 写入完成！当前真理账本有效 Key 数量: %d\n", len(truthMap))
	fmt.Println("   ... 强制等待 3 秒，让所有冷热数据下沉，Vlog 彻底落盘 ...")
	time.Sleep(3 * time.Second)

	// ==========================================
	// 3. 全库点查校验 (Point-Lookup Check)
	// ==========================================
	fmt.Println("\n>>> [阶段 2] 全库点查一致性校验 (验证 Vlog 指针是否错乱)...")
	err = db.View(func(txn *badger.Txn) error {
		for keyStr, expectedVal := range truthMap {
			item, err := txn.Get([]byte(keyStr))
			if err != nil {
				return fmt.Errorf("致命错误: Key [%s] 在真理账本中存在，但 DB 中丢失! Error: %v", keyStr, err)
			}
			val, err := item.ValueCopy(nil)
			if err != nil {
				return err
			}
			if string(val) != expectedVal {
				return fmt.Errorf("致命冲突 (幽灵读 或 Vlog指错): Key [%s]\n期望值长度: %d\n实际读到长度: %d", keyStr, len(expectedVal), len(val))
			}
		}
		return nil
	})
	if err != nil {
		log.Fatalf("❌ 点查校验失败: %v", err)
	}
	fmt.Println("   ✅ 全库点查 100% 匹配，Vlog 双轨指针完美解析，无任何数据错乱！")

	// ==========================================
	// 4. 全库范围迭代校验 (Iterator Check)
	// ==========================================
	fmt.Println("\n>>> [阶段 3] 全库迭代器一致性校验...")
	dbKeyCount := 0
	err = db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.PrefetchValues = true // 强制 Prefetch，疯狂并发去 Vlog 捞数据
		it := txn.NewIterator(opts)
		defer it.Close()

		for it.Rewind(); it.Valid(); it.Next() {
			item := it.Item()
			keyStr := string(item.Key())

			if !bytes.HasPrefix(item.Key(), []byte("hot_key_")) && !bytes.HasPrefix(item.Key(), []byte("cold_key_")) {
				continue
			}

			dbKeyCount++
			expectedVal, exists := truthMap[keyStr]
			if !exists {
				return fmt.Errorf("致命错误: Iterator 扫到了被删除的脏数据: [%s]", keyStr)
			}

			val, _ := item.ValueCopy(nil)
			if string(val) != expectedVal {
				return fmt.Errorf("致命冲突 (迭代器读到旧版本): Key [%s]", keyStr)
			}
		}
		return nil
	})
	if err != nil {
		log.Fatalf("❌ 迭代器校验失败: %v", err)
	}

	if dbKeyCount != len(truthMap) {
		log.Fatalf("❌ 致命错误: 迭代器扫描到的数量 (%d) 与 真理账本数量 (%d) 不匹配！", dbKeyCount, len(truthMap))
	}
	fmt.Println("   ✅ 迭代器扫描 100% 匹配，Prefetch 疯狂并发读取 Vlog 无数据污染！")

	fmt.Println("\n📊 关机前 LSM 树物理大盘：")
	fmt.Println(db.LevelsToString())
	fmt.Println(db.VlogStatsToString())

	// ==========================================
	// 5. 关机重启校验 (Durability / Manifest Check)
	// ==========================================
	fmt.Println("\n>>> [阶段 4] 模拟服务器重启 (验证双轨 Vlog 文件扫描截断是否正确)...")
	db.Close()
	fmt.Println("   ... 数据库已安全关闭，正在从磁盘重新挂载双轨大巴 ...")

	dbReopened, err := badger.Open(opt)
	if err != nil {
		log.Fatalf("❌ 重启失败，双轨 Vlog 解析可能发生越界或 Panic: %v", err)
	}
	defer dbReopened.Close()
	fmt.Println("   ✅ 数据库重启成功，无越界 Panic！")

	fmt.Println("\n>>> [阶段 5] 重启后终极点查校验...")
	err = dbReopened.View(func(txn *badger.Txn) error {
		for keyStr, expectedVal := range truthMap {
			item, err := txn.Get([]byte(keyStr))
			if err != nil {
				return fmt.Errorf("重启后数据丢失: [%s]", keyStr)
			}
			val, _ := item.ValueCopy(nil)
			if string(val) != expectedVal {
				return fmt.Errorf("重启后读到旧版本数据错乱: [%s]", keyStr)
			}
		}
		return nil
	})
	if err != nil {
		log.Fatalf("❌ 重启后校验失败: %v", err)
	}

	fmt.Println("   ✅ 重启后大 KV 数据 100% 完好无损，冷热 Vlog 完美复原！")

	fmt.Println("\n🎉🎉🎉 恭喜！双轨 Vlog 极限大容量一致性测试全部通过！ 🎉🎉🎉")
}

func stringsRepeat(s string, count int) string {
	b := make([]byte, len(s)*count)
	for i := 0; i < count; i++ {
		copy(b[i*len(s):], s)
	}
	return string(b)
}
