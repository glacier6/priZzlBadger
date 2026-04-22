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
	fmt.Println("🚀 启动 HeatLSM 地狱级数据一致性测试")
	fmt.Println("=====================================================")

	// ==========================================
	// 1. 初始化数据库 (制造频繁 Flush 的恶劣环境)
	// ==========================================
	opt := badger.DefaultOptions(dbPath).
		WithMemTableSize(1 << 20). // 1MB，极速撑爆
		WithBaseTableSize(1 << 20).
		WithValueThreshold(32 << 10). // 缩小阈值，让大KV也参与流转
		WithNumLevelZeroTables(5).
		WithNumMemtables(5).
		WithSyncWrites(false)

	db, err := badger.Open(opt)
	if err != nil {
		log.Fatalf("❌ 数据库打开失败: %v", err)
	}

	// ==========================================
	// 2. 混沌写入阶段 (Chaos Writes)
	// 包含新增、疯狂修改(培养热点)、以及删除
	// ==========================================
	fmt.Println("\n>>> [阶段 1] 混沌写入与热点培养中 (制造数十万版本冲突)...")

	const totalOps = 1000000
	const hotKeyCount = 750       // 热数据
	const coldKeyCount = 10000000 // 冷数据，做背景干扰

	// 	const totalOps = 3000000
	// const hotKeyCount = 1000    // 只有50个热点Key，被疯狂覆写
	// const coldKeyCount = 2000000 // 两万个冷数据，做背景干扰
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
			paddingBytes := make([]byte, 8192)
			rand.Read(paddingBytes) // math/rand 直接生成纯随机字节
			// 写入操作，Value 必须携带严格的 Version 标记，用于防范幽灵读
			valStr := fmt.Sprintf("value_payload_version_%d_data_%s", i, paddingBytes)
			err = db.Update(func(txn *badger.Txn) error {
				return txn.Set(key, []byte(valStr))
			})
			if err != nil {
				log.Fatalf("❌ 写入失败: %v", err)
			}
			truthMap[keyStr] = valStr // 同步更新真理账本
		}

		if i%500000 == 0 {
			fmt.Printf("   ... 已执行 %d 次操作，等待后台 L98/L99 合并...\n", i)
			time.Sleep(1 * time.Second) // 故意停顿，给后台 compaction 制造竞态机会
		}
	}

	fmt.Printf("   ✅ 写入完成！当前真理账本有效 Key 数量: %d\n", len(truthMap))
	fmt.Println("   ... 强制等待 2 秒，让所有冷热数据下沉至 L98/L99/BaseLevel ...")
	time.Sleep(2 * time.Second)

	// ==========================================
	// 3. 全库点查校验 (Point-Lookup Check)
	// ==========================================
	fmt.Println("\n>>> [阶段 2] 全库点查一致性校验 (防范旧版本遮蔽与数据丢失)...")
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
				return fmt.Errorf("致命冲突 (幽灵读): Key [%s]\n期望值: %s\n实际读到: %s", keyStr, expectedVal, string(val))
			}
			// fmt.Printf("   ✅ 查询完成！ %s\n", keyStr)
		}
		return nil
	})
	if err != nil {
		log.Fatalf("❌ 点查校验失败: %v", err)
	}
	fmt.Println("   ✅ 全库点查 100% 匹配，无任何数据丢失或幽灵读！")

	// ==========================================
	// 4. 全库范围迭代校验 (Iterator Check)
	// 测试 appendIteratorsReversed 的多路归并是否正常
	// ==========================================
	fmt.Println("\n>>> [阶段 3] 全库迭代器一致性校验 (验证 L98/L99 Iterator 挂载)...")
	dbKeyCount := 0
	err = db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.PrefetchValues = true
		it := txn.NewIterator(opts)
		defer it.Close()

		for it.Rewind(); it.Valid(); it.Next() {
			item := it.Item()
			keyStr := string(item.Key())

			// 只校验我们的测试数据
			if !bytes.HasPrefix(item.Key(), []byte("hot_key_")) && !bytes.HasPrefix(item.Key(), []byte("cold_key_")) {
				continue
			}

			dbKeyCount++
			expectedVal, exists := truthMap[keyStr]
			if !exists {
				item, err := txn.Get([]byte(keyStr))
				itemV, err := item.ValueCopy(nil)
				if err != nil {
					return fmt.Errorf("Iterator 扫到了被删除的，但点查Key [%s] 确实查不到，value[%s] 为 Error: %v", keyStr, string(itemV), err)
				}
				fmt.Printf("点查也查出来 Key [%s]，value [%s]", keyStr, string(itemV))
				// badger.ZzlDumpTrace(keyStr)
				return fmt.Errorf("致命错误: Iterator 扫到了被删除的脏数据 (未被压实或墓碑失效): [%s]", keyStr)
			}

			val, _ := item.ValueCopy(nil)
			if string(val) != expectedVal {
				return fmt.Errorf("致命冲突 (迭代器读到旧版本): Key [%s]\n期望值: %s\n实际读到: %s", keyStr, expectedVal, string(val))
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
	fmt.Println("   ✅ 迭代器扫描 100% 匹配，多路归并排序完全正确！")

	// 打印一下当前的物理大盘，记录关机前的状态
	fmt.Println("\n📊 关机前 LSM 树物理大盘：")
	fmt.Println(db.LevelsToString())
	fmt.Println(db.VlogStatsToString())

	// ==========================================
	// 5. 关机重启校验 (Durability / Manifest Check)
	// 这是最容易翻车的地方，验证你的 Manifest 拦截逻辑
	// ==========================================
	fmt.Println("\n>>> [阶段 4] 模拟服务器重启 (验证 Manifest 重放与内存恢复)...")
	db.Close()
	fmt.Println("   ... 数据库已安全关闭，正在从磁盘重新挂载 ...")

	dbReopened, err := badger.Open(opt)
	if err != nil {
		log.Fatalf("❌ 重启失败，Manifest 解析可能发生越界或 Panic: %v", err)
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
				return fmt.Errorf("重启后读到旧版本: [%s]\n期望: %s\n实际: %s", keyStr, expectedVal, string(val))
			}
		}
		return nil
	})
	if err != nil {
		log.Fatalf("❌ 重启后校验失败: %v", err)
	}

	fmt.Println("   ✅ 重启后数据 100% 完好无损，HotTier 完美复原！")

	fmt.Println("\n🎉🎉🎉 恭喜！所有极限一致性测试全部通过！HeatLSM 架构坚如磐石！ 🎉🎉🎉")

	fmt.Println("\n📊 重启后 LSM 树物理大盘 (检查 L98/L99 是否挂载成功)：")
	fmt.Println(dbReopened.LevelsToString())
}

// 辅助函数，用来快速撑大 Value，促使 MemTable 满载
func stringsRepeat(s string, count int) string {
	b := make([]byte, len(s)*count)
	for i := 0; i < count; i++ {
		copy(b[i*len(s):], s)
	}
	return string(b)
}
