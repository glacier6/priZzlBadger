package main

import (
	"fmt"
	"log"
	"os"
	"time"

	// 注意：这里确保你的 go.mod 已经用 replace 指向了你本地魔改过的 badger 目录
	badger "github.com/dgraph-io/badger/v4"
)

func main() {
	// 每次测试前清理一下旧数据，保证环境纯净
	dbPath := "./zzl_badger_data_debug"
	os.RemoveAll(dbPath)

	fmt.Println(">>> 1. 正在初始化 BadgerDB (开启极限小 Memtable 模式)...")
	// 开启极限模式：MemTableSize 设为 1MB (原版是 64MB)
	// 目的：随便写几万条数据就能疯狂触发 Flush，瞬间激活热力图快照！
	opt := badger.DefaultOptions(dbPath).
		WithMemTableSize(1 << 20).    // Memtable 大小缩到 1MB
		WithBaseTableSize(1 << 20).   // L1 的目标文件大小也缩到 1MB
		WithValueThreshold(32 << 10). // 💥 核心修复：把 Value 阈值压缩到 32KB！(远小于 150KB)
		WithNumLevelZeroTables(1).    // L0 只要出现 1 个表，立刻触发向下压实！
		WithNumMemtables(5).
		WithSyncWrites(false).
		WithLogger(nil) // 调试时嫌吵可以关掉底层日志

	db, err := badger.Open(opt)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	hotKey := []byte("usertable:user_super_hot")

	// =================================================================
	// 🎯 【第一阶段：疯狂覆写，培养热点】
	// =================================================================
	fmt.Println(">>> 2. 开始注入热点流量，培养热点...")
	// 💡 调试建议：在 heatLSM 的 AddSample 和 OnMemtableFlush 处打断点
	for i := 0; i < 80000; i++ {
		err = db.Update(func(txn *badger.Txn) error {
			// 故意把 value 写大一点，加速 Memtable 撑爆
			val := []byte(fmt.Sprintf("hot_value_data_payload_%010d_padding_padding", i))
			return txn.Set(hotKey, val)
		})
		if err != nil {
			log.Fatal(err)
		}
	}

	// =================================================================
	// 🎯 【第二阶段：等待快照生成】
	// =================================================================
	fmt.Println(">>> 3. 流量注入完毕，等待 Flush 和热点快照生成 (2秒)...")
	// 💡 调试建议：在 EpochDecayAndSnapshot 的 traverse 递归函数里打断点
	// 看看绝对路径是怎么拼接并塞进 newZones 的！
	time.Sleep(2 * time.Second)

	// =================================================================
	// 🎯 【第三阶段：冷热混合写入，触发 L0 拦截】
	// =================================================================
	fmt.Println(">>> 4. 开始混合写入，触发底层 L0 -> L99 拦截分流...")
	// 此时系统应该已经判定 hotKey 是热点了。
	// 我们写一堆冷数据，中间夹杂着 hotKey，把它们逼进压缩流程 (Compaction)
	for i := 0; i < 30000; i++ {
		db.Update(func(txn *badger.Txn) error {
			if i%10 == 0 {
				return txn.Set(hotKey, []byte("the_ultimate_hot_value"))
			}
			coldKey := []byte(fmt.Sprintf("usertable:user_cold_%d", i))
			return txn.Set(coldKey, []byte("cold"))
		})
	}

	// 等待后台的 compactBuildTables 把数据刷进 Level 99
	// 💡 调试建议：去 levels.go 的 compactBuildTables 函数里，
	// 找到 if s.hotTier != nil && s.heatmapManager.IsHotKey(...) 的地方打断点！
	// 亲眼看着你的热数据是怎么被塞进 hotBuilder 的。
	time.Sleep(3 * time.Second)

	// =================================================================
	// 🎯 【第四阶段：联合读取测试】
	// =================================================================
	fmt.Println(">>> 5. 开始验证从 Level 99 读取最新数据...")
	// 💡 调试建议：去 levels.go 的 get 函数和 getHotTier 函数里打断点！
	// 看看请求是怎么精准命中 Level 99 并早停返回的。
	err = db.View(func(txn *badger.Txn) error {
		item, err := txn.Get(hotKey)
		if err != nil {
			return err
		}
		val, err := item.ValueCopy(nil)
		if err != nil {
			return err
		}
		fmt.Printf("✅ 成功读取到热点数据！Value: %s\n", string(val))
		return nil
	})
	if err != nil {
		log.Fatal(err)
	}

	// =================================================================
	// 🎯 【第五阶段：大盘检阅】
	// =================================================================
	fmt.Println("\n>>> 6. 最终 LSM 树物理大盘：")
	// 期待在这里看到 Level 99 [H] 里面有几十 MB 的数据！
	fmt.Println(db.LevelsToString())

	// 如果需要，也可以调用你的打印树的方法
	// db.lc.heatmapManager.Print()
}
