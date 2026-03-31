package heatLSM

import (
	"fmt"
	"math/rand"
	"testing"
	"time"

	"github.com/valyala/fastrand"
)

// 辅助函数：生成测试 Key
// 格式：prefix + random_suffix
func generateKey(prefix string, length int) Key {
	k := make(Key, length)
	copy(k, prefix)
	// 填充随机后缀
	for i := len(prefix); i < length; i++ {
		k[i] = byte(rand.Intn(26) + 'a')
	}
	return k
}

// go test -run TestHeatNode_Evolve_Integration
func TestHeatNode_Evolve_Integration(t *testing.T) {
	rand.Seed(time.Now().UnixNano())

	// ==========================================
	// 1. 初始化根节点
	// 【重构】：使用全新的 Manager 架构初始化
	// ==========================================
	manager := NewHeatmapManager()
	root := manager.MotherTree.Root // 获取绑定了物理内存池的根节点

	// 2. 构造数据模式：
	// 我们希望产生几个热点区间，例如 "apple...", "banana...", "cherry..."
	// 这样分裂时应该能识别出 "a", "b", "c" 或者更长的前缀
	prefixes := []string{"apple", "banana", "cherry", "date"}

	totalOps := 1000000 // 足够触发分裂 (Threshold=1024)
	var leaf *HeatNode
	fmt.Println("--- Phase 1: Injecting Data ---")
	for i := 0; i < totalOps; i++ {
		// 随机选择一个前缀
		p := prefixes[rand.Intn(len(prefixes))]
		key := generateKey(p, 10) // 长度10的key

		// 模拟读取写入操作 (isRead = true)
		if fastrand.Uint32n(3) == 0 {
			// 写入
			// 【重构】：SearchLeaf 必须带上 manager 传递到底层
			leaf = root.SearchLeaf(key, false, manager)
		} else {
			// 读取
			leaf = root.SearchLeaf(key, true, manager)
		}

		if leaf == nil {
			t.Fatalf("SearchLeaf returned nil for key: %s", key)
		}
	}

	fmt.Println("--- Phase 2: Verify Split ---")
	// 此时应该已经触发了至少一次分裂
	// 检查根节点是否变成了非叶子节点
	if root.IsLeaf {
		// 【重构】：原本这里输出了 root.ReadCount，但在新架构中，
		// Root 变成内部节点后 StatsID 为 -1，不再直接持有 ReadCount，所以仅报错即可。
		t.Errorf("Root should have evolved into non-leaf node.")
	} else {
		fmt.Printf("Root successfully evolved! Children count: %d\n", len(root.Children))
	}

	// 打印树结构进行人工检查
	// 【重构】：直接调用 Manager 提供的高级打印方法
	manager.Print()

	// 3. 验证路由正确性
	// 分裂后，我们再次查找之前的 Key，应该能找到正确的叶子节点，且 PathSegment 匹配
	fmt.Println("\n--- Phase 3: Verify Routing ---")
	testKeys := []string{"appletest", "bananaxyz", "cherrycgh", "datetime"}

	for _, ks := range testKeys {
		k := Key(ks)
		// 【重构】：带上 manager 进行路由
		leaf = root.SearchLeaf(k, true, manager)
		if leaf == nil {
			t.Errorf("Failed to find leaf for key: %s", k)
			continue
		}

		// 验证找到的叶子节点是否包含该 Key 的前缀
		// 注意：叶子节点的 PathSegment 是相对于父节点的。
		// 在多层分裂后，我们需要验证路径的连通性。
		// 这里简单验证叶子节点本身不是 Root
		if leaf == root {
			t.Errorf("Key %s should be in a child node, but found in root", k)
		}

		// 【重构】：因为 leaf 身上只有 StatsID，想看它的访问量需要去内存池里拿
		var readCount int64 = 0
		if leaf.StatsID != -1 {
			stats := manager.getStats(leaf.StatsID)
			readCount = stats.ReadCount
		}

		fmt.Printf("Key [%s] routed to Leaf with PathSegment [%s], Level: [%d], Range:[%s-%s), RealReadCount: %d\n",
			k, leaf.PathSegment, leaf.Level, leaf.RangeStart, leaf.RangeEnd, readCount)
	}
}
