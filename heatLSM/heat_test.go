package heatlsm

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

// 辅助函数：深度优先遍历打印树结构
func printTree(node *HeatNode, prefix string) {
	fmt.Printf("%sLvl:%d Path:[%s] Range:[%s-%s) Leaf:%v Read:%d Write:%d Sample:%d\n",
		prefix,
		node.Level,
		string(node.PathSegment),
		string(node.RangeStart),
		string(node.RangeEnd),
		node.IsLeaf,
		node.ReadCount,
		node.WriteCount,
		len(node.RSuffixReservoir),
	)
	for i, child := range node.Children {
		printTree(child, prefix+"  ")
		if i < len(node.SplitRangeKey) {
			fmt.Printf("%s  [Split: %s]\n", prefix, string(node.SplitRangeKey[i]))
		}
	}
}

func TestHeatNode_Evolve_Integration(t *testing.T) {
	rand.Seed(time.Now().UnixNano())

	// 1. 初始化根节点
	// Level 0, 初始 SplitThreshold 设小一点以便容易触发分裂
	// 注意：代码中 SplitThreshold 是 1<<(Level+10)，即 1024。我们需要插入足够多的数据。
	root := newHeatNode(0, Key{}, Key(nil), Key{}, []Key{}, []Key{}, 0, 0)

	// 为了测试方便，我们可以临时调低 SplitThreshold 或者插入大量数据
	// 这里我们通过 mock 的方式修改 SplitThreshold，或者直接插入 > 1024 次读取
	// 但为了模拟真实场景，我们遵循默认逻辑。

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
			leaf = root.SearchLeaf(key, false)
		} else {
			// 读取
			leaf = root.SearchLeaf(key, true)
		}

		if leaf == nil {
			t.Fatalf("SearchLeaf returned nil for key: %s", key)
		}
	}

	fmt.Println("--- Phase 2: Verify Split ---")
	// 此时应该已经触发了至少一次分裂
	// 检查根节点是否变成了非叶子节点
	if root.IsLeaf {
		t.Errorf("Root should have evolved into non-leaf node. ReadCount: %d", root.ReadCount)
	} else {
		fmt.Printf("Root successfully evolved! Children count: %d\n", len(root.Children))
	}

	// 打印树结构进行人工检查
	printTree(root, "")

	// 3. 验证路由正确性
	// 分裂后，我们再次查找之前的 Key，应该能找到正确的叶子节点，且 PathSegment 匹配
	fmt.Println("\n--- Phase 3: Verify Routing ---")
	testKeys := []string{"appletest", "bananaxyz", "cherrycgh", "datetime"}

	for _, ks := range testKeys {
		k := Key(ks)
		leaf = root.SearchLeaf(k, true)
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

		fmt.Printf("Key [%s] routed to Leaf with PathSegment [%s],Level: [%d],Range:[%s-%s) \n",
			k, leaf.PathSegment, leaf.Level, leaf.RangeStart, leaf.RangeEnd)
	}
}

// // 针对 SplitArrayEvenly 和 FindTopNPrefixGroups 的单元测试
// // 因为这两个函数逻辑比较独立且复杂
// func TestSplitLogic(t *testing.T) {
// 	// 模拟一组已排序的样本数据（相对于父节点）
// 	// 假设父节点 PathSegment="user_"
// 	// 样本后缀：
// 	// 001... (apple)
// 	// 001...
// 	// 002... (banana)
// 	// 002...
// 	// 002...
// 	// 003... (cherry)

// 	samples := []Key{
// 		Key("001_a"), Key("001_b"),
// 		Key("002_a"), Key("002_b"), Key("002_c"),
// 		Key("003_a"),
// 	}

// 	// 1. 测试 FindTopNPrefixGroups
// 	// 期望找到 3 个组: "001", "002", "003"
// 	// 注意：你的 FindTopNPrefixGroups 实现需要能识别出这些前缀
// 	// 假设你已经按照之前的建议实现了它
// 	groups := FindTopNPrefixGroups(samples, 4)

// 	fmt.Println("\n--- Test FindTopNPrefixGroups ---")
// 	for i, g := range groups {
// 		fmt.Printf("Group %d: Range[%d-%d) Count:%d Prefix:%s\n",
// 			i, g.Start, g.End, g.Count, string(g.CommonPrefix))
// 	}

// 	if len(groups) < 3 {
// 		// 注意：如果样本太少或者前缀区分度不够，可能分不出那么多组。
// 		// 这里仅做演示，具体取决于你的 FindTopN 实现细节。
// 		t.Log("Warning: Groups count less than expected (check commonPrefixLen logic)")
// 	}

// 	// 2. 测试 splitReservoir
// 	// 构造一个 Mock 父节点
// 	parent := newHeatNode(0, Key{}, Key(nil), Key("user_"), true, []Key{}, 0, 0)
// 	parent.ReadCount = 100

// 	// 还要对 groups 进行排序 (按 Start)
// 	sort.Slice(groups, func(i, j int) bool {
// 		return groups[i].Start < groups[j].Start
// 	})

// 	children, splitKeys := parent.splitReservoir(samples, groups)

// 	fmt.Println("\n--- Test splitReservoir ---")
// 	fmt.Printf("Generated %d children and %d split keys\n", len(children), len(splitKeys))

// 	for i, child := range children {
// 		fmt.Printf("Child %d: Path=%s Start=%s End=%s IsLeaf=%v Samples=%d\n",
// 			i, string(child.PathSegment), string(child.RangeStart), string(child.RangeEnd),
// 			child.IsLeaf, len(child.SuffixReservoir))
// 	}

// 	// 验证逻辑：
// 	// 1. 间隙节点是否存在？ (001 和 002 之间是否有间隙？)
// 	//    Key("001") 的 NextKeySameLength 是 "002"，如果紧邻则无间隙。
// 	// 2. 子节点 PathSegment 是否正确？
// 	//    实体节点的 PathSegment 应该是 "001", "002", "003"
// }

// func main() {

// }
