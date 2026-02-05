package heatlsm

import (
	"bytes"
	"fmt"
	"math"
	"sort"
	"sync"

	"github.com/valyala/fastrand"
)

const (
	// 蓄水池容量：叶子节点最多存多少个样本触发分裂检查
	ReservoirCap = 64

	// 树节点读写比达到多少停止分裂
	TargetRatio = 3

	// 树节点分裂时，将蓄水池最多划分多少个有效范围（即一般情况下会划分出（MaxValidRange * 2 + 1）个范围，除非有效范围首位相连了）
	MaxValidRange = 4

	// // 聚类间隙阈值 (Gap Threshold)
	// // 在排序后的样本中，如果相邻两个 Key 的差值超过此值，视为“断层”，需要切分出冷桶
	// // 注意：对于 []byte，这需要一个特定的距离计算函数
	// GapTolerance = 10
)

type Key []byte

type HeatNode struct {
	// --- 树形结构与前缀压缩 ---
	Level          int64 // 当前节点所在层级
	SplitThreshold int64 // 读分裂阈值：读热度超过此值，且样本满了，才允许分裂（只读热分裂）
	isTargetRange  bool  // 判断是否是达到TargetRatio的目标节点
	RangeStart     Key
	RangeEnd       Key // nil 代表无穷大
	PathSegment    Key // 当前节点代表的公共前缀片段。完整 Key = 父节点Prefix + ... + 当前Prefix + Suffix

	// --- 热度统计 ---
	ReadCount  int64 // 原子计数
	WriteCount int64 // 原子计数

	// --- 结构控制 ---
	IsLeaf        bool
	Children      []*HeatNode // 这里使用有序切片存储子节点，分裂不固定为2,可为N个
	SplitRangeKey []Key       // 分裂点列表，即Children中前【len(Children)-1】个的RangeEnd值

	// --- 进化基因 (仅叶子节点有效) ---
	// 只在叶子节点存在，且存储的是去掉从 Root 到当前节点所有 Prefix 后的剩余部分
	// 一旦分裂，此切片立即被置为 nil，释放内存
	SuffixReservoir  []Key // 蓄水池
	TotalSamplesSeen int64 //这一轮（自上次分裂或创建以来）总共见过了多少个 Key，用于蓄水池算法计算替换概率（不能用ReadCount+WriteCount，因为这两个会衰减！）

	// 锁
	sync.RWMutex
}

// 包装单独的一棵树
type HeatmapTree struct {
	Root *HeatNode
}

type HeatmapManager struct {
	TotalRead  int64
	TotalWrite int64

	// 母树：结构稳定，负责指导路由，记录长期衰减热度
	MotherTree *HeatmapTree

	// 子树：结构易变，负责收集近期突发流量
	// 子树的结构会定期“重置”或“对齐”母树
	ChildTree *HeatmapTree

	// 全局锁：仅在 "母子合并 (Merge)" 和 "结构进化" 时使用
	evolutionLock sync.Mutex
}

// 创建一个新的节点
func newHeatNode(Level int64, start, end Key, pathSeg Key, isLeaf bool) *HeatNode {
	n := &HeatNode{
		Level:          Level,
		SplitThreshold: 1 << (Level + 10), // TODO:目前第一层1024,第二层2048，第三层4096，需要根据实验调整
		RangeStart:     start,
		RangeEnd:       end,
		PathSegment:    pathSeg,
		IsLeaf:         isLeaf,
		ReadCount:      0,
		WriteCount:     0,
		isTargetRange:  false,
	}
	if isLeaf {
		// 预分配蓄水池，避免频繁扩容
		n.SuffixReservoir = make([]Key, 0, ReservoirCap)
	}
	return n
}

// 初始化管理器
func NewHeatmapManager() *HeatmapManager {
	// 定义全域范围：空字节到 nil (无穷大)
	// 初始 PathSegment 为空，因为还没有公共前缀
	rootStart := Key{}
	rootEnd := Key(nil)

	// 1. 初始化母树
	motherRoot := newHeatNode(0, rootStart, rootEnd, Key{}, true)
	mother := &HeatmapTree{Root: motherRoot}

	// 2. 初始化子树
	// 子树初始结构必须与母树一致（完全同构）
	childRoot := newHeatNode(0, rootStart, rootEnd, Key{}, true)
	child := &HeatmapTree{Root: childRoot}

	return &HeatmapManager{
		MotherTree: mother,
		ChildTree:  child,
		TotalRead:  0,
		TotalWrite: 0,
	}
}

// NOTE:核心逻辑：向蓄水池添加样本，现在下面这个是针对全局的。TODO:看看是否有必要将近期查询KEY进入的可能性拉大 // TODO:当某个key刚好等于路径上的片段key拼接呢？
// keySuffix: 已经剥离了当前节点 Prefix 的后缀部分
func (n *HeatNode) AddSample(keySuffix Key) {
	n.Lock()
	defer n.Unlock()

	// 1. 安全检查：只有叶子才存样本
	if !n.IsLeaf {
		return
	}

	// 2. 计数器自增 (代表这是第 N 个流过的数据)
	n.TotalSamplesSeen++

	// 3. 场景 A: 蓄水池未满 -> 直接追加
	if len(n.SuffixReservoir) < ReservoirCap {
		// 【关键】必须深拷贝！
		// Badger 的 key 往往复用 buffer，不拷贝会导致数据损坏
		k := make(Key, len(keySuffix))
		copy(k, keySuffix)

		n.SuffixReservoir = append(n.SuffixReservoir, k)
		return
	}

	// 4. 场景 B: 蓄水池已满 -> 随机替换 (Algorithm R)
	// 逻辑：以 k/n 的概率决定是否保留当前元素。NOTE:不能用到来就直接一定保留，而应该用下面这种法法，下面这种方法所有来到的key最终存在于蓄水池的概率在数学上是完全相等的
	// n = TotalSamplesSeen, k = ReservoirCap

	// 生成一个 [0, n) 的随机数，此处引入一个高性能随机数库
	// 因为标准库 math/rand通常使用较为复杂的伪随机算法（如 Lagged Fibonacci 或 PCG 算法）。这些算法产生的随机数分布质量很高，周期很长，可以通过统计学检验。但计算步骤相对繁琐。
	limit := uint32(n.TotalSamplesSeen)
	r := fastrand.Uint32n(limit) // Uint32n 返回范围在 [0..maxN) 内的伪随机 uint32 值。从并发的goroutine中调用此函数是安全的。

	// 如果随机数落在 [0, k) 区间内，则替换掉对应下标的元素
	if r < uint32(ReservoirCap) {
		// 【关键】深拷贝
		k := make(Key, len(keySuffix))
		copy(k, keySuffix)

		// 替换掉旧样本
		n.SuffixReservoir[r] = k
	}

	// 否则：直接丢弃该样本，什么都不做
}

// Evolve 是核心分裂方法，通常由后台 Worker 调用，或者在 Write 路径中异步触发
func (n *HeatNode) Evolve() {
	n.Lock()
	defer n.Unlock()

	// 1. 基础检查
	// 如果不是叶子，或者样本太少，则放弃分裂
	if !n.IsLeaf || n.isTargetRange || n.SplitThreshold > n.ReadCount || len(n.SuffixReservoir) < ReservoirCap {
		return
	}

	// 当前范围符合标准，暂停继续分裂
	if n.WriteCount > 0 && n.ReadCount/n.WriteCount > TargetRatio {
		// TODO:当前范围符合标准，暂停分裂，将当前范围添加到某个地方记录起来
	}

	// 2. 先对蓄水池中的Key后缀进行排序
	sort.Slice(n.SuffixReservoir, func(i, j int) bool {
		return bytes.Compare(n.SuffixReservoir[i], n.SuffixReservoir[j]) < 0
	})

	// NOTE:怎么具体分裂？
	// （1）在已排序的蓄水池上先找到前缀不同的候补分裂点（如111,121,211，269,359的候补分裂点应该是index为2,4之前的位置），然后尽可能均分的找到 MaxValidRange-1（最多） 个分裂点   PS:这是为了尽可能扁平化，加速查找流程
	// （2）生成对应个数个子节点，并且对于各有效区域且有公共前缀的在子节点设置上前缀名，即设置上PathSegment
	// （3）将子节点挂载到父节点上，并且在父节点的SplitRangeKey中更新分裂点列表
	alternateIndex := []int{}
	lastByte := n.SuffixReservoir[0][0]
	for i := 1; i < ReservoirCap; i++ {
		var currByte byte
		if len(n.SuffixReservoir[i]) > 0 {
			currByte = n.SuffixReservoir[i][0]
		}
		if currByte != lastByte {
			alternateIndex = append(alternateIndex, i)
			lastByte = currByte
		}
	}
	childrenNodes, splitKeys := n.splitReservoir(n.SuffixReservoir, alternateIndex, MaxValidRange)
	fmt.Print(childrenNodes)
	fmt.Print(splitKeys)
	// TODO:下面还需要补充一些逻辑

}

// splitReservoir 将样本数据 data 基于 potentialIndices 进行分割，并构造子节点
// data: 父节点的蓄水池样本（相对 Key）
// potentialIndices: 候选切割点的下标列表(是分裂点之后的那个key的下标)
// n: 目标最大块数
func (node *HeatNode) splitReservoir(data []Key, potentialIndices []int, targetBlocks int) ([]*HeatNode, []Key) {
	totalLen := len(data)
	var chosenIndices []int // 切割下标

	// 1. 边界检查：如果只要1块或没法分，直接返回原数组
	if len(potentialIndices) != 0 {
		neededCuts := targetBlocks - 1
		if len(potentialIndices) <= neededCuts {
			// 情况A：提供的切割点不够用，或者刚好够用
			chosenIndices = potentialIndices
		} else {
			// 情况B：提供的切割点绰绰有余，我们需要贪心选择最均匀的点
			chosenIndices = make([]int, 0, neededCuts)
			step := float64(totalLen) / float64(targetBlocks) // 理想步长
			lastIdxInPotentials := -1                         // 记录上一次在 potentialIndices 中选中的下标，防止回头或重复
			// 下面开始贪心找切割点
			for i := 1; i <= neededCuts; i++ {
				// 当前这一刀的理想位置
				idealPos := step * float64(i)

				// 在 potentialIndices 中二分查找最接近 idealPos 的位置
				// SearchInts 返回第一个 >= idealPos 的下标
				idx := sort.SearchInts(potentialIndices, int(idealPos))

				// 寻找最接近的下标 (idx 还是 idx-1 ?)
				bestMatchIdx := -1

				// 边界处理
				if idx == 0 {
					bestMatchIdx = 0
				} else if idx == len(potentialIndices) {
					bestMatchIdx = len(potentialIndices) - 1
				} else {
					// 比较 idx 和 idx-1 谁离理想值更近
					valAfter := potentialIndices[idx]
					valBefore := potentialIndices[idx-1]
					if math.Abs(float64(valAfter)-idealPos) < math.Abs(float64(valBefore)-idealPos) {
						bestMatchIdx = idx
					} else {
						bestMatchIdx = idx - 1
					}
				}

				// --- 关键修正逻辑 ---

				// 1. 必须大于上一次选中的下标（保证不重复选同一个点，且顺序往后）
				if bestMatchIdx <= lastIdxInPotentials {
					bestMatchIdx = lastIdxInPotentials + 1
				}

				// 2. 必须为后面还没切的刀数预留足够的点位
				// 还需要切 cutsRemaining 刀
				cutsRemaining := neededCuts - i
				// 后面还剩多少个候选点
				candidatesRemaining := len(potentialIndices) - 1 - bestMatchIdx

				// 如果选了这个点，导致后面剩下的候选点不够切了，就必须强行把当前点往前移
				if candidatesRemaining < cutsRemaining {
					bestMatchIdx = len(potentialIndices) - 1 - cutsRemaining
				}

				// 选中该点
				chosenIndices = append(chosenIndices, potentialIndices[bestMatchIdx])
				lastIdxInPotentials = bestMatchIdx
			}
		}
	}

	// 5. 根据选中的下标构造子节点
	var children []*HeatNode
	var splitKeys []Key
	start := 0

	// 为了正确设置 RangeStart 和 RangeEnd，我们需要知道父节点的绝对 Start 吗？
	// 假设我们在设计中使用相对 Key。
	// 第一个子节点的 Start = 父节点的 Start (逻辑上，如果是相对值则是空)
	// 最后一个子节点的 End = 父节点的 End

	// 在这里，我们只维护相对逻辑：
	// Child.RangeStart = (相对于 Child.PathSegment 的空 byte?)
	// 实际上，RangeStart/End 在 LSM 中更多是用于 Seek/Iterate 的边界检查。
	// 既然 Node 里有 PathSegment，我们让 RangeStart/End 也是相对于 PathSegment 的。

	// 需要追加一个“虚拟”的结束点 totalLen，方便循环
	cutPoints := append(chosenIndices, totalLen)
	for i, cutPos := range cutPoints {
		// 提取分片样本
		if cutPos > totalLen {
			cutPos = totalLen
		}
		chunk := data[start:cutPos]
		rangeSize := int64(cutPos - start)

		// 计算这一组样本的公共前缀，作为子节点的 PathSegment
		commonSeg := calcCommonPrefix(chunk)

		// 构造子节点
		// 注意：RangeStart 和 RangeEnd 在这里比较难精确定义，除非我们传递上下文。
		// 简单起见，我们暂且置空或设为 nil，因为核心路由靠 SplitRangeKey。
		child := newHeatNode(
			node.Level+1,
			nil, // RangeStart TODO:写范围
			nil, // RangeEnd
			commonSeg,
			true,
		)

		// 继承热度 (简单均分)
		child.ReadCount = node.ReadCount / (rangeSize / ReservoirCap)
		child.WriteCount = node.WriteCount / (rangeSize / ReservoirCap)

		// TODO:下面的需要再看一下逻辑
		// 将样本迁移进子节点
		// 【关键】：必须剥离掉子节点的 PathSegment (commonSeg)
		for _, key := range chunk {
			if len(key) >= len(commonSeg) {
				// 剥离前缀
				suffix := key[len(commonSeg):]
				// 需要深拷贝吗？AddSample 内部会做深拷贝。
				// 但这里的 suffix 是基于 data 的切片，data 是 n.SuffixReservoir。
				// 如果 n.SuffixReservoir 被置 nil，底层数组还能用吗？
				// Go 的 GC 会管理引用，只要 AddSample 拷贝了就没问题。
				child.AddSample(suffix)
			}
		}

		children = append(children, child)

		// 记录分裂点 (SplitKey)
		// SplitKey 应该是前一个子节点的“上界”或后一个子节点的“下界”。
		// 在 B+ 树中，通常 SplitKey 是右子树的最小值。
		// 这里，我们取 chunk[0] (该组的第一个 Key) 作为该组的下界？
		// 不，SplitKeys 列表长度应该是 len(Children) - 1。
		// SplitKeys[i] 分隔 Children[i] 和 Children[i+1]。
		// 所以 SplitKeys[i] 应该是 Children[i+1] 的逻辑下界。

		// 这里的逻辑下界是：Children[i+1].PathSegment + ...
		// 但 SplitRangeKey 存储在父节点，父节点看来，
		// Key = Child.PathSegment + Child.Suffix
		// 所以 SplitKey 应该是：下一个 Chunk 的第一个样本（全量相对父节点）。

		if i < len(cutPoints)-1 {
			// 获取下一个 Chunk 的第一个元素
			nextChunkStartIdx := cutPos
			if nextChunkStartIdx < totalLen {
				splitKey := make(Key, len(data[nextChunkStartIdx]))
				copy(splitKey, data[nextChunkStartIdx])
				splitKeys = append(splitKeys, splitKey)
			}
		}

		start = cutPos
	}

	return children, splitKeys
}
