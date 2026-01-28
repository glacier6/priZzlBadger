package heatlsm

import (
	"bytes"
	"sort"
	"sync"

	"github.com/valyala/fastrand"
)

const (
	// 蓄水池容量：叶子节点最多存多少个样本触发分裂检查
	ReservoirCap = 64

	// 分裂阈值：读+写热度超过此值，且样本满了，才允许分裂
	SplitThreshold = 1000

	// // 聚类间隙阈值 (Gap Threshold)
	// // 在排序后的样本中，如果相邻两个 Key 的差值超过此值，视为“断层”，需要切分出冷桶
	// // 注意：对于 []byte，这需要一个特定的距离计算函数
	// GapTolerance = 10
)

type Key []byte

type HeatNode struct {
	// --- 树形结构与前缀压缩 ---
	RangeStart  Key
	RangeEnd    Key // nil 代表无穷大
	PathSegment Key // 当前节点代表的公共前缀片段。完整 Key = 父节点Prefix + ... + 当前Prefix + Suffix

	// --- 热度统计 ---
	ReadCount  int64 // 原子计数
	WriteCount int64 // 原子计数

	// --- 结构控制 ---
	IsLeaf   bool
	Children []*HeatNode // 这里使用有序切片存储子节点，分裂不固定为2,可为N个

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
	// 母树：结构稳定，负责指导路由，记录长期衰减热度
	MotherTree *HeatmapTree

	// 子树：结构易变，负责收集近期突发流量
	// 子树的结构会定期“重置”或“对齐”母树
	ChildTree *HeatmapTree

	// 全局锁：仅在 "母子合并 (Merge)" 和 "结构进化" 时使用
	evolutionLock sync.Mutex
}

// 创建一个新的节点
func newHeatNode(start, end Key, pathSeg Key, isLeaf bool) *HeatNode {
	n := &HeatNode{
		RangeStart:  start,
		RangeEnd:    end,
		PathSegment: pathSeg,
		IsLeaf:      isLeaf,
		ReadCount:   0,
		WriteCount:  0,
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
	motherRoot := newHeatNode(rootStart, rootEnd, Key{}, true)
	mother := &HeatmapTree{Root: motherRoot}

	// 2. 初始化子树
	// 子树初始结构必须与母树一致（完全同构）
	childRoot := newHeatNode(rootStart, rootEnd, Key{}, true)
	child := &HeatmapTree{Root: childRoot}

	return &HeatmapManager{
		MotherTree: mother,
		ChildTree:  child,
	}
}

// NOTE:核心逻辑：向蓄水池添加样本，现在下面这个是针对全局的。TODO:看看是否有必要将近期查询KEY进入的可能性拉大
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
	if !n.IsLeaf || len(n.SuffixReservoir) < ReservoirCap {
		return
	}

	// 2. 对蓄水池中的后缀进行排序
	// 这是找中位数的前提
	sort.Slice(n.SuffixReservoir, func(i, j int) bool {
		return bytes.Compare(n.SuffixReservoir[i], n.SuffixReservoir[j]) < 0
	})

	// 3. 提取最长公共前缀 (LCP)
	// 只需要比对排序后的第一个和最后一个样本
	first := n.SuffixReservoir[0]
	last := n.SuffixReservoir[len(n.SuffixReservoir)-1]
	lcp := longestCommonPrefix(first, last)

	// 4. TODO:边界处理：如果所有样本完全一样 (LCP == sample)，无法分裂
	// 这通常发生在对同一个 Key 进行疯狂写入时
	// if len(lcp) == len(first) && len(lcp) == len(last) {
	// 	// 策略：清空样本，重新收集，或者标记为不可分裂节点
	// 	n.SuffixReservoir = n.SuffixReservoir[:0]
	// 	n.TotalSamplesSeen = 0
	// 	return
	// }

	// 5. 更新当前节点的路径信息 (Push Down LCP)
	// 如果发现了新的公共前缀，将其合并到当前节点的 PathSegment 中
	if len(lcp) > 0 {
		n.PathSegment = append(n.PathSegment, lcp...)

		// 关键：所有样本都需要剥离掉这个新的 LCP
		for i := range n.SuffixReservoir {
			n.SuffixReservoir[i] = n.SuffixReservoir[i][len(lcp):]
		}
	}

	// 6. 寻找中位数作为分裂点
	// 注意：此时的样本已经剥离了最新的 LCP
	midIdx := len(n.SuffixReservoir) / 2

	// SplitSuffix 是右子树的起始边界（相对路径）
	// splitSuffix := n.SuffixReservoir[midIdx]

	// 7. 创建子节点
	// 继承一半的热度 (简单衰减策略，防止分裂后瞬间变冷)
	halfRead := n.ReadCount / 2
	halfWrite := n.WriteCount / 2

	// 左孩子：覆盖 [Min, splitSuffix)
	leftChild := &HeatNode{
		PathSegment: nil, // 左孩子相对于父节点没有额外的固定前缀
		ReadCount:   halfRead,
		WriteCount:  halfWrite,
		IsLeaf:      true,
		// 将左半部分样本遗传给左孩子
		SuffixReservoir:  n.SuffixReservoir[:midIdx],
		TotalSamplesSeen: int64(midIdx),
	}

	// 右孩子：覆盖 [splitSuffix, Max)
	// 注意：右孩子的 PathSegment 就是 splitSuffix 的第一个字节吗？
	// 不一定。为了简单起见，我们在 Radix Tree 中通常不给子节点预设 Path，
	// 而是等子节点下次分裂时自己去提取 LCP。
	// 但为了路由正确，我们需要知道右边的范围从哪里开始。
	// 在这种简化模型下，路由逻辑通常由父节点根据 SplitKey 判断。

	rightChild := &HeatNode{
		PathSegment: nil,
		ReadCount:   halfRead,
		WriteCount:  halfWrite,
		IsLeaf:      true,
		// 将右半部分样本遗传给右孩子
		SuffixReservoir:  n.SuffixReservoir[midIdx:],
		TotalSamplesSeen: int64(len(n.SuffixReservoir) - midIdx),
	}

	// *修正：如果是多路树结构，我们需要把 splitSuffix 存下来作为路由依据
	// 但在这里，我们先构建 Children 列表，路由时依赖子节点的 Range 或者辅助索引
	// 为了简化，假设父节点变成中间节点后，路由逻辑是：
	// Key < splitSuffix -> Child[0]
	// Key >= splitSuffix -> Child[1]
	// 我们需要把这个 splitSuffix 记录在某个地方，或者让右孩子的 PathSegment 暂时承载它。
	// 这里最简单的做法是：不预设子节点的 PathSegment，依靠下一次迭代提取。

	n.Children = []*HeatNode{leftChild, rightChild}

	// 8. 状态变更
	n.IsLeaf = false
	n.SuffixReservoir = nil // 清空父节点样本
	n.TotalSamplesSeen = 0
}

// 辅助函数：计算两个字节切片的公共前缀
func longestCommonPrefix(a, b Key) Key {
	maxLen := len(a)
	if len(b) < maxLen {
		maxLen = len(b)
	}
	i := 0
	for i < maxLen && a[i] == b[i] {
		i++
	}
	return a[:i]
}
