package heatLSM

// NOTE:所有范围遵循左闭右开原则
// 如 [A,C) [C,G) [G,Z)   其中的分裂点SplitRangeKey是C，G

// YCSB用的BadgerDB的读位置NOTE:2026031800 写位置NOTE:2026031801
import (
	"bytes"
	"fmt"
	"sort"
	"sync"

	"github.com/valyala/fastrand"
)

const (
	// 蓄水池容量：叶子节点最多存多少个样本触发分裂检查
	ReservoirCap  = 256 // NOTE:需要大于等于256，避免极端情况下找不到一个有公共前缀的区间
	WReservoirCap = 64
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
	RangeStart     Key   // RangeStart和RangeEnd构造出左闭右开的区间（注意这俩是针对父亲的范围分割，且同一层的节点范围合并起来就是父节点的全域，即向上看。且注意存储的是逐层拼接的增量Key）
	RangeEnd       Key   // nil 代表无穷大
	PathSegment    Key   // 当前节点代表的公共前缀片段。完整 Key = 父节点Prefix + ... + 当前Prefix + Suffix（注意是逐层拼接的增量Key）

	// --- 热度统计 ---
	ReadCount  int64 // 原子计数
	WriteCount int64 // 原子计数

	// --- 结构控制 ---
	IsLeaf        bool
	Children      []*HeatNode // 这里使用有序切片存储子节点，分裂不固定为2,可为N个  TODO:这个Children和下面的SplitRangeKey的长度大小是否要直接固定,这样的话虽然空间变大,但是因为空间是连续的了,所以cpu cache会命中率很高
	SplitRangeKey []Key       // 分裂点列表，即Children中前【len(Children)-1】个的RangeEnd值（注意存的是逐层拼接的增量Key）

	// --- 进化基因 (仅叶子节点有效) ---
	// 只在叶子节点存在，且存储的是去掉从 Root 到当前节点所有 Prefix 后的剩余部分
	// 一旦分裂，此切片立即被置为 nil，释放内存
	RSuffixReservoir []Key // 读蓄水池
	WSuffixReservoir []Key // 写蓄水池
	TotalSamplesSeen int64 // 这一轮（自上次分裂或创建以来）总共见过了多少个 Key，用于蓄水池算法计算替换概率（不能用ReadCount+WriteCount，因为这两个会衰减！）
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
func newHeatNode(Level int64, start, end Key, pathSeg Key, iniRReservoir []Key, iniWReservoir []Key, iniReadCount int64, iniWriteCount int64) *HeatNode {
	n := &HeatNode{
		Level:          Level,
		SplitThreshold: 1 << (Level + 10), // TODO:目前第一层1024,第二层2048，第三层4096，需要根据实验调整
		RangeStart:     start,
		RangeEnd:       end,
		PathSegment:    pathSeg,
		IsLeaf:         true,
		ReadCount:      iniReadCount,
		WriteCount:     iniWriteCount,
		isTargetRange:  false,
	}
	// 预分配蓄水池，避免频繁扩容
	n.RSuffixReservoir = make([]Key, 0, ReservoirCap)
	n.WSuffixReservoir = make([]Key, 0, WReservoirCap)

	// 将蓄水池对应部分迁移进子节点
	// 【关键】：必须剥离掉子节点的 PathSegment (commonSeg)
	for _, key := range iniRReservoir {
		if len(key) >= len(pathSeg) {
			// 剥离前缀
			suffix := key[len(pathSeg):]
			// 需要深拷贝吗？AddSample 内部会做深拷贝。
			// 但这里的 suffix 是基于 data 的切片，data 是 n.SuffixReservoir。
			// 如果 n.SuffixReservoir 被置 nil，底层数组还能用吗？
			// Go 的 GC 会管理引用，只要 AddSample 拷贝了就没问题。
			n.AddSample(suffix, true)
		}
	}
	for _, key := range iniWReservoir {
		if len(key) >= len(pathSeg) {
			// 剥离前缀
			suffix := key[len(pathSeg):]
			// 需要深拷贝吗？AddSample 内部会做深拷贝。
			// 但这里的 suffix 是基于 data 的切片，data 是 n.SuffixReservoir。
			// 如果 n.SuffixReservoir 被置 nil，底层数组还能用吗？
			// Go 的 GC 会管理引用，只要 AddSample 拷贝了就没问题。
			n.AddSample(suffix, false)
		}
	}
	return n
}

// 初始化管理器 初始位置在NOTE:2026031802
func NewHeatmapManager() *HeatmapManager {
	// 定义全域范围：空字节到 nil (无穷大)
	// 初始 PathSegment 为空，因为还没有公共前缀
	rootStart := Key{}
	rootEnd := Key(nil)

	// 1. 初始化母树
	motherRoot := newHeatNode(0, rootStart, rootEnd, Key{}, []Key{}, []Key{}, 0, 0)
	mother := &HeatmapTree{Root: motherRoot}

	// 2. 初始化子树
	// 子树初始结构必须与母树一致（完全同构）
	// childRoot := newHeatNode(0, rootStart, rootEnd, Key{}, []Key{}, []Key{}, 0, 0)
	// child := &HeatmapTree{Root: childRoot}

	return &HeatmapManager{
		MotherTree: mother,
		// ChildTree:  child,
		TotalRead:  0,
		TotalWrite: 0,
	}
}

// NOTE:核心逻辑：向蓄水池添加样本，现在下面这个是针对全局的，注意调用下面这个函数需要先找到目标叶子节点，并且确保当前Key在目标叶子节点的边界内（左闭右开）。
// TODO:看看是否有必要将近期查询KEY进入的可能性拉大
// keySuffix: 已经剥离了当前节点 Prefix 的后缀部分
func (n *HeatNode) AddSample(keySuffix Key, isRead bool) {
	n.Lock()
	defer n.Unlock()

	// 1. 安全检查：只有叶子才存样本
	if !n.IsLeaf {
		return
	}

	// 2. 计数器自增 (代表这是第 N 个流过的数据)
	// n.TotalSamplesSeen++
	if isRead {
		n.ReadCount++
		// 3. 场景 A: 蓄水池未满 -> 直接追加
		if len(n.RSuffixReservoir) < ReservoirCap {
			// 【关键】必须深拷贝！
			// Badger 的 key 往往复用 buffer，不拷贝会导致数据损坏
			k := make(Key, len(keySuffix))
			copy(k, keySuffix)

			n.RSuffixReservoir = append(n.RSuffixReservoir, k)
			return
		}

		// 4. 场景 B: 蓄水池已满 -> 随机替换 (Algorithm R)
		// 逻辑：以 k/n 的概率决定是否保留当前元素。NOTE:不能用到来就直接一定保留，而应该用下面这种法法，下面这种方法所有来到的key最终存在于蓄水池的概率在数学上是完全相等的
		// n = TotalSamplesSeen, k = ReservoirCap

		// 生成一个 [0, n) 的随机数，此处引入一个高性能随机数库
		// 因为标准库 math/rand通常使用较为复杂的伪随机算法（如 Lagged Fibonacci 或 PCG 算法）。这些算法产生的随机数分布质量很高，周期很长，可以通过统计学检验。但计算步骤相对繁琐。
		limit := uint32(n.ReadCount)
		r := fastrand.Uint32n(limit) // Uint32n 返回范围在 [0..maxN) 内的伪随机 uint32 值。从并发的goroutine中调用此函数是安全的。

		// 如果随机数落在 [0, k) 区间内，则替换掉对应下标的元素
		if r < uint32(ReservoirCap) {
			// 【关键】深拷贝
			k := make(Key, len(keySuffix))
			copy(k, keySuffix)

			// 替换掉旧样本
			n.RSuffixReservoir[r] = k
		}

		// 判断是否满足分裂条件，写'&&'性能更高(不需要比较所有即可得出结果)
		if n.ReadCount > n.SplitThreshold && len(n.RSuffixReservoir) == ReservoirCap {
			// if n.ReadCount > n.SplitThreshold && n.WriteCount > 0 && n.ReadCount/n.WriteCount > TargetRatio && len(n.SuffixReservoir) == ReservoirCap {
			// 当前范围符合标准，允许分裂
			// TODO:将当前范围添加到某个地方记录起来
			n.Evolve() // NOTE:核心操作，进行分裂
		}
	} else {
		n.WriteCount++
		// 场景 A: 写蓄水池未满 -> 直接追加
		if len(n.WSuffixReservoir) < ReservoirCap {
			// 【关键】必须深拷贝！
			// Badger 的 key 往往复用 buffer，不拷贝会导致数据损坏
			k := make(Key, len(keySuffix))
			copy(k, keySuffix)

			n.WSuffixReservoir = append(n.WSuffixReservoir, k)
			return
		}

		// 场景 B: 蓄水池已满 -> 随机替换 (Algorithm R)
		// 逻辑：以 k/n 的概率决定是否保留当前元素。NOTE:不能用到来就直接一定保留，而应该用下面这种法法，下面这种方法所有来到的key最终存在于蓄水池的概率在数学上是完全相等的
		// n = TotalSamplesSeen, k = ReservoirCap

		// 生成一个 [0, n) 的随机数，此处引入一个高性能随机数库
		// 因为标准库 math/rand通常使用较为复杂的伪随机算法（如 Lagged Fibonacci 或 PCG 算法）。这些算法产生的随机数分布质量很高，周期很长，可以通过统计学检验。但计算步骤相对繁琐。
		limit := uint32(n.WriteCount)
		r := fastrand.Uint32n(limit) // Uint32n 返回范围在 [0..maxN) 内的伪随机 uint32 值。从并发的goroutine中调用此函数是安全的。

		// 如果随机数落在 [0, k) 区间内，则替换掉对应下标的元素
		if r < uint32(WReservoirCap) {
			// 【关键】深拷贝
			k := make(Key, len(keySuffix))
			copy(k, keySuffix)

			// 替换掉旧样本
			n.WSuffixReservoir[r] = k
		}
		return
	}

}

// Evolve 是核心分裂方法，通常由后台 Worker 调用，或者在 Write 路径中异步触发
// NOTE:调用前请确保n满足分裂的条件！！
func (n *HeatNode) Evolve() {
	// NOTE:因为这个函数目前是调用在AddSample，而该函数就已经加了写锁了，这里先不加了
	// n.Lock() // 加写锁
	// defer n.Unlock()

	// 先对蓄水池中的Key后缀进行排序
	// 对读蓄水池排序
	sort.Slice(n.RSuffixReservoir, func(i, j int) bool {
		return bytes.Compare(n.RSuffixReservoir[i], n.RSuffixReservoir[j]) < 0
	})
	// 对写蓄水池排序
	sort.Slice(n.WSuffixReservoir, func(i, j int) bool {
		return bytes.Compare(n.WSuffixReservoir[i], n.WSuffixReservoir[j]) < 0
	})

	// NOTE:怎么具体分裂？
	// （1）在已排序的蓄水池上先找到前缀相同的分裂区间（如111,121,411，469,659，799，755,955的分裂区间应该是[100~199],[400~499],[700~799]），然后找最长的MaxValidRange个分裂区间   PS:这是为了尽可能扁平化，加速查找流程
	// （2）生成对应个数个子节点（包括区间以及间隙，各个块之间首尾必定相连，并且加起来等于父节点的范围），并且对于各有效区域且有公共前缀的在子节点设置上前缀名，即设置上PathSegment
	// （3）将子节点挂载到父节点上，并且在父节点的SplitRangeKey中更新分裂点列表（分裂点列表直接取各子节点的头部）
	PrefixGroups := FindTopNPrefixGroups(n.RSuffixReservoir, MaxValidRange)

	// 按照相对位置排序
	sort.Slice(PrefixGroups, func(i, j int) bool {
		return PrefixGroups[i].Start < PrefixGroups[j].Start
	})

	childrenNodes, splitKeys := n.splitReservoir(PrefixGroups)
	if len(childrenNodes) > 0 {
		n.IsLeaf = false
		n.Children = childrenNodes
		n.SplitRangeKey = splitKeys
		n.RSuffixReservoir = nil
		n.WSuffixReservoir = nil
	} else {
		fmt.Print("出错了，子节点的个数为0？？？")
	}
}

// NOTE:现在有个问题，到底要不要用前缀PathSegment
// 1.如果用了话，就必须增加空隙节点（会增多节点的个数），但感觉会更精准，而且空隙节点也不会多很多？ NOTE:先实现这一个吧！！！
// 2.而如果不用的话，完整key存放，此时虽然减少了空隙节点，但是会极大增加每个节点的大小，而且应该如何去找分裂点呢？
// 3.又或者取一个折衷的方式？用前缀，但PathSegment不是看蓄水池key的公共前缀，而是看当前节点的首尾范围前缀？（但这个首尾的基本不就一定不会有公共前缀了？）

// splitReservoir 将样本数据 data 基于 PrefixGroups 进行分割，并构造子节点
// data: 父节点的蓄水池样本（相对 Key）
// PrefixGroups: 具有公共前缀且最长的的区间列表(NOTE:是分裂点之后的那个key的下标)
func (node *HeatNode) splitReservoir(PrefixGroups []ResIndexRange) ([]*HeatNode, []Key) {
	var children []*HeatNode
	var splitKeys []Key

	RSuffixReservoir := node.RSuffixReservoir
	WSuffixReservoir := node.WSuffixReservoir
	nextLevel := node.Level + 1
	rangeRation := 0.0
	wRangeRation := 0.0
	lastWRangeIndex := 0
	currentWRangeIndex := 0
	rangeReadCount := float64(node.ReadCount)
	rangeWriteCount := float64(node.WriteCount)
	// 遍历选中的区间（每一轮最多可以增加3个子节点【头部间隙节点、当前具有公共前缀的区间节点、尾部间隙节点】）
	for i, oneRange := range PrefixGroups {
		// 为子节点拼接路径 NOTE:先只保存相对Key
		// childPathSegment := MergeKey(node.PathSegment, oneRange.CommonPrefix)

		// 注意头部间隙是必定存在的，因为oneRange区间必定有共有前缀，所以头部间隙的头部和第一个区间的头部必定不同
		if i == 0 {
			// 现在需要加头部间隙
			// frontRS :=
			rangeRation = float64(oneRange.Start) / float64(ReservoirCap)
			wRangeRation, currentWRangeIndex = CalculateRangeRatio(lastWRangeIndex, oneRange.CommonPrefix, node.WSuffixReservoir)
			frontRangeStart := Key{} // 如果父节点有公共前缀，那么父节点的起始节点就应该等于公共前缀，所以下一层头部间隙就应该为空
			if len(node.PathSegment) == 0 {
				// 如果父节点无公共前缀，那么下一层头部间隙就应该等于父节点的开始
				frontRangeStart = node.RangeStart
			}

			frontChild := newHeatNode(
				nextLevel,
				frontRangeStart,       // 设置父节点的开始为第一个孩子的开始
				oneRange.CommonPrefix, // 设置第一个区间的start（即区间的公共前缀）为第一个孩子的结尾
				Key{},                 // 空隙节点无公共前缀
				RSuffixReservoir[0:oneRange.Start],
				WSuffixReservoir[lastWRangeIndex:currentWRangeIndex],
				int64(rangeReadCount*rangeRation),
				int64(rangeWriteCount*wRangeRation),
			)
			lastWRangeIndex = currentWRangeIndex
			children = append(children, frontChild)
			splitKeys = append(splitKeys, oneRange.CommonPrefix)
		}

		// 加入目前区域的子节点
		childRangeEnd := NextKeySameLength(oneRange.CommonPrefix)         // 将前缀+1设置为结尾
		rangeRation = float64(oneRange.End-oneRange.Start) / ReservoirCap // 自动转型
		wRangeRation, currentWRangeIndex = CalculateRangeRatio(lastWRangeIndex, childRangeEnd, node.WSuffixReservoir)

		// 构造子节点
		child := newHeatNode(
			nextLevel,
			oneRange.CommonPrefix, // 设置当前区间的共有前缀为开始
			childRangeEnd,         // 设置当前区间的共有前缀+1为结束
			oneRange.CommonPrefix, // 设置上路径片段
			RSuffixReservoir[oneRange.Start:oneRange.End], // 传递当前区间的Key
			WSuffixReservoir[lastWRangeIndex:currentWRangeIndex],
			int64(rangeReadCount*rangeRation),
			int64(rangeWriteCount*wRangeRation),
		)
		lastWRangeIndex = currentWRangeIndex
		children = append(children, child)
		splitKeys = append(splitKeys, childRangeEnd)
		var afterChildRangeEnd Key // 尾部间隙在Key值范围上结束的值
		var afterChildIndexEnd int // 尾部间隙在蓄水池上结束的索引
		if i == len(PrefixGroups)-1 {
			// 如果是最后一个区间
			afterChildIndexEnd = ReservoirCap
			afterChildRangeEnd = Key{} // NOTE:如果父节点有公共前缀，那么子节点的尾部间隙同头部间隙一样，均设值为空！！
			if len(node.PathSegment) == 0 {
				// 如果父节点无公共前缀，那么下一层尾部间隙就应该等于父节点的结尾
				afterChildRangeEnd = node.RangeEnd
			}
		} else {
			// 如果是普通的区间之间间隙
			afterChildRangeEnd = PrefixGroups[i+1].CommonPrefix
			afterChildIndexEnd = PrefixGroups[i+1].Start
		}

		// 如果当前区间的尾部和间隙的尾部相同，那么就不用加间隙了！
		if CompareKey(afterChildRangeEnd, childRangeEnd) != 0 {
			// 加尾部间隙
			rangeRation = float64(afterChildIndexEnd-oneRange.End) / ReservoirCap
			wRangeRation, currentWRangeIndex = CalculateRangeRatio(lastWRangeIndex, afterChildRangeEnd, node.WSuffixReservoir)
			afterChild := newHeatNode(
				nextLevel,
				childRangeEnd,      // 将当前区间的尾部设置为间隙的开始
				afterChildRangeEnd, // 将下一区间的头部(或者父亲的尾)设置为间隙的结束
				Key{},              // 空隙节点无公共前缀
				RSuffixReservoir[oneRange.End:afterChildIndexEnd],
				WSuffixReservoir[lastWRangeIndex:currentWRangeIndex],
				int64(rangeReadCount*rangeRation),
				int64(rangeWriteCount*wRangeRation),
			)
			lastWRangeIndex = currentWRangeIndex
			children = append(children, afterChild)
			if i != len(PrefixGroups)-1 { // 不是最后一个区间，才需要加分裂点
				splitKeys = append(splitKeys, afterChildRangeEnd)
			}
		}
	}

	return children, splitKeys
}

// SearchLeaf 根据输入的 Key 查找其所属的叶子节点（注意用的是迭代，这样避免了锁的竞争，提高了高并发行能）
// Key: 完整的 Key（绝对路径）
// needAddSample: 是否需要增加样本
// 返回值: 包含该 Key 范围的 *HeatNode，如果路径不匹配则可能返回 nil
func (n *HeatNode) SearchLeaf(key Key, isRead bool) *HeatNode {
	current := n
	// searchSuffix 随着层级下沉，会不断被切掉前缀，变成相对 Key
	searchSuffix := key

	for {
		// 1. 加读锁，保护当前节点的 PathSegment, SplitRangeKey, Children 等结构
		current.RLock()

		// 2. 检查前缀匹配
		// 既然是 Trie 树结构，Key 必须包含当前节点的 PathSegment
		if !bytes.HasPrefix(searchSuffix, current.PathSegment) {
			current.RUnlock()
			// 如果前缀不匹配，说明这个 Key 不在这个树的分支路径上
			// 视具体业务需求，这里可以返回 nil，或者返回当前节点（作为最接近的节点）
			// 这里返回 nil 表示“路径不通”
			return nil
		}

		// 3. 剥离前缀，准备下一层的路由
		// 下一层子节点的 SplitKey 是相对于当前节点 PathSegment 之后的后缀
		prefixLen := len(current.PathSegment)
		// 此时 searchSuffix 长度一定 >= prefixLen，因为前面 HasPrefix 检查过了
		remainingKey := searchSuffix[prefixLen:]

		// 4. 如果是叶子节点，这就是我们要找的目标
		if current.IsLeaf {
			current.RUnlock()
			current.AddSample(remainingKey, isRead) // NOTE:核心操作，尝试追加样本
			return current
		}

		// 5. 二分查找确定子节点索引
		// SplitRangeKey 存储的是分割点（子节点的上界，左闭右开原则）
		// 我们需要找到第一个 SplitKey > remainingKey 的位置 index
		// 这样 remainingKey 就属于 Children[index]
		idx := sort.Search(len(current.SplitRangeKey), func(i int) bool { // TODO:二分查找是if/else,而SplitRangeKey很小,是否需要顺序遍历以提速CPU
			// 比较：SplitKey > remainingKey
			return bytes.Compare(current.SplitRangeKey[i], remainingKey) > 0
		})

		// 6. 确定下一跳子节点
		// 如果 idx == len(SplitRangeKey)，说明 remainingKey >= 所有分割点，属于最后一个子节点
		// 注意：Children 的数量通常比 SplitRangeKey 多 1
		if idx >= len(current.Children) {
			// 防御性检查，理论上 SplitKey 数量 = len(Children) - 1
			// 如果出现越界，回退到最后一个孩子
			idx = len(current.Children) - 1
		}

		nextChild := current.Children[idx]

		// 7. 释放当前节点锁，指针下移，更新搜索用的后缀
		current.RUnlock()
		current = nextChild
		searchSuffix = remainingKey
	}
}

// CalculateTreeMemory 递归计算整棵树的近似内存占用（单位：字节）
func (n *HeatNode) CalculateTreeMemory() int64 {
	if n == nil {
		return 0
	}

	// 1. 结构体本身的基础大小 (64位机器下，指针、int64、切片头等加起来大概 150 字节左右)
	var size int64 = 150

	// 2. 累加 PathSegment 和边界 Key 的底层字节大小
	size += int64(len(n.PathSegment))
	size += int64(len(n.RangeStart))
	size += int64(len(n.RangeEnd))

	// 3. 累加 SplitRangeKey 数组的大小
	// 切片本身有一定的容量开销，加上每个 []byte 内部的真实长度
	size += int64(cap(n.SplitRangeKey) * 24) // 切片头开销
	for _, key := range n.SplitRangeKey {
		size += int64(len(key))
	}

	// 4. 累加读写蓄水池的内存占用 (这是内存大头)
	size += int64(cap(n.RSuffixReservoir) * 24)
	for _, key := range n.RSuffixReservoir {
		size += int64(len(key))
	}

	size += int64(cap(n.WSuffixReservoir) * 24)
	for _, key := range n.WSuffixReservoir {
		size += int64(len(key))
	}

	// 5. 递归计算所有子节点的内存
	size += int64(cap(n.Children) * 8) // 子节点指针数组的开销
	for _, child := range n.Children {
		size += child.CalculateTreeMemory()
	}

	return size
}
