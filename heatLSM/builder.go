package heatLSM

// NOTE:所有范围遵循左闭右开原则
// 如 [A,C) [C,G) [G,Z)   其中的分裂点SplitRangeKey是C，G

// YCSB用的BadgerDB的读位置NOTE:2026031800 写位置NOTE:2026031801
import (
	"bytes"
	"fmt"
	"sort"
	"sync"

	"github.com/axiomhq/hyperloglog"
	"github.com/valyala/fastrand"
)

const (
	// 蓄水池容量：叶子节点最多存多少个样本触发分裂检查
	ReservoirCap  = 256 // NOTE:需要大于等于256，避免极端情况下找不到一个有公共前缀的区间
	WReservoirCap = 64
	// 树节点读写比达到多少停止分裂
	TargetRatio = 3

	// 树节点分裂时，将蓄水池最多划分多少个有效范围
	MaxValidRange = 4

	// --- 【新增】：分块内存池大小 ---
	// 每次向系统申请连续存放 1024 个统计数据的物理内存块
	StatsChunkSize = 1024
)

type Key []byte

// --- 【新增】：肉体（集中存储的统计数据） ---
// 这部分数据将被紧凑地存放在 HeatmapManager 的二维分块数组里
type RegionStats struct {
	IsActive bool // 标记该槽位是否在使用中，方便复用

	ReadCount  int64 // 读写计数用int类型,这样在cpu内只需要执行一次自增即可,花费时间最少
	WriteCount int64

	OverwriteRation float64 // 覆写率,NOTE:注意覆写率不能直接实时计算,所以这里的OverwriteRation实际是上一轮周期结束时的 本轮周期覆写率和上轮覆写率的权重合
	// inheritOverwriteRation float64             // 从父节点或者上一轮周期继承的覆写率
	EpochStartWrite int64               // 本轮周期的开始时的写入量,WriteCount - EpochStartWrite等于当前周期的写入量
	CurrentHLL      *hyperloglog.Sketch // 用于覆写率

	TotalSamplesSeen int64
	RSuffixReservoir []Key // 注意这个还有下面这个俩蓄水池在StatsChunks存储的是一个指针,并不是直接在StatsChunks内
	WSuffixReservoir []Key
}

type HeatNode struct {
	// --- 树形结构与前缀压缩 ---
	Level          int64 // 当前节点所在层级
	SplitThreshold int64 // 读分裂阈值：读热度超过此值，且样本满了，才允许分裂（只读热分裂）
	isTargetRange  bool  // 判断是否是达到TargetRatio的目标节点
	RangeStart     Key   // RangeStart和RangeEnd构造出左闭右开的区间（注意这俩是针对父亲的范围分割，且同一层的节点范围合并起来就是父节点的全域，即向上看。且注意存储的是逐层拼接的增量Key）
	RangeEnd       Key   // nil 代表无穷大
	PathSegment    Key   // 当前节点代表的公共前缀片段。完整 Key = 父节点Prefix + ... + 当前Prefix + Suffix（注意是逐层拼接的增量Key）

	// --- 结构控制 ---
	IsLeaf        bool
	Children      []*HeatNode // 这里使用有序切片存储子节点，分裂不固定为2,可为N个  TODO:这个Children和下面的SplitRangeKey的长度大小是否要直接固定,这样的话虽然空间变大,但是因为空间是连续的了,所以cpu cache会命中率很高
	SplitRangeKey []Key       // 分裂点列表，即Children中前【len(Children)-1】个的RangeEnd值（注意存的是逐层拼接的增量Key）

	// --- 【核心重构：灵魂绑定】 ---
	// 剥离了原本庞大的 Count 和 蓄水池，现在仅用一个 32 位整型指向内存池！
	StatsID int32

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

	// --- 【新增：集中式分块物理内存池 (Chunked Pool)】 ---
	// 使用二维数组可以完美避免 append 扩容导致的底层内存搬迁问题，并发绝对安全(第一层是指每次分配的StatsChunkSize个Chunk,第二层则是当次分配的具体的各个Chunk)
	StatsChunks [][]RegionStats
	FreeList    []int32    // 垃圾回收站，存放被合并/销毁的 StatsID，用于 O(1) 复用
	poolLock    sync.Mutex // 仅在 Allocate 和 Free 时加锁，不影响高频的 AddSample
}

// --- 【新增】：内存池管理器方法 ---

// getStats 通过 StatsID 快速定位物理内存地址 (O(1) 寻址)
func (m *HeatmapManager) getStats(id int32) *RegionStats {
	return &m.StatsChunks[id/StatsChunkSize][id%StatsChunkSize]
}

// AllocateStats 申请一块新的统计内存，优先复用空洞
func (m *HeatmapManager) AllocateStats() int32 {
	m.poolLock.Lock()
	defer m.poolLock.Unlock()

	// 1. 如果回收站有空槽位，直接复用 (O(1))
	if len(m.FreeList) > 0 {
		id := m.FreeList[len(m.FreeList)-1]
		m.FreeList = m.FreeList[:len(m.FreeList)-1]
		stats := m.getStats(id)
		stats.IsActive = true
		stats.ReadCount = 0
		stats.WriteCount = 0
		stats.EpochStartWrite = 0
		stats.CurrentHLL = hyperloglog.New14() // 构造一个新的Sparse HLL
		stats.OverwriteRation = 0.0            // 默认覆盖率等均为0,如果有遗传,那么外面再覆盖它

		stats.RSuffixReservoir = stats.RSuffixReservoir[:0] // 保留容量，清空数据
		stats.WSuffixReservoir = stats.WSuffixReservoir[:0]
		return id
	}

	// 2. 如果没有空槽位，看看当前最后一个 Chunk 是否满了
	numChunks := len(m.StatsChunks)
	if numChunks == 0 || len(m.StatsChunks[numChunks-1]) == StatsChunkSize {
		// 分配一个新的连续 Chunk
		newChunk := make([]RegionStats, 0, StatsChunkSize) // 再分配1024个块区域
		m.StatsChunks = append(m.StatsChunks, newChunk)
		numChunks++
	}

	// 3. 在当前 Chunk 追加分配 (因为 Chunk 内置容量为 StatsChunkSize，这里的 append 绝对不会发生搬迁)
	chunkIdx := numChunks - 1
	offset := len(m.StatsChunks[chunkIdx])

	m.StatsChunks[chunkIdx] = append(m.StatsChunks[chunkIdx], RegionStats{
		IsActive:         true,
		CurrentHLL:       hyperloglog.New14(),
		OverwriteRation:  0.0,
		RSuffixReservoir: make([]Key, 0, ReservoirCap),
		WSuffixReservoir: make([]Key, 0, WReservoirCap),
	})

	return int32(chunkIdx*StatsChunkSize + offset)
}

// FreeStats 释放内存，送入回收站
func (m *HeatmapManager) FreeStats(id int32) {
	m.poolLock.Lock()
	defer m.poolLock.Unlock()
	stats := m.getStats(id)
	stats.IsActive = false
	// 注意：不释放切片的底层数组，留作下次分配复用，减少 GC 压力
	stats.RSuffixReservoir = stats.RSuffixReservoir[:0]
	stats.WSuffixReservoir = stats.WSuffixReservoir[:0]
	m.FreeList = append(m.FreeList, id)
}

// ==========================================

// 创建一个新的节点 (重构：不再接收蓄水池，而是接收一个预分配好的 StatsID)
func newHeatNode(Level int64, start, end Key, pathSeg Key, statsID int32) *HeatNode {
	n := &HeatNode{
		Level:          Level,
		SplitThreshold: 1 << (Level + 10), // TODO:目前第一层1024,第二层2048，第三层4096，需要根据实验调整
		RangeStart:     start,
		RangeEnd:       end,
		PathSegment:    pathSeg,
		IsLeaf:         true,
		isTargetRange:  false,
		StatsID:        statsID, // 绑定灵魂！
	}
	return n
}

// 初始化管理器 初始位置在NOTE:2026031802
func NewHeatmapManager() *HeatmapManager {
	m := &HeatmapManager{
		TotalRead:   0,
		TotalWrite:  0,
		StatsChunks: make([][]RegionStats, 0),
		FreeList:    make([]int32, 0),
	}

	rootStart := Key{}
	rootEnd := Key(nil)

	// 1. 初始化母树
	rootStatsID := m.AllocateStats()
	motherRoot := newHeatNode(0, rootStart, rootEnd, Key{}, rootStatsID)
	m.MotherTree = &HeatmapTree{Root: motherRoot}

	return m
}

// NOTE:核心逻辑：向蓄水池添加样本，现在下面这个是针对全局的，注意调用下面这个函数需要先找到目标叶子节点，并且确保当前Key在目标叶子节点的边界内（左闭右开）。
// TODO:看看是否有必要将近期查询KEY进入的可能性拉大
// keySuffix: 已经剥离了当前节点 Prefix 的后缀部分
// 【重构】：增加 manager 参数，以直接定位底层物理内存
func (n *HeatNode) AddSample(keySuffix Key, isRead bool, m *HeatmapManager) {
	n.Lock()
	defer n.Unlock()

	if !n.IsLeaf {
		return
	}

	// 【核心改变】：通过 StatsID 从连续内存池中获取真实数据！
	stats := m.getStats(n.StatsID)

	if isRead {
		stats.ReadCount++
		// 3. 场景 A: 蓄水池未满 -> 直接追加
		if len(stats.RSuffixReservoir) < ReservoirCap {
			k := make(Key, len(keySuffix))
			copy(k, keySuffix)
			stats.RSuffixReservoir = append(stats.RSuffixReservoir, k)
			return
		}

		// 4. 场景 B: 蓄水池已满 -> 随机替换 (Algorithm R)
		limit := uint32(stats.ReadCount)
		r := fastrand.Uint32n(limit)

		if r < uint32(ReservoirCap) {
			k := make(Key, len(keySuffix))
			copy(k, keySuffix)
			stats.RSuffixReservoir[r] = k
		}

		if stats.ReadCount > n.SplitThreshold && len(stats.RSuffixReservoir) == ReservoirCap {
			n.Evolve(m) // 传入 manager
		}
	} else {
		stats.WriteCount++
		stats.CurrentHLL.Insert(keySuffix)               // 喂给HLL,来算覆写率
		if len(stats.WSuffixReservoir) < WReservoirCap { // 这里修复了你原代码中小 bug，写入应判断 WReservoirCap
			k := make(Key, len(keySuffix))
			copy(k, keySuffix)
			stats.WSuffixReservoir = append(stats.WSuffixReservoir, k)
			return
		}

		limit := uint32(stats.WriteCount)
		r := fastrand.Uint32n(limit)

		if r < uint32(WReservoirCap) {
			k := make(Key, len(keySuffix))
			copy(k, keySuffix)
			stats.WSuffixReservoir[r] = k
		}
		return
	}
}

// Evolve 是核心分裂方法
func (n *HeatNode) Evolve(m *HeatmapManager) {
	// 获取自身的物理数据
	stats := m.getStats(n.StatsID)

	// 先对蓄水池中的Key后缀进行排序
	sort.Slice(stats.RSuffixReservoir, func(i, j int) bool {
		return bytes.Compare(stats.RSuffixReservoir[i], stats.RSuffixReservoir[j]) < 0
	})
	sort.Slice(stats.WSuffixReservoir, func(i, j int) bool {
		return bytes.Compare(stats.WSuffixReservoir[i], stats.WSuffixReservoir[j]) < 0
	})

	PrefixGroups := FindTopNPrefixGroups(stats.RSuffixReservoir, MaxValidRange)

	sort.Slice(PrefixGroups, func(i, j int) bool {
		return PrefixGroups[i].Start < PrefixGroups[j].Start
	})

	// 将 manager 传递给 splitReservoir，以便为子节点分配新的 Stats 内存
	childrenNodes, splitKeys := n.splitReservoir(PrefixGroups, m)
	if len(childrenNodes) > 0 {
		n.IsLeaf = false
		n.Children = childrenNodes
		n.SplitRangeKey = splitKeys

		// 【神级回收】：当前节点已经变成普通路由节点，它不再需要保留笨重的统计数据了！
		// 释放它占用的内存池槽位，供给未来新的叶子节点使用！
		m.FreeStats(n.StatsID)
		n.StatsID = -1
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

// splitReservoir 将样本数据 data 基于 PrefixGroups 进行分割，并构造子节点
func (node *HeatNode) splitReservoir(PrefixGroups []ResIndexRange, m *HeatmapManager) ([]*HeatNode, []Key) {
	var children []*HeatNode
	var splitKeys []Key

	// 获取父节点的物理数据
	stats := m.getStats(node.StatsID)
	RSuffixReservoir := stats.RSuffixReservoir
	WSuffixReservoir := stats.WSuffixReservoir

	nextLevel := node.Level + 1
	rangeRation := 0.0
	wRangeRation := 0.0
	fatherOverwriteRation := 0.0 // 存的是父节点当前的覆写率
	lastWRangeIndex := 0
	currentWRangeIndex := 0
	rangeReadCount := float64(stats.ReadCount)
	rangeWriteCount := float64(stats.WriteCount)
	if stats.WriteCount > 0 {
		uniqueKeys := float64(stats.CurrentHLL.Estimate())
		fatherCurentOverwriteRation := 1.0 - (uniqueKeys / rangeWriteCount)
		fatherOverwriteRation = fatherCurentOverwriteRation*0.5 + stats.OverwriteRation*0.5 // 计算出当前父节点的覆写率,TODO:比例需要确定
		// TODO:如何确定父节点的覆写率,这个和衰退周期有关!需要确定了衰退周期再决定.感觉应该随着每衰退一轮,就计算一次覆写率,计算的同时清空HLL
		if fatherOverwriteRation < 0 {
			fatherOverwriteRation = 0
		}
	}
	// 遍历选中的区间（每一轮最多可以增加3个子节点【头部间隙节点(仅第一个元素会加)、当前具有公共前缀的区间节点、尾部间隙节点】）
	for i, oneRange := range PrefixGroups {
		if i == 0 {
			// NOTE:如果是第一个元素,需要加入头部间隙
			rangeRation = float64(oneRange.Start) / float64(ReservoirCap)
			wRangeRation, currentWRangeIndex = CalculateRangeRatio(lastWRangeIndex, oneRange.CommonPrefix, WSuffixReservoir)
			frontRangeStart := Key{} // 如果父节点有公共前缀，那么父节点的起始节点就应该等于公共前缀，所以下一层头部间隙就应该为空
			if len(node.PathSegment) == 0 {
				// 如果父节点无公共前缀，那么下一层头部间隙就应该等于父节点的开始
				frontRangeStart = node.RangeStart
			}

			// 【重构核心】：为新的间隙节点向大内存池申请一块空间
			frontChildStatsID := m.AllocateStats()
			frontChildStats := m.getStats(frontChildStatsID)
			frontChildStats.ReadCount = int64(rangeReadCount * rangeRation)
			frontChildStats.WriteCount = int64(rangeWriteCount * wRangeRation)
			frontChildStats.EpochStartWrite = frontChildStats.WriteCount
			if frontChildStats.WriteCount > 0 {
				frontChildStats.OverwriteRation = fatherOverwriteRation
			}
			// 继承并拷贝对应的蓄水池数据 (因为是间隙节点，无公共前缀，不需要剥离)
			for _, key := range RSuffixReservoir[0:oneRange.Start] {
				k := make(Key, len(key))
				copy(k, key)
				frontChildStats.RSuffixReservoir = append(frontChildStats.RSuffixReservoir, k)
			}
			for _, key := range WSuffixReservoir[lastWRangeIndex:currentWRangeIndex] {
				k := make(Key, len(key))
				copy(k, key)
				frontChildStats.WSuffixReservoir = append(frontChildStats.WSuffixReservoir, k)
			}

			frontChild := newHeatNode(nextLevel, frontRangeStart, oneRange.CommonPrefix, Key{}, frontChildStatsID)
			lastWRangeIndex = currentWRangeIndex
			children = append(children, frontChild)
			splitKeys = append(splitKeys, oneRange.CommonPrefix)
		}

		// NOTE:加入目前区域的子节点
		childRangeEnd := NextKeySameLength(oneRange.CommonPrefix)         // 将前缀+1设置为结尾
		rangeRation = float64(oneRange.End-oneRange.Start) / ReservoirCap // 自动转型
		wRangeRation, currentWRangeIndex = CalculateRangeRatio(lastWRangeIndex, childRangeEnd, WSuffixReservoir)

		// 【重构核心】：为当前具有公共前缀的有效区间申请空间
		childStatsID := m.AllocateStats()
		childStats := m.getStats(childStatsID)
		childStats.ReadCount = int64(rangeReadCount * rangeRation)
		childStats.WriteCount = int64(rangeWriteCount * wRangeRation)
		childStats.EpochStartWrite = childStats.WriteCount
		if childStats.WriteCount > 0 {
			childStats.OverwriteRation = fatherOverwriteRation
		}

		// 重点：剥离前缀！(oneRange.CommonPrefix)
		pathSegLen := len(oneRange.CommonPrefix)
		for _, key := range RSuffixReservoir[oneRange.Start:oneRange.End] {
			if len(key) >= pathSegLen {
				suffix := key[pathSegLen:]
				k := make(Key, len(suffix))
				copy(k, suffix)
				childStats.RSuffixReservoir = append(childStats.RSuffixReservoir, k)
			}
		}
		for _, key := range WSuffixReservoir[lastWRangeIndex:currentWRangeIndex] {
			if len(key) >= pathSegLen {
				suffix := key[pathSegLen:]
				k := make(Key, len(suffix))
				copy(k, suffix)
				childStats.WSuffixReservoir = append(childStats.WSuffixReservoir, k)
			}
		}

		child := newHeatNode(nextLevel, oneRange.CommonPrefix, childRangeEnd, oneRange.CommonPrefix, childStatsID)
		lastWRangeIndex = currentWRangeIndex
		children = append(children, child)
		splitKeys = append(splitKeys, childRangeEnd)

		// NOTE:下面开始加入尾部间隙节点
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
			wRangeRation, currentWRangeIndex = CalculateRangeRatio(lastWRangeIndex, afterChildRangeEnd, WSuffixReservoir)

			// 申请尾部间隙空间
			afterChildStatsID := m.AllocateStats()
			afterChildStats := m.getStats(afterChildStatsID)
			afterChildStats.ReadCount = int64(rangeReadCount * rangeRation)
			afterChildStats.WriteCount = int64(rangeWriteCount * wRangeRation)
			afterChildStats.EpochStartWrite = afterChildStats.WriteCount
			if afterChildStats.WriteCount > 0 {
				afterChildStats.OverwriteRation = fatherOverwriteRation
			}

			for _, key := range RSuffixReservoir[oneRange.End:afterChildIndexEnd] {
				k := make(Key, len(key))
				copy(k, key)
				afterChildStats.RSuffixReservoir = append(afterChildStats.RSuffixReservoir, k)
			}
			for _, key := range WSuffixReservoir[lastWRangeIndex:currentWRangeIndex] {
				k := make(Key, len(key))
				copy(k, key)
				afterChildStats.WSuffixReservoir = append(afterChildStats.WSuffixReservoir, k)
			}

			afterChild := newHeatNode(nextLevel, childRangeEnd, afterChildRangeEnd, Key{}, afterChildStatsID)
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
// 【重构】：把 Manager 一路传下去，方便底层调用 AddSample 时寻址
func (n *HeatNode) SearchLeaf(key Key, isRead bool, m *HeatmapManager) *HeatNode {
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
			current.AddSample(remainingKey, isRead, m) // NOTE:核心操作，尝试追加样本
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
// 如果想统计全貌，可以在 Manager 侧通过 len(StatsChunks) 计算大块物理内存
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

	size += int64(cap(n.Children) * 8)
	for _, child := range n.Children {
		size += child.CalculateTreeMemory()
	}

	return size
}

// CalculatePoolMemory 精确计算集中式内存池占据的物理内存大小 (单位：字节)
func (m *HeatmapManager) CalculatePoolMemory() int64 {
	var size int64 = 0

	if m == nil {
		return 0
	}

	// 1. 外层切片 StatsChunks 本身的开销 (切片头 24 字节)
	size += 24
	// 外层切片底层数组的开销 (存的是内层切片的头，每个 24 字节)
	size += int64(cap(m.StatsChunks) * 24)

	// 2. 遍历每一个 Chunk 计算内存
	for _, chunk := range m.StatsChunks {
		// RegionStats 结构体本身的基础大小预估：
		// IsActive(1字节+7字节对齐) + ReadCount(8) + WriteCount(8) + TotalSamplesSeen(8)
		// + RSuffixReservoir切片头(24) + WSuffixReservoir切片头(24) = 80 字节
		size += int64(cap(chunk) * 80)

		// 3. 深入当前 Chunk 的每一个槽位，计算动态蓄水池的开销
		// 注意：即使 IsActive 为 false (空洞)，只要它的蓄水池没被设为 nil，容量开销就还在！
		for i := range chunk {
			stats := &chunk[i]

			// --- 读蓄水池 ---
			// 蓄水池底层数组的开销：存的是 Key([]byte) 的切片头，每个 24 字节
			size += int64(cap(stats.RSuffixReservoir) * 24)
			// 遍历实际装入的 Key，累加底层真实 byte 数组的长度 (真实堆内存)
			for _, key := range stats.RSuffixReservoir {
				size += int64(len(key))
			}

			// --- 写蓄水池 ---
			size += int64(cap(stats.WSuffixReservoir) * 24)
			for _, key := range stats.WSuffixReservoir {
				size += int64(len(key))
			}
		}
	}

	// 4. 计算回收站 (FreeList) 的开销
	size += 24                         // FreeList 切片头
	size += int64(cap(m.FreeList) * 4) // 底层数组存的是 int32，每个 4 字节

	return size
}

// CalculateTotalMemory 宏观统计：灵魂(树) + 肉体(池) 的总内存大小
func (m *HeatmapManager) CalculateTotalMemory() int64 {
	var treeMem int64 = 0
	if m.MotherTree != nil && m.MotherTree.Root != nil {
		treeMem = m.MotherTree.Root.CalculateTreeMemory()
	}

	poolMem := m.CalculatePoolMemory()

	// 如果你有单独打印需求，可以解开下面的注释
	// fmt.Printf("[内存统计] 树(路由)占用: %d bytes, 池(数据)占用: %d bytes\n", treeMem, poolMem)

	return treeMem + poolMem
}

// Print 打印整棵母树的热力分布
func (m *HeatmapManager) Print() {
	fmt.Println("================ 热力树拓扑结构 ================")
	if m.MotherTree != nil && m.MotherTree.Root != nil {
		PrintTree(m.MotherTree.Root, "", m)
	}
	fmt.Println("================================================")

	// 你甚至可以顺手打印一下内存池的利用率，感受 DOD 架构的魅力
	activeCount := 0
	for _, chunk := range m.StatsChunks {
		for _, stat := range chunk {
			if stat.IsActive {
				activeCount++
			}
		}
	}
	fmt.Printf("[内存池状态] 活跃节点数: %d, 回收站空洞数: %d\n", activeCount, len(m.FreeList))
	fmt.Printf("当前热力图总内存占用: %s\n", FormatBytes(m.CalculateTotalMemory()))

}
