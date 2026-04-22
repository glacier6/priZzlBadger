package heatLSM

// NOTE:所有范围遵循左闭右开原则
// 如 [A,C) [C,G) [G,Z)   其中的分裂点SplitRangeKey是C，G

// YCSB用的BadgerDB的读位置NOTE:2026031800 写位置NOTE:2026031801
import (
	"bytes"
	"fmt"
	"sort"
	"sync"
	"sync/atomic"

	"github.com/axiomhq/hyperloglog"
	"github.com/valyala/fastrand"
)

const (
	// NOTE:蓄水池容量：叶子节点最多存多少个样本触发分裂检查，从4096倒着来/2或许不错，会在第四层达到最小值256
	MinReservoirCap = 256
	MaxReservoirCap = 1024

	// 树节点分裂时，将蓄水池最多划分多少个有效范围
	// NOTE:先按照个数对所有有共享前缀的样本排名，然后依次去取最高的，先取4个（如果不够四个那就有多少个取多少个），然后再去判断后续的，如果后续的出现次数大于0.05，那么也算一个有效范围，
	// 最小保底分裂分支数 (前 4 名无条件建国)
	GuaranteedFanOut = 4
	// 弹性扩招的密度阈值 (第 5 名开始，必须占当前蓄水池的 5% 以上)
	MinPrefixRatio = 0.05

	// --- 【新增】：分块内存池大小 ---
	// 每次向系统申请连续存放 1024 个统计数据的物理内存块
	StatsChunkSize = 1024

	// CurrentEpochWeight 决定了本周期“最新覆写率”在“总覆写率”中的占比。
	// 取值范围 [0.0, 1.0]。
	// - 值越大 (如 0.8)：系统对近期突发热点越敏感，但也更容易产生抖动。
	// - 值越小 (如 0.2)：系统对历史热度记忆越长，平滑度极高，适合长尾热点，而且值约小约容易导致成长不起来！
	// TODO:比例需要确定
	// =========================================================
	CurrentEpochWeight = 0.5

	// 高低覆写率的分界线
	// NOTE:写成排名制的，然后最好还要关联每个节点具体的写入量！因为最终这个参数要控制的是进热层的数据量！
	HotVolumePercentage = 0.20 // 1. 常规配额：取当期全局总Key量的前 20%
	MinHotRatioFloor    = 0.30 // 2. 入场底线：覆写率低于 0.3 的，哪怕配额没满也绝对不要
	HighHeatBypassRatio = 0.50 // 3. VVIP特权：覆写率 >= 0.5 的，哪怕配额满了也强行加座！

	// 多少次memtable的转换触发一次衰减及更新高覆写率范围视图
	// NOTE:因为HLL依赖大数定律，所以要和热范围视图的更新频率区分开来，最好是与当前节点所处层级的那个分裂阈值SplitThreshold挂钩，比如只有HLL累积了SplitThreshold/4才代表本次采集结果具备有效性，允许更新！
	// 但是随着每次纪元的结束,依旧会进行一次覆写率和高覆写快照的更新,只是不会重置HLL和EpochStartWrite.此外,需要注意的是,为了保证统计的有效性,当本轮HLL的write数小于1024时,跳过,避免冷启动污染
	flushThreshold = 1
	// 目前不知道为啥,10000000条数据与操作时,为1和普通版本速度基本一致(频繁触发写入Hot),为4慢6%左右(触发写入Hot),为100慢3%左右(完全不触发写入Hot)

)

type Key []byte

// 记录高覆写范围的原子单位
type HotZone struct {
	Start Key
	End   Key
}

// --- 【新增】：肉体（集中存储的统计数据） ---
// 这部分数据将被紧凑地存放在 HeatmapManager 的二维分块数组里
// NOTE:NOTE:这个原本是打算在衰减的时候用的,但是现在貌似用不到?而是因为HeatNode将负责读,而RegionStats负责写,做到读写分离,这样锁就不会加的很乱.
// NOTE:NOTE:而且一个RegionStats加起来不大于64字节,可以放到一个CPU Cache Line(64字节内),可以提高Cpu cache命中率
type RegionStats struct {
	IsActive bool // 标记该槽位是否在使用中，方便复用

	WriteCount       uint32 // 读写计数用int类型,这样在cpu内只需要执行一次自增即可,花费时间最少
	WindowStartWrite uint32 // (原 EpochStartWrite) 统计学窗口游标：用于 HLL 结算置信度
	EpochStartWrite  uint32 // 本轮周期的开始时的写入量,WriteCount - EpochStartWrite等于当前周期的写入量

	// 注意一起被计算的数据，就应该被存放在一起,所以下面这三个就放在一起好了
	OverwriteRation float64 // 覆写率,NOTE:注意覆写率不能直接实时计算,所以这里的OverwriteRation实际是上一轮周期结束时的 本轮周期覆写率和上轮覆写率的权重合
	// inheritOverwriteRation float64             // 从父节点或者上一轮周期继承的覆写率
	CurrentHLL *hyperloglog.Sketch // 用于覆写率

	WSuffixReservoir []Key // 注意这个蓄水池在StatsChunks存储的是一个指针,并不是直接在StatsChunks内,所以遍历RegionStats的时候尽量不要碰这个,会导致cpu cache失效!
}

type HeatNode struct {
	// --- 树形结构与前缀压缩 ---
	Level          int64 // 当前节点所在层级
	SplitThreshold int64 // 读分裂阈值：写热度超过此值，且样本满了，才允许分裂
	RangeStart     Key   // RangeStart和RangeEnd构造出左闭右开的区间（注意这俩是针对父亲的范围分割，且同一层的节点范围合并起来就是父节点的全域，即向上看。且注意存储的是逐层拼接的增量Key）
	RangeEnd       Key   // nil 代表无穷大
	PathSegment    Key   // 当前节点代表的公共前缀片段。完整 Key = 父节点Prefix + ... + 当前Prefix + Suffix（注意是逐层拼接的增量Key）

	// --- 结构控制 ---
	IsLeaf        bool
	Children      []*HeatNode // 这里使用有序切片存储子节点，分裂不固定为2,可为N个  TODO:TODO:这个Children和下面的SplitRangeKey的长度大小是否要直接固定,这样的话虽然空间变大,但是因为空间是连续的了,所以cpu cache会命中率很高
	SplitRangeKey []Key       // 分裂点列表，即Children中前【len(Children)-1】个的RangeEnd值（注意存的是逐层拼接的增量Key）

	// --- 【核心重构：灵魂绑定】 ---
	// 剥离了原本庞大的 Count 和 蓄水池，现在仅用一个 32 位整型指向内存池！
	StatsID int32

	ReservoirCap int

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
	// ChildTree *HeatmapTree

	// 全局锁：仅在 "母子合并 (Merge)" 和 "结构进化" 时使用
	evolutionLock sync.Mutex

	// --- 【新增：集中式分块物理内存池 (Chunked Pool)】 ---
	// 使用二维数组可以完美避免 append 扩容导致的底层内存搬迁问题，并发绝对安全(第一层是指每次分配的StatsChunkSize个Chunk,第二层则是当次分配的具体的各个Chunk)
	StatsChunks [][]RegionStats // NOTE:注意这个是定长内存池,用于瘦节点以及提高cpu cache的命中率.且因为是逻辑回收,不会触发GO语言的垃圾回收,学名叫做Arena Allocator（竞技场内存分配）??
	FreeList    []int32         // 垃圾回收站，存放被合并/销毁的 StatsID，用于 O(1) 复用
	poolLock    sync.Mutex      // 仅在 Allocate 和 Free 时加锁，不影响高频的 AddSample

	// NOTE:NOTE:下面这个存储的是上一周期得出的高覆写的范围快照,这个数据结构比较快
	// atomic.Value 本质上是一个无锁的原子指针替换,读的时候没有任何加锁动作.而在写的时候,在后台开辟一块全新的内存，新的建好之后,会瞬间切过去
	hotZonesSnapshot atomic.Value

	flushThreshold int32 // 衰减和更新快照的触发阈值，例如每 4 次 Memtable Flush 更新一次快照

	flushCount int32 // 原子计数器，记录发生了多少次 Flush memtable

	isDecaying int32 // 标记当前是否正在执行后台衰减（0=空闲，1=正在执行

	// --- 【新增：异步采样通道】 ---
	sampleCh  chan Key
	stopCh    chan struct{} // 用于优雅关闭后台协程
	DropCount int64         // 记录因为队列满而丢弃的样本数
}

// --- 【新增】：内存池管理器方法 ---

// getStats 通过 StatsID 快速定位物理内存地址 (O(1) 寻址)
func (m *HeatmapManager) getStats(id int32) *RegionStats {
	return &m.StatsChunks[id/StatsChunkSize][id%StatsChunkSize]
}

// AllocateStats 申请一块新的统计内存，优先复用空洞
func (m *HeatmapManager) AllocateStats(capacity int) int32 {
	m.poolLock.Lock()
	defer m.poolLock.Unlock()

	// 1. 如果回收站有空槽位，直接复用 (O(1))
	if len(m.FreeList) > 0 {
		id := m.FreeList[len(m.FreeList)-1]
		m.FreeList = m.FreeList[:len(m.FreeList)-1]
		stats := m.getStats(id)
		stats.IsActive = true
		stats.WriteCount = 0
		stats.EpochStartWrite = 0
		stats.WindowStartWrite = 0
		stats.CurrentHLL = hyperloglog.New14() // 构造一个新的Sparse HLL
		stats.OverwriteRation = 0.0            // 默认覆盖率等均为0,如果有遗传,那么外面再覆盖它

		stats.WSuffixReservoir = stats.WSuffixReservoir[:0] // 保留容量，清空数据
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
		WSuffixReservoir: make([]Key, 0, capacity),
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
	stats.WSuffixReservoir = stats.WSuffixReservoir[:0]
	m.FreeList = append(m.FreeList, id)
}

// ==========================================

// 创建一个新的节点 (重构：不再接收蓄水池，而是接收一个预分配好的 StatsID)
func newHeatNode(Level int64, start, end Key, pathSeg Key, statsID int32) *HeatNode {
	cap := MaxReservoirCap >> Level
	if cap < MinReservoirCap {
		cap = MinReservoirCap
	}
	n := &HeatNode{
		Level:          Level,
		SplitThreshold: 1 << (Level + 10), // NOTE:目前第一层2048,第二层2048，第三层4096，后续继续*2
		ReservoirCap:   cap,
		RangeStart:     start,
		RangeEnd:       end,
		PathSegment:    pathSeg,
		IsLeaf:         true,
		StatsID:        statsID, // 绑定灵魂！
	}
	if Level == 0 {
		n.SplitThreshold = 1 << 11
	}
	return n
}

// 初始化管理器 初始位置在NOTE:2026031802
func NewHeatmapManager() *HeatmapManager {
	m := &HeatmapManager{
		TotalRead:      0,
		TotalWrite:     0,
		StatsChunks:    make([][]RegionStats, 0),
		FreeList:       make([]int32, 0),
		flushThreshold: flushThreshold,
		sampleCh:       make(chan Key, 100000),
		stopCh:         make(chan struct{}),
	}

	rootStart := Key{}
	rootEnd := Key(nil)

	// 初始化母树
	rootStatsID := m.AllocateStats(MaxReservoirCap) // 根节点使用最大容量
	motherRoot := newHeatNode(0, rootStart, rootEnd, Key{}, rootStatsID)
	m.MotherTree = &HeatmapTree{Root: motherRoot}

	// 初始化快照组
	m.hotZonesSnapshot.Store(make([]HotZone, 0))
	go m.asyncProcessSamples()
	return m
}

// NOTE:有损异步采样队列
// RecordWriteAsync 极速异步记录写操作 (无锁、防阻塞)
func (m *HeatmapManager) RecordWriteAsync(key Key) {
	// 拷贝一份 Key，防止底层引擎复用 slice 导致后台消费时数据错乱！
	// (Badger 的 Key 生命周期极短，这步 copy 必不可少)
	kCopy := make(Key, len(key))
	copy(kCopy, key)

	select {
	case m.sampleCh <- kCopy:
		// 成功塞入队列
	default:
		atomic.AddInt64(&m.DropCount, 1)
		// 💥 核心：如果并发太高导致队列满了，直接丢弃该样本！
		// 对于热点统计来说，洪峰期的偶尔丢样完全不影响最终大盘。
		// 宁可丢失统计精度，绝不阻塞用户真实的写入！
	}
}

// asyncProcessSamples 后台慢慢消化采样队列
func (m *HeatmapManager) asyncProcessSamples() {
	for {
		select {
		case key := <-m.sampleCh:
			// 从通道中拿出 Key，在这里执行你原来笨重的同步操作
			if m.MotherTree != nil && m.MotherTree.Root != nil {
				// 假设全是写操作 (isRead = false)
				m.MotherTree.Root.SearchLeaf(key, false, m)
			}
		case <-m.stopCh:
			// 收到关闭信号，退出协程
			return
		}
	}
}

// 优雅关闭
func (m *HeatmapManager) Close() {
	close(m.stopCh)
}

// NOTE:核心逻辑：向蓄水池添加样本，现在下面这个是针对全局的，注意调用下面这个函数需要先找到目标叶子节点，并且确保当前Key在目标叶子节点的边界内（左闭右开）。
// keySuffix: 已经剥离了当前节点 Prefix 的后缀部分
// 【重构】：增加 manager 参数，以直接定位底层物理内存
func (n *HeatNode) AddSample(keySuffix Key, isRead bool, m *HeatmapManager) {
	if isRead {
		return
	}

	n.Lock()
	defer n.Unlock()

	// 💥 防御机制：如果当前节点刚刚被父节点的后台减枝操作回收了，直接丢弃本次采样！
	if !n.IsLeaf || n.StatsID == -1 {
		return
	}

	// 【核心改变】：通过 StatsID 从连续内存池中获取真实数据！
	stats := m.getStats(n.StatsID)

	stats.WriteCount++
	stats.CurrentHLL.Insert(keySuffix)                // 喂给HLL,来算覆写率
	if len(stats.WSuffixReservoir) < n.ReservoirCap { // 这里修复了你原代码中小 bug，写入应判断 WReservoirCap
		k := make(Key, len(keySuffix))
		copy(k, keySuffix)
		stats.WSuffixReservoir = append(stats.WSuffixReservoir, k)
		return
	}

	limit := uint32(stats.WriteCount)
	r := fastrand.Uint32n(limit)

	if r < uint32(n.ReservoirCap) {
		k := make(Key, len(keySuffix))
		copy(k, keySuffix)
		stats.WSuffixReservoir[r] = k
	}
	if stats.WriteCount > uint32(n.SplitThreshold) && len(stats.WSuffixReservoir) >= n.ReservoirCap {
		if stats.OverwriteRation >= 0.85 {
			// 如果覆写率已经极高,那么就暂停分裂,且
			n.SplitThreshold *= 2
			return
		}
		n.Evolve(m) // 传入 manager
	}
	return
}

// Evolve 是核心分裂方法
func (n *HeatNode) Evolve(m *HeatmapManager) {
	// 获取自身的物理数据
	stats := m.getStats(n.StatsID)

	// 先对蓄水池中的Key后缀进行排序
	sort.Slice(stats.WSuffixReservoir, func(i, j int) bool {
		return bytes.Compare(stats.WSuffixReservoir[i], stats.WSuffixReservoir[j]) < 0
	})

	PrefixGroups := FindTopNPrefixGroups(stats.WSuffixReservoir)

	sort.Slice(PrefixGroups, func(i, j int) bool {
		return PrefixGroups[i].Start < PrefixGroups[j].Start
	})

	// 将 manager 传递给 splitReservoir，以便为子节点分配新的 Stats 内存
	childrenNodes, splitKeys := n.splitReservoir(PrefixGroups, m)
	if len(childrenNodes) > 0 {
		n.IsLeaf = false
		n.Children = childrenNodes
		n.SplitRangeKey = splitKeys
		// 【回收】：当前节点已经变成普通路由节点，它不再需要保留笨重的统计数据了！
		// 释放它占用的内存池槽位，供给未来新的叶子节点使用！
		m.FreeStats(n.StatsID)
		n.StatsID = -1
	} else {
		fmt.Print("出错了，子节点的个数为0？？？")
		// 不要只是打印！如果是极其倾斜的数据导致无法分裂，我们采用“指数退避”：
		// 直接把阈值翻倍，等攒够更多、更杂的数据再来尝试分裂！
		n.SplitThreshold *= 2
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
	WSuffixReservoir := stats.WSuffixReservoir

	nextLevel := node.Level + 1
	wRangeRation := 0.0
	fatherOverwriteRation := 0.0 // 存的是父节点当前的覆写率
	lastWRangeIndex := 0
	currentWRangeIndex := 0
	rangeWriteCount := float64(stats.WriteCount)
	// 计算出下一层子节点的样本池容量
	childCap := MaxReservoirCap >> nextLevel
	if childCap < MinReservoirCap {
		childCap = MinReservoirCap
	}

	// 下面这个if先计算出当前父节点的覆写率
	windowWrites := stats.WriteCount - stats.WindowStartWrite // 算出本周期真实的写入量
	if windowWrites >= uint32(MinReservoirCap) {
		uniqueKeys := float64(stats.CurrentHLL.Estimate())
		fatherCurentOverwriteRation := 1.0 - (uniqueKeys / float64(windowWrites))
		fatherOverwriteRation = fatherCurentOverwriteRation*CurrentEpochWeight + stats.OverwriteRation*(1.0-CurrentEpochWeight) // 计算出当前父节点的覆写率
		// 随着每衰退一轮,就计算一次覆写率,计算的同时清空HLL
	} else {
		// 样本不足（或者刚好被清空过，或者一条没写），无条件信任历史稳态分数！
		fatherOverwriteRation = stats.OverwriteRation
	}
	if fatherOverwriteRation < 0 {
		fatherOverwriteRation = 0
	}
	childBaseThreshold := uint32(1 << (nextLevel + 10))
	// 遍历选中的区间（每一轮最多可以增加3个子节点【头部间隙节点(仅第一个元素会加)、当前具有公共前缀的区间节点、尾部间隙节点】）
	for i, oneRange := range PrefixGroups {
		if i == 0 {
			// NOTE:如果是第一个元素,需要加入头部间隙
			wRangeRation, currentWRangeIndex = CalculateRangeRatio(lastWRangeIndex, oneRange.CommonPrefix, WSuffixReservoir)
			frontRangeStart := Key{} // 如果父节点有公共前缀，那么父节点的起始节点就应该等于公共前缀，所以下一层头部间隙就应该为空
			if len(node.PathSegment) == 0 {
				// 如果父节点无公共前缀，那么下一层头部间隙就应该等于父节点的开始
				frontRangeStart = node.RangeStart
			}

			// 【重构核心】：为新的间隙节点向大内存池申请一块空间
			frontChildStatsID := m.AllocateStats(childCap)
			frontChildStats := m.getStats(frontChildStatsID)
			inheritedWrites := uint32(rangeWriteCount * wRangeRation)
			// 🛡️ 防分裂雪崩：强制把继承下来的写入量卡在子节点阈值的 50% 以下！
			// 这样子节点出生后，至少还要再吃满一半的阈值，才会被允许再次分裂。
			safeMaxWrites := childBaseThreshold / 2
			if inheritedWrites > safeMaxWrites {
				inheritedWrites = safeMaxWrites
			}
			frontChildStats.WriteCount = inheritedWrites
			frontChildStats.EpochStartWrite = frontChildStats.WriteCount
			frontChildStats.WindowStartWrite = frontChildStats.WriteCount
			if frontChildStats.WriteCount > 0 {
				frontChildStats.OverwriteRation = fatherOverwriteRation
			}

			// 继承并拷贝对应的蓄水池数据 (因为是间隙节点，无公共前缀，不需要剥离)
			for _, key := range WSuffixReservoir[lastWRangeIndex:currentWRangeIndex] {
				// 💥 【新增】：容量截断保护，防止继承超过自身物理上限！
				if len(frontChildStats.WSuffixReservoir) >= childCap {
					break
				}
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
		childRangeEnd := NextKeySameLength(oneRange.CommonPrefix) // 将前缀+1设置为结尾
		wRangeRation, currentWRangeIndex = CalculateRangeRatio(lastWRangeIndex, childRangeEnd, WSuffixReservoir)

		// 【重构核心】：为当前具有公共前缀的有效区间申请空间
		childStatsID := m.AllocateStats(childCap)
		childStats := m.getStats(childStatsID)
		inheritedWrites := uint32(rangeWriteCount * wRangeRation)
		// 🛡️ 防分裂雪崩：强制把继承下来的写入量卡在子节点阈值的 50% 以下！
		// 这样子节点出生后，至少还要再吃满一半的阈值，才会被允许再次分裂。
		safeMaxWrites := childBaseThreshold / 2
		if inheritedWrites > safeMaxWrites {
			inheritedWrites = safeMaxWrites
		}
		childStats.WriteCount = inheritedWrites
		childStats.EpochStartWrite = childStats.WriteCount
		childStats.WindowStartWrite = childStats.WriteCount
		if childStats.WriteCount > 0 {
			childStats.OverwriteRation = fatherOverwriteRation
		}

		// 重点：剥离前缀！(oneRange.CommonPrefix)
		pathSegLen := len(oneRange.CommonPrefix)
		for _, key := range WSuffixReservoir[lastWRangeIndex:currentWRangeIndex] {
			// 💥 【新增】：容量截断保护，防止继承超过自身物理上限！
			if len(childStats.WSuffixReservoir) >= childCap {
				break
			}
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
		if i == len(PrefixGroups)-1 {
			// 如果是最后一个区间
			afterChildRangeEnd = Key{} // NOTE:如果父节点有公共前缀，那么子节点的尾部间隙同头部间隙一样，均设值为空！！
			if len(node.PathSegment) == 0 {
				// 如果父节点无公共前缀，那么下一层尾部间隙就应该等于父节点的结尾
				afterChildRangeEnd = node.RangeEnd
			}
		} else {
			// 如果是普通的区间之间间隙
			afterChildRangeEnd = PrefixGroups[i+1].CommonPrefix
		}
		// 如果当前区间的尾部和间隙的尾部相同，那么就不用加间隙了！
		if CompareKey(afterChildRangeEnd, childRangeEnd) != 0 {
			// 加尾部间隙
			wRangeRation, currentWRangeIndex = CalculateRangeRatio(lastWRangeIndex, afterChildRangeEnd, WSuffixReservoir)

			// 申请尾部间隙空间
			afterChildStatsID := m.AllocateStats(childCap)
			afterChildStats := m.getStats(afterChildStatsID)
			inheritedWrites := uint32(rangeWriteCount * wRangeRation)
			// 🛡️ 防分裂雪崩：强制把继承下来的写入量卡在子节点阈值的 50% 以下！
			// 这样子节点出生后，至少还要再吃满一半的阈值，才会被允许再次分裂。
			safeMaxWrites := childBaseThreshold / 2
			if inheritedWrites > safeMaxWrites {
				inheritedWrites = safeMaxWrites
			}
			afterChildStats.WriteCount = inheritedWrites
			afterChildStats.EpochStartWrite = afterChildStats.WriteCount
			afterChildStats.WindowStartWrite = afterChildStats.WriteCount
			if afterChildStats.WriteCount > 0 {
				afterChildStats.OverwriteRation = fatherOverwriteRation
			}

			for _, key := range WSuffixReservoir[lastWRangeIndex:currentWRangeIndex] {
				if len(afterChildStats.WSuffixReservoir) >= childCap {
					break
				}
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

// 💥 【新增】：用于快照排位赛的候选人结构
type zoneCandidate struct {
	Start  Key
	End    Key
	Ratio  float64
	Volume uint64 // 记录该区间当前的物理体积（通过 HLL 的 Unique Keys 估算）
}

// 下面是触发衰减以及更新高覆写率的视图
func (m *HeatmapManager) EpochDecayAndSnapshot() {
	var newZones []HotZone
	var candidates []zoneCandidate
	var globalEpochVolume uint64 = 0 // 💥 记录本周期的全局总物理体积
	m.evolutionLock.Lock()
	defer m.evolutionLock.Unlock()

	// 💥 修复核心：增加 currentPrefix, parentAbsStart, parentAbsEnd 参数
	var traverse func(node *HeatNode, currentPrefix Key, parentAbsStart Key, parentAbsEnd Key)
	traverse = func(node *HeatNode, currentPrefix Key, parentAbsStart Key, parentAbsEnd Key) {
		if node == nil {
			return
		}

		node.Lock()
		isLeaf := node.IsLeaf
		statsID := node.StatsID

		// =========================================================
		// 🛡️ 绝对路径还原引擎：通过时空上下文，还原出该节点真实的物理边界
		// =========================================================
		var absStart, absEnd Key

		// 只有当前是叶子节点，且需要参与排位赛时，我们才真正分配内存 (深拷贝)！
		// 路由节点只需要传递逻辑视图，极大地削减了 90% 的 GC 内存分配！
		buildKey := func(prefix, suffix Key) Key {
			res := make(Key, 0, len(prefix)+len(suffix))
			res = append(res, prefix...)
			res = append(res, suffix...)
			return res
		}
		if len(node.RangeStart) > 0 {
			// 如果是叶子，分配真实内存；如果不是，暂存闭包延后分配
			if isLeaf {
				absStart = buildKey(currentPrefix, node.RangeStart)
			}
		} else {
			absStart = parentAbsStart
		}
		if len(node.RangeEnd) > 0 {
			if isLeaf {
				absEnd = buildKey(currentPrefix, node.RangeEnd)
			}
		} else {
			absEnd = parentAbsEnd
		}

		// nextPrefix 必须分配，因为要向下传递给所有子节点
		nextPrefix := buildKey(currentPrefix, node.PathSegment)
		// =========================================================

		var childrenCopy []*HeatNode
		if !isLeaf {
			childrenCopy = make([]*HeatNode, len(node.Children))
			copy(childrenCopy, node.Children)
		}

		if isLeaf && statsID != -1 {
			stats := m.getStats(node.StatsID)
			windowWrites := stats.WriteCount - stats.WindowStartWrite
			epochWrites := stats.WriteCount - stats.EpochStartWrite
			stats.EpochStartWrite = stats.WriteCount

			// 获取动态自适应结算门槛 (必须攒够节点寿命的 1/4)
			minEpochWrites := uint32(node.SplitThreshold / 4)
			if minEpochWrites < uint32(MinReservoirCap) {
				minEpochWrites = uint32(MinReservoirCap)
			}

			currentUniqueKeys := float64(stats.CurrentHLL.Estimate())
			volume := uint64(currentUniqueKeys)
			// NOTE:累加当前节点的物理体积到全局总盘子
			globalEpochVolume += volume
			if epochWrites > 0 {
				// NOTE:置信度闸门 (Confidence Gate)
				// 只有当新窗口积累了足够的底线样本（MinReservoirCap），算出的暂态覆写率才有意义。
				// 如果样本太少，直接跳过 EWMA 计算，完美信任它的历史分数，绝不让冷启动引发暴跌！
				if windowWrites >= uint32(MinReservoirCap) {
					currentRatio := 1.0 - (currentUniqueKeys / float64(windowWrites))
					stats.OverwriteRation = currentRatio*CurrentEpochWeight + stats.OverwriteRation*(1.0-CurrentEpochWeight)

					if stats.OverwriteRation < 0 {
						stats.OverwriteRation = 0
					}
				}
				// NOTE:只有真正攒够了样本，才允许清空 HLL 开启下次统计！
				if windowWrites >= minEpochWrites {
					stats.WindowStartWrite = stats.WriteCount
					stats.CurrentHLL = hyperloglog.New14()
				}
			} else {
				// 没有写入,只需要衰减
				stats.OverwriteRation = 0.0*CurrentEpochWeight + stats.OverwriteRation*(1.0-CurrentEpochWeight)
				// 💥 幽灵体积回收机制 (Ghost GC)
				// 既然彻底没人写了，如果它的分数已经跌到完全不可能进热区了（比如跌破 0.05），
				// 强制清空它的 HLL！防止它这辈子永远用旧数据污染 globalEpochVolume！
				if stats.OverwriteRation < 0.05 {
					stats.OverwriteRation = 0
					stats.WindowStartWrite = stats.WriteCount
					stats.CurrentHLL = hyperloglog.New14()
				}
			}

			// NOTE:底线过滤：0.3 以下直接淘汰，0.3 及以上的进入排位海选
			if stats.OverwriteRation >= MinHotRatioFloor {
				candidates = append(candidates, zoneCandidate{
					Start:  absStart,
					End:    absEnd,
					Ratio:  stats.OverwriteRation,
					Volume: volume,
				})
			}
		}
		node.Unlock()

		for _, child := range childrenCopy {
			// 将算好的绝对边界作为“父边界”传给子节点
			traverse(child, nextPrefix, absStart, absEnd)
		}
		// =========================================================
		// 💥【新增：后序结构减枝 (Structure Pruning)】💥
		// 子节点全部衰减完毕，向上回溯时，检查当前路由节点能否“折叠”
		// =========================================================
		if !isLeaf {
			node.Lock() // 重新锁住自己，准备可能的手术

			allChildrenAreLeaves := true
			var totalRatio float64 = 0.0
			var totalEpochWrites uint32 = 0

			// 检查所有子节点的状态
			for _, child := range node.Children {
				child.RLock()
				if !child.IsLeaf {
					allChildrenAreLeaves = false
					child.RUnlock()
					break // 只要有一个子节点还是路由节点，当前节点就不能折叠
				}
				if child.StatsID != -1 {
					childStats := m.getStats(child.StatsID)
					// 累加子节点的覆写率和本周期的绝对写入量
					totalRatio += childStats.OverwriteRation
					totalEpochWrites += (childStats.WriteCount - childStats.EpochStartWrite)
				}
				child.RUnlock()
			}

			// 🔪 触发减枝的条件：
			// 1. 所有子节点都已经是叶子了（一层一层从底向上折叠，防止暴力塌缩）
			// 2. 子节点的总体覆写率极低（比如加起来都不到 0.1）
			// 3. 绝对写入量也极低（确实没人写了，彻底冷透）
			if allChildrenAreLeaves && totalRatio < 0.1 && totalEpochWrites < 100 {

				// 1. 清理门户：把所有子节点占用的物理内存槽位全部释放，送入回收站！
				for _, child := range node.Children {
					child.Lock() // 锁住子节点，防止与并发的 AddSample 冲突！
					if child.StatsID != -1 {
						m.FreeStats(child.StatsID)
						child.StatsID = -1
					}
					child.Unlock()
				}

				// 2. 结构塌缩：丢弃所有孩子和分割键，降级为叶子节点
				node.IsLeaf = true
				node.Children = nil
				node.SplitRangeKey = nil

				// 3. 灵魂重铸：既然变成了叶子，就要为它分配一个新的统计内存池
				node.StatsID = m.AllocateStats(node.ReservoirCap)
				newStats := m.getStats(node.StatsID)

				// 继承一点残余的热度（取平均值或直接归零都可以，这里安全起见赋 0）
				newStats.OverwriteRation = 0.0

				// 可选：打个日志看看减枝效果
				// fmt.Printf("🔪 [减枝触发] 塌缩节点 Lv%d, 前缀: %s, 释放了 %d 个空洞\n", node.Level, string(node.PathSegment), len(childrenCopy))
			}
			node.Unlock()
		}
	}

	// 发起一体化遍历，根节点的前缀为空，边界为绝对的全域 [-∞, +∞)
	if m.MotherTree != nil && m.MotherTree.Root != nil {
		traverse(m.MotherTree.Root, Key{}, Key{}, Key(nil))
	}
	// 排位赛:带 VVIP 特权的弹性配额截断算法
	// A. 计算本次衰减周期的常规物理配额 (例如总盘子的 20%)
	targetQuota := uint64(float64(globalEpochVolume) * HotVolumePercentage)
	var currentAdmittedVolume uint64 = 0

	// B. 按覆写率从大到小降序排名 (最热的排前面抢名额)
	sort.Slice(candidates, func(i, j int) bool {
		return candidates[i].Ratio > candidates[j].Ratio
	})

	// C. 依次发放入场券，执行 VVIP 特权逻辑
	for _, c := range candidates {
		if currentAdmittedVolume+c.Volume > targetQuota {
			// 常规配额已满！开启 VVIP 检查
			if c.Ratio < HighHeatBypassRatio {
				// 配额已满，且热度不足以触发特权，果断关门！
				break
			}
			// 💥 触发特权：虽然配额满了，但覆写率极其爆表，必须强行加座！
		}

		newZones = append(newZones, HotZone{
			Start: c.Start,
			End:   c.End,
		})
		currentAdmittedVolume += c.Volume
	}

	// 将收集到的热区排序（二分查找的先决条件）,供给底层 IsHotKey 的二分查找使用
	sort.Slice(newZones, func(i, j int) bool {
		return bytes.Compare(newZones[i].Start, newZones[j].Start) < 0
	})

	// RCU 原子替换
	m.hotZonesSnapshot.Store(newZones)
}

// OnMemtableFlush 是负载驱动的入口
func (m *HeatmapManager) OnMemtableFlush() {
	// 1. 原子增加计数
	count := atomic.AddInt32(&m.flushCount, 1)

	// 2. 检查是否达到阈值
	if count >= m.flushThreshold {
		// 重置计数器
		atomic.StoreInt32(&m.flushCount, 0)

		// 3. 🛡️ 终极防御：利用 CAS 实现 TryLock，保证全局只会有一个协程在做衰减！
		// 只有当 isDecaying 是 0 时，才能把它变成 1，并返回 true。否则返回 false。
		if atomic.CompareAndSwapInt32(&m.isDecaying, 0, 1) {
			go func() {
				// 协程结束时，务必将状态重置为 0，允许下一次触发
				defer atomic.StoreInt32(&m.isDecaying, 0)

				m.EpochDecayAndSnapshot()
			}()
		} else {
			// 如果走到这里，说明上一个衰减还没跑完，本次触发被平滑丢弃，保护 CPU！
			// fmt.Println("[heatLSM] 衰减太频繁，本次触发已丢弃")
		}
	}
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
		idx := sort.Search(len(current.SplitRangeKey), func(i int) bool {
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

func (hm *HeatmapManager) IsHotKey(key Key) bool {
	// 1. 无锁获取当前最新的热区快照 (极速)
	zones := hm.hotZonesSnapshot.Load().([]HotZone)
	if len(zones) == 0 {
		return false
	}
	// 2. 二分查找：寻找第一个 End > key 的区间
	idx := sort.Search(len(zones), func(i int) bool {
		// End 为 nil 或长度为 0 代表正无穷大
		if len(zones[i].End) == 0 {
			return true
		}
		return bytes.Compare(zones[i].End, key) > 0
	})

	// 3. 校验该区间是否真的包含这个 key
	if idx < len(zones) {
		start := zones[idx].Start
		// 既然 End > key 已经满足，只要 key >= Start，就说明命中了！
		if len(start) == 0 || bytes.Compare(key, start) >= 0 {
			return true
		}
	}

	return false
}

// =========================================================================
// 宏观统计与可视化打印模块
// =========================================================================
// =========================================================================
// 内存精确计算模块 (用于论文数据的极致严谨性)
// =========================================================================

// CalculateTreeMemory 递归计算整棵路由树的近似内存占用（单位：字节）
func (n *HeatNode) CalculateTreeMemory() int64 {
	if n == nil {
		return 0
	}

	// 1. 结构体本身的基础大小
	// (64位机器下，Slice头24字节, int64/指针8字节, RWMutex24字节。
	// 新增了 SplitEpoch 后，通过内存对齐，单节点基础大小约为 192 字节)
	var size int64 = 192

	// 2. 累加 PathSegment 和边界 Key 的底层真实字节大小
	size += int64(len(n.PathSegment))
	size += int64(len(n.RangeStart))
	size += int64(len(n.RangeEnd))

	// 3. 累加 SplitRangeKey 数组的大小
	size += int64(cap(n.SplitRangeKey) * 24) // 切片头开销
	for _, key := range n.SplitRangeKey {
		size += int64(len(key))
	}

	// 4. 累加子节点指针数组开销及递归
	size += int64(cap(n.Children) * 8)
	for _, child := range n.Children {
		size += child.CalculateTreeMemory()
	}

	return size
}

// CalculatePoolMemory 精确计算集中式内存池 (RegionStats) 占据的物理内存大小
func (m *HeatmapManager) CalculatePoolMemory() int64 {
	var size int64 = 0

	if m == nil {
		return 0
	}

	// 1. 外层切片 StatsChunks 本身的开销
	size += 24 // 切片头
	size += int64(cap(m.StatsChunks) * 24)

	// 2. 遍历每一个 Chunk 计算内存
	for _, chunk := range m.StatsChunks {
		// 🚀 瘦身后的 RegionStats 结构体预估：
		// IsActive(1) + 填充(3) + WriteCount(4) + WindowStartWrite(4) + EpochStartWrite(4)
		// + OverwriteRatio(8) + CurrentHLL指针(8) + WSuffixReservoir切片头(24) = 56 字节。
		// Go 底层按 8 字节对齐，完美占据 64 字节 (刚好一条 CPU Cache Line)！
		size += int64(cap(chunk) * 64)

		// 3. 深入当前 Chunk 的每一个槽位，计算动态蓄水池的开销
		for i := range chunk {
			stats := &chunk[i]
			// --- 写蓄水池真实堆内存 ---
			size += int64(cap(stats.WSuffixReservoir) * 24)
			for _, key := range stats.WSuffixReservoir {
				size += int64(len(key))
			}
			// 注：Sparse HLL 极轻量，这里暂不计算其偶尔膨胀为 Dense 时的 16KB，
			// 因为绝大多数冷节点都是 0 分配，如果追求极度严谨，你可以在未来给 HLL 加个 Size() 方法
		}
	}

	// 4. 计算回收站 (FreeList) 的开销
	size += 24                         // FreeList 切片头
	size += int64(cap(m.FreeList) * 4) // int32 是 4 字节

	return size
}

// CalculateTotalMemory 宏观统计：灵魂(树) + 肉体(池) 的总内存大小
func (m *HeatmapManager) CalculateTotalMemory() int64 {
	var treeMem int64 = 0
	if m.MotherTree != nil && m.MotherTree.Root != nil {
		treeMem = m.MotherTree.Root.CalculateTreeMemory()
	}

	poolMem := m.CalculatePoolMemory()
	return treeMem + poolMem
}

// PrintTree 递归美化打印树节点及其覆写率 (核心可视化逻辑)
// isLast 用于控制漂亮的树形连接线符号 (├── vs └──)
func PrintTree(node *HeatNode, prefix string, isLast bool, m *HeatmapManager) {
	if node == nil {
		return
	}

	// 优化边界 Key 的显示：空字节数组在逻辑上代表负无穷或正无穷
	startStr := string(node.RangeStart)
	if len(node.RangeStart) == 0 {
		startStr = "-∞"
	}
	endStr := string(node.RangeEnd)
	if len(node.RangeEnd) == 0 {
		endStr = "+∞"
	}
	pathStr := string(node.PathSegment)
	if len(node.PathSegment) == 0 {
		pathStr = "ROOT"
	}

	// 确定树形分支的符号
	marker := "├──"
	if isLast {
		marker = "└──"
	}

	// 核心输出：分别处理叶子节点和路由节点
	if node.IsLeaf {
		if node.StatsID != -1 {
			// 🚀 灵魂与肉体结合：去内存池里把真实的统计数据捞出来
			stats := m.getStats(node.StatsID)

			// 打印极其重要的覆写率和绝对写入量！
			fmt.Printf("%s%s [Lv%d 叶子] 范围:[%s, %s) | 前缀:'%s' | 💥写次数:%d | 🔥覆写率: %.4f\n",
				prefix, marker, node.Level, startStr, endStr, pathStr, stats.WriteCount, stats.OverwriteRation)
		} else {
			// 防御性输出（理论上不会发生）
			fmt.Printf("%s%s [Lv%d 叶子] 范围:[%s, %s) | 前缀:'%s' | ❌ 丢失StatsID\n",
				prefix, marker, node.Level, startStr, endStr, pathStr)
		}
	} else {
		// 路由节点不需要去查内存池，它只负责切分空间
		fmt.Printf("%s%s [Lv%d 路由] 范围:[%s, %s) | 前缀:'%s' | 🌿子节点数:%d\n",
			prefix, marker, node.Level, startStr, endStr, pathStr, len(node.Children))

		// 准备递归遍历子节点的缩进前缀
		newPrefix := prefix
		if isLast {
			newPrefix += "    " // 如果父节点是最后一个，子节点前缀为空白
		} else {
			newPrefix += "│   " // 否则需要一条向下的垂线
		}

		// 递归遍历所有子节点
		for i, child := range node.Children {
			isChildLast := (i == len(node.Children)-1)
			PrintTree(child, newPrefix, isChildLast, m)
		}
	}
}

// Print 打印整棵母树的热力分布与内存池状态
func (m *HeatmapManager) Print() {
	fmt.Println("\n================ 🌳 heatLSM 热力树拓扑与覆写率全景图 🌳 ================")
	if m.MotherTree != nil && m.MotherTree.Root != nil {
		// 从根节点开始递归打印，初始前缀为空，且根节点作为其所在层级的“最后一个节点”
		PrintTree(m.MotherTree.Root, "", true, m)
	}
	fmt.Println("========================================================================")

	activeCount := 0
	for _, chunk := range m.StatsChunks {
		for _, stat := range chunk {
			if stat.IsActive {
				activeCount++
			}
		}
	}
	fmt.Printf("📊 [系统内存雷达] 活跃区块数: %d, 回收站空洞数: %d\n", activeCount, len(m.FreeList))
	fmt.Printf("💾 [总物理内存开销] %s\n\n", FormatBytes(m.CalculateTotalMemory()))
}
