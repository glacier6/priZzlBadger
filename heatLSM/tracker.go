package heatLSM

import (
	"fmt"
	"sort"
	"sync"
)

// AccessStats 记录单个 Key 的读写频次
type AccessStats struct {
	ReadCount  uint64
	WriteCount uint64
}

// Total 返回总访问次数
func (s *AccessStats) Total() uint64 {
	return s.ReadCount + s.WriteCount
}

// GlobalKeyTracker 用于精确记录 Key 频次（读写分离）
type GlobalKeyTracker struct {
	mu sync.RWMutex
	// value 改为结构体指针，方便直接修改内部字段
	counts map[string]*AccessStats
}

// 初始化 Tracker
func NewGlobalKeyTracker() *GlobalKeyTracker {
	return &GlobalKeyTracker{
		counts: make(map[string]*AccessStats),
	}
}

// 内部辅助方法：获取或初始化 Stats
func (kt *GlobalKeyTracker) getOrInit(key string) *AccessStats {
	if stats, exists := kt.counts[key]; exists {
		return stats
	}
	stats := &AccessStats{}
	kt.counts[key] = stats
	return stats
}

// RecordRead 记录一次读访问
func (kt *GlobalKeyTracker) RecordRead(key []byte) {
	kt.mu.Lock()
	defer kt.mu.Unlock()
	kt.getOrInit(string(key)).ReadCount++
}

// RecordWrite 记录一次写访问
func (kt *GlobalKeyTracker) RecordWrite(key []byte) {
	kt.mu.Lock()
	defer kt.mu.Unlock()
	kt.getOrInit(string(key)).WriteCount++
}

// PrintTopK 生成并打印读写分离榜单
func (kt *GlobalKeyTracker) PrintTopK(k int) {
	kt.mu.RLock()
	defer kt.mu.RUnlock()

	// 1. 将 map 数据转移到 slice 中以便进行排序
	type kv struct {
		Key   string
		Stats *AccessStats
	}
	var ss []kv
	for keyName, stats := range kt.counts {
		ss = append(ss, kv{keyName, stats})
	}

	// 2. 按照【总访问次数】降序排序
	sort.Slice(ss, func(i, j int) bool {
		return ss[i].Stats.Total() > ss[j].Stats.Total()
	})

	// 3. 打印格式化榜单
	totalKeys := len(ss)
	fmt.Println("\n================ 绝对精确 Key 访问频次榜单 (读写分离) ================")

	displayCount := k
	if totalKeys < k {
		displayCount = totalKeys
	}

	fmt.Printf("共有 %d 个独立的 Key, 仅展示 Top %d:\n", totalKeys, displayCount)

	for i := 0; i < displayCount; i++ {
		stats := ss[i].Stats
		// 格式化输出，加入读写明细
		// %-6d 保证数字对齐美观
		fmt.Printf("Top %03d | Key: %-25s | 总计: %-6d (读: %-6d, 写: %d)\n",
			i+1, ss[i].Key, stats.Total(), stats.ReadCount, stats.WriteCount)
	}
}
