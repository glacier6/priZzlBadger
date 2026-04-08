package heatLSM

import (
	"bytes"
	"fmt"
	"sort"
)

// 辅助函数：计算两个 Key 的最长公共前缀长度
func commonPrefixLen(a, b Key) int {
	n := len(a)
	if len(b) < n {
		n = len(b)
	}
	for i := 0; i < n; i++ {
		if a[i] != b[i] {
			return i
		}
	}
	return n
}

// FormatBytes 将字节数转换为人类可读的格式 (KB, MB, GB)
func FormatBytes(b int64) string {
	const unit = 1024
	if b < unit {
		return fmt.Sprintf("%d B", b)
	}
	div, exp := int64(unit), 0
	for n := b / unit; n >= unit; n /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.2f %cB", float64(b)/float64(div), "KMGTPE"[exp])
}

// 辅助函数：计算一组 Key 的最长公共前缀
// keys 必须非空
func calcCommonPrefix(keys []Key) Key {
	if len(keys) == 0 {
		return Key{}
	}
	// 初始前缀设为第一个 key
	prefix := keys[0]
	for i := 1; i < len(keys); i++ {
		// 每次比较更新前缀长度
		pl := commonPrefixLen(prefix, keys[i])
		prefix = prefix[:pl]
		if len(prefix) == 0 {
			break
		}
	}
	// 必须深拷贝，因为 keys[0] 可能会被回收或修改
	res := make(Key, len(prefix))
	copy(res, prefix)
	return res
}

// ResIndexRange 代表一个蓄水池下标区间
type ResIndexRange struct {
	Start        int    // 起始下标（包含）
	End          int    // 结束下标（不包含，即 open interval [Start, End)）
	Count        int    // 区间内元素数量
	CommonPrefix []byte // 该区间内所有 Key 的公共前缀（调试用）
}

// 辅助函数：获取 Key 在指定 index 处的字节
// 如果 index 超出 Key 的长度，返回 -1 作为特殊标记（字典序中短字符串排在前面）
func getByteAt(k Key, index int) int16 {
	if index >= len(k) {
		return -1
	}
	return int16(k[index])
}

// FindTopNPrefixGroups 寻找包含 Key 最多的前 n 个公共前缀区间
// keys: 必须是按字典序排好序的
// n: 需要返回的区间个数
func FindTopNPrefixGroups(keys []Key, n int) []ResIndexRange {
	if len(keys) == 0 {
		return nil
	}
	if n <= 0 {
		return nil
	}

	// 1. 计算整个数组的“基准公共前缀”长度
	// 因为是有序的，只需要比较第一个和最后一个即可确定全量的公共前缀
	baseDepth := commonPrefixLen(keys[0], keys[len(keys)-1])

	var groups []ResIndexRange
	startIndex := 0

	// 2. 遍历数组，根据 baseDepth 位置的字节变化来切分区间
	for i := 1; i < len(keys); i++ {
		// 获取当前 Key 和上一个 Key 在 baseDepth 位置的字节
		// 如果 key 长度不够，用 -1 (int16) 表示结束符，保证短 Key 也能被区分
		b1 := getByteAt(keys[i-1], baseDepth)
		b2 := getByteAt(keys[i], baseDepth)

		// 如果字节不同，说明前缀发生了变化，前面的 [startIndex, i) 是一个完整组
		if b1 != b2 {
			groups = append(groups, ResIndexRange{
				Start:        startIndex,
				End:          i,
				Count:        i - startIndex,
				CommonPrefix: calcCommonPrefix(keys[startIndex:i]), // 仅用于展示，可优化去掉
			})
			startIndex = i
		}
	}

	// 3. 不要忘记添加最后一组
	groups = append(groups, ResIndexRange{
		Start:        startIndex,
		End:          len(keys),
		Count:        len(keys) - startIndex,
		CommonPrefix: calcCommonPrefix(keys[startIndex:]),
	})

	// 4. 按区间内包含的 Key 数量降序排序 (热度排序)
	sort.Slice(groups, func(i, j int) bool {
		return groups[i].Count > groups[j].Count
	})

	// 5. 如果分组不足 n 个，直接返回所有；否则返回前 n 个
	if len(groups) > n {
		return groups[:n]
	}
	return groups
}

// CalculateRangeRatio 计算区间 [lastSplitKey（下标为lastSplitIndex）, currentSplitKey) 在写入蓄水池中占据的比例
// 注意：传入的 reservoir 必须是已经按字典序排序过的！
// 返回值：[0.0, 1.0] 之间的浮点数
func CalculateRangeRatio(lastSplitIndex int, currentSplitKey Key, reservoir []Key) (float64, int) {
	totalLen := len(reservoir)

	// 防御性检查：如果蓄水池为空（比如极其罕见的纯读/纯写极端情况导致的空池），直接返回 0
	if totalLen == 0 {
		return 0.0, 0
	}

	// 2. 寻找结束点 endIndex (第一个 >= currentSplitKey 的元素位置)
	endIndex := totalLen
	if len(currentSplitKey) > 0 { // 如果 currentSplitKey 为空或 nil，说明是绝对尾部，直接取 totalLen
		endIndex = sort.Search(totalLen, func(i int) bool {
			return bytes.Compare(reservoir[i], currentSplitKey) >= 0
		})
	}

	// 防御性检查：如果区间非法（比如传参反了），返回 0
	if lastSplitIndex > endIndex {
		return 0.0, endIndex
	}

	// 3. 计算区间内元素个数并求比例
	// 因为 endIndex 是第一个 >= currentSplitKey 的元素，而区间是左闭右开，
	// 所以区间内的元素正好是 reservoir[startIndex:endIndex]，个数就是 endIndex - startIndex
	count := endIndex - lastSplitIndex

	return float64(count) / float64(totalLen), endIndex
}

func MergeKey(k1, k2 Key) Key {
	// 1. 预分配：一次性申请好所有需要的内存
	// len=0, cap=len(k1)+len(k2)
	result := make([]byte, 0, len(k1)+len(k2))

	// 2. 追加
	result = append(result, k1...)
	result = append(result, k2...)

	return result
}

// CompareKey 比较两个 Key 的字典序大小
// 返回值：
//
//	-1 : a < b
//	 0 : a == b
//	+1 : a > b
func CompareKey(a, b Key) int {
	return bytes.Compare(a, b)
}

// NextKeySameLength 返回在字典序上比输入 key 大 1 的 Key。
// 约束：返回的 Key 长度与输入 Key 严格一致。
// 注意：如果输入是全 0xFF (例如 [255, 255])，加 1 后会发生溢出回绕变成全 0x00。
func NextKeySameLength(key Key) Key {
	// 1. 深拷贝：创建一个新数组，避免修改传入的原始 key
	// 在 LSM-Tree 实现中，Key 的内存管理非常重要，必须 Copy
	newKey := make(Key, len(key))
	copy(newKey, key)

	// 2. 从最后一位开始向前遍历
	for i := len(newKey) - 1; i >= 0; i-- {
		// 当前位加 1
		newKey[i]++

		// 3. 检查是否发生进位
		if newKey[i] != 0 {
			// 如果当前位加 1 后不等于 0 (说明没有从 255 溢出变成 0)
			// 则进位结束，直接返回结果
			return newKey
		}

		// 如果 newKey[i] 变成了 0，说明之前是 255 (0xFF)，加 1 后溢出。
		// 逻辑上需要向高位 (i-1) 进位，所以循环继续...
	}

	// 4. 处理全量溢出情况 TODO:处理一下
	// 如果代码运行到这里，说明每一位都产生了进位 (例如输入是 FF FF FF)。
	// 此时 newKey 已经变成了 00 00 00。
	// 根据你的要求“不允许增加字节”，这里只能返回全 0。
	// 此时 newKey 在字典序上其实是变小了（回到了起点）。
	return newKey
}
