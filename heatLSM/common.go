package heatlsm

// 辅助函数：计算两个 Key 的最长公共前缀长度
func commonPrefixLen(a, b Key) int {
	i := 0
	for i < len(a) && i < len(b) && a[i] == b[i] {
		i++
	}
	return i
}
