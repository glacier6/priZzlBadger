package heatlsm

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
