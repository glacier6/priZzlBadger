package heatlsm

// NOTE:怎么具体分裂？
// （1）在已排序的蓄水池上先找到前缀不同的候补分裂点（如111,121,211，269,359的候补分裂点应该是index为2,4之前的位置），然后尽可能均分的找到 MaxValidRange-1（最多） 个分裂点   PS:这是为了尽可能扁平化，加速查找流程
// （2）生成对应个数个子节点，并且对于各有效区域且有公共前缀的在子节点设置上前缀名，即设置上PathSegment
// （3）将子节点挂载到父节点上，并且在父节点的SplitRangeKey中更新分裂点列表
// alternateIndex := []int{}
// lastByte := n.SuffixReservoir[0][0]
// for i := 1; i < ReservoirCap; i++ {
// 	var currByte byte
// 	if len(n.SuffixReservoir[i]) > 0 {
// 		currByte = n.SuffixReservoir[i][0]
// 	}
// 	if currByte != lastByte {
// 		alternateIndex = append(alternateIndex, i)
// 		lastByte = currByte
// 	}
// }
// childrenNodes, splitKeys := n.splitReservoir(n.SuffixReservoir, alternateIndex, MaxValidRange)

// splitReservoir2 将样本数据 data 基于 potentialIndices 进行分割，并构造子节点
// data: 父节点的蓄水池样本（相对 Key）
// potentialIndices: 候选切割点的下标列表(NOTE:是分裂点之后的那个key的下标)
// targetBlocks: 目标最大块数
// func (node *HeatNode) splitReservoir2(data []Key, potentialIndices []int, targetBlocks int) ([]*HeatNode, []Key) {
// 	totalLen := len(data)
// 	var chosenIndices []int // 切割下标

// 	// 1. 边界检查：如果只要1块或没法分，直接返回原数组
// 	if len(potentialIndices) != 0 {
// 		neededCuts := targetBlocks - 1
// 		if len(potentialIndices) <= neededCuts {
// 			// 情况A：提供的切割点不够用，或者刚好够用
// 			chosenIndices = potentialIndices
// 		} else {
// 			// 情况B：提供的切割点绰绰有余，我们需要贪心选择最均匀的点
// 			chosenIndices = make([]int, 0, neededCuts)
// 			step := float64(totalLen) / float64(targetBlocks) // 理想步长
// 			lastIdxInPotentials := -1                         // 记录上一次在 potentialIndices 中选中的下标，防止回头或重复
// 			// 下面开始贪心找切割点
// 			for i := 1; i <= neededCuts; i++ {
// 				// 当前这一刀的理想位置
// 				idealPos := step * float64(i)

// 				// 在 potentialIndices 中二分查找最接近 idealPos 的位置
// 				// SearchInts 返回第一个 >= idealPos 的下标
// 				idx := sort.SearchInts(potentialIndices, int(idealPos))

// 				// 寻找最接近的下标 (idx 还是 idx-1 ?)
// 				bestMatchIdx := -1

// 				// 边界处理
// 				if idx == 0 {
// 					bestMatchIdx = 0
// 				} else if idx == len(potentialIndices) {
// 					bestMatchIdx = len(potentialIndices) - 1
// 				} else {
// 					// 比较 idx 和 idx-1 谁离理想值更近
// 					valAfter := potentialIndices[idx]
// 					valBefore := potentialIndices[idx-1]
// 					if math.Abs(float64(valAfter)-idealPos) < math.Abs(float64(valBefore)-idealPos) {
// 						bestMatchIdx = idx
// 					} else {
// 						bestMatchIdx = idx - 1
// 					}
// 				}

// 				// --- 关键修正逻辑 ---

// 				// 1. 必须大于上一次选中的下标（保证不重复选同一个点，且顺序往后）
// 				if bestMatchIdx <= lastIdxInPotentials {
// 					bestMatchIdx = lastIdxInPotentials + 1
// 				}

// 				// 2. 必须为后面还没切的刀数预留足够的点位
// 				// 还需要切 cutsRemaining 刀
// 				cutsRemaining := neededCuts - i
// 				// 后面还剩多少个候选点
// 				candidatesRemaining := len(potentialIndices) - 1 - bestMatchIdx

// 				// 如果选了这个点，导致后面剩下的候选点不够切了，就必须强行把当前点往前移
// 				if candidatesRemaining < cutsRemaining {
// 					bestMatchIdx = len(potentialIndices) - 1 - cutsRemaining
// 				}

// 				// 选中该点
// 				chosenIndices = append(chosenIndices, potentialIndices[bestMatchIdx])
// 				lastIdxInPotentials = bestMatchIdx
// 			}
// 		}
// 	}

// 	// 5. 根据选中的下标构造子节点
// 	var children []*HeatNode
// 	var splitKeys []Key
// 	start := 0

// 	// 第一个子节点的 Start = 父节点的 Start (逻辑上，如果是相对值则是空)
// 	// 最后一个子节点的 End = 父节点的 End

// 	// RangeStart/End 也是相对于 PathSegment 的。
// 	// NOTE:这里先不分裂出空隙范围，因为没想到有什么意义（因为本质上只是找到高读写比的区域而已，无需分的太细）

// 	// 需要追加一个“虚拟”的结束点 totalLen，方便循环
// 	cutPoints := append(chosenIndices, totalLen)
// 	var commonSeg, nextCommonSeg Key
// 	var chunk, nextChunk []Key
// 	for i, cutPos := range cutPoints {
// 		var childRangeStart, childRangeEnd Key
// 		// 提取分片样本
// 		if cutPos > totalLen {
// 			cutPos = totalLen
// 		}

// 		// 如果不是第一个，就直接用上一轮的结果就好
// 		if i != 0 {
// 			chunk = nextChunk
// 			commonSeg = nextCommonSeg
// 		} else {
// 			// 是第一个，需要自己计算
// 			chunk := data[start:cutPos]
// 			// 计算这一组样本的公共前缀，作为子节点的 PathSegment
// 			commonSeg = calcCommonPrefix(chunk)
// 		}
// 		rangeSize := int64(cutPos - start)

// 		// 先计算出下一个chunk及其公共前缀
// 		if i < len(cutPoints)-1 {
// 			// 获取下一个 Chunk 的第一个元素
// 			nextChunkEndIdx := cutPoints[i+1]
// 			nextChunk = data[cutPos:nextChunkEndIdx]
// 			nextCommonSeg = calcCommonPrefix(nextChunk) //TODO:不能拿下一组的公共前缀当尾部，因为下一组未必会有公共前缀！！

// 			splitKey := make(Key, len(nextCommonSeg))
// 			copy(splitKey, nextCommonSeg)
// 			splitKeys = append(splitKeys, splitKey)
// 		}

// 		// 计算子节点的逻辑范围并在父节点中记录下分裂位置（位置是分裂点后那一个key的公共前缀值，所以说整个范围是一个左闭右开的区间）
// 		if i == 0 {
// 			childRangeStart = node.RangeStart
// 		} else {
// 			childRangeStart = commonSeg
// 		}
// 		if cutPos == totalLen {
// 			childRangeEnd = node.RangeEnd
// 		} else {
// 			childRangeEnd = nextCommonSeg
// 		}

// 		// 构造子节点
// 		// 注意：RangeStart 和 RangeEnd 在这里比较难精确定义，除非我们传递上下文。
// 		// 简单起见，我们暂且置空或设为 nil，因为核心路由靠 SplitRangeKey。
// 		child := newHeatNode(
// 			node.Level+1,
// 			childRangeStart, // RangeStart
// 			childRangeEnd,   // RangeEnd
// 			commonSeg,
// 			true,
// 		)

// 		// 继承热度 (按分裂比例均分)
// 		child.ReadCount = node.ReadCount / (rangeSize / ReservoirCap)
// 		child.WriteCount = node.WriteCount / (rangeSize / ReservoirCap)

// 		// 将蓄水池对应部分迁移进子节点
// 		// 【关键】：必须剥离掉子节点的 PathSegment (commonSeg)
// 		for _, key := range chunk {
// 			if len(key) >= len(commonSeg) {
// 				// 剥离前缀
// 				suffix := key[len(commonSeg):]
// 				// 需要深拷贝吗？AddSample 内部会做深拷贝。
// 				// 但这里的 suffix 是基于 data 的切片，data 是 n.SuffixReservoir。
// 				// 如果 n.SuffixReservoir 被置 nil，底层数组还能用吗？
// 				// Go 的 GC 会管理引用，只要 AddSample 拷贝了就没问题。
// 				child.AddSample(suffix)
// 			}
// 		}

// 		children = append(children, child)
// 		start = cutPos
// 	}

// 	return children, splitKeys
// }
