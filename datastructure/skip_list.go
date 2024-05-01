package datastructure

import (
	"math/rand"
)

const (
	// DefaultMaxLevel 默认最大索引层数
	SkipListMaxLevel = 64
	// MinLevel 自定义层数时最小的层数
	SkipListMinLevel = 16
)

// SkipListFindRange 根据scores查找元素的条件
type SkipListFindRange struct {
	Min, Max       float64 //最大值和最小值
	MinBra, MaxBra bool
	MinInf, MaxInf bool //是否为正无穷和负无穷
}

type SkipList struct {
	head, tail *SkipListNode
	size       int64
	level      int
	maxLevel   int
}

func NewDefaultSkipList() *SkipList {
	return &SkipList{
		head:     NewSkipListNode(SkipListMaxLevel, 0, nil),
		size:     0,
		level:    1,
		maxLevel: SkipListMaxLevel,
	}
}

func NewSkipList(maxLevel int) *SkipList {
	if maxLevel < SkipListMinLevel {
		maxLevel = SkipListMinLevel
	}
	if maxLevel > SkipListMaxLevel {
		maxLevel = SkipListMaxLevel
	}
	return &SkipList{
		head:     NewSkipListNode(maxLevel, 0, nil),
		level:    1,
		size:     0,
		maxLevel: maxLevel,
	}
}

func (list *SkipList) randLevel() int {
	level := 1
	for rand.Intn(100) < 25 && level < list.maxLevel {
		level++
	}
	return level
}
func (list *SkipList) Size() int64 {
	return list.size
}

func (list *SkipList) InsertByScore(score float64, value ISkipListNode) *SkipListNode {
	rank := make([]int64, list.maxLevel)
	updateList := make([]*SkipListNode, list.maxLevel)
	t := list.head
	for i := list.level - 1; i >= 0; i-- {
		if i < list.level-1 {
			rank[i] = rank[i+1]
		}
		for t.Next(i) != nil && (t.Next(i).score < score || (t.Next(i).score == value.Score() && t.Next(i).value.Compare(value) < 0)) {
			rank[i] += t.level[i].span
			t = t.Next(i)
		}
		updateList[i] = t
	}
	level := list.randLevel()
	if level > list.level {
		for i := list.level; i < level; i++ {
			rank[i] = 0
			updateList[i] = list.head
			updateList[i].SetSpan(i, list.size)
		}
		list.level = level
	}
	newNode := NewSkipListNode(level, score, value)
	for i := 0; i < level; i++ {
		newNode.SetNext(i, updateList[i].Next(i))
		updateList[i].SetNext(i, newNode)
		newNode.SetSpan(i, updateList[i].Span(i)-(rank[0]-rank[i]))
		updateList[i].SetSpan(i, rank[0]-rank[i]+1)
	}
	//处理新增节点的span
	for i := level; i < list.level; i++ {
		updateList[i].level[i].span++
	}
	//处理后退指针
	if updateList[0] == list.head {
		newNode.backward = nil
	} else {
		newNode.backward = updateList[0]
	}
	//判断新插入的节点是否为最后一个节点
	if newNode.Next(0) != nil {
		newNode.Next(0).backward = newNode
	} else {
		//如果是最后一个节点,就让tail指针指向这新插入的节点
		list.tail = newNode
	}
	list.size++
	return newNode
}

func (list *SkipList) UpdateScore(node *SkipListNode, score float64) {
	if node.score == score {
		return
	}
	//更新后,分数还是 < next node的位置不用变
	if score > node.score {
		if node.Next(0) != nil && node.Next(0).score > score {
			node.score = score
			return
		}
	}
	//更新后,分数还是 > per node的位置不用变
	if score < node.score {
		if node.Pre() != nil && score > node.Pre().score {
			node.score = score
			return
		}
	}

	//删掉node，重新插入
	updateList := list.GetUpdateList(node)
	list.Delete(node, updateList)
	//重新插入
	list.InsertByScore(score, node.value)
}

func (list *SkipList) GetUpdateList(node *SkipListNode) (updateList []*SkipListNode) {
	updateList = make([]*SkipListNode, list.maxLevel)
	t := list.head
	for i := list.level - 1; i >= 0; i-- {
		for t.Next(i) != nil && (t.Next(i).score < node.score || (t.Next(i).score == node.score && t.Next(i).value.Compare(node.value) < 0)) {
			t = t.Next(i)
		}
		updateList[i] = t
	}
	return
}

func (list *SkipList) Delete(node *SkipListNode, updateList []*SkipListNode) {
	if node == nil || node == list.head {
		return
	}
	for i := 0; i < list.level; i++ {
		if updateList[i].Next(i) == node {
			updateList[i].SetSpan(i, updateList[i].Span(i)+node.Span(i)-1)
			updateList[i].SetNext(i, node.Next(i))
		} else {
			updateList[i].level[i].span--
		}
	}
	//处理后指针
	if node.Next(0) == nil {
		list.tail = updateList[0]
	} else {
		node.Next(0).backward = updateList[0]
	}
	//处理删掉的是最高level的情况,当前的level要对应的--
	for list.level > 1 && list.head.Next(list.level-1) == nil {
		list.level--
	}
	list.size--
}

func (list *SkipList) GetNodeByScore(findRange *SkipListFindRange) (result []*SkipListNode) {
	if findRange == nil || list.size == 0 {
		return
	}
	if !list.ScoreRange(findRange) {
		return
	}
	t := list.head
	if findRange.MinInf {
		//从头开始查找
		t = list.head.Next(0)
	} else {
		//不是从头开始查找
		for i := list.level - 1; i >= 0; i-- {
			for t.Next(i) != nil && t.Next(i).score < findRange.Min {
				t = t.Next(i)
			}
		}
	}

	for {
		//符合范围的条件 (从负无穷 || 当前的score >= 查找的最小值) && (到正无穷 || 当前元素 <= 查找的最大值)
		if (findRange.MinInf || (!findRange.MinBra && t.score >= findRange.Min)) && (findRange.MaxInf || (!findRange.MaxBra && t.score <= findRange.Max)) {
			result = append(result, t)
		} else if (findRange.MinInf || (findRange.MinBra && t.score > findRange.Min)) && (findRange.MaxInf || (!findRange.MaxBra && t.score <= findRange.Max)) {
			result = append(result, t)
		} else if (findRange.MinInf || (!findRange.MinBra && t.score >= findRange.Min)) && (findRange.MaxInf || (findRange.MaxBra && t.score < findRange.Max)) {
			result = append(result, t)
		} else if (findRange.MinInf || (findRange.MinBra && t.score > findRange.Min)) && (findRange.MaxInf || (findRange.MaxBra && t.score < findRange.Max)) {
			result = append(result, t)
		}
		if t.Next(0) == nil || (!findRange.MaxInf && t.Next(0).score > findRange.Max) {
			return
		} else {
			t = t.Next(0)
		}

	}
	return
}

func (list *SkipList) GetNodeByRank(left, right int64) (result []*SkipListNode) {
	if list.size == 0 || left == 0 || right == 0 || right < left || left > list.Size() {
		return
	}
	tRank := int64(0)
	result = make([]*SkipListNode, 0, right-left+1)
	t := list.head
	for i := list.level - 1; i >= 0; i-- {
		for t.Next(i) != nil && tRank+t.level[i].span <= left {
			tRank += t.level[i].span
			t = t.Next(i)
		}
		if tRank == left {
			for ; t != nil && tRank <= right; t = t.Next(0) {
				result = append(result, t)
				tRank++
			}
			return
		}
	}
	return
}

func (list *SkipList) GetNodeRank(node *SkipListNode) int64 {
	rank := int64(0)
	t := list.head
	for i := list.level - 1; i >= 0; i-- {
		for t.Next(i) != nil && t.Next(i).score <= node.score {
			rank += t.level[i].span
			if t.Next(i).score == node.score && t.Next(i).value.Compare(node.value) == 0 {
				return rank
			}
			t = t.Next(i)
		}
	}
	return rank
}

func (list *SkipList) ScoreRange(findRange *SkipListFindRange) bool {
	if !findRange.MaxInf && findRange.Max < list.head.Next(0).score {
		return false
	}
	if !findRange.MinInf && findRange.Min > list.tail.score {
		return false
	}
	return true
}
