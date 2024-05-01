package datastructure

// ISortSet 有序集必须实现的接口
type ISortSet interface {
	Key() string
	ISkipListNode
}

// StItem 插入元素
type StItem struct {
	F float64
	K string
}

func (s *StItem) Key() string {
	return s.K
}

func (s *StItem) Score() float64 {
	return s.F
}

func (s *StItem) Compare(value ISkipListNode) int {
	i := value.(*StItem)
	if s.Key() > i.Key() {
		return 1
	} else if s.Key() == i.Key() {
		return 0
	} else {
		return -1
	}

}

type SortSet struct {
	//使用map记录当前集合所有的元素
	// map的key为StItem.k+StItem.F
	member map[string]*SkipListNode
	sl     *SkipList
}

func NewDefaultSortSet() *SortSet {
	return &SortSet{
		member: map[string]*SkipListNode{},
		sl:     NewDefaultSkipList(),
	}
}

// NewSortSet 初始化一个有序集合, 可以设置底层跳表的最大层数
// 没有特殊情况,不建议自定义层数
func NewSortSet(level int) *SortSet {
	return &SortSet{
		member: map[string]*SkipListNode{},
		sl:     NewSkipList(level),
	}
}

func (set *SortSet) GetAllKeysAndScores() map[string]float64 {
	result := make(map[string]float64)
	for k, v := range set.member {
		result[k] = v.value.Score()
	}
	return result

}

// GetAllKeys 返回sortSet中所有的keys
func (set *SortSet) GetAllKeys() []string {
	var result []string
	for k, _ := range set.member {
		result = append(result, k)
	}
	return result
}

func (set *SortSet) GetMember(key string) *SkipListNode {
	return set.member[key]
}

func (set *SortSet) addMember(key string, member *SkipListNode) {
	set.member[key] = member
}

func (set *SortSet) delMember(key string) {
	delete(set.member, key)
}

// Add 向SortSet中添加元素,返回成功添加的个数
func (set *SortSet) Add(items ...ISortSet) int {
	l := len(items)
	if l == 0 {
		return 0
	}

	//记录添加了多少个元素
	op := make(map[string]struct{})
	l--
	for l >= 0 {
		if _, ok := op[items[l].Key()]; ok {
			l--
			continue
		}
		if member := set.GetMember(items[l].Key()); member == nil {
			node := set.sl.InsertByScore(items[l].Score(), items[l])
			set.addMember(items[l].Key(), node)
		} else {
			set.sl.UpdateScore(member, items[l].Score())
		}
		op[items[l].Key()] = struct{}{}
		l--
	}
	return len(op)
}

func (set *SortSet) Count() int64 {
	return set.sl.Size()
}

// Rank 返回有序集合中指定的成员索引（从0开始）不存在返回-1
func (set *SortSet) Rank(key string) int64 {
	member := set.GetMember(key)
	if member == nil {
		return -1
	}
	return set.sl.GetNodeRank(member) - 1
}

// RevRank 倒序返回有序集合中指定成员的索引(从0开始)不存在返回 -1
func (set *SortSet) RevRank(key string) int64 {
	member := set.GetMember(key)
	if member == nil {
		return -1
	}
	rank := set.sl.GetNodeRank(member)
	return set.sl.Size() - rank
}

func (set *SortSet) Score(key string) float64 {
	member := set.GetMember(key)
	if member == nil {
		return 0
	}
	return member.score
}

// Remove 移除sortSet中的一个或多个元素
func (set *SortSet) Remove(keys ...string) int {
	removed := 0
	for _, key := range keys {
		if member := set.GetMember(key); member != nil {
			set.delMember(key)
			set.sl.Delete(member, set.sl.GetUpdateList(member))
			removed++
		}
	}
	return removed
}

// RemoveRangeByRank 移除集合中给定排名区间的所有成员
func (set *SortSet) RemoveRangeByRank(min, max int64) int {
	// 现根据rank范围查找node
	result := set.Range(min, max)
	if len(result) == 0 {
		return 0
	}
	var updateList []*SkipListNode
	//删除数据需要的各层结点信息(路径)
	//想一下,为啥只需要获取一次路径就行呢?????
	//因为是按顺序返回的，第一个就是删除的起点后续删除的updateList都是一样的
	for _, key := range result {
		if member := set.GetMember(key.Key()); member != nil {
			if updateList == nil {
				updateList = set.sl.GetUpdateList(member)
			}
			set.delMember(key.Key())
			set.sl.Delete(member, updateList)
		}
	}
	return len(result)
}

// RemoveRangeByScore 移除有序集合中给定的分数区间的所有成员
func (set *SortSet) RemoveRangeByScore(min, max float64, minBra, maxBra, minInf, maxInf bool) int {
	result := set.RangeByScore(&SkipListFindRange{
		Min:    min,
		Max:    max,
		MinBra: minBra,
		MaxBra: maxBra,
		MinInf: minInf,
		MaxInf: maxInf,
	})
	if len(result) == 0 {
		return 0
	}
	var updateList []*SkipListNode
	for _, key := range result {
		if member := set.GetMember(key.Key()); member != nil {
			if updateList == nil {
				updateList = set.sl.GetUpdateList(member)
			}
			set.delMember(key.Key())
			set.sl.Delete(member, updateList)
		}
	}
	return len(result)
}

// Range 通过索引区间返回有序集合指定区间内的成员,分数从低到高
func (set *SortSet) Range(min, max int64) (result []ISortSet) {
	if set.sl.size == 0 {
		return
	}
	if min < 0 {
		min = set.sl.Size() + min
	}
	if max < 0 {
		max = set.sl.size + max
	}
	if min > max {
		return
	}
	//索引是从0开始的, 跳表中的rank是从1开始,所以这里要 +1
	nodes := set.sl.GetNodeByRank(min+1, max+1)
	if len(nodes) == 0 {
		return
	}
	result = make([]ISortSet, len(nodes))
	for i, node := range nodes {
		result[i] = node.value.(ISortSet)
	}
	return
}

// RevRange 返回有序集中指定区间内的成员，通过索引，分数从高到低排序
func (set *SortSet) RevRange(min, max int64) (result []ISortSet) {
	if set.sl.Size() == 0 {
		return
	}
	//反向查找也是按照正向查找来做的
	//只不过是把反向查找的范围转换成正向查找的范围
	//最后把查找的结果反向
	if min < 0 {
		min = -min
	} else {
		if set.sl.Size() >= min {
			min = set.sl.Size() - min
		} else {
			min = set.sl.Size()
		}
	}
	if max < 0 {
		max = -max
	} else {
		if set.sl.Size() > max {
			max = set.sl.Size() - max
		} else {
			max = 1
		}
	}
	if max > min {
		return
	}
	nodes := set.sl.GetNodeByRank(max, min)
	if len(nodes) == 0 {
		return
	}
	l := len(nodes)
	result = make([]ISortSet, l)
	l--
	for i, node := range nodes {
		result[l-i] = node.value.(ISortSet)
	}
	return
}

// RangeByScore 返回有序集中指定分数区间内的成员，分数从低到高排序
func (set *SortSet) RangeByScore(findRange *SkipListFindRange) (result []ISortSet) {
	if findRange == nil || set.sl.Size() == 0 {
		return
	}
	nodes := set.sl.GetNodeByScore(findRange)
	if len(nodes) == 0 {
		return
	}
	result = make([]ISortSet, len(nodes))
	for i, node := range nodes {
		result[i] = node.value.(ISortSet)
	}
	return
}

// RevRangeByScore 返回有序集中指定分数区间内的成员，分数从高到低排序
func (set *SortSet) RevRangeByScore(findRange *SkipListFindRange) (result []ISortSet) {
	if findRange == nil || set.sl.Size() == 0 {
		return
	}

	//分数从高到低查找, 本质上和从低到高查找是一样的
	//不同点是,从高到低查找时,给的范围要调换
	//最后再把查找的结果翻转一下
	findRange.Max, findRange.Min = findRange.Min, findRange.Max
	findRange.MaxInf, findRange.MinInf = findRange.MinInf, findRange.MaxInf
	nodes := set.sl.GetNodeByScore(findRange)
	l := len(nodes)
	if l == 0 {
		return
	}
	result = make([]ISortSet, l)
	l--
	for i, node := range nodes {
		result[l-i] = node.value.(ISortSet)
	}
	return
}
