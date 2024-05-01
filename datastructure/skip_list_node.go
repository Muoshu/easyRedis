package datastructure

type SkipListLevel struct {
	//指向下一个节点
	forward *SkipListNode
	//到下一个节点的距离
	span int64
}

type ISkipListNode interface {
	Score() float64
	// Compare 根据返回值判断这两个值的大小
	//-1 this < value
	//0 this == value
	//1 this > value
	Compare(value ISkipListNode) int
}

type SkipListNode struct {
	//指向上一个节点
	backward *SkipListNode
	//索引层
	level []SkipListLevel
	score float64
	//存储的值
	value ISkipListNode
}

func NewSkipListNode(level int, score float64, value ISkipListNode) *SkipListNode {
	return &SkipListNode{
		backward: nil,
		level:    make([]SkipListLevel, level),
		score:    score,
		value:    value,
	}
}

func (node *SkipListNode) Score() float64 {
	return node.score
}

func (node *SkipListNode) SetScore(score float64) {
	node.score = score
}

// Next 获取第i层的下一个元素
func (node *SkipListNode) Next(i int) *SkipListNode {
	return node.level[i].forward
}

// SetNext 设置第i层的下一个元素
func (node *SkipListNode) SetNext(i int, next *SkipListNode) {
	node.level[i].forward = next
}

// Span 获取第i层的span值
func (node *SkipListNode) Span(i int) int64 {
	return node.level[i].span
}

// SetSpan 设置第i层的span值
func (node *SkipListNode) SetSpan(i int, span int64) {
	node.level[i].span = span
}

// Pre 获取上一个元素
func (node *SkipListNode) Pre() *SkipListNode {
	return node.backward
}
