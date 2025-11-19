package formats

import "bytes"

type HeapItem struct {
	Key           []byte
	Value         []byte
	IteratorIndex int
	Seq           uint64
	Tombstone     bool
}

type MinHeap []*HeapItem

func (h MinHeap) Len() int { return len(h) }
func (h MinHeap) Less(i, j int) bool {
	cmp := bytes.Compare(h[i].Key, h[j].Key)
	if cmp == 0 {
		return h[i].Seq < h[j].Seq
	}
	return cmp < 0
}
func (h MinHeap) Swap(i, j int) { h[i], h[j] = h[j], h[i] }

func (h *MinHeap) Push(x interface{}) {
	*h = append(*h, x.(*HeapItem))
}

func (h *MinHeap) Pop() interface{} {
	old := *h
	n := len(old)
	item := old[n-1]
	*h = old[0 : n-1]
	return item
}
