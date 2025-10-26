package memtable

import "sync"

type Node struct {
	value  []byte
	key    string
	parent *Node
	left   *Node
	right  *Node
	color  int
}

type MemTable struct {
	root *Node
	mu   sync.Mutex
}

func Start() *Node {
	memTree := Node{make([]byte, 0), "", nil, nil, nil, 0}
	memTree.left = leafNode(&memTree)
	memTree.right = leafNode(&memTree)
	return &memTree
}

func leafNode(parent *Node) *Node {
	return &Node{make([]byte, 0), "", parent, nil, nil, 0}
}

func addNode(root *Node, parent *Node, key string, value []byte, inserted *Node) *Node {
	if root.left == nil && root.right == nil {
		root.key = key
		newVal := make([]byte, 0)
		copy(newVal, value)
		root.value = newVal
		root.color = 1
		root.left = leafNode(root)
		root.right = leafNode(root)
		inserted = root
		return root
	}
	if root.key > key {
		root.left = addNode(root.left, root, key, value, inserted)
	} else {
		root.right = addNode(root.right, root, key, value, inserted)
	}
	return root
}

func fixInsert(tree *MemTable, newNode *Node) {
	for newNode.parent.color == 1 {
		if newNode.parent == newNode.parent.parent.left {
			uncle := newNode.parent.parent.right
			if uncle.color == 1 {
				newNode.parent.color = 0
				uncle.color = 0
				newNode.parent.parent.color = 1
				newNode = newNode.parent.parent
			} else {

			}
		} else {

		}
	}
}

func (mem *MemTable) Insert(key string, value []byte) *Node {
	if mem.root.key == "" {
		mem.root.key = key
		mem.root.value = value
	} else {
		var inserted *Node
		addNode(mem.root, nil, key, value, inserted)
	}

}
