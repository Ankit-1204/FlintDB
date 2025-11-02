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

const (
	BLACK = 0
	RED   = 1
)

type MemTable struct {
	root *Node
	mu   sync.Mutex
}

func Start() *MemTable {
	memTree := MemTable{root: &Node{make([]byte, 0), "", nil, nil, nil, BLACK}}
	memTree.root.left = leafNode(memTree.root)
	memTree.root.right = leafNode(memTree.root)
	return &memTree
}

func leftRotate(tree *MemTable, x *Node) {
	if x == nil || x.right == nil {
		return
	}
	y := x.right
	x.right = y.left
	if y.left != nil {
		y.left.parent = x
	}
	y.parent = x.parent
	if x.parent == nil {
		tree.root = y
	} else if x == x.parent.left {
		x.parent.left = y
	} else {
		x.parent.right = y
	}
	y.left = x
	x.parent = y
}
func rightRotate(tree *MemTable, x *Node) {
	if x == nil || x.left == nil {
		return
	}
	y := x.left
	x.left = y.right
	if y.right != nil {
		y.right.parent = x
	}
	y.parent = x.parent
	if x.parent == nil {
		tree.root = y
	} else if x == x.parent.left {
		x.parent.left = y
	} else {
		x.parent.right = y
	}
	y.right = x
	x.parent = y
}

func leafNode(parent *Node) *Node {
	return &Node{make([]byte, 0), "", parent, nil, nil, BLACK}
}

func addNode(root *Node, parent *Node, key string, value []byte) (*Node, *Node) {
	if root.left == nil && root.right == nil {
		root.key = key
		newVal := make([]byte, len(value))
		copy(newVal, value)
		root.value = newVal
		root.color = RED
		root.left = leafNode(root)
		root.right = leafNode(root)

		return root, root
	}
	var inserted *Node
	if root.key > key {

		root.left, inserted = addNode(root.left, root, key, value)
	} else {
		root.right, inserted = addNode(root.right, root, key, value)
	}
	return root, inserted
}
func fixInsert(tree *MemTable, newNode *Node) {
	if newNode == nil {
		return
	}
	for newNode.parent != nil && newNode.parent.color == RED {
		grand := newNode.parent.parent
		if grand == nil {
			break
		}
		if newNode.parent == grand.left {
			uncle := grand.right
			if uncle != nil && uncle.color == RED {
				newNode.parent.color = BLACK
				uncle.color = BLACK
				grand.color = RED
				newNode = grand
			} else {
				if newNode == newNode.parent.right {
					newNode = newNode.parent
					leftRotate(tree, newNode)
				}
				newNode.parent.color = BLACK
				grand = newNode.parent.parent
				if grand != nil {
					grand.color = RED
					rightRotate(tree, grand)
				}
			}
		} else {
			uncle := grand.left
			if uncle != nil && uncle.color == RED {
				newNode.parent.color = BLACK
				uncle.color = BLACK
				grand.color = RED
				newNode = grand
			} else {
				if newNode == newNode.parent.left {
					newNode = newNode.parent
					rightRotate(tree, newNode)
				}
				newNode.parent.color = BLACK
				grand = newNode.parent.parent
				if grand != nil {
					grand.color = RED
					leftRotate(tree, grand)
				}
			}
		}
	}
	if tree.root != nil {
		tree.root.color = BLACK
	}
}

func (mem *MemTable) Insert(key string, value []byte) {
	mem.mu.Lock()
	defer mem.mu.Unlock()

	if mem.root.key == "" {
		mem.root.key = key
		mem.root.value = make([]byte, len(value))
		copy(mem.root.value, value)
		mem.root.color = BLACK
		return
	} else {
		var inserted *Node
		_, inserted = addNode(mem.root, nil, key, value)
		fixInsert(mem, inserted)
		return
	}

}
func find(root *Node, key string) []byte {
	if root.key == key {
		rval := make([]byte, len(root.value))
		copy(rval, root.value)
		return rval
	}
	if root.key > key {
		return find(root.left, key)
	} else {
		return find(root.right, key)
	}
}

func (mem *MemTable) Search(key string) []byte {
	mem.mu.Lock()
	defer mem.mu.Unlock()
	return find(mem.root, key)

}
