package memtable

type Node struct {
	value []byte
	key   string
	left  *Node
	right *Node
	color int
}

func Start() *Node {
	memTree := Node{make([]byte, 0), "", nil, nil, 0}
	return &memTree
}

func (root *Node) Insert(key string, value []byte) *Node {
	if root.left != nil && root.left.key < key {
		root.left = root.left.Insert(key, value)
	} else if root.right != nil && root.right.key > key {
		root.right = root.right.Insert(key, value)
	}
	return &Node{key: key, value: value, left: nil, right: nil, color: 1}
}
