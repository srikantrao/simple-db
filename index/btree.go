package index

import (
	"encoding/binary"
	"fmt"
)

const (
	BNode_Node = iota // internal node without values
	BNode_Leaf        // leaf node with values
)

const (
	Header          = 4
	BTreePageSize   = 4096
	BTreeMaxKeySize = 1000
	BTreeMaxValSize = 3000
)

//A node consists of:
//1. A fixed-sized header containing the type of the node (leaf node or internal node) and the number of keys.
//2. A list of pointers to the child nodes. (Used by internal nodes).
//3. A list of offsets pointing to each key-value pair.
//4. Packed KV pairs.
//| type | nkeys | pointers   | offsets    | key-values
//| 2B   | 2B    | nkeys * 8B | nkeys * 2B | ...
//This is the format of the KV pair. Lengths followed by data.
//| klen | vlen | key | val |
//| 2B   | 2B   | ... | ... |
//To keep things simple, both leaf nodes and internal nodes use the same format.

type BNode struct {
	data []byte
}

func (node *BNode) bType() uint16 {
	return binary.LittleEndian.Uint16(node.data[:2])
}

func (node *BNode) numKeys() uint16 {
	return binary.LittleEndian.Uint16(node.data[2:4])
}

func (node *BNode) setHeader(bType uint16, numKeys uint16) {
	binary.LittleEndian.PutUint16(node.data[:2], bType)
	binary.LittleEndian.PutUint16(node.data[2:4], numKeys)
}

func (node *BNode) getPtr(i uint16) uint64 {
	pos := Header + i*8
	return binary.LittleEndian.Uint64(node.data[pos : pos+8])
}

func (node *BNode) setPtr(i uint16, ptr uint64) {
	pos := Header + i*8
	binary.LittleEndian.PutUint64(node.data[pos:pos+8], ptr)
}

func (node *BNode) getOffset(idx uint16) uint16 {
	pos := offsetPos(node, idx)
	return binary.LittleEndian.Uint16(node.data[pos : pos+2])
}

func (node *BNode) setOffset(idx uint16, offset uint16) {
	pos := offsetPos(node, idx)
	binary.LittleEndian.PutUint16(node.data[pos:pos+2], offset)
}

func (node *BNode) kvPos(idx uint16) uint16 {
	assert(idx <= node.numKeys(), "kvPos: idx out of range")
	return Header + 8*node.numKeys() + 2*node.numKeys() + node.getOffset(idx)
}

func (node *BNode) getKey(idx uint16) []byte {
	assert(idx <= node.numKeys(), "getKey: idx out of range")
	pos := node.kvPos(idx)
	klen := binary.LittleEndian.Uint16(node.data[pos : pos+2])
	return node.data[pos+4 : pos+4+klen] // 4 bytes are taken up by klen and vlen
}

func (node *BNode) getVal(idx uint16) []byte {
	assert(idx <= node.numKeys(), "getVal: idx out of range")
	pos := node.kvPos(idx)
	klen := binary.LittleEndian.Uint16(node.data[pos : pos+2])
	vlen := binary.LittleEndian.Uint16(node.data[pos+2 : pos+4])
	return node.data[pos+4+klen : pos+4+klen+vlen]
}

// node size in bytes
func (node *BNode) numBytes() uint16 {
	return node.kvPos(node.numKeys())
}

// offset list
// The offset list is used to locate the nth KV pair quickly
func offsetPos(node *BNode, idx uint16) uint16 {
	assert(1 <= idx && idx <= node.numKeys(), "offsetPos: idx out of range")
	return Header + node.numKeys()*8 + (idx-1)*2
}

// BTree is a B+Tree implementation
// in-memory pointers cannot be used since the pointers are page numbers referencing the disk instead
// of in-memory pointers referencing the heap
type BTree struct {
	// pointer (a non-zero page number) to the root node
	root uint64

	// callbacks for managing on-disk pages
	// these are used to read/write pages from/to disk
	// and to allocate new pages
	get func(uint64) BNode // dereference a page number to a node
	new func(BNode) uint64 // allocate a new page and return its page number
	del func(uint64)       // deallocate a page
}

func assert(cond bool, msg string) {
	if !cond {
		panic(fmt.Sprintf("assertion failed: %s", msg))
	}
}
