/*

This package specifies the common structure used by server and dfslib
to pass RPC arguments

*/
package comm

import (
	"fmt"
)

type NewClient struct {
	Ip_port, Local_path string
}

type ExistingClient struct {
	Client_id, Ip_port, Local_path string
}

type FileName string

const ChunkSize int = 32
type Chunk []byte

type ChunkPacket struct {
	Chunk Chunk
	Version int
	ChunkNumber int
}

type NewestChunkRequest struct {
	FileName FileName
	ChunkNumber uint8
}

type ChunkRequest struct {
	FilePath string
	ChunkNumber int
}

// Chunk's meta data
type ChunkMetaPacket struct {
	Version int
	ChunkNumber int
}
// Send from Client to server to update its file version numbers
type ClientFileInfo struct {
	ClientID string
	FileName FileName
	ChunkMetaPackets *[]ChunkMetaPacket
}

// Client requestiong the write lock from server
type WriteLockRequest struct {
	ClientID string
	ClientPID int
	FileName FileName
}

// Check if precodition holds, if error is not nil, panic.
func CheckPrecond(err error) {
	if err != nil {
		fmt.Println(err)
		panic("precondition fail!")
	}
}



