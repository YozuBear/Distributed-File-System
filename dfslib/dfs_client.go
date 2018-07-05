/*

This package specifies the implementation to
DFS_Client, functions called by server

*/

package dfslib

import (
	"../comm"
	"net"
	"net/rpc"
	"os"
	"strconv"
)

type DFS_Client int

// Set up client's side server for DFS server to connect to.
func Mount(clientIP string) (port string, tcpListener *net.TCPListener, err error) {
	tcpAddr, err := net.ResolveTCPAddr("tcp", clientIP+":0")
	if err != nil {
		return "", nil, DisconnectedError(clientIP)
	}
	listener, err := net.ListenTCP("tcp", tcpAddr)
	if err != nil {
		return "", nil, DisconnectedError(clientIP)
	}

	// Obtain the auto-assigned port
	port = strconv.Itoa(listener.Addr().(*net.TCPAddr).Port)

	// Register client
	dfs_client := new(DFS_Client)
	rpc.Register(dfs_client)

	go rpc.Accept(listener)

	return port, listener, nil

}

// Inform server that the client is still on-line, return process ID
func (t *DFS_Client) Alive(args int, reply *int) error {
	*reply = os.Getpid()
	return nil
}

// send the requested chunk to server
func (t *DFS_Client) RetrieveChunk(args comm.ChunkRequest, chunk *comm.Chunk) error {
	filePath := args.FilePath
	chunkNumber := args.ChunkNumber

	if LocalPathExists(filePath) {
		osFile, err := os.Open(filePath)
		if err != nil {
			return FileDoesNotExistError(filePath)
		}
		// enforce chunk size to be 32 bytes
		data := make([]byte, ChunkSize)
		_, err = osFile.ReadAt(data, int64(ChunkSize*chunkNumber))
		if err != nil {
			return ChunkUnavailableError(chunkNumber)
		}
		// Succesfully obtain the data
		*chunk = comm.Chunk(data)
	} else {
		return FileDoesNotExistError(filePath)
	}

	return nil
}
