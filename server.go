package main

import (
	"./comm"
	"crypto/md5"
	"encoding/hex"
	"errors"
	"fmt"
	"math/rand"
	"net"
	"net/rpc"
	"os"
	"path/filepath"
	"strconv"
	"time"
)

// RPC server type
type DFS_Server int

// Client meta-data stored by the server
type ClientInfo struct {
	Ip_address string
	Local_path string
}

// File meta-data stored by server
type FileInfo struct {
	ChunksInfo       [256]ChunkInfo
	DirtyChunks      []int
	CurrentWriter    string
	CurrentWriterPID int // Used to verify it's the same process responding.
}

// Specifies Chunk info
type ChunkInfo struct {
	Version int          // The newest version number of the chunk
	Owners  []ChunkOwner // The list of clients that own some version of the chunk
}

// Chunk owner
type ChunkOwner struct {
	ClientID string
	Version  int
}

// Table storing client information, maps from client ID to ClientInfo
var client_table = make(map[string]ClientInfo)

// Table storing dile information, maps from file name to FileInfo
var file_table = make(map[comm.FileName]*FileInfo)

func main() {
	// Register DFS_Server
	dfs_server := new(DFS_Server)
	rpc.Register(dfs_server)

	server_ip_port := os.Args[1]
	tcpAddr, err := net.ResolveTCPAddr("tcp", server_ip_port)
	checkError(err)

	listener, err := net.ListenTCP("tcp", tcpAddr)
	checkError(err)

	rpc.Accept(listener)


}

func (t *DFS_Server) RegisterClient_New(client *comm.NewClient, reply *string) error {
	// Assign ID to new client
	client_local_path := client.Local_path
	client_id := AssignClientID(client_local_path)

	// Assign new client to client_table
	assign_client_info(client_id, client.Ip_port, client_local_path)

	// Inform client of its new client ID
	*reply = client_id
	return nil
}

// Register reconnecting client, update client_table if the client's IP address has changed
func (t *DFS_Server) RegisterClient_Existing(client *comm.ExistingClient, reply *int) error {
	client_ID := client.Client_id
	ip_address := client.Ip_port
	table_ip_address := client_table[client_ID].Ip_address
	if ip_address != table_ip_address {
		assign_client_info(client_ID, ip_address, client.Local_path)
	}
	// do nothing to reply

	return nil
}

func (t *DFS_Server) FileExist(fileName *comm.FileName, reply *bool) error {
	_, exists := file_table[*fileName]
	*reply = exists

	return nil
}

// return the newest version number of the given chunk info
func (t *DFS_Server) VersionNum(ncr comm.NewestChunkRequest, reply *int) (err error) {
	fileName := ncr.FileName
	chunkNum := ncr.ChunkNumber
	chunkInfo := file_table[fileName].ChunksInfo[chunkNum]
	*reply = chunkInfo.Version

	return nil
}

// Retrieve only the newest version chunk
// If newest version is unavailable, return error
func (t *DFS_Server) RetrieveLatestChunk(ncr comm.NewestChunkRequest, reply *comm.Chunk) (err error) {
	fileName := ncr.FileName
	chunkNum := ncr.ChunkNumber
	chunkInfo := file_table[fileName].ChunksInfo[chunkNum]
	newestVersion := chunkInfo.Version
	owners := chunkInfo.Owners
	ownerVersion := newestVersion
	for i := len(owners) - 1; i >= 0 && ownerVersion == newestVersion; i-- {
		// Start from the owners with latest version
		ownerID := owners[i].ClientID
		clientInfo := client_table[ownerID]
		ownerVersion = owners[i].Version
		chunk, errC := RetrieveChunk(clientInfo, string(fileName), int(chunkNum))
		if errC == nil && ownerVersion == newestVersion {
			*reply = chunk
			return nil
		}
	}
	// Can't find latest chunk, return error
	return errors.New("ChunkUnavailableError")
}

// Retrieve file from owner client to requestion client
// Best effort to retrieve file, if some chunk is unavailable, return error
// Precondition: file exists
func (t *DFS_Server) RetrieveFile(fileName comm.FileName, reply *[]comm.ChunkPacket) (err error) {
	fileInfo := file_table[fileName]
	var chunkPackets = []comm.ChunkPacket{}
	// File exists, but no one has written yet.
	if len(fileInfo.DirtyChunks) == 0 {
		return nil
	}
	// Loop through dirty chunks that need to be obtained
	for _, dirtyChunkNum := range fileInfo.DirtyChunks {
		dirtyChunkOwners := fileInfo.ChunksInfo[dirtyChunkNum].Owners
		var chunk comm.Chunk
		ownerVersion := 0
		for i := len(dirtyChunkOwners) - 1; i >= 0; i-- {
			// Start from the owners with latest version
			ownerID := dirtyChunkOwners[i].ClientID
			clientInfo := client_table[ownerID]
			chunk, err = RetrieveChunk(clientInfo, string(fileName), dirtyChunkNum)
			if err == nil {
				ownerVersion = dirtyChunkOwners[i].Version
				break
			}
		}
		if ownerVersion != 0 {
			// Found the chunk
			chunkPacket := comm.ChunkPacket{
				Chunk:       chunk,
				Version:     ownerVersion,
				ChunkNumber: dirtyChunkNum,
			}
			chunkPackets = append(chunkPackets, chunkPacket)
		} else {
			// Can't find any available chunk from all owners
			// FileUnavailableError

			return errors.New("FileUnavailableError for chunk #: " + strconv.Itoa(dirtyChunkNum))
		}

	}

	*reply = chunkPackets
	return nil

}

// Used to verify there's still server connection
func (t *DFS_Server) ServerConnection(args int, reply *bool) (err error) {
	*reply = true
	return nil
}

// Retrieve chunk from the given client
func RetrieveChunk(clientInfo ClientInfo, fileName string, chunkNumber int) (chunk comm.Chunk, err error) {
	clientIP := clientInfo.Ip_address
	client, err := rpc.Dial("tcp", clientIP)
	if err != nil {
		return nil, err
	}
	defer client.Close()

	filePath := filepath.Join(clientInfo.Local_path, fileName+".dfs")
	fileRequest := comm.ChunkRequest{
		FilePath:    filePath,
		ChunkNumber: chunkNumber,
	}
	err = client.Call("DFS_Client.RetrieveChunk", fileRequest, &chunk)

	return chunk, err

}

// Update File table of client's new version chunks upon open
func (t *DFS_Server) UpdateClientFileInfo(cfi *comm.ClientFileInfo, reply *bool) error {
	clientID := cfi.ClientID
	fileName := cfi.FileName

	// A new file has been created by client
	if cfi.ChunkMetaPackets == nil {
		var fileInfo = FileInfo{}
		file_table[fileName] = &fileInfo //TODO might be wrong
		*reply = true
		return nil
	}
	for _, chunkMeta := range *cfi.ChunkMetaPackets {
		chunkOwners := file_table[fileName].ChunksInfo[chunkMeta.ChunkNumber].Owners

		// If current client is already an owner, delete history
		// Always put the client with newer version to the end of array
		for i, owner := range chunkOwners {
			if owner.ClientID == clientID {
				// Delete the old version metadata
				chunkOwners = append(chunkOwners[:i], chunkOwners[i+1:]...)
				break
			}
		}

		// update FileInfo.DirtyChunks
		chunkNum := chunkMeta.ChunkNumber
		alreadyDirtyChunk := false
		dirtyChunks := file_table[fileName].DirtyChunks
		for _, b := range dirtyChunks {
			if b == chunkNum {
				alreadyDirtyChunk = true
			}
		}
		if !alreadyDirtyChunk {
			// Add current chunk to dirty chunks if it doesn't already exist
			file_table[fileName].DirtyChunks = append(dirtyChunks, chunkNum)
		}

		// Add new info to the end of owner array
		newChunkOwner := ChunkOwner{
			ClientID: clientID,
			Version:  chunkMeta.Version,
		}
		chunkOwners = append(chunkOwners, newChunkOwner)

		// Write back to file_table map
		file_table[fileName].ChunksInfo[chunkMeta.ChunkNumber].Owners = chunkOwners

		// Update the newest version number
		if chunkMeta.Version > file_table[fileName].ChunksInfo[chunkMeta.ChunkNumber].Version {
			file_table[fileName].ChunksInfo[chunkMeta.ChunkNumber].Version = chunkMeta.Version
		}

	}

	*reply = true
	return nil
}

// For Client to request write lock of given file.
// Reply true if write lock is granted
func (t *DFS_Server) RequestWriteLock(request comm.WriteLockRequest, reply *bool) error {
	clientID := request.ClientID
	clientPID := request.ClientPID
	fileName := request.FileName
	lockHolderID := file_table[fileName].CurrentWriter
	if lockHolderID == "" {
		// Simple case, no lock holder
		*reply = true
		file_table[fileName].CurrentWriter = clientID
		file_table[fileName].CurrentWriterPID = clientPID
	} else {
		// There is a write lock holder, check if it's alive
		lockHolderIP := client_table[lockHolderID].Ip_address
		lockHolderPID := file_table[fileName].CurrentWriterPID
		isAlive := isClientConnected(lockHolderIP, lockHolderPID)
		if isAlive {
			// If lock holder is alive, do not grant the lock
			*reply = false
		} else {
			// lock holder is not responsive, give lock to current client
			*reply = true
			file_table[fileName].CurrentWriter = clientID
			file_table[fileName].CurrentWriterPID = clientPID
		}

	}
	return nil
}

// Client releases the wrote lock upon closing the file
// Only client with same pid and ID can release lock
func (t *DFS_Server) ReleaseWriteLock(request comm.WriteLockRequest, reply *bool) error {
	clientID := request.ClientID
	clientPID := request.ClientPID
	fileName := request.FileName
	lockClientID := file_table[fileName].CurrentWriter
	lockPID := file_table[fileName].CurrentWriterPID
	if clientID == lockClientID && clientPID == lockPID {
		// Same writer trying to release write lock
		file_table[fileName].CurrentWriter = ""
		file_table[fileName].CurrentWriterPID = 0
		*reply = true
	} else {
		// Different writer trying to release lock
		*reply = false
	}
	return nil
}

// reply == true if the requestion client is still the lock holder
// i.e. hasn't been replaced
func (t *DFS_Server) LockHolder(request comm.WriteLockRequest, reply *bool) error {
	clientID := request.ClientID
	clientPID := request.ClientPID
	fileName := request.FileName
	lockClientID := file_table[fileName].CurrentWriter
	lockPID := file_table[fileName].CurrentWriterPID
	if clientID == lockClientID && clientPID == lockPID {
		*reply = true
	} else {
		*reply = false
	}
	return nil
}

// Assign unique cliend ID to client using MD5 hashing
// store the client's IP address and local storage path to client_table
func AssignClientID(client_local_path string) (hashedStr string) {
	// Compute hash string using local path and random string
	randomStr := randomStr()
	h := md5.New()
	h.Write([]byte(client_local_path + randomStr))
	hashedStr = hex.EncodeToString(h.Sum(nil))

	// check if new client ID clashes with existing ones in client_table
	_, ok := client_table[hashedStr]
	for ok {
		// If client ID already exists in client_table,
		// rehash the string with new random value
		hashedStr = AssignClientID(client_local_path)
	}

	return hashedStr
}

// Random String generator
func randomStr() string {
	s1 := rand.NewSource(time.Now().UnixNano())
	r1 := rand.New(s1)
	randNum := r1.Intn(1000000000)
	return strconv.Itoa(randNum)
}

// Store client information to client_table
// Overwrite data if client already existed, in case client changed IP address
func assign_client_info(client_ID, client_IP_address, client_local_path string) {
	client_table[client_ID] = ClientInfo{
		Ip_address: client_IP_address,
		Local_path: client_local_path}
}

// Print error if err is not nil
func checkError(err error) {
	if err != nil {
		fmt.Println("server check error hit")
		fmt.Println(err)
	}
}

// Check Client connection, with timeout of 2s
func isClientConnected(clientIP string, clientPID int) (isConnected bool) {
	// https://stackoverflow.com/questions/23330024/does-rpc-have-a-timeout-mechanism
	// Timeout after 2 seconds.
	client, err := rpc.Dial("tcp", clientIP)
	if err != nil {
		isConnected = false
	}
	defer client.Close()

	timeout := 2 * time.Second
	var args int
	var currentClientPID int

	c := make(chan error, 1)
	go func() { c <- client.Call("DFS_Client.Alive", args, &currentClientPID) }()
	select {
	case err := <-c:
		if err != nil {
			isConnected = false
		} else {
			// the lock holder must have the same PID as when it obtained the lock
			isConnected = currentClientPID == clientPID
		}
	case <-time.After(timeout):
		isConnected = false
	}

	return isConnected
}
