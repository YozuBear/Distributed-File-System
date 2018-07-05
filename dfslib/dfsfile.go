package dfslib

/*
	Implementation for DFSFile's Read mode methods
*/
import (
	"../comm"
	"net/rpc"
	"os"
)

type DFSFile_Connected struct {
	ClientID string
	FileName comm.FileName
	File     *os.File
	Versions []int // the version number of each chunk, size=FileSize
	Server   *rpc.Client
}

type DFSFile_Read struct {
	ClientID string
	FileName comm.FileName
	File     *os.File
	Versions []int // the version number of each chunk, size=FileSize
	Server   *rpc.Client
}

func (dfsFile DFSFile_Read) Read(chunkNum uint8, chunk *Chunk) (err error) {
	dfsFileConn := DFSFile_Connected{
		ClientID: dfsFile.ClientID,
		FileName: dfsFile.FileName,
		File:     dfsFile.File,
		Versions: dfsFile.Versions,
		Server:   dfsFile.Server,
	}
	err = connectedRead(dfsFileConn, chunkNum, chunk)

	return err
}
func (dfsFile DFSFile_Read) Write(chunkNum uint8, chunk *Chunk) (err error) {

	return BadFileModeError(READ)
}
func (dfsFile DFSFile_Read) Close() (err error) {
	// check connection
	var args int
	var alive bool
	err = dfsFile.Server.Call("DFS_Server.ServerConnection", args, &alive)
	if err != nil || !alive {
		err = DisconnectedError("DFSFile_Read.Close")
	}

	// Set everything back to default values
	dfsFile.File.Close()
	dfsFile = DFSFile_Read{}

	return err
}

/*
	Implementation for DFSFile's Write mode methods
*/
type DFSFile_Write struct {
	ClientID string
	FileName comm.FileName
	File     *os.File
	Versions []int // the version number of each chunk, size=FileSize
	Server   *rpc.Client
}

func (dfsFile DFSFile_Write) Read(chunkNum uint8, chunk *Chunk) (err error) {
	dfsFileConn := DFSFile_Connected{
		ClientID: dfsFile.ClientID,
		FileName: dfsFile.FileName,
		File:     dfsFile.File,
		Versions: dfsFile.Versions,
		Server:   dfsFile.Server,
	}

	err = connectedRead(dfsFileConn, chunkNum, chunk)

	return err
}
func (dfsFile DFSFile_Write) Write(chunkNum uint8, chunk *Chunk) (err error) {
	// check that client still has the lock to the file
	args := comm.WriteLockRequest{
		ClientID:  dfsFile.ClientID,
		ClientPID: os.Getpid(),
		FileName:  dfsFile.FileName,
	}
	var isHolding bool
	err = dfsFile.Server.Call("DFS_Server.LockHolder", args, &isHolding)
	if err != nil {
		err = DisconnectedError("DFS_Server.LockHolder")
	}
	if !isHolding {
		// If no longer the lock holder, close the file
		dfsFile.Close()
		return
	}

	// Obtained the newest version available during Open,
	// Now overwrite it.
	dfsFile.File.WriteAt(chunk[:], int64(chunkNum)*int64(ChunkSize))
	dfsFile.File.Sync()

	// Get newest version
	var newestVersion int
	ncr := comm.NewestChunkRequest{
		FileName:    dfsFile.FileName,
		ChunkNumber: chunkNum,
	}
	err = dfsFile.Server.Call("DFS_Server.VersionNum", ncr, &newestVersion)
	if err != nil {
		return DisconnectedError("DFS_Server.VersionNum")
	}

	// Update its own local version number, deep copy to struct
	dfsFile.Versions[chunkNum] = newestVersion + 1
	newVersions := dfsFile.Versions
	copy(dfsFile.Versions, newVersions)

	// Update server new version number / dirty bits
	dfsFileConn := DFSFile_Connected{
		ClientID: dfsFile.ClientID,
		FileName: dfsFile.FileName,
		File:     dfsFile.File,
		Versions: dfsFile.Versions,
		Server:   dfsFile.Server,
	}
	UpdateClientFileVersionNumber(dfsFileConn, chunkNum, newestVersion+1)

	return nil
}

// Closes file and release write lock
func (dfsFile DFSFile_Write) Close() (err error) {
	// Release write lock to the file
	args := comm.WriteLockRequest{
		ClientID:  dfsFile.ClientID,
		ClientPID: os.Getpid(),
		FileName:  dfsFile.FileName,
	}
	var success bool
	err = dfsFile.Server.Call("DFS_Server.ReleaseWriteLock", args, &success)
	if err != nil {
		err = DisconnectedError("DFS_Server.ReleaseWriteLock")
	}
	// success == false means this client doesn't have the lock anymore

	// Set everything back to default values
	dfsFile.File.Close()
	dfsFile = DFSFile_Write{}
	return nil
}

/*
	Implementation for DFSFile's DREAD mode methods
*/
type DFSFile_DRead struct {
	File *os.File
}

func (dfsFile DFSFile_DRead) Read(chunkNum uint8, chunk *Chunk) (err error) {
	CleanChunk(chunk)
	Read(dfsFile.File, chunkNum, chunk)
	return nil
}
func (dfsFile DFSFile_DRead) Write(chunkNum uint8, chunk *Chunk) (err error) {
	return BadFileModeError(DREAD)
}
func (dfsFile DFSFile_DRead) Close() (err error) {
	// Set everything back to default values
	dfsFile.File.Close()
	dfsFile = DFSFile_DRead{}

	// Return DisconnectedError to show network status
	return DisconnectedError("in DREAD mode")
}

// Common fns
//
func CleanChunk(chunk *Chunk) {
	for i := 0; i < ChunkSize; i++ {
		chunk[i] = 0
	}
}

// Read the latest chunk in connected mode
func connectedRead(dfsFile DFSFile_Connected, chunkNum uint8, chunk *Chunk) (err error) {
	// Check if this client has the latest chunk version
	currVersion := dfsFile.Versions[chunkNum]
	var newestVersion int
	ncr := comm.NewestChunkRequest{
		FileName:    dfsFile.FileName,
		ChunkNumber: chunkNum,
	}
	err = dfsFile.Server.Call("DFS_Server.VersionNum", ncr, &newestVersion)
	if err != nil {
		return DisconnectedError("DFS_Server.VersionNum")
	}
	if newestVersion > currVersion {
		// If newest version is greater, have to fetch the latest chunk
		err = dfsFile.Server.Call("DFS_Server.RetrieveLatestChunk", ncr, chunk)
		if err != nil {
			return ChunkUnavailableError(chunkNum)
		}
		// update new version on server
		UpdateClientFileVersionNumber(dfsFile, chunkNum, newestVersion)
	} else {
		// current version is the latest version, read local chunk
		Read(dfsFile.File, chunkNum, chunk)
	}

	return nil

}

// Read into chunk
func Read(File *os.File, chunkNum uint8, chunk *Chunk) {
	offset := int64(chunkNum) * int64(ChunkSize)
	data := make([]byte, ChunkSize)
	File.ReadAt(data, offset)
	Byte2Chunk(data, chunk)
}

// copy []byte to defined chunk
func Byte2Chunk(b []byte, chunk *Chunk) {
	for i := 0; i < ChunkSize; i++ {
		chunk[i] = b[i]
	}
}

func UpdateClientFileVersionNumber(dfsFile DFSFile_Connected, chunkNum uint8, version int) (err error) {
	chunkPacket := comm.ChunkMetaPacket{
		Version:     version,
		ChunkNumber: int(chunkNum),
	}
	var chunkPackets = []comm.ChunkMetaPacket{
		chunkPacket,
	}
	clientFileInfo := comm.ClientFileInfo{
		ClientID:         dfsFile.ClientID,
		FileName:         dfsFile.FileName,
		ChunkMetaPackets: &chunkPackets,
	}

	// Send from Client to server to update its file version numbers
	var reply bool
	err = dfsFile.Server.Call("DFS_Server.UpdateClientFileInfo", &clientFileInfo, &reply)

	// Error handling
	if err != nil || !reply {
		err = DisconnectedError("DFS_Server.UpdateClientFileInfo")
	}

	return err
}
