/*

This package specifies the application's interface to the distributed
file system (DFS) system to be used in assignment 2 of UBC CS 416
2017W2.

*/

package dfslib

import (
	"../comm"
	"fmt"
	"io/ioutil"
	"net/rpc"
	"os"
	"path/filepath"
)

// A Chunk is the unit of reading/writing in DFS.
const ChunkSize int = 32
const FileSize int = 256

type Chunk [32]byte
type File [FileSize]Chunk

// Represents a type of file access.
type FileMode int

const (
	// Read mode.
	READ FileMode = iota

	// Read/Write mode.
	WRITE

	// Disconnected read mode.
	DREAD
)

////////////////////////////////////////////////////////////////////////////////////////////
// <ERROR DEFINITIONS>

// These type definitions allow the application to explicitly check
// for the kind of error that occurred. Each API call below lists the
// errors that it is allowed to raise.
//
// Also see:
// https://blog.golang.org/error-handling-and-go
// https://blog.golang.org/errors-are-values

// Contains serverAddr
type DisconnectedError string

func (e DisconnectedError) Error() string {
	return fmt.Sprintf("DFS: Not connnected to server [%s]", string(e))
}

// Contains chunkNum that is unavailable
type ChunkUnavailableError uint8

func (e ChunkUnavailableError) Error() string {
	return fmt.Sprintf("DFS: Latest verson of chunk [%d] unavailable", e)
}

// Contains filename
type OpenWriteConflictError string

func (e OpenWriteConflictError) Error() string {
	return fmt.Sprintf("DFS: Filename [%s] is opened for writing by another client", string(e))
}

// Contains file mode that is bad.
type BadFileModeError FileMode

func (e BadFileModeError) Error() string {
	return fmt.Sprintf("DFS: Cannot perform this operation in current file mode [%s]", string(e))
}

// Contains filename.
type WriteModeTimeoutError string

func (e WriteModeTimeoutError) Error() string {
	return fmt.Sprintf("DFS: Write access to filename [%s] has timed out; reopen the file", string(e))
}

// Contains filename
type BadFilenameError string

func (e BadFilenameError) Error() string {
	return fmt.Sprintf("DFS: Filename [%s] includes illegal characters or has the wrong length", string(e))
}

// Contains filename
type FileUnavailableError string

func (e FileUnavailableError) Error() string {
	return fmt.Sprintf("DFS: Filename [%s] is unavailable", string(e))
}

// Contains local path
type LocalPathError string

func (e LocalPathError) Error() string {
	return fmt.Sprintf("DFS: Cannot access local path [%s]", string(e))
}

// Contains filename
type FileDoesNotExistError string

func (e FileDoesNotExistError) Error() string {
	return fmt.Sprintf("DFS: Cannot open file [%s] in D mode as it does not exist locally", string(e))
}

// </ERROR DEFINITIONS>
////////////////////////////////////////////////////////////////////////////////////////////

// Represents a file in the DFS system.
type DFSFile interface {
	// Reads chunk number chunkNum into storage pointed to by
	// chunk. Returns a non-nil error if the read was unsuccessful.
	//
	// Can return the following errors:
	// - DisconnectedError (in READ,WRITE modes)
	// - ChunkUnavailableError (in READ,WRITE modes)
	Read(chunkNum uint8, chunk *Chunk) (err error)

	// Writes chunk number chunkNum from storage pointed to by
	// chunk. Returns a non-nil error if the write was unsuccessful.
	//
	// Can return the following errors:
	// - BadFileModeError (in READ,DREAD modes)
	// - DisconnectedError (in WRITE mode)
	Write(chunkNum uint8, chunk *Chunk) (err error)

	// Closes the file/cleans up. Can return the following errors:
	// - DisconnectedError
	Close() (err error)
}

// Represents a connection to the DFS system.
type DFS interface {
	// Check if a file with filename fname exists locally (i.e.,
	// available for DREAD reads).
	//
	// Can return the following errors:
	// - BadFilenameError (if filename contains non alpha-numeric chars or is not 1-16 chars long)
	LocalFileExists(fname string) (exists bool, err error)

	// Check if a file with filename fname exists globally.
	//
	// Can return the following errors:
	// - BadFilenameError (if filename contains non alpha-numeric chars or is not 1-16 chars long)
	// - DisconnectedError
	GlobalFileExists(fname string) (exists bool, err error)

	// Opens a filename with name fname using mode. Creates the file
	// in READ/WRITE modes if it does not exist. Returns a handle to
	// the file through which other operations on this file can be
	// made.
	//
	// Can return the following errors:
	// - OpenWriteConflictError (in WRITE mode)
	// - DisconnectedError (in READ,WRITE modes)
	// - FileUnavailableError (in READ,WRITE modes)
	// - FileDoesNotExistError (in DREAD mode)
	// - BadFilenameError (if filename contains non alpha-numeric chars or is not 1-16 chars long)
	Open(fname string, mode FileMode) (f DFSFile, err error)

	// Disconnects from the server. Can return the following errors:
	// - DisconnectedError
	UMountDFS() (err error)
}

// The constructor for a new DFS object instance. Takes the server's
// IP:port address string as parameter, the localIP to use to
// establish the connection to the server, and a localPath path on the
// local filesystem where the client has allocated storage (and
// possibly existing state) for this DFS.
//
// The returned dfs instance is singleton: an application is expected
// to interact with just one dfs at a time.
//
// This call should succeed regardless of whether the server is
// reachable. Otherwise, applications cannot access (local) files
// while disconnected.
//
// Can return the following errors:
// - LocalPathError
// - Networking errors related to localIP or serverAddr
func MountDFS(serverAddr string, localIP string, localPath string) (dfs DFS, err error) {
	var clientID string
	// Obtain the absolute file path
	localPath, _ = filepath.Abs(localPath)
	// Check if localPath exists
	if !LocalPathExists(localPath) {
		return nil, LocalPathError(localPath)
	}

	// Connect to server using RPC
	server, connectionError := rpc.Dial("tcp", serverAddr)
	if connectionError == nil {
		// If able to connect to server, register client ID with server.
		clientID_file_path := filepath.Join(localPath, "client_id")

		// Set up client connection so server can connects to it.
		port, listener, err := Mount(localIP)
		if err != nil {
			return nil, err
		}
		// Resolve localIP address, localIP address does not come with port.
		clientIP_Port := localIP + ":" + port

		if !LocalPathExists(clientID_file_path) {
			// This is a new client
			// Obtain an unique client ID from server
			args := comm.NewClient{
				Ip_port:    clientIP_Port,
				Local_path: localPath}
			var reply string
			err = server.Call("DFS_Server.RegisterClient_New", &args, &reply)
			if err == nil {
				// store client ID to clientID_file_path
				clientID = reply
				WriteClientID(clientID_file_path, clientID)

			} else {
				// Cannot reach DFS_Server.RegisterClient_New
				return nil, DisconnectedError(serverAddr)
			}

		} else {
			// This is an existing client
			// retrieve  client ID from clientID_file_path
			clientID = ReadClientID(clientID_file_path)

			// update server of its potential new IP
			args := comm.ExistingClient{
				Client_id:  clientID,
				Ip_port:    clientIP_Port,
				Local_path: localPath}
			var reply int
			err = server.Call("DFS_Server.RegisterClient_Existing", &args, &reply)

			if err != nil {
				// can't reach DFS_Server.RegisterClient_Existing
				return nil, DisconnectedError(serverAddr)
			}
		}
		dfs = DFS_On{
			Client_id:  clientID,
			Local_path: localPath,
			Server:     server,
			Listener:   listener,
		}

	} else {
		// Cannot connect to server
		// Return DFS in disconnected mode
		dfs = DFS_Off{
			Local_path: localPath}

	}

	return dfs, err
}

// Write cliet ID to file, stored in specified path
// Precondition: valid path
func WriteClientID(path, clientID string) {
	f, err := os.Create(path)
	comm.CheckPrecond(err)
	f.WriteString(clientID)
	f.Sync()
	defer f.Close()
}

// Read client ID from specified path
// Precondition: file path exists
func ReadClientID(path string) string {
	dat, err := ioutil.ReadFile(path)
	comm.CheckPrecond(err)
	return string(dat)
}

// Common fns
// Check if the file pointed by filePath exists locally
func LocalPathExists(filePath string) bool {
	if _, err := os.Stat(filePath); os.IsNotExist(err) {
		// filePath does not exist
		return false
	} else {
		// filePath exists
		return true
	}

}
