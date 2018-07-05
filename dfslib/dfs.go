/*

This package specifies the implementation to
the interface specified in dfslib.go

*/

package dfslib

import (
	"../comm"
	"net"
	"net/rpc"
	"os"
	"path/filepath"
	"regexp"
)

/*
	Implementation for DFS's Online mode methods
*/
type DFS_On struct {
	Client_id  string           // client ID assigned by the server
	Local_path string           // client local path
	Server     *rpc.Client      // client for calling rpc
	Listener   *net.TCPListener // Listener to close
}

func (dfs DFS_On) LocalFileExists(fname string) (exists bool, err error) {
	filePath := filepath.Join(dfs.Local_path, fname+".dfs")
	exists = LocalPathExists(filePath)
	_, err = FileName(fname)
	return exists, err
}
func (dfs DFS_On) GlobalFileExists(fname string) (exists bool, err error) {
	var fn comm.FileName
	fn, err = FileName(fname)
	if err != nil {
		return false, err
	}

	err = dfs.Server.Call("DFS_Server.FileExist", &fn, &exists)
	if err != nil {
		err = DisconnectedError(dfs.Client_id + " can't reach DFS_Server.FileExist")
	}

	return exists, err

}

// - OpenWriteConflictError (in WRITE mode)
// - DisconnectedError (in READ,WRITE modes)
// - FileUnavailableError (in READ,WRITE modes)
func (dfs DFS_On) Open(fname string, mode FileMode) (f DFSFile, err error) {
	// Check if filename is valid
	fn, err := FileName(fname)
	if err != nil {
		return nil, err
	}
	switch mode {
	case READ:
		// Check global file exists
		var exists bool
		exists, err = dfs.GlobalFileExists(fname)
		if err != nil {
			return nil, err
		}
		var osFile *os.File
		var versions = make([]int, FileSize)
		if exists {
			// If global file exists, retrieve the file
			osFile, versions, err = RetrieveGlobalFile(dfs, fname)
			if err != nil {
				return nil, err
			}

		} else {
			// If global file does not exist, create empty file
			newFilePath := filepath.Join(dfs.Local_path, fname+".dfs")
			osFile = CreateNewFile(newFilePath)

			// update server that a new file is created
			var chunkMetaPackets = []comm.ChunkMetaPacket{}
			cfi := comm.ClientFileInfo{
				ClientID:         dfs.Client_id,
				FileName:         fn,
				ChunkMetaPackets: &chunkMetaPackets,
			}
			var success bool
			err = dfs.Server.Call("DFS_Server.UpdateClientFileInfo", cfi, &success)
			if err != nil {
				return nil, DisconnectedError("can't reach DFS_Server.UpdateClientFileInfo")
			}
		}
		f = DFSFile_Read{
			ClientID: dfs.Client_id,
			FileName: fn,
			File:     osFile,
			Versions: versions,
			Server:   dfs.Server,
		}

	case WRITE:
		// Check global file exists
		var exists bool
		exists, err = dfs.GlobalFileExists(fname)
		if err != nil {
			return nil, err
		}
		var osFile *os.File
		var versions = make([]int, FileSize)
		if exists {
			// check write lock on the server first
			var lockObtained bool
			args := comm.WriteLockRequest{
				ClientID:  dfs.Client_id,
				ClientPID: os.Getpid(),
				FileName:  fn,
			}
			err = dfs.Server.Call("DFS_Server.RequestWriteLock", args, &lockObtained)
			if err != nil {
				return nil, DisconnectedError(dfs.Client_id + " can't reach DFS_Server.RequestWriteLock")
			}

			if !lockObtained {
				return nil, OpenWriteConflictError(fname)
			} else {
				// If global file exists, and lock is obtained, retrieve the file
				osFile, versions, err = RetrieveGlobalFile(dfs, fname)
				if err != nil {
					return nil, err
				}
			}

		} else {
			// If global file does not exist, create empty file
			newFilePath := filepath.Join(dfs.Local_path, fname+".dfs")
			osFile = CreateNewFile(newFilePath)
			// update server that a new file is created
			var chunkMetaPackets = []comm.ChunkMetaPacket{}
			cfi := comm.ClientFileInfo{
				ClientID:         dfs.Client_id,
				FileName:         fn,
				ChunkMetaPackets: &chunkMetaPackets,
			}
			var success bool
			err = dfs.Server.Call("DFS_Server.UpdateClientFileInfo", cfi, &success)
			if err != nil {
				return nil, DisconnectedError("can't reach DFS_Server.UpdateClientFileInfo")
			}

			// Request Lock
			var lockObtained bool
			args := comm.WriteLockRequest{
				ClientID:  dfs.Client_id,
				ClientPID: os.Getpid(),
				FileName:  fn,
			}
			err = dfs.Server.Call("DFS_Server.RequestWriteLock", args, &lockObtained)
			if err != nil {
				return nil, DisconnectedError(dfs.Client_id + " can't reach DFS_Server.RequestWriteLock")
			}

		}
		f = DFSFile_Write{
			ClientID: dfs.Client_id,
			FileName: fn,
			File:     osFile,
			Versions: versions,
			Server:   dfs.Server,
		}

	case DREAD:
		f, err = RetrieveLocalFile(fname, dfs.Local_path)
	}
	return f, nil
}

func (dfs DFS_On) UMountDFS() (err error) {
	// Deconstructing the contents of DFS_ON
	dfs.Client_id = ""
	dfs.Local_path = ""

	// Close rpc connection to server
	err = dfs.Server.Close()
	if err != nil {
		err = DisconnectedError(err.Error())
	}

	// Close the local server
	dfs.Listener.Close()

	return err
}

/*
	Implementation for DFS's Offline mode methods
*/
type DFS_Off struct {
	Local_path string // client local path
}

func (dfs DFS_Off) LocalFileExists(fname string) (exists bool, err error) {
	filePath := filepath.Join(dfs.Local_path, fname+".dfs")
	exists = LocalPathExists(filePath)
	_, err = FileName(fname)
	return exists, err
}
func (dfs DFS_Off) GlobalFileExists(fname string) (exists bool, err error) {
	err = DisconnectedError("Cannot connect to server in DFS_Off mode")
	return false, err
}
func (dfs DFS_Off) Open(fname string, mode FileMode) (f DFSFile, err error) {
	// Check file name
	_, err = FileName(fname)
	if err != nil {
		return nil, err
	}
	switch mode {
	case READ:
		err = DisconnectedError("READ mode disallowed in DFS_Off")
	case WRITE:
		err = DisconnectedError("WRITE mode disallowed in DFS_Off")
	case DREAD:
		f, err = RetrieveLocalFile(fname, dfs.Local_path)
	}
	return f, err
}

func (dfs DFS_Off) UMountDFS() (err error) {
	// Deconstructing the contents of DFS_Off
	dfs.Local_path = ""

	return nil
}

// Common methods
//

// Retrieve Local File in Read-only mode.
// Return FileDoesNotExistError if file does not exist
func RetrieveLocalFile(fname string, localPath string) (f DFSFile_DRead, err error) {
	filePath := filepath.Join(localPath, fname+".dfs")
	if LocalPathExists(filePath) {
		osFile, err := os.Open(filePath)
		if err != nil {
			err = FileDoesNotExistError(err.Error())
		}
		f = DFSFile_DRead{
			File: osFile}
	} else {
		err = FileDoesNotExistError(fname)
	}

	return f, err

}

// Retrieve File from server
// Precondition: fname is valid file name
// Precondition: GlobalFileExists(fname)
func RetrieveGlobalFile(dfs DFS_On, fname string) (f *os.File, versions []int, err error) {
	versions = make([]int, FileSize)
	newFilePath := filepath.Join(dfs.Local_path, fname+".dfs")
	f = CreateNewFile(newFilePath)
	var chunkMetaPackets = []comm.ChunkMetaPacket{}
	fn, _ := FileName(fname)
	// best effort retrieve file
	var filePackets []comm.ChunkPacket
	err = dfs.Server.Call("DFS_Server.RetrieveFile", fn, &filePackets)
	if err != nil {
		return nil, versions, FileUnavailableError(fname)
	}
	for _, chunkPacket := range filePackets {
		// write filePacket content to the newly created file
		chunkNumber := chunkPacket.ChunkNumber
		chunkVersion := chunkPacket.Version
		f.WriteAt(chunkPacket.Chunk, int64(chunkNumber*ChunkSize))
		f.Sync()

		// Write the chunk version the client has to ChunkMetaPacket
		newMeta := comm.ChunkMetaPacket{
			Version:     chunkVersion,
			ChunkNumber: chunkNumber,
		}
		chunkMetaPackets = append(chunkMetaPackets, newMeta)

		// For client's own reference
		versions[chunkNumber] = chunkVersion
	}

	// update server that this client now contains the file
	cfi := comm.ClientFileInfo{
		ClientID:         dfs.Client_id,
		FileName:         fn,
		ChunkMetaPackets: &chunkMetaPackets,
	}
	var success bool
	err = dfs.Server.Call("DFS_Server.UpdateClientFileInfo", cfi, &success)
	if err != nil {
		return nil, versions, DisconnectedError("can't reach DFS_Server.UpdateClientFileInfo")
	}

	return f, versions, nil
}

// Create a new file locally at specified file path
// Return handle to the file or error
// Precondition: file does not exist yet
// Precondition: valid file name
func CreateNewFile(filePath string) *os.File {
	f, err := os.Create(filePath)
	comm.CheckPrecond(err)
	chunk := make([]byte, ChunkSize*FileSize)
	f.Write(chunk)
	f.Sync()

	return f
}

// File name constructor
// BadFilenameError (if filename contains non alpha-numeric chars or is not 1-16 chars long)
func FileName(name string) (fn comm.FileName, err error) {
	isAlphaNumeric := regexp.MustCompile(`^[a-z0-9]+$`).MatchString

	stringLength := len(name)
	isCorrectLength := stringLength > 0 && stringLength <= 16

	if !isAlphaNumeric(name) {
		err = BadFilenameError(name + " is not alpha-numeric with lower case letters.")
	} else if !isCorrectLength {
		err = BadFilenameError(name + " is not 1-16 chars long.")
	} else {
		fn = comm.FileName(name)
	}

	return fn, err
}
