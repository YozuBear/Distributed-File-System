/*

A trivial application to illustrate how the dfslib library can be used
from an application in assignment 2 for UBC CS 416 2017W2.

Usage:
go run app.go
*/

package main

// Expects dfslib.go to be in the ./dfslib/ dir, relative to
// this app.go file
import "./dfslib"

import "fmt"
import "os"

func main() {
	serverAddr := "127.0.0.1:8080"
	localIP := "127.0.0.1"
	// TODO: change back localPath
	localPath := "/Users/yozubear/Desktop/CPSC416/go/src/as2_g4c9/tmp/dfs-dev/k/"

	//localPath := "/tmp/dfs-dev/"

	// Connect to DFS.
	dfs, err := dfslib.MountDFS(serverAddr, localIP, localPath)

	if checkError(err) != nil {
		// TODO: delete
		fmt.Println("error occured")
		fmt.Println(err)

		return
	}

	fmt.Println("point 1")
	// Close the DFS on exit.
	// Defers are really cool, check out: https://blog.golang.org/defer-panic-and-recover
	defer dfs.UMountDFS()

	// Check if hello.txt file exists in the global DFS.
	exists, err := dfs.GlobalFileExists("helloworld")
	if checkError(err) != nil {
		return
	}
	fmt.Println("point 2")
	if exists {
		fmt.Println("File already exists, mission accomplished")
		return
	}
	fmt.Println("point 3")
	// Open the file (and create it if it does not exist) for writing.
	f, err := dfs.Open("helloworld", dfslib.WRITE)
	if checkError(err) != nil {
		return
	}
	fmt.Println("point 4: open")
	// Close the file on exit.
	defer f.Close()

	// Create a chunk with a string message.
	var chunk dfslib.Chunk
	const str = "Hello friends!"
	copy(chunk[:], str)

	// Write the 0th chunk of the file.
	err = f.Write(0, &chunk)
	if checkError(err) != nil {
		return
	}
	fmt.Println("point 5: write")
	// Read the 0th chunk of the file.
	var chunkR dfslib.Chunk
	err = f.Read(0, &chunkR)
	fmt.Println(chunkR)
	fmt.Println(string(chunkR[:]))
	checkError(err)
}

// If error is non-nil, print it out and return it.
func checkError(err error) error {
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error ", err.Error())
		return err
	}
	return nil
}
