# Distributed File System  
[Full project description](https://www.cs.ubc.ca/~bestchai/teaching/cs416_2017w2/assign2/)  </br></br>
There are two kinds of nodes in the system: clients and a single server. The clients are connected in a star topology to the server: none of the clients interact directly with each other and instead communicate indirectly through the central server. Each client is composed of a DFS client library and an application that imports that library and uses it to interact with the DFS using the DFS API. Each client also has access to local persistent disk storage.  
 
In DFS the server is used for coordination between clients and does not store any application data. All application data live on the clients, specifically on their local disks (to survive client failures). Although clients communicate indirectly through the server, a client does not make assumptions about other clients in the system -- about other clients' identities, how many clients there are at any point in time, which client stores what files, etc. Such client meta-data is stored at the server.  

The DFS system provide serializable file access semantics and gracefully handle:   
(1) joining clients  
(2) failing clients  
(3) clients that access file contents while they are disconnected from the server  

 A file could be opened by clients in three different modes:  
- (READ) mode for reading. In READ mode, the application observes the latest file contents using Read calls. In this mode Write calls return an error.
- (WRITE) mode for reading/writing. In WRITE mode, the application observes the latest file contents using Read and is also able to make modifications using Write calls.
- (DREAD) mode for reading while being potentially disconnected (from the server). In DREAD mode, the application observes potentially stale file contents using Read calls. In this mode Write calls return an error.
