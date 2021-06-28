# pRDMA


RDMA and NVM provide opportunities for efficient persistent data transmission. while existing RPC research tightly couples data persistence and RPC processing, they fails to leverage the potential of the emerging hardware. To solve this problem, pRDMA proposes persistent RPC designs. Persistent RPCs use several hardware-supported RDMA Flush primitives to decouple the data persisting from the complicated RPC processing. Also, pRDMA implements several RPC transmission models of state-of-the-art RPC work for performance comparison.   
The major contributions are summarized as follows:
* pRDMA designs a set of RNIC hardware-supported RDMA primitives to flush data from the volatile cache of RNICs to the PM. 
* pRDMA implements several durable RPCs based on the proposed RDMA Flush primitives to support remote data persistence and fast failure recovery. 
* pRDMA emulates the performance of RDMA Flush primitives through other existing RDMA primitives, and runs in a real testbed equipped with Intel Optane DC Persistent Memory Modules and InfiniBand networks. 

pRDMA implement the previous RDMA-based RPC transmission models for extensive comparisons. pRDMA can perform with micro-benchmarks and real-world applications, such as compute-intensive PageRank and latency-sensitive YCSB benchmark. Also, the experiments of pRDMA with micro-benchmarks can show the properties of these RPCs under different runtime environments and programming conditions.

More detailed instructions on code compiling, running, and testing are elaborated in the following.

Code Structure, Dependencies, Compiling, and Running
------------
## 1. Code Structure
 1. `pRDMA_client/` contains the source code on client side  
 2. `pRDMA_client/bin` contains the executable files on client side, which are generated from `pRDMA_client/test`, and the datasets for PageRank
 3. `pRDMA_client/include` contains the header files needed by client side   
 4. `pRDMA_client/src` contains the source code of RDMA connection, RDMA RPC task generating and processing  
 5. `pRDMA_client/test` contains the source code of micro-benchmark/macro-benchmark and client initialization  
 6. `client.c`/`mul_client.c`/`single_client.c` are source code for micro-benchmark and `ycsb.c`/`dblp.c`/`word.c`/`enron.c` are source code for macro-benchmark, more detail will be described at `5 Performance Test Section`  
 7. `pRDMA_server/bin` contains the executable files on server side, which are generated from `pRDMA_server/test`  
 8. `pRDMA_server/include` contains the header files needed by server side   
 9. `pRDMA_server/src` contains the source code of RDMA connection, RDMA RPC task processing and space management on NVM   
 10. `pRDMA_server/test` contains the source code of server initialization  
 11. `pRDMA_server/stress` and `pRDMA_client/stress` contains the source code for `5.2.3 CPU Load Test`

## 2. External Dependencies  
Before running pRDMA codes, it's essential that you have already install dependencies listing below.

* g++ (GCC) 4.8.5
* numactl-devel
* ndctl
* [RDMA driver](https://www.mellanox.com/products/infiniband-drivers/linux/mlnx_ofed) (Packages, such as libibverbs/librdmacm/etc. , may need to be installed for RDMA driver)  

## 3. Compiling 
To run pRDMA, the entire project needs to be compiled. The project requires some hardware and software supports and configurations, such as the network setup and NVM use modules.

**3.1 Environment Setting**    

&#160; &#160; &#160; &#160;First, the <kbd>config.xml</kbd> need to be configured according to the run-time environment.  
```
[server/client node] cd bin
[server/client node] vim config.xml
```
&#160; &#160; &#160; &#160;In the <kbd>config.xml</kbd>，the following information should be configured on the client and server sides.  
```
<server>
    <nic_name>ib0</nic_name>        //RDMA Card Name by ifconfig command
    <addr>xxx.xxx.xxx.xxx</addr>        //Server's RDMA Card Address
    <port>39300</port>          //Server's listen port
</server>
```

To use NVM as memory at the server, this project uses NVM in \textit{APP Direct Mode} and configures it as NUMA node using ndctl. The way to use NVM can be referred to a blog [How To Extend Volatile System Memory (RAM) using Persistent Memory on Linux](https://stevescargall.com/2019/07/09/how-to-extend-volatile-system-memory-ram-using-persistent-memory-on-linux/?tdsourcetag=s_pctim_aiomsg). The project use NVM in a NUMA node by default.


**3.2 Compiling** 

&#160; &#160; &#160; &#160; The following commands are used to compile the entire project:
```
[server/client node] mkdir build && cd build
[server/client node] cmake .. && make
```

The executable <kbd>server</kbd> and <kbd>client</kbd> programs are generated in the folder <kbd>bin/</kbd>, and the server and client projects are packaged separately for convenience.

## 4. Running   
The executable files can be found in the directories `pRDMA_client/bin` and `pRDMA_server/bin` directory.   
&#160; &#160; &#160; &#160;Like other RPC systems, the pRDMA server should start at first to serve the client's requests.
```
[server node] cd bin
[server node] ./server
```
&#160; &#160; &#160; &#160;Then, a client can start and issue RPC requests. To run micro-benchmarks, the commands at the client are listed as follows:
```
[client node] cd bin
[client node] ./client [Object Size in Byte] [Number of Accesses] 
```
&#160; &#160; &#160; &#160;For example, the following show the commands to request objects of 64 Byte and access them 500000 times.
```
[client node] ./client 64 500000
```
  
## 5. Performance Test 

**5.1 Macro-benchmark**  
pRDMA can run [PageRank](http://pr.efactory.de/) and [YCSB](https://github.com/basicthinker/YCSB-C) as Macro-benchmarks to evaluate the performance of pRDMA. Since PageRank and YCSB need to be executed with a database, pRDMA extends the native PageRank and YCSB benchmarks to execute them in a RDMA-based distributed persist memory system.


***5.1.1 PageRank***   
pRDMA runs PageRank as compute-intensive application. pRDMA places the graph data in the remote node with NVM and places the generated intermediate data in the client locally.  
For the PageRank algorithm, pRDMA uses different graph datasets as follows. [Word association-2011](http://law.di.unimi.it/webdata/dblp-2011/) contains 10K nodes and 72K edges. [Enron](http://law.di.unimi.it/webdata/enron/) contains 69K nodes and 276K edges. [Dblp-2010](http://law.di.unimi.it/webdata/wordassociation-2011/) contains 326K nodes and 1615K edges.    
&#160; &#160; &#160; &#160;The command to run PageRank :
```
[client node] ./dblp        //Dblp-2010 Dataset
[client node] ./enron       //Enron Dataset
[client node] ./word        //Word Association-2011 Dataset
```
***5.1.2 YCSB***  
pRDMA runs YCSB as a latency-sensitive application, and uses time33 algorithm as the hash function. We extend YCSB-C, a branch to implement RPC communication interfaces.   
&#160; &#160; &#160; &#160;The following commands are used to run different workloads of YCSB:
```
[client node] ./ycsb [workload A-F] [Object Size in Byte] [Number of Accesses]      
```
&#160; &#160; &#160; &#160;For example, the following command performs 300000 accesses to objects of 256 Byte for workload A:
```
[client node] ./ycsb A 256 300000
```  
**5.2 Micro-benchmark**  
The experiments of pRDMA for micro-benchmark show the characteristics of diffident  RPCs under different runtime setups and RDMA communication models.

***5.2.1 Multiple RPC transmission Models***  
pRDMA implements several RPC transmission models of state-of-the-art RPC systems for comparison. To test other transmission models, some macro definitions in file `pRDMA_client/include/dhmp.h` and `pRDMA_server/include/dhmp.h` need to be specified. The following list shows the macro definitions and the corresponding RPC models.


|  RPC Transmission Model   | Macro Definition  |
|  ----  | ----  |
| L5  | #define L5_MODEL |
| RFP  | #define RFP_MODEL |
| octopus  | #define OCTOPUS_MODEL |
| scaleRPC  | #define SCALERPC_MODEL |
| DaRPC  | #define DARPC_MODEL |
| SRFlush-RPC  | #define SRFLUSHRPC_MODEL |
| SFlush-RPC  | #define SFLUSHRPC_MODEL |
| WRFlush-RPC  | #define WRFLUSHRPC_MODEL |
| WFlush-RPC  | #define WFLUSHRPC_MODEL |

***5.2.2 Load of RDMA Networks***  
To evaluate the performance of RPC models under different RDMA network load, the following macro definitions in `pRDMA_client/include/dhmp.h` need to be set.


|  Macro Definition   | Explanation |
|  ----  | ----  |
| #define RDMA_STRESS  | Adding RDMA Network Load |
| #define STRESS_NUM 5  | Adjusting RDMA Network Load Level |

***5.2.3 CPU Load***  
To evaluate the impact of CPU load on performance of RPC models, user needs to run two programs, one is used to generate the background CPU load, and the other runs the RPC client.  
&#160; &#160; &#160; &#160;For example, the following commands are used to evaluate the performance of RPC with high CPU load at the client node:
```
[client node, window 1] numactl --physcpubind=1 ./cpu_client

[client node, window 2] numactl --physcpubind=1 ./client 64 500000
```

***5.2.4 The Number of Concurrent Senders***  
To evaluate the performance of RPC models which use multiple senders to communicate with a single receiver concurrently, the `pRDMA_Client/bin/Nclient.c` file should be used after `pRDMA_Client/bin/client` has been generated.  

&#160; &#160; &#160; &#160;In order to evaluate the capability of concurrent accessing to a single server, the following commands are executed:
```
[client node] vim Nclient.c         

<Nclient.c>
    <#define NUM 10>        //Modify this to adjust the number of concurrent client
    <execl("./client","./client","65536","5",NULL);>    //Modify this to modify execution commands at client
<Nclient.c>

[client node] gcc -o NUM_client Nclient.c
[client node] ./NUM_client
```


