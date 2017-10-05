Here is the description to run benchmarking:

bin/main.go is the main file that runs the benchmarking. It takes as an argument a config file (example can be found in conf/conf.json). The command to run the benchmark is: 
  go run main.go <path to config file>

The db connection URL and other DB details, total number of transaction to run, number of operations per transaction, number of warm up transactions to perform, etc., can be added in the config file. The benchmark has 4 parameters that are configurable:

1. Concurrent transactions: This parameter is used to define the number of transactions that occur concurrently or simultaneously, with a default value of 50. We create 50 threads (or the defined number of threads) and each thread runs a transaction sequentially i.e., for example, if a transaction is performing 3 reads and 2 writes, each of the operation is blocking and the thread executes these operations one after the other. 

2. Contention ratio: In most real world applications, the entire dataset is not uniformly accessed. There will be some subset of data that is accessed more than the rest. To mimic similar behavior in our benchmarking, we defined contention ratio which indicated the percent of contention on a subset of data. The contention ratio 70:30 indicates 70% of the data items will have 30% contention while remaining 30% of the data items will have 70% contention; so the 30% data is accessed 0.7 times more than the remaining 70% of the data. The default contention ratio is 50:50.

3. Read-only ratio: In order to see how the dynamic time-stamp ordering works for various scenarios such as write intensive transactions or read dominant transactions, we introduced 2 more configurable parameters. Read-only ratio defined the ratio or the percent of transactions that perform only read operations. For the default value 50:50, 50\% of the transactions have only read operations and the rest may have write operations based on the parameter defined below. Before beginning a new transacation, we toss a coin with the defined bias and decide whether the transaction is going to be read-only or a read-write one.

4. Read-write ratio: This parameter is at each operation level used to decide if an operation will be a read operation or a write operation. For every read-write trasaction (which is decided on the above parameter), based on the defined read-write ratio, a decision is made for each operation to be either a read or a write operation. The default ratio is 50:50, indicating the in each read-write transaction, 50% of the operations will be read and 50% will be write.


Finally, once the experiment is ready, it creates a log file and adds the log there.
