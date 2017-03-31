To Compline for native OS 
> make build

To Compile for 16.04 OS
> build/builder.sh make build
> build/builder.sh make install
You will find the binary in $GOPATH/bin/docker_amd64

To run with debug use --vmodule=*=3 flag

To Run Benchmark
1) To run concurrency experiment
	a) Without stop starting cockroachDB accross iterartions
		- run concurrency binary in <PATH>/benchmark/concurrency directory with needed concurrency range
	b) with stop starting cockroachDB accross iterartions
		- enable the function call "concurrency_experiment()" and disable other calls in stop_start.py in <PATH>/benchmark/misc.
		- edit "concurrency_experiment()" function for sleep time and start and stop of experiment
		- run stop_start.py

2) To run contention experiment
	a) Without stop starting cockroachDB accross iterartions
		- run contention binary in <PATH>/benchmark/contention directory with needed contention range and concurrency
	b) with stop starting cockroachDB accross iterartions
		- enable the function call "contention_experiment()" and disable other calls in stop_start.py in <PATH>/benchmark/misc.
		- edit "contention_experiment()" function for sleep time and start and stop of experiment
		- run stop_start.py

3) To run write-read ratio experiment
	- change the function call in <PATH>/benchmark/base/main.go (line 855) to call the appropriate write-read function.
	- comiple (go build) and run the contention experiement with needed contention and concurrency.



