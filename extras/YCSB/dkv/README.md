## Quick Start

This section describes how to run YCSB on DKV. 

#### 1. Start DKV

#### 2. Install Java and Maven

#### 3. Set Up YCSB

Git clone YCSB and compile:

    cd <dkv_root>/extras/YCSB
    mvn -pl site.ycsb:dkv-binding -am clean package

#### 4. Provide DKV Connection Parameters
    
Set host, port, password, and cluster mode in the workload you plan to run. 

- `dkv.addr`

Or, you can set configs with the shell command, EG:

    ./bin/ycsb load dkv -s -P workloads/workloada -p "dkv.addr=127.0.0.1:8080" > outputLoad.txt

#### 5. Load data and run tests

Load the data:

    ./bin/ycsb load dkv -s -P workloads/workloada > outputLoad.txt

Run the workload test:

    ./bin/ycsb run dkv -s -P workloads/workloada > outputRun.txt
