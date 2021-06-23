## Quick Start

This section describes how to run YCSB on DKV. 

#### 1. Start DKV

#### 2. Install Java and Maven

#### 3. Set Up YCSB

```bash
cd <dkv_root>/extras/YCSB
mvn -pl site.ycsb:dkv-binding -am clean package
```

#### 4. Provide DKV Connection Parameters
    
- Create a `shard.json` file with host, port, password, and cluster mode for the workload you plan to run. 


#### 5. Load data and run tests

Load the data:

```bash
./bin/ycsb load dkv -s -P workloads/workloada -p "dkv.conf=./dkv/src/test/resources/single_shard.json" 
```

Run the workload test:

```bash
./bin/ycsb run dkv -s -P workloads/workloada -p "dkv.conf=./dkv/src/test/resources/single_shard.json"
```


