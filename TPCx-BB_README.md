Copyright (c) 2015, Intel Corporation.

This Big Data Benchmark for BigBench Specification ("Software") is furnished under license and may only be used or copied in accordance with the terms of that license. No license, express or implied, by estoppel or otherwise, to any intellectual property rights is granted by this document. The Software is subject to change without notice, and should not be construed as a commitment by Intel Corporation to market, license, sell or support any product or technology. Unless otherwise provided for in the license under which this Software is provided, the Software is provided AS IS, with no warranties of any kind, express or implied. Except as expressly permitted by the Software license, neither Intel Corporation nor its suppliers assumes any responsibility or liability for any errors or inaccuracies that may appear herein. Except as expressly permitted by the Software license, no part of the Software may be reproduced, stored in a retrieval system, transmitted in any form, or distributed by any means without the express written consent of Intel Corporation.

UNDER DEVELOPMENT -- Post your questions on Google Groups https://groups.google.com/forum/#!forum/big-bench for help in running the workload.

To collect performance metrics from Hadoop nodes and analyze the resource utilization draw automated charts using MS-Excel, PAT is available for download.

https://github.com/intel-hadoop/PAT

======

This document is a development version and describes the BigBench installation and execution on our test machines.

# Preparation

## Cluster Environment

**HADOOP DISTRIBUTION**

* Version 1.2 is tested on Cloudera CDH 5.8 & Hive on Spark as the primary engine

* Q28 Depending on the Hadoop distribution version can fail automated Engine Validation due to empty space characters when the output is written to HDFS
* Manually open the result file and validate the reference values and written values.

**Java**

Java 1.7 (Oracle/OpenJDK) is required. 64 bit is recommended. A suitable JDK is installed along with Cloudera (if using the parcel installation method)
Other JVM's are not supported.

**Hadoop**

* Hadoop 2.3 >= A suitable hadoop version is installed along with Cloudera (if using the parcel installation method)
* Hive 0.14 >= recommended. A suitable hive version is installed along with Cloudera (if using the parcel installation method)
* Spark 1.6 >= recommended as ML framework. A suitable Spark version may not be included (yet) in your chosen hadoop distribution
* (Deprecated) Mahout 0.9 A suitable mahout version is installed along with Cloudera (if using the parcel installation method)


**Other necessary system packages**

* bash
* findutils
* coreutils
* grep
* tar
* zip
* pssh
* snakebite (optional) faster hdfs client.  https://github.com/spotify/snakebite Only required on your gateway machine from which you run Big-Data-Benchmark-For-Big-Bench. You can activate snakebite (if installed) in: conf/userSettings.conf (end of file).

## Installation

On the SUT, clone the github repository into a folder stored in $INSTALL_DIR:

```
export INSTALL_DIR="$HOME" # adapt this to your location
cd "$INSTALL_DIR"
## *APPLICABLE FOR PUBLIC VERSION ONLY* git clone https://github.com/intel-hadoop/Big-Data-Benchmark-for-Big-Bench.git
```

## Configuration

Check if the hadoop related variables are correctly set in the user settings file:

`vi "$INSTALL_DIR/Big-Bench/conf/userSettings.conf"`

Major settings, Specify your cluster environment:

```
BIG_BENCH_HADOOP_LIBS_NATIVE  (optional but speeds up hdfs access)
BIG_BENCH_HADOOP_CONF         most important: core-site.xml and hdfs-site.xml
```
Minor settings:
```
BIG_BENCH_USER                     the user who is running the benchmark (used to determine the base directory where PDGF stores its data)
BIG_BENCH_DATAGEN_DFS_REPLICATION  replication count used during generation of the big bench table
BIG_BENCH_DATAGEN_HADOOP_JVM_ENV   -Xmx300m is sufficient for one worker per map task, increase if you use more than one worker per map task (by 100m per additional worker)
```
# Run the workload

There are two different methods for running the workload: use the driver to simply perform a complete benchmark run or use the bash scripts to do atomic tasks. As the driver calls the bash scripts internally, both methods yield the same results. Using the bash scripts directly is for experts, only use it if you know what you are doing.

## Common hints

There is a configuration file with variables set by the user. This file is located here:
```
"$INSTALL_DIR/conf/userSettings.conf"
```

Apart from specific environment settings, the user can also specify default values for most benchmark options in this file.


There is a second configuration file,
```
"$INSTALL_DIR/conf/bigBench.properties"
```
which configures the driver. This file has lots of comments about the differnt options, so please look into it for a detailed explanation of its options.



### Accept license

When running the data generator for the first time, the user must accept its license:

```
By using this software you must first agree to our terms of use. Press [ENTER] to show them
... # license is displayed
If you have read and agree to these terms of use, please type (uppercase!): YES and press [ENTER]
YES
```

### Data Generation - with PDGF
The data are being generated parallel directly into HDFS (into the $BIG_BENCH_HDFS_RELATIVE_INIT_DATA_DIR directory, absolute HDFS path is $BIG_BENCH_HDFS_ABSOLUTE_INIT_DATA_DIR).
The degree of parallelism is determined by the driver option "-m" The number of map tasks used for data generation. (default: $BIG_BENCH_DEFAULT_MAP_TASKS)

Default HDFS replication count is 1 (data is only stored on the generating node). 
`BIG_BENCH_DATAGEN_DFS_REPLICATION=<Replication count>'.

Default settings can be changed in the $INSTALL_DIR/conf/userSettings.conf file.

## Using the BigBench driver

The BigBench driver is started with a script. To show all available options, you can call the help first:
```
"$INSTALL_DIR/bin/bigBench" runBenchmark -h
```

### Quick start

If a complete benchmark run should be performed and no data were generated previously, this is the command which should be executed:

```
"$INSTALL_DIR/bin/bigBench" runBenchmark [-m <number of map tasks for data generation>] [-f <scale factor of dataset>] [-s <number of parallel streams in the throughput test>] [-i <benchmark phases to perform>] [-j <queries to run>]
```

This command will generate data, run the load-, power- and throughput-test and calculate the BigBench result.

So a complete benchmark run with all stages can be done by running (e.g., 4 map tasks, scale factor 100, 2 streams):
```
"$INSTALL_DIR/bin/bigBench runBenchmark -m 4 -f 100 -s 2
```

For the format and valid options of the benchmark phases and the query definition, take a look into the driver's properties file:
```
"$INSTALL_DIR/conf/bigBench.properties"
```


If one of the options is omitted, the script uses the default values defined in $INSTALL_DIR/conf/userSettings.conf (BIG_BENCH_DEFAULT_MAP_TASKS, BIG_BENCH_DEFAULT_SCALE_FACTOR, BIG_BENCH_DEFAULT_NUMBER_OF_PARALLEL_STREAMS).

After the benchmark finished, two log files are written: BigBenchResult.txt (which contains the driver's sysout messages) as well as BigBenchTimes.csv (which contains all measured timestamps/durations).

### More detailed explanation

There are multiple phases the driver traverses (only three are benchmarked though): DATA_GENERATION, LOAD_TEST, POWER_TEST, THROUGHPUT_TEST_1. To add or remove phases from execution, modify the workload property in the bigBench,properties file or provide the "-i" option to the runBenchmark module with the appropriate string as argument.

#### Data generation

The data generation phase is not benchmarked by BigBench. The internal benchnark phase is called DATA_GENERATION. Skipping this phase is a good idea if data were already generated previously and the complete benchmark should be repeated with the same dataset size. In that case, generating data is not necessary as PDGF would generate the exact same data as in the previous run. If data generation is not skipped, two options can be provided to the driver: "-m" sets the number of map tasks for PDGF's data generation, "-f" sets the scale factor determining the dataset size (1 scale factor equals 1 GiB).

#### Load test

Population of the engine metastore is the first phase that is benchmarked by BigBench. The driver recognizes the phase as LOAD_TEST. Re-populating the metastore is technically only necessary if the dataset has changed. Nevertheless, metastore population is part of the benchmark, so if this phase is skipped then the overall BigBench result is invalid.

#### Power test

This is the second phase that is benchmarked by BigBench. All queries run sequentially in one stream. The internal phase is called POWER_TEST. Skipping it invalidates the final BigBench result.

#### Throughput test

The throughput test is the last benchmark phase. All queries run in parallel streams in different order. If this phase is not skipped, "-s" can be set specifying the number of parallel streams used in this phase. THROUGPUT_TEST_1 is the internal name for the phase.

#### Query result validation

The query result validation is not benchmarked by BigBench. It will be performed when VALIDATE_POWER_TEST or VALIDATE_THROUGHPUT_TEST_1 is in the phase list of the driver. If a run with scale factor 1 was performed, an exact validation of the query results will be done. When using other scale factors, only basic sanity checks are performed.

#### Engine validation

The engine validation is a special phase which performs a complete circle of data generation, load, power and validation run with scale factor 1 to perform an exact result validation against the scale factor 1 dataset. The phases are called ENGINE_VALIDATION_DATA_GENERATION, ENGINE_VALIDATION_LOAD_TEST, ENGINE_VALIDATION_POWER_TEST and ENGINE_VALIDATION_RESULT_VALIDATION.

## Using the bigBench bash script

The driver internally calls the $BIG_BENCH_BIN_DIR/bigBench bash script along with a module name. So every step the driver performs (apart from the more complicated "query mixing" and multi-stream execution logic) can be run manually by executing this script with the proper options.
WARNING: The driver takes care for the correct option setting and execution order. Lots of options must match for the bash modules to work. Therefore, using the driver is STRICTLY recommended.

### Overview

The general syntax for running the bigBench script is
```
"$INSTALL_DIR/bin/bigBench" $MODULE_NAME [options]
```

There is a global help for the bigBench script available which can be called with
```
"$INSTALL_DIR/bin/bigBench" -h
```
as well as a specific help for each module
```
"$INSTALL_DIR/bin/bigBench" $MODULE_NAME -h
```

### Available options

#### bigBench script options
* -d: Defines the database to use. (default: $BIG_BENCH_DEFAULT_DATABASE)
* -D: Some more complex queries are split into multiple internal parts. This option chooses which internal query part will be executed. This is a developer only option. ONLY USE IF YOU KNOW WHAT YOU ARE DOING
* -e: This option chooses which binary will be used for the benchmark. (default: $BIG_BENCH_DEFAULT_ENGINE)
* -f: The scale factor for PDGF. It is used by the dataGen module. (default: $BIG_BENCH_DEFAULT_SCALE_FACTOR)
* -h: Show help
* -m: The map tasks used for data generation. It is used by the dataGen module. (default: $BIG_BENCH_DEFAULT_MAP_TASKS)
* -p: Defines the benchmark phase to use (default: $BIG_BENCH_DEFAULT_BENCHMARK_PHASE)
* -q: Defines the query number to be executed
* -s: This option defines the number of parallel streams to use. It is only of any use with the runBenchmark and the runQueryInParallel modules. (default: $BIG_BENCH_DEFAULT_NUMBER_OF_PARALLEL_STREAMS)
* -v: Use the provided file as initial metastore population script. (default: $BIG_BENCH_POPULATE_METASTORE_FILE)
* -w: Use the provided file as metastore refresh script. (default: $BIG_BENCH_REFRESH_METASTORE_FILE)
* -y: Use the provided file for custom query parameters. (global: $BIG_BENCH_QUERY_PARAMS_FILE)
* -z: Use the provided file for custom engine settings. (global: $BIG_BENCH_ENGINE_SETTINGS_FILE)

#### Driver specific options
* -a: Only pretend command execution.
* -b: Print stdout of called bash scripts during execution.
* -i phases the driver performs (see $BIG_BENCH_CONF_DIR/bigBench.properties for details on format)
* -j queries the driver runs (see $BIG_BENCH_CONF_DIR/bigBench.properties for details on format)

### Modules usage examples

* cleanData: cleans all data/query results related to BigBench. WARNING: There is no confirmation message, EVERYTHING BigBench related will be purged.
```
"$INSTALL_DIR/bin/bigBench" cleanAll [-h]
```

* cleanData: cleans the dataset directory in HDFS. This module is automatically run by the data generator module to remove the dataset from the HDFS.
```
"$INSTALL_DIR/bin/bigBench" cleanData [-h]
```

* cleanLogs: Cleans the log directory of leftover files from previous runs. Deletes log folders older than a day to free space. It does not touch the zip archives.
```
"$INSTALL_DIR/bin/bigBench" cleanLogs [-h]
```

* cleanMetastore: cleans the metastore dataset tables.
```
"$INSTALL_DIR/bin/bigBench" cleanMetastore [-d <database name>] [-h] [-z <engine settings>]
```

* cleanQuery: cleans metastore tables and result directories in HDFS for one query. Needs the query number to be set.
```
"$INSTALL_DIR/bin/bigBench" cleanQuery [-d <database name>] [-h] [-p <benchmark phase>] -q <query number> [-t <stream number] [-z <engine settings>]
```

* dataGen: generates data using a hadoop job.
```
"$INSTALL_DIR/bin/bigBench" dataGen [-h] [-m <map tasks>] [-f <scale factor>]
```

* populateMetastore: populates the metastore with the dataset tables.
```
"$INSTALL_DIR/bin/bigBench" populateMetastore [-d <database name>] [-h] [-v <population script>] [-z <engine settings>]
```

* refreshMetastore: refreshes the metastore with the refresh dataset.
```
"$INSTALL_DIR/bin/bigBench" refreshMetastore [-d <database name>] [-h] [-w <refresh script>] [-z <engine settings>]
```

* runBenchmark: runs the driver.
```
"$INSTALL_DIR/bin/bigBench" runBenchmark [-d <database name>] [-e <engine name>] [-f <scale factor>] [-h] [-m <map tasks>] [-s <number of parallel streams>] [-v <population script>] [-w <refresh script>] [-y <query parameters>] [-z <engine settings>] [-a] [-b] [-c] [-x] [-1] [-2] [-3] [-4] [-5] [-6] [-7]
```

* runQuery: runs one query. Needs the query number to be set.
```
"$INSTALL_DIR/bin/bigBench" runQuery [-d <database name>] [-D <debug query part] [-h] [-p <benchmark phase>] -q <query number> [-t <stream number] [-y <query parameters>] [-z <engine settings>]
```

* showErrors: parses query errors in the log files after query runs.
```
"$INSTALL_DIR/bin/bigBench" showErrors [-h] [-q]
```

* showTimes: parses execution times in the log files after query runs.
```
"$INSTALL_DIR/bin/bigBench" showTimes [-h] [-q]
```

* showValidation: parses query validation results in the log files after query runs.
```
"$INSTALL_DIR/bin/bigBench" showValidation [-h] [-q]
```

* validateQuery: validates one query. Needs the query number to be set.
```
"$INSTALL_DIR/bin/bigBench" validateQuery [-d <database name>] [-D <debug query part] [-h] [-p <benchmark phase>] -q <query number> [-t <stream number] [-y <query parameters>] [-z <engine settings>]
```

* zipLogs: generates a zip file of all logs in the logs directory. It is run after each complete benchmark run. All log files as well as the environment information zip files and the configuration files are moved into a timestamped subfolder. This subfolder is zipped additionally.
```
"$INSTALL_DIR/bin/bigBench" zipLogs [-h]
```

# FAQ
This benchmark does not favour any platform and we ran this benchmark on many different distributions. But you got to start somewhere.
It is not HIVE specify as well, but hive happens to be the first engine to be implemented.

This FAQ is mostly based on our experiments with Hive on Yarn with CDH 5.x

## DataGeneration stage fails

```
[..]
==========
Please check the log files for details
==============
Benchmark run terminated
Reason: An error occured while running a command
==============
java.io.IOException: Error while generating dataset. More information in logfile: bigbench/logs/dataGeneration-run_query.log
	at io.bigdatabenchmark.v1.driver.BigBench.generateData(BigBench.java:759)
	at io.bigdatabenchmark.v1.driver.BigBench.run(BigBench.java:415)
	at io.bigdatabenchmark.v1.driver.RunBigBench.main(RunBigBench.java:52)
```

The data generation tool stage is the first thing Big-Data-Benchmark-for-Big-Bench executes on your cluster. So its natural that this stage will be the first to hit any miss-configurations and incompatibilities.
* you tried to run with an JDK other then Oracle/OpenJDK 1.7 64bit.
* issues with access rights, locally or on the worker nodes.
* the driver scripts are written for bash. Other shells may goof up. For instance: Mac OS is not compatible.

You may want to check your MR/YARN logs if the data generator job was even started.
If the job was successfully stared but terminated during execution, please manually check the logs from the yarn containers task attempts or similar logs from the worker nodes.


## Where do i put my cluster specific settings?
Here: Big-Bench/conf/userSettings.conf

Where is my core-site.xml/hdfs-site.xml for BIG_BENCH_HADOOP_CONF (usually the one in /etc/hadoop/...):

`find /etc -name "hdfs-site.xml" 2> /dev/null`

Where is my hdfs native libs folder for BIG_BENCH_HADOOP_LIBS_NATIVE?

`find / -name "libhadoop.so" 2> /dev/null`


What is my name node address for BIG_BENCH_HDFS_NAMENODE?

Look inside your hdfs-site.xml and locate this property value:
```
<property>
    <name>dfs.namenode.servicerpc-address</name>
    <value>host.domain:8022</value>
</property>
```

## Where do i put benchmark specific hive options?
Big-Bench/engines/hive/conf/engineSettings.sql

There are already a number of documented settings in there.


## Where do i put query specific hive options?
You can place an optional file "hiveLocalSettings.sql" into a queries folder e.g.:

Big-Bench/engines/hive/queries/q??/hiveLocalSettings.sql

You can put your query specific settings into this file, and the benchmark will automatically load the file. The hiveLocalSettings.sql file gets loaded last, which allows you to override any previously made settings in ,e.g., Big-Bench/engines/hive/conf/engineSettings.sql.
This way your settings are not overwritten by future github updates and there won't be any conflicts when updating the query files.


## Underutilized cluster

### cluster setup
Before "tuning" or asking in the google group, please ensure that your cluster is well configured and able to utilize all resources (cpu/mem/storage/netIO).


There are a lot of things you have to configure, depending on your hadoop distribution and your hardware.
Some important variables regarding MapReduce task performance:
```
mapreduce.reduce.memory.mb
mapreduce.map.memory.mb
mapreduce.map.memory.mb
mapreduce.map.memory.mb
mapreduce.reduce.java.opts;
mapreduce.map.java.opts
mapreduce.map.java.opts
mapreduce.task.io.sort.mb
mapreduce.task.io.sort.mb
...
```

Basically, you may want to have at least as much (yarn) "containers" (container may hold a map or a reduce task) on your cluster as you have CPU cores or hardware threads.
Despite that, you configure your container count based on available memory in your cluster. 1-2GB of memory per container may be a good starting point. For e.g: If you have 64 GB of memory and 32 threads, start with 2GB per container to tune 32 containers per node.

In CDH you can do this with: (just example values! follow a more sophisticated tutorial on how to set up your cluster!):

**Gateway**
Gateway BaseGroup --expand--> Resource management
```
Container_Size (e.g.:  1,5Gb can be sufficient but you may require more if you run into "OutOfMemory" or "GC overhead exceeded" errors while executing this benchmark)
mapreduce.map.memory.mb=Container_Size
mapreduce.reduce.memory.mb=Container_Size
mapreduce.map.java.opts.max.heap =0.75*Container_Size
mapreduce.reduce.java.opts.max.heap =0.75*Container_Size
Client Java Heap Size in Bytes =0.75*Container_Size
```

**Nodemanager**
Nodemanager BaseGroup --expand--> Resource management
```
 -container memory
 how many memory ,all containers together, can allocate (physical "free" resources on nodes)
 - yarn.nodemanager.resource.cpu-vcores (same rules as container memory)
```

**ResourceManager**
ResourceManager BaseGroup --expand--> Resource management
```
 -yarn.scheduler.minimum-allocation-mb  (set to 512mb or 1GB)
 -yarn.scheduler.maximum-allocation-mb  (hint: container memory/container max mem == minimum amount of containers per node)
 -yarn.scheduler.increment-allocation-mb  set to 512MB
 -yarn.scheduler.maximum-allocation-vcores  set to min amount of containers
```

**Dynamic resource pools**
(cluster -> dynamic resource pools -> configuration)
```
If everything runs fine, do not set anything here (no additional restrictions).
If you experience yarn deadlocks (yarn trying to allocate resources, but fails leading to MR-jobs waiting indefinitely for containers)  you may want set a limit.
```


### datagen stage: Tuning the DataGeneration tool

**right settings for number of map tasks (bigBench -m option)**

Short answer:
One map task per virtual CPU/hardware thread is best to utilize all CPU resources.

But settings in your cluster may not allow you executing this number of map tasks in parallel. Basically you can not run more parallel map tasks then available (yarn-) containers.
Another thing to consider when testing on big noisy clusters is the non homogeneous runtime of nodes or node failures. Most mappers may finish long before certain others. To address for this skew in mapper runtime we suggest to set the number of mappers to a multiple (2-3 times) of available containers/threads in your cluster, reducing the runtime of a single mapper and making it cheaper to restart a task.
But be aware that a to short runtime per map task also hurts performance, because launching a task is associated with a considerable amount of overhead. In addition to that, more map tasks produce more intermediate files and thus causing more load for the HDFS namenode.
Try targeting run times per mapper not shorter than 3 minutes.

For a "small cluster" (4nodes á 40 hardware threads) (4Nodes * 40Threads) * 2 = 320 MapTasks may be a good value.


**advanced settings**

If your cluster has more available threads then concurrently runnable containers, your cluster may be CPU underutilized.
In this case you can increase the number of threads available to the data generation tool. The data generation tool will then allocate the specified number of threads per map task.

Please open your Big-Bench/conf/userSettings.conf configuration file and see lines:
  export BIG_BENCH_DATAGEN_HADOOP_JVM_ENV="java -DDEBUG_PRINT_PERIODIC_THREAD_DUMPS=5000 -Xmx300m "
and:
  export BIG_BENCH_DATAGEN_HADOOP_OPTIONS=" -workers 1 -ap 3000 "

You could set

  export BIG_BENCH_DATAGEN_HADOOP_OPTIONS=" -workers 4 -ap 3000 "

telling the data generation tool to use 4 threads per map task.
Note: increasing the number of threads requires larger internal buffers so please add 100Mb of memory to BIG_BENCH_DATAGEN_HADOOP_JVM_ENV per additional thread.

Your final settings for 4 threads per map task should look like this:
  export BIG_BENCH_DATAGEN_HADOOP_JVM_ENV="java -DDEBUG_PRINT_PERIODIC_THREAD_DUMPS=5000 -Xmx600m "
  export BIG_BENCH_DATAGEN_HADOOP_OPTIONS=" -workers 4 -ap 3000 "

### Hive "loading"-stage is slow
The hive loading stage is not only "moving" file in hdfs from the data/ dir into the hive/warehouse.

Big-Bench hive does not work on the plain CSV files, but instead transforms the files into the ORC file format, more efficient and native to hive.
Big-Bench models a set of long running analytic queries. Thus it is more realistic not to store the tables in plain text format but in a optimized fashion.

Transforming data into ORC is a very expensive tasks (index creation, compression, splitting and distributing/replication across the cluster) and loading the tables into hive is done by a single hive job. Since there are 23 distinct tables, hive will always create at least 23 hadoop jobs to do the CSV->ORC processing.

You could test if activating the following options on your cluster work for you:
hive.exec.parallel=true
hive.exec.parallel.thread.number=8

They allow hive to run multiple uncorrelated jobs in parallel (like creating tables). But be warned, this feature is still considered unstable (Hive 0.12).
If you cant modify your hive-site.xml cluster globally, you can uncomment/add these options in:
  Big-Bench/engines/hive/conf/engineSettings.sql
to enable them for the whole benchmark, including the queries.

### Hive Query's are running slow
Unfortunately there is no generic answer to this.
First: this is a long running benchmark with hundreds of distinct mr-Jobs. Each mr-Job has a significant amount of "scheduling" overhead of around ~1minute. So even if you are only processing no data at all, you still have to pay the price of scheduling everything (which is arround 1,5 hours! for a single wave of all 30 queries).
There are several projects trying to reduces this problem like TEZ from the stinger imitative or Hive on Spark or SparkSQL. But with Hive on yarn, there is nothing you can really do about this.

**Enough data (/bigBench -f <SF> option) ?**

Make sure you run your benchmark with a big enough dataset to reduce the ratio of fixed overhead time vs. total runtime. Besides from initial testing your cluster setup, never run with a scaling factor of smaller than 100 ( -f 100 ==100GB). For fat nodes (E.g. Nodes with 128GB+ RAM, 32+ Threads, 24+ HDD's) experiement with 250GB/Node for cluster with 8 DataNodes  2-3TB Datset size is a good starting point.



**Enough map/reduce tasks per query stage ?**


Look in your logs and search for lines like this:
```
Hadoop job information for Stage-2: number of mappers: 1; number of reducers: 1
```

If the number of mappers/reducers is < than your available (yarn) slots or "tasks" you cluster is able to run (Rough estimate:  slots == number of CPU# in your cluster or: TotalClusterRAM/slotMaxMem), the query is not using all your clusters resources.
But don't generalize this. Some stages simply don't have enough data to justify more than 1 map job (e.g. the final stage of a "... limit 100 SORT BY X;" query only has to sort 100 lines).
Or the processed table is just to small (like the time or date table).
Remember that more map/reduce tasks also implies more overhead. So don't overdo it as to much map tasks can hurt performance just like to few tasks.

You can tune some parameters in the hive/engineSettings.sql file.
Hive determines the number of map/reduce tasks based on the tables size. If you have a table of 670MB and set the max.split.size to 67000000 bytes, hive will start 10 map tasks to process this table (or maybe less if hive is able to reduce the dataset by using partitioning/bucketing)

```
set mapred.max.split.size=67108864;
set mapred.min.split.size=1;
set hive.exec.reducers.max=99999;
```
** Example on how to boost your SF1GB runtime **

MR guide (does not apply to TEZ or SPARK)
------------------------------------------------------------------
The most important tuning parameter is  mapreduce.input.fileinputformat.split.maxsize which determines the number of map tasks started for a job.

For SF1 start with rather small values:
* set  values for in \Big-Data-Benchmark-for-Big-Bench\engines\hive\conf\engineSettings.sql to: (4Mb/8MB)
set mapreduce.input.fileinputformat.split.minsize=4198400
set mapreduce.input.fileinputformat.split.maxsize=8396800
set hive.exec.reducers.bytes.per.reducer=8396800

For SF 1000 you may want to start with 64MB or 128MB

NOTICE: you wont get good cluster utilization by running with only SF 1 (=1GB) . Most time spent is from lauching hive/mahout instances (starting of jobs) and preparing and isolating the tests from each other

There is not definitive formula to determine the correct split size settings for YOUR cluster as they are ScaleFactor SF and cluster size dependent.

As a rule of thumb you want hive to start more map jobs then you have vcores in your cluster if you aim to 100% utilize your cpu resources during processing. Approximately 1-4 times the number of vcores is ideal if you can afford to fully saturate your cluster with one job. If you are working on a shared cluster you DONT want to do that and be more conservative about this setting and aim for ~ 1/2 to 1/4 of available vcores.


Look at the logs of some longer running stages of some average running task. If the numbers for mappers and reducers of the first 2-3 stage of a job are smaller then the number of containers, you should decrease the split size.
If the numbers are several times greater then your number of vcores you want to increase the split size, as to much tasks per stage is counterproductive in terms of performance as the start of a maptask has an overhead associate with it.
You have to find a balance between your cluster showing good utilization and not spending most of his time just starting and stopping tasks.

A good query log to start with is query 10, as it is fairly simple.  If your settings are right, query 10 can show a ~98% CPU utilization of your cluster if you are not limited by I/O.

TODO: some example values for sample cluster. We will provide some sample settings for SF 1000 (1TB)


## More detailed log files

The aggregated yarn application log file created for a yarn job contains much more information than the default printout you see on your screen.
This log file is especially helpful to debug child-processes started by hadoop MR-jobs. e.g. java/pyhton scripts in certain streaming api using queries), or the "dataGen" task which executes the data generator program.

To retrieve this log please follow these steps:

In your Big-Bench/logs/ folder files or on screen you will find a line similar to this:

14/06/17 19:40:12 INFO impl.YarnClientImpl: Submitted application application_1403017220075_0022

To extract this line from the log file(s) execute:
```
grep "Submitted application" ${BIG_BENCH_LOGS_DIR}/<log file of interest>.log
```

The important part is the application ID (e.g. application_1403017220075_0022) itself.
Take this ID and request the associate yarn log file using the following command line:
```
yarn logs -applicationId <applicationID>  > yarnApplicationLog.log
```


## Exceptions/Errors you may encounter


### Execution of a MR Stage progresses quickly but then seems to "hang" at ~99%.

This indicates a skew in the data. This means: most reducers handle only very little data, and some (1-2) have to handle most of the data.  This happens if some keys are very frequent in comparison to others.
e.g.: this is the case for user_sk in web_clickstreams. 50% of all clicks have user_sk == NULL (indicating that the click-stream did not result in a purchase).
When a query uses the "distribute by " keyword, hive distributes the workload by this key. This implies that every reducer handles a specific set of keys. The single reducer responsible for the "null" key then effectively has to process >50% of the total workload (as 50% of all keys are null).

We did our best to filter out null keys within the querys, if the null values are irrelevant for the query result.  This does not imply that all hive querys are "skew-free". Hive offers some settings to tune this:
```
set hive.optimize.skewjoin=true;
set hive.optimize.skewjoin.compiletime=true;
set hive.groupby.skewindata=true;
set hive.skewjoin.key=100000;
-- read: https://issues.apache.org/jira/browse/HIVE-5888
```
But be aware that turning on these options will produce worse! running times for data/queries that are not heavily skewed, which is the reason they are disabled by default.


### Execution failed with exit status: 3
```
Execution failed with exit status: 3
FAILED: Execution Error, return code 3 from org.apache.hadoop.hive.ql.exec.mr.MapredLocalTask
```

Hive converted a join into a locally running and faster 'mapjoin', but ran out of memory while doing so.
This is a good thing, because it will significantly increase performance. It works by building a hash table LOCALY (in the vm starting the hive query job)  and distributing this table to the cluster. The downside is: its memory hungry and the default settings in your cluster are probably to low.

There are two ways to address this, the prefered way is A)

A) assign more memory to the local! Hadoop/HIVE JVM client because map-join child jvm will inherit the parents jvm settings

Cloudera:
 * In cloudera manager home, click on "HIVE" service,
 * then on the HIVE service page click on "Configuration"
 * Gateway Default Group --(expand)--> Resource Management -> Client Java Heap Size in Bytes -> to e.g. 4Gb/8GB/...

Hortonworks/Ambari:
Its very well and counterintuitively hidden in
* HDFS Service
* Configuration -> Advanced -> General --(expand)--> "Hadoop maximum Java heap size" -> to e.g. 4Gb/8GB/...

B) Tune hives metric of estimating if joins should be autoconverted
```
set hive.mapjoin.smalltable.filesize;
```
The threshold (in bytes) for the input file size of the small tables; if the file size is smaller than this threshold, it will try to convert the common join into map join.
```
set hive.auto.convert.join.noconditionaltask.size;
```
Whether Hive enables the optimization about converting common join into mapjoin based on the input file size. If this parameter is on, and the sum of size for n-1 of the tables/partitions for an n-way join is smaller than the size specified by hive.auto.convert.join.noconditionaltask.size, the join is directly converted to a mapjoin (there is no conditional task).



### Cannot allocate memory

```
Cannot allocate memory
There is insufficient memory for the Java Runtime Environment to continue.
```

Native memory allocation (malloc) failed to allocate x bytes for committing reserved memory.

Basically your kernel handed out more memory than actually available, in expectants that most programs actually never use (allocate) every last bit of memory they request. Now a program (in this case java) tries to allocate something in its virtual reserved memory area, but the kernel was wrong with his estimation of application memory consumption and there is no physical memory left available to fulfill the applications malloc request.
http://www.oracle.com/technetwork/articles/servers-storage-dev/oom-killer-1911807.html

**WARNING:**
Some "fixes" suggest disabling "vm.overcommit_memory" in the kernel.
If you are already in an "overcommitted" state DO NOT SET sysctl vm.overcommit_memory=2 on the running machine to "cure" it! If you do, you will no longer be able to execute ANY program or shell command, as this would require a memory allocation of which nothing is left. This essentially will deadlock you machine, requiring you to forcefully physically reboot the system.


### java.io.IOException: Exceeded MAX_FAILED_UNIQUE_FETCHES;
```
java.io.IOException: Exceeded MAX_FAILED_UNIQUE_FETCHES;
bailing-out.
```

This cryptic exception basically translates to:
Could not communicate with  node(s). Tried to copy results between nodes but
we failed after to many retries.

Causes:
* some nodes cannot communicate between each other
* disturbed network
* some node terminated


###  Caused by: java.lang.InstantiationException: org.apache.hadoop.hive.ql.parse.ASTNodeOrigin ###
OR

* https://issues.apache.org/jira/browse/HIVE-6765
* https://issues.apache.org/jira/browse/HIVE-5068

###  java.lang.Exception: XMLEncoder: discarding statement XMLEncoder.writeObject(MapredWork);
related to:
* Caused by: java.lang.InstantiationException: org.apache.hadoop.hive.ql.parse.ASTNodeOrigin

```
java.lang.RuntimeException: Cannot serialize object
    at org.apache.hadoop.hive.ql.exec.Utilities$1.exceptionThrown(Utilities.java:652)
Caused by: java.lang.Exception: XMLEncoder: discarding statement XMLEncoder.writeObject(MapredWork);
			...
```

* https://issues.apache.org/jira/browse/HIVE-5068


### FAILED: SemanticException [Error 10016]: Line 7:69 Argument type mismatch '0.0': The expression after ELSE should have the same type as those after THEN: "bigint" is expected but "double" is found
* https://issues.apache.org/jira/browse/HIVE-5825


### Error: GC overhead limit exceeded
```
Diagnostic Messages for this Task:
Error: GC overhead limit exceeded

FAILED: Execution Error, return code 2 from org.apache.hadoop.hive.ql.exec.mr.MapRedTask
```
Not enough (remote) mapper/reducer memory to complete the job.
You have to increase your mapper/reducer job memory limits (and/or yarn container limits).

Please read the chapter **cluster setup** from this FAQ section.

Note that this error is different from:
```
Execution failed with exit status: 3
FAILED: Execution Error, return code 3 from org.apache.hadoop.hive.ql.exec.mr.MapredLocalTask
```
as "Exit status: 3" indicates a memory overflow in the "LOCAL" jvm (the jvm that started your hive task) where as "Error, return code 2" indicates a "REMOTE" problem. (A jvm started by e.g. YARN on a Node to process your job)
