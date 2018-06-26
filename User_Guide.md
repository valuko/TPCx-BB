1.	Get your cluster ready with HW/SW 
2. 	Setup Passwordless ssh for pssh to work.
3.	Edit "userSettings.conf" and insert your intended variables.
		export BIG_BENCH_DEFAULT_DATABASE="bigbench" */ Name of Hive Metastore */
		export BIG_BENCH_DEFAULT_ENGINE="hive"		*/ Default framework Engine */
		export BIG_BENCH_DEFAULT_MAP_TASKS="80"		*/ Number of map tasks to generate data, read extended readme on selecting one * /
		export BIG_BENCH_DEFAULT_SCALE_FACTOR="10"	*/ Scale Factor you would like to test 1000=1TB, 3000=3TB, 10000=10TB, 30000=30TB, 100000=100TB, 300000=300TB, 1000000=1PB*/
		export BIG_BENCH_DEFAULT_NUMBER_OF_PARALLEL_STREAMS="2"	*/Number of concurrent streams to use during throughput phase, default is 2 */
		export BIG_BENCH_DEFAULT_BENCHMARK_PHASE="run_query"
		export BIG_BENCH_HADOOP_CONF="/etc/hadoop/conf.cloudera.hdfs" */ Adjust this to whatever distrubution of Hadoop you are using */
		export BIG_BENCH_HADOOP_LIBS_NATIVE="/opt/cloudera/parcels/CDH/lib/hadoop/lib/native" */ Adjust this to whatever distrubution of Hadoop you are using */
		export BIG_BENCH_DATAGEN_DFS_REPLICATION="1" */ Use this setting to select number of replicas for your generated data on your HDFS file system, "1" means no replica, "3" is HDFS default of 3 copies. WARNING:* This setting has no bearing on default HDFS replication for all other files, which is set to 3"
		export BIG_BENCH_STOP_AFTER_FAILURE="1" */ the default behaviour is to stop when a query error occurs, set this to 0 to keep on running when an error occurs*/
4.	Run	$/bin/TPCxBB_Validation.sh */ Engine Validation Phase on SF1" 
5.	Run $/TPCxBB_Benchmarkrun.sh	*/To execute the Benchmark, run twice to obtain "Performance" and "Repeatibility" numbers.
6.	Copy 3 set of logs from $../logs 1. From Engine Validation Phase logs. 2. Performance run logs. 3. Repeatability run logs.  E.g logfile from one run:logs-20151029-135147-hive-sf1xxx.zip
7. 	Engage the auditor and submit the report for publication. 

Please contact bhaskar.gowda@intel.com for any questions.
