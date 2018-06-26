--
--Copyright (C) 2016 Transaction Processing Performance Council (TPC) and/or
--its contributors.
--
--This file is part of a software package distributed by the TPC.
--
--The contents of this file have been developed by the TPC, and/or have been
--licensed to the TPC under one or more contributor license agreements.
--
-- This file is subject to the terms and conditions outlined in the End-User
-- License Agreement (EULA) which can be found in this distribution (EULA.txt)
-- and is available at the following URL:
-- http://www.tpc.org/TPC_Documents_Current_Versions/txt/EULA.txt
--
--Unless required by applicable law or agreed to in writing, this software
--is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF
--ANY KIND, either express or implied, and the user bears the entire risk as
--to quality and performance as well as the entire cost of service or repair
--in case of defect.  See the EULA for more details.
--

--
--Copyright 2015 Intel Corporation All Rights Reserved.
--
--The source code contained or described herein and all documents related to the source code ("Material") are owned by Intel Corporation or its suppliers or licensors. Title to the Material remains with Intel Corporation or its suppliers and licensors. The Material contains trade secrets and proprietary and confidential information of Intel or its suppliers and licensors. The Material is protected by worldwide copyright and trade secret laws and treaty provisions. No part of the Material may be used, copied, reproduced, modified, published, uploaded, posted, transmitted, distributed, or disclosed in any way without Intel's prior express written permission.
--
--No license under any patent, copyright, trade secret or other intellectual property right is granted to or conferred upon you by disclosure or delivery of the Materials, either expressly, by implication, inducement, estoppel or otherwise. Any license under such intellectual property rights must be express and approved by Intel in writing.

--########### READ ME ################
-- The default way to set hive options is doing it globally for your whole cluster (e.g. cloudera manager, ambari, hive-site.xml, ...)
-- However, if for some reasons you cant or wont change your cluster global config, you can enable hive specific tuning options in this file.
-- Below are listed some commonly used settings. The values you see in this file may not apply to your own cluster! we used some of them on our 3 node (16cores 60gb ram) test instances
--#####################################


--###########################
-- EXECUTION ENGINE
--###########################
-- values: mr, tez, spark
-- set hive.execution.engine=mr;

-- ###########################
-- parallel order by. required by queries:
-- Note the "bigbench." prefix! Actual enabling is query statement specific and 
-- only activated where required to achive a deterministic output.
-- ###########################
set bigbench.hive.optimize.sampling.orderby=true;
set bigbench.hive.optimize.sampling.orderby.number=20000;
set bigbench.hive.optimize.sampling.orderby.percent=0.1;

-- ###########################
-- output and itermediate table settings 
-- ###########################
-- BIG_BENCH_hive_default_fileformat_tmp_table is defined in ../engines/hive/conf/engineSettings.conf
-- all CREATE <NEW/Temporary> TABLES/VIEWS will use the fileformat specified here.
-- "LOAD_STAGE" file format and query result fileformat can be controlled independently.
set hive.default.fileformat=${env:BIG_BENCH_hive_default_fileformat_tmp_table};

-- if you cluster has good cpu's but limited network bandwith, this could speed up the exchange of intermediate results (this option should be turund on if you cluster has high 'net wait i/o%'
-- set hive.exec.compress.intermediate=true;
-- set mapred.map.output.compression.codec=org.apache.hadoop.io.compress.SnappyCodec;

-- default is to keep the created result tables human readable.
-- set hive.exec.compress.output=false;
-- set mapred.output.compression.codec=org.apache.hadoop.io.compress.DefaultCodec;



-- ###########################
-- mappers settings 
-- ###########################
-- Number of mappers used by HIVE, based on table sizes. If you experience underutilization or to much mappers/reducers, you can play with these settings
-- The number of physical files a table consists of is irrelevant for hives metric for estimating number of mappers. (Hive uses HiveCombineInputFormat, joining the files)
-- the following two parameters are most effective in influencing hives estimation of mappers. To low settings may result in to many map tasks, while to high size settings result in to few map tasks and underutilization of the cluster.
-- both extremes are harmful to the performance. For small data set sizes of 1-100GB a good value  for max.split.size may be 134217728 (128MB). As an estimation, take a medium sized table and divide its size by the number of map tasks you need to utilize your cluster.

-- set mapreduce.input.fileinputformat.split.minsize=1048576;
-- set mapreduce.input.fileinputformat.split.maxsize=67108864;

-- ###########################
-- reducer settings 
-- ###########################
-- Number of reducers used by HIVE
-- hives metric for estimating reducers is mostly controlled by the following settings. Node: Some Query functions like count(*) or Distinct will lead to hive always using only 1 reducer
-- 1GB default
-- set hive.exec.reducers.bytes.per.reducer=33554432;

-- ###########################
-- optimizations for joins. 
-- ###########################
-- things like mapjoins are done in memory and require a lot of it
-- README!
-- Hive 0.12 bug, hive ignores  'hive.mapred.local.mem' resulting in out of memory errors in map joins!
-- (more exactly: bug in Hadoop 2.2 where hadoop-env.cmd sets the -xmx parameter multiple times, effectively overriding the user set hive.mapred.local.mem setting. see: https://issues.apache.org/jira/browse/HADOOP-10245
-- There are 3 workarounds: 
-- 1) assign more memory to the local!! Hadoop JVM client (not! mapred.map.memory)-> map-join child vm will inherit the parents jvm settings
-- 2) reduce "hive.smalltable.filesize" to ~1MB (depends on your cluster settings for the local JVM)
-- 3) turn off "hive.auto.convert.join" to prevent hive from converting the join to a mapjoin.

-- MAP join settings:
-- set hive.auto.convert.join.noconditionaltask.size=100000;

-- set hive.auto.convert.join=true;
-- set hive.optimize.mapjoin.mapreduce=true;
-- set hive.mapred.local.mem=1024;
-- default:25MB, max size of tables considered for local in memory map join. Beware! ORC files have only little file size but huge in memory data size! a 25MB ORC easily consumes 512MB.. related: https://issues.apache.org/jira/browse/HIVE-2601
-- set hive.mapjoin.smalltable.filesize=10000; 
-- set hive.mapjoin.localtask.max.memory.usage=0.90;
-- set hive.auto.convert.sortmerge.join=true;
-- set hive.auto.convert.sortmerge.join.noconditionaltask=true;
-- set hive.auto.convert.join.noconditionaltask.size=100000;
-- set hive.optimize.bucketmapjoin=true;
-- set hive.optimize.bucketmapjoin.sortedmerge=false;
-- set hive.optimize.skewjoin=true; --READ FIRST: https://issues.apache.org/jira/browse/HIVE-5888
-- set hive.optimize.skewjoin.compiletime=true;
-- set hive.groupby.skewindata=true;

-- ###########################
-- Other tuning options
-- ###########################
-- exec.parallel is still considered unstable, but has the potential to increase you utilization by running multiple independent stages of a query in parallel
set hive.exec.parallel=true;
set hive.exec.parallel.thread.number=8;
-- set hive.cbo.enable=false;
-- set hive.auto.convert.join=true;
-- set hive.mapred.local.mem=2048;
-- set hive.execution.engine=spark;

-- you should really turn these options on for your whole cluster, not just for bigbench
-- predicate pushdown for ORC-files (eager filtering of columns)
-- set hive.optimize.ppd=true;
-- set hive.optimize.ppd.storage=true;
-- set hive.ppd.recognizetransivity=false;
-- set hive.optimize.index.filter=true;
-- set hive.stats.autogather=true;
-- set hive.auto.convert.sortmerge.join=true;
-- set hive.vectorized.execution.enabled=true;
-- set hive.vectorized.execution.reduce.enabled=true;
-- set hive.cbo.enable=true;
-- set hive.compute.query.using.stats=true;
-- set hive.stats.fetch.column.stats=true;
-- set hive.stats.fetch.partition.stats=true;
-- set hive.script.operator.truncate.env=true;


-- ============================;
-- Print most important properties;
-- ============================;
--exec engine and optimizer
set hive.execution.engine;
set hive.cbo.enable;
set hive.stats.fetch.partition.stats;
set hive.script.operator.truncate.env;
set hive.compute.query.using.stats;
set hive.vectorized.execution.enabled;
set hive.vectorized.execution.reduce.enabled;
set hive.stats.autogather;
--input output
set mapreduce.input.fileinputformat.split.minsize;
set mapreduce.input.fileinputformat.split.maxsize;
set hive.exec.reducers.bytes.per.reducer; 
set hive.exec.reducers.max;
set hive.exec.parallel;
set hive.exec.parallel.thread.number;
set hive.exec.compress.intermediate;
set hive.exec.compress.output;
set mapred.map.output.compression.codec;
set mapred.output.compression.codec;
set hive.default.fileformat;
--join optimizations
set hive.auto.convert.sortmerge.join;
set hive.auto.convert.sortmerge.join.noconditionaltask;
set hive.optimize.bucketmapjoin;
set hive.optimize.bucketmapjoin.sortedmerge;
set hive.auto.convert.join.noconditionaltask.size;
set hive.auto.convert.join;
set hive.optimize.mapjoin.mapreduce;
set hive.mapred.local.mem;
set hive.mapjoin.smalltable.filesize; 
set hive.mapjoin.localtask.max.memory.usage;
set hive.optimize.skewjoin;
set hive.optimize.skewjoin.compiletime;
-- filter optimizations (predicate pushdown to storage level)
set hive.optimize.ppd;
set hive.optimize.ppd.storage;
set hive.ppd.recognizetransivity;
set hive.optimize.index.filter;
--other
set hive.optimize.sampling.orderby;
set hive.optimize.sampling.orderby.number;
set hive.optimize.sampling.orderby.percent;
set bigbench.hive.optimize.sampling.orderby;
set bigbench.hive.optimize.sampling.orderby.number;
set bigbench.hive.optimize.sampling.orderby.percent;
set hive.groupby.skewindata;
set hive.exec.submit.local.task.via.child;

-- Database - DO NOT DELETE OR CHANGE
CREATE DATABASE IF NOT EXISTS ${env:BIG_BENCH_DATABASE};
use ${env:BIG_BENCH_DATABASE};


