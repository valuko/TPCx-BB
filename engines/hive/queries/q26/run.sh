#!/usr/bin/env bash

#
# Copyright (C) 2016 Transaction Processing Performance Council (TPC) and/or
# its contributors.
#
# This file is part of a software package distributed by the TPC.
#
# The contents of this file have been developed by the TPC, and/or have been
# licensed to the TPC under one or more contributor license agreements.
#
#  This file is subject to the terms and conditions outlined in the End-User
#  License Agreement (EULA) which can be found in this distribution (EULA.txt)
#  and is available at the following URL:
#  http://www.tpc.org/TPC_Documents_Current_Versions/txt/EULA.txt
#
# Unless required by applicable law or agreed to in writing, this software
# is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF
# ANY KIND, either express or implied, and the user bears the entire risk as
# to quality and performance as well as the entire cost of service or repair
# in case of defect.  See the EULA for more details.
#

#
#Copyright 2015 Intel Corporation All Rights Reserved. 
#
#The source code contained or described herein and all documents related to the source code ("Material") are owned by Intel Corporation or its suppliers or licensors. Title to the Material remains with Intel Corporation or its suppliers and licensors. The Material contains trade secrets and proprietary and confidential information of Intel or its suppliers and licensors. The Material is protected by worldwide copyright and trade secret laws and treaty provisions. No part of the Material may be used, copied, reproduced, modified, published, uploaded, posted, transmitted, distributed, or disclosed in any way without Intel's prior express written permission.
#
#No license under any patent, copyright, trade secret or other intellectual property right is granted to or conferred upon you by disclosure or delivery of the Materials, either expressly, by implication, inducement, estoppel or otherwise. Any license under such intellectual property rights must be express and approved by Intel in writing.

HDFS_RESULT_FILE="${RESULT_DIR}/cluster.txt"

query_run_main_method () {
 
 QUERY_SCRIPT="$QUERY_DIR/$QUERY_NAME.sql"
 if [[ $BIG_BENCH_ENGINE_HIVE_ML_FRAMEWORK == 'mahout' || $BIG_BENCH_ENGINE_HIVE_ML_FRAMEWORK == 'spark-csv' ]] ; then
   #store table as CSV - this code path is deprecated 
   QUERY_SCRIPT="$QUERY_DIR/${QUERY_NAME}-mahout.sql"
 fi
 
 if [ ! -r "$QUERY_SCRIPT" ]
  then
    echo "SQL file $QUERY_SCRIPT can not be read."
    exit 1
  fi

  #EXECUTION Plan:
  #step 1.  hive q26.sql    :  Run hive querys to extract kmeans input data
  #step 2.  do clustering 
  #step 3.  hive && hdfs     :  cleanup.sql && hadoop fs rm MH

  MAHOUT_TEMP_DIR="$TEMP_DIR/mahout_temp"

  if [[ -z "$DEBUG_QUERY_PART" || $DEBUG_QUERY_PART -eq 1 ]] ; then
    echo "========================="
    echo "$QUERY_NAME Step 1/3: Executing hive queries"
    echo "tmp output: ${TEMP_DIR}"
    echo "========================="
    # Write input for k-means into temp table
    runCmdWithErrorCheck runEngineCmd -f "$QUERY_SCRIPT"
    RETURN_CODE=$?
    if [[ $RETURN_CODE -ne 0 ]] ;  then return $RETURN_CODE; fi
  fi

  if [[ -z "$DEBUG_QUERY_PART" || $DEBUG_QUERY_PART -eq 2 ]] ; then

    ##########################
    #run with spark (default) 
    ##########################
    if [[ -z "$BIG_BENCH_ENGINE_HIVE_ML_FRAMEWORK" || "$BIG_BENCH_ENGINE_HIVE_ML_FRAMEWORK" == "spark" ]] ; then
        
        input="--fromHiveMetastore true --input ${BIG_BENCH_DATABASE}.${TEMP_TABLE}"
        output="${RESULT_DIR}/"
        cluster_centers=8
        clustering_iterations=20
        initialClusters="" #empty: random initial cluster (fixed seed)
        #initialClusters="--initialClustersFile <file>"        
        echo "========================="
        echo "$QUERY_NAME Step 2/3: Calculating KMeans with spark"
        echo "intput: ${input}"
        echo "result output: $output"
        echo "========================="
       
                        echo $BIG_BENCH_ENGINE_HIVE_ML_FRAMEWORK_SPARK_BINARY  --class io.bigdatabenchmark.v1.queries.KMeansClustering "$BIG_BENCH_QUERIES_DIR/Resources/bigbench-ml-spark.jar" $input --output "$output" --num-clusters $cluster_centers --iterations $clustering_iterations --query-num $QUERY_NAME ${initialClusters} --saveClassificationResult true --saveMetaInfo true --verbose false 
        runCmdWithErrorCheck $BIG_BENCH_ENGINE_HIVE_ML_FRAMEWORK_SPARK_BINARY  --class io.bigdatabenchmark.v1.queries.KMeansClustering "$BIG_BENCH_QUERIES_DIR/Resources/bigbench-ml-spark.jar" $input --output "$output" --num-clusters $cluster_centers --iterations $clustering_iterations --query-num $QUERY_NAME ${initialClusters} --saveClassificationResult true --saveMetaInfo true --verbose false 

        RETURN_CODE=$?
        if [[ $RETURN_CODE -ne 0 ]] ;  then return $RETURN_CODE; fi

    ##########################
    #run with spark, but use CSV as transport from HIVE -> spark (DEPRECATED)
    ##########################
    elif [[ -z "$BIG_BENCH_ENGINE_HIVE_ML_FRAMEWORK" || "$BIG_BENCH_ENGINE_HIVE_ML_FRAMEWORK" == "spark-csv" ]] ; then
      
        input="--fromHiveMetastore false --input ${TEMP_DIR}"
        output="${RESULT_DIR}/"
        cluster_centers=8
        clustering_iterations=20
        initialClusters="" #empty: random initial cluster (fixed seed)
        #initialClusters="--initialClustersFile <file>"        
        echo "========================="
        echo "$QUERY_NAME Step 2/3: Calculating KMeans with spark using CSV as intermediate format(DEPRECATED)"
        echo "intput: ${input}"
        echo "result output: $output"
        echo "========================="
       
                        echo $BIG_BENCH_ENGINE_HIVE_ML_FRAMEWORK_SPARK_BINARY  --class io.bigdatabenchmark.v1.queries.KMeansClustering "$BIG_BENCH_QUERIES_DIR/Resources/bigbench-ml-spark.jar" --csvInputDelimiter ' ' $input --output "$output" --num-clusters $cluster_centers --iterations $clustering_iterations --query-num $QUERY_NAME ${initialClusters} --saveClassificationResult true --saveMetaInfo true --verbose false 
        runCmdWithErrorCheck $BIG_BENCH_ENGINE_HIVE_ML_FRAMEWORK_SPARK_BINARY  --class io.bigdatabenchmark.v1.queries.KMeansClustering "$BIG_BENCH_QUERIES_DIR/Resources/bigbench-ml-spark.jar" --csvInputDelimiter ' ' $input --output "$output" --num-clusters $cluster_centers --iterations $clustering_iterations --query-num $QUERY_NAME ${initialClusters} --saveClassificationResult true --saveMetaInfo true --verbose false 

        RETURN_CODE=$?
        if [[ $RETURN_CODE -ne 0 ]] ;  then return $RETURN_CODE; fi
        
    ##########################
    #run with mahout
    ##########################
    elif [[ $BIG_BENCH_ENGINE_HIVE_ML_FRAMEWORK == 'mahout' ]] ; then
        echo "========================="
        echo "$QUERY_NAME Step 2/3: Calculating KMeans with mahout (DEPRECATED)"
        echo "result output: ${HDFS_RESULT_FILE}"
        echo "========================="

        echo "----------------------------------------------------------"
        echo "$QUERY_NAME Step 2/3: part 1: Generating sparse vectors for mahout"
        echo "Command "mahout org.apache.mahout.clustering.conversion.InputDriver -i "${TEMP_DIR}" -o "${TEMP_DIR}/Vec" -v org.apache.mahout.math.RandomAccessSparseVector #-c UTF-8 
        echo "tmp output: ${TEMP_DIR}/Vec"
        echo "----------------------------------------------------------"
        runCmdWithErrorCheck mahout org.apache.mahout.clustering.conversion.InputDriver -i "${TEMP_DIR}" -o "${TEMP_DIR}/Vec" -v org.apache.mahout.math.RandomAccessSparseVector #-c UTF-8 
        RETURN_CODE=$?
        if [[ $RETURN_CODE -ne 0 ]] ;  then return $RETURN_CODE; fi
      

        echo "----------------------------------------------------------"
        echo "$QUERY_NAME Step 2/3: part 2: Calculating k-means"
        echo "Command "mahout kmeans -i "$TEMP_DIR/Vec" -c "$TEMP_DIR/init-clusters" -o "$TEMP_DIR/kmeans-clusters" -dm org.apache.mahout.common.distance.CosineDistanceMeasure -x 10 -ow -cl  -xm $BIG_BENCH_ENGINE_HIVE_MAHOUT_EXECUTION
        echo "tmp output: $TEMP_DIR/kmeans-clusters"
        echo "----------------------------------------------------------"
        echo "upload initial clusters $QUERY_DIR/initialClusters  to hdfs: $TEMP_DIR/init-clusters"
        hadoop fs -put -f $QUERY_DIR/init-clusters $TEMP_DIR/init-clusters

        runCmdWithErrorCheck mahout kmeans --tempDir "$MAHOUT_TEMP_DIR" -i "$TEMP_DIR/Vec" -c "$TEMP_DIR/init-clusters" -o "$TEMP_DIR/kmeans-clusters" -dm org.apache.mahout.common.distance.CosineDistanceMeasure -x 10 -ow -cl  -xm $BIG_BENCH_ENGINE_HIVE_MAHOUT_EXECUTION
        RETURN_CODE=$?
        if [[ $RETURN_CODE -ne 0 ]] ;  then return $RETURN_CODE; fi
      

        local CLUSTERS_OUT="`mktemp`"
        echo "----------------------------------------------------------"
        echo "$QUERY_NAME Step 2/3: part 3: Converting result and store in hdfs $HDFS_RESULT_FILE"
        echo "command: mahout clusterdump --tempDir \"$MAHOUT_TEMP_DIR\" -i \"$TEMP_DIR\"/kmeans-clusters/clusters-*-final -dm org.apache.mahout.common.distance.CosineDistanceMeasure -of TEXT -o $CLUSTERS_OUT ; hadoop fs -copyFromLocal $CLUSTERS_OUT \"$HDFS_RESULT_FILE\" "
        echo "----------------------------------------------------------"
        runCmdWithErrorCheck mahout clusterdump --tempDir "$MAHOUT_TEMP_DIR" -i "$TEMP_DIR"/kmeans-clusters/clusters-*-final -dm org.apache.mahout.common.distance.CosineDistanceMeasure -of TEXT -o $CLUSTERS_OUT
        RETURN_CODE=$?
        if [[ $RETURN_CODE -ne 0 ]] ;  then return $RETURN_CODE; fi
    
        hadoop fs -copyFromLocal -f $CLUSTERS_OUT "$HDFS_RESULT_FILE"
        RETURN_CODE=$?
        if [[ $RETURN_CODE -ne 0 ]] ;  then return $RETURN_CODE; fi
    
    else
      echo "BIG_BENCH_ENGINE_HIVE_ML_FRAMEWORK parameter has no matching implmentation or was empty: $BIG_BENCH_ENGINE_HIVE_ML_FRAMEWORK  "
      return 1
    fi
  fi      

  if [[ -z "$DEBUG_QUERY_PART" || $DEBUG_QUERY_PART -eq 3 ]] ; then
        echo "========================="
        echo "$QUERY_NAME Step 3/3: Clean up"
        echo "========================="
        runCmdWithErrorCheck runEngineCmd -f "${QUERY_DIR}/cleanup.sql"
        RETURN_CODE=$?
        if [[ $RETURN_CODE -ne 0 ]] ;  then return $RETURN_CODE; fi
        runCmdWithErrorCheck hadoop fs -rm -r -f "$TEMP_DIR"
        RETURN_CODE=$?
        rm -rf "$CLUSTERS_OUT"
        if [[ $RETURN_CODE -ne 0 ]] ;  then return $RETURN_CODE; fi
  fi    
}

query_run_clean_method () {
  runCmdWithErrorCheck runEngineCmd -e "DROP TABLE IF EXISTS $TEMP_TABLE; DROP TABLE IF EXISTS $RESULT_TABLE;"
  runCmdWithErrorCheck hadoop fs -rm -r -f "$HDFS_RESULT_FILE"
  return $?
}

query_run_validate_method () {
  # perform exact result validation if using SF 1, else perform general sanity check
  if [ "$BIG_BENCH_SCALE_FACTOR" -eq 1 ]
  then
    local VALIDATION_PASSED="1"

    if [ ! -f "$VALIDATION_RESULTS_FILENAME" ]
    then
      echo "Golden result set file $VALIDATION_RESULTS_FILENAME not found"
      VALIDATION_PASSED="0"
    fi

    if diff -q "$VALIDATION_RESULTS_FILENAME" <(hadoop fs -cat "$RESULT_DIR/*")
    then
      echo "Validation of $VALIDATION_RESULTS_FILENAME passed: Query returned correct results"
    else
      echo "Validation of $VALIDATION_RESULTS_FILENAME failed: Query returned incorrect results"
      VALIDATION_PASSED="0"
    fi
    if [ "$VALIDATION_PASSED" -eq 1 ]
    then
      echo "Validation passed: Query results are OK"
    else
      echo "Validation failed: Query results are not OK"
      return 1
    fi
  else
    if [ `hadoop fs -cat "$RESULT_DIR/*" | head -n 10 | wc -l` -ge 1 ]
    then
      echo "Validation passed: Query returned results"
    else
      echo "Validation failed: Query did not return results"
      return 1
    fi
  fi
}
