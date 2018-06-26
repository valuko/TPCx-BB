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

TEMP_TABLE1="${TEMP_TABLE}_training"
TEMP_DIR1="$TEMP_DIR/$TEMP_TABLE1"
TEMP_TABLE2="${TEMP_TABLE}_testing"
TEMP_DIR2="$TEMP_DIR/$TEMP_TABLE2"

BINARY_PARAMS+=(--hiveconf TEMP_TABLE1=$TEMP_TABLE1 --hiveconf TEMP_DIR1=$TEMP_DIR1 --hiveconf TEMP_TABLE2=$TEMP_TABLE2 --hiveconf TEMP_DIR2=$TEMP_DIR2)

HDFS_RESULT_FILE="${RESULT_DIR}/classifierResult.txt"
##HDFS_RAW_RESULT_FILE="${RESULT_DIR}/classifierResult_raw.txt"

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

  if [[ -z "$DEBUG_QUERY_PART" || $DEBUG_QUERY_PART -eq 1 ]] ; then
    echo "========================="
    echo "$QUERY_NAME step 1/3: Executing hive queries"
    echo "tmp result1 (training data) in:" ${TEMP_DIR1}
    echo "tmp result2 (test data)     in:" ${TEMP_DIR2}
    echo "========================="

    # Write input for k-means into temp tables
    runCmdWithErrorCheck runEngineCmd -f "$QUERY_SCRIPT"
    RETURN_CODE=$?
    if [[ $RETURN_CODE -ne 0 ]] ;  then return $RETURN_CODE; fi
  fi

  SEQ_FILE_1="$TEMP_DIR/Seq1"
  SEQ_FILE_2="$TEMP_DIR/Seq2"
  VEC_FILE_1="$TEMP_DIR/Vec1"
  VEC_FILE_2="$TEMP_DIR/Vec2"

  if [[ -z "$DEBUG_QUERY_PART" || $DEBUG_QUERY_PART -eq 2 ]] ; then

    ##########################
    #run with spark (default) 
    ##########################
    if [[ -z "$BIG_BENCH_ENGINE_HIVE_ML_FRAMEWORK" || "$BIG_BENCH_ENGINE_HIVE_ML_FRAMEWORK" == "spark" ]] ; then

      # if pre-split by hive:
      inputTableTraining="${BIG_BENCH_DATABASE}.${TEMP_TABLE1}"
      inputTableTesting="${BIG_BENCH_DATABASE}.${TEMP_TABLE2}"
      input="--fromHiveMetastore true --inputTraining ${inputTableTraining} --inputTesting ${inputTableTesting} --lambda 0"
      #not pre-split by hive. let spark split
      #inputTableTraining="${BIG_BENCH_DATABASE}.${TEMP_TABLE}"
      #input="--fromHiveMetastore true --inputTraining ${inputTableTraining} --training-ratio 0.9 --lambda 0"
      
      output="${RESULT_DIR}/"
      
      echo "========================="
      echo "$QUERY_NAME step 2/3: Train and Test Naive Bayes Classifier with spark - read from metastore table"
      echo "training and testing data:" ${inputTableTraining}
      echo "test data    :" ${TEMP_DIR2}
      echo "OUT: $RESULT_DIR"
      echo "========================="

      
                      echo $BIG_BENCH_ENGINE_HIVE_ML_FRAMEWORK_SPARK_BINARY --class io.bigdatabenchmark.v1.queries.q28.NaiveBayesClassifier "$BIG_BENCH_QUERIES_DIR/Resources/bigbench-ml-spark.jar" ${input} --output ${output} --saveClassificationResult true --saveMetaInfo true --verbose false
      runCmdWithErrorCheck $BIG_BENCH_ENGINE_HIVE_ML_FRAMEWORK_SPARK_BINARY --class io.bigdatabenchmark.v1.queries.q28.NaiveBayesClassifier "$BIG_BENCH_QUERIES_DIR/Resources/bigbench-ml-spark.jar" ${input} --output ${output} --saveClassificationResult true --saveMetaInfo true --verbose false

      RETURN_CODE=$?
      if [[ $RETURN_CODE -ne 0 ]] ;  then return $RETURN_CODE; fi
      
    ##########################
    #run with spark, but use CSV as transport from HIVE -> spark (DEPRECATED)
    ##########################
    elif [[ -z "$BIG_BENCH_ENGINE_HIVE_ML_FRAMEWORK" || "$BIG_BENCH_ENGINE_HIVE_ML_FRAMEWORK" == "spark-csv" ]] ; then
      
      csvDelimiter=`echo -e "\t"` # tab separated 
      
      # if pre-split by hive:
      inputTableTraining="${TEMP_DIR1}/"
      inputTableTesting="${TEMP_DIR2}/"
      input="--fromHiveMetastore false --inputTraining ${inputTableTraining} --inputTesting ${inputTableTesting} --lambda 0"

      #not pre-split by hive. let spark split
      #inputTableTraining="${BIG_BENCH_DATABASE}.${TEMP_TABLE}"
      #input="--fromHiveMetastore true --inputTraining ${inputTableTraining} --training-ratio 0.9 --lambda 0"
      
      output="${RESULT_DIR}/"
      
      echo "========================="
      echo "$QUERY_NAME step 2/3: Train and Test Naive Bayes Classifier with spark using CSV as intermediate format(DEPRECATED)"
      echo "training data:" ${TEMP_DIR1}
      echo "test data    :" ${TEMP_DIR2}
      echo "OUT: $output"
      echo "========================="

      
                      echo $BIG_BENCH_ENGINE_HIVE_ML_FRAMEWORK_SPARK_BINARY --class io.bigdatabenchmark.v1.queries.q28.NaiveBayesClassifier "$BIG_BENCH_QUERIES_DIR/Resources/bigbench-ml-spark.jar"  --csvInputDelimiter "${csvDelimiter}" ${input} --output ${output} --saveClassificationResult true --saveMetaInfo true --verbose false
      runCmdWithErrorCheck $BIG_BENCH_ENGINE_HIVE_ML_FRAMEWORK_SPARK_BINARY --class io.bigdatabenchmark.v1.queries.q28.NaiveBayesClassifier "$BIG_BENCH_QUERIES_DIR/Resources/bigbench-ml-spark.jar"  --csvInputDelimiter "${csvDelimiter}" ${input} --output ${output} --saveClassificationResult true --saveMetaInfo true --verbose false

      RETURN_CODE=$?
      if [[ $RETURN_CODE -ne 0 ]] ;  then return $RETURN_CODE; fi
    ##########################
    #run with mahout (DEPRECATED)
    ##########################
    elif [[ $BIG_BENCH_ENGINE_HIVE_ML_FRAMEWORK == 'mahout' ]] ; then

        MAHOUT_TEMP_DIR="$TEMP_DIR/mahout_temp"
        echo "========================="
        echo "$QUERY_NAME step 2/3: Train and Test Naive Bayes Classifier with mahout (DEPRECATED)"
        echo "training data:" ${TEMP_DIR1}
        echo "test data    :" ${TEMP_DIR2}
        echo "OUT: $HDFS_RESULT_FILE"
        echo "========================="

        echo "----------------------------------------------------------"
        echo "$QUERY_NAME step 2/3: part 1: Generating sequence files"
        echo "training data:" ${TEMP_DIR1}
        echo "test data    :" ${TEMP_DIR2}
        echo "Used Command: hadoop jar \"${BIG_BENCH_QUERIES_DIR}/Resources/bigbenchqueriesmr.jar\" io.bigdatabenchmark.v1.queries.q28.ToSequenceFile \"${TEMP_DIR1}\" \"$SEQ_FILE_1\""
        echo "Used Command: hadoop jar \"${BIG_BENCH_QUERIES_DIR}/Resources/bigbenchqueriesmr.jar\" io.bigdatabenchmark.v1.queries.q28.ToSequenceFile \"${TEMP_DIR2}\" \"$SEQ_FILE_2\""
        echo "tmp result in: $SEQ_FILE_1"
        echo "tmp result in: $SEQ_FILE_2"
        echo "----------------------------------------------------------"
        runCmdWithErrorCheck hadoop jar "${BIG_BENCH_QUERIES_DIR}/Resources/bigbenchqueriesmr.jar" io.bigdatabenchmark.v1.queries.q28.ToSequenceFile "${TEMP_DIR1}" "$SEQ_FILE_1"
        RETURN_CODE=$?
        if [[ $RETURN_CODE -ne 0 ]] ;  then return $RETURN_CODE; fi
        runCmdWithErrorCheck hadoop jar "${BIG_BENCH_QUERIES_DIR}/Resources/bigbenchqueriesmr.jar" io.bigdatabenchmark.v1.queries.q28.ToSequenceFile "${TEMP_DIR2}" "$SEQ_FILE_2"
        RETURN_CODE=$?
        if [[ $RETURN_CODE -ne 0 ]] ;  then return $RETURN_CODE; fi


        echo "----------------------------------------------------------"
        echo "$QUERY_NAME step 2/3: part 2: Generating sparse vectors from sequence files"
        echo "Used Command: mahout seq2sparse -i \"$SEQ_FILE_1\" -o \"$VEC_FILE_1\" -ow -lnorm -nv -wt tfidf"
         echo "Used Command: mahout seq2sparse -i \"$SEQ_FILE_2\" -o \"$VEC_FILE_2\" -ow -lnorm -nv -wt tfidf"
        echo "tmp result in: $VEC_FILE_1"
        echo "tmp result in: $VEC_FILE_2"
        echo "----------------------------------------------------------"
        runCmdWithErrorCheck mahout seq2sparse -i "$SEQ_FILE_1" -o "$VEC_FILE_1" -ow -lnorm -nv -wt tfidf
        RETURN_CODE=$?
        if [[ $RETURN_CODE -ne 0 ]] ;  then return $RETURN_CODE; fi
        runCmdWithErrorCheck mahout seq2sparse -i "$SEQ_FILE_2" -o "$VEC_FILE_2" -ow -lnorm -nv -wt tfidf
        RETURN_CODE=$?
        if [[ $RETURN_CODE -ne 0 ]] ;  then return $RETURN_CODE; fi

        echo "----------------------------------------------------------"
        echo "$QUERY_NAME step 2/3: part 3: Training Classifier"
        echo "Used Command:  mahout trainnb --tempDir \"$MAHOUT_TEMP_DIR\" -i \"$VEC_FILE_1/tfidf-vectors\" -o \"$TEMP_DIR/model\" -el -li \"$TEMP_DIR/labelindex\" -ow"
        echo "tmp result in: $TEMP_DIR/model"
        echo "----------------------------------------------------------"
        runCmdWithErrorCheck mahout trainnb --tempDir "$MAHOUT_TEMP_DIR" -i "$VEC_FILE_1/tfidf-vectors" -o "$TEMP_DIR/model" -el -li "$TEMP_DIR/labelindex" -ow
        RETURN_CODE=$?
        if [[ $RETURN_CODE -ne 0 ]] ;  then return $RETURN_CODE; fi
     

        echo "----------------------------------------------------------"
        echo "$QUERY_NAME step 2/3: part 4: Testing Classifier"
        echo "Used Command:  mahout testnb --tempDir \"$MAHOUT_TEMP_DIR\" -i \"$VEC_FILE_2/tfidf-vectors\" -m \"$TEMP_DIR/model\" -l \"$TEMP_DIR/labelindex\" -ow -o \"$TEMP_DIR/result\" "
        echo "tmp result in: $TEMP_DIR/result"
        echo "----------------------------------------------------------"

        runCmdWithErrorCheck mahout testnb --tempDir "$MAHOUT_TEMP_DIR" -i "$VEC_FILE_2/tfidf-vectors" -m "$TEMP_DIR/model" -l "$TEMP_DIR/labelindex" -ow -o "$TEMP_DIR/result" |& tee >( grep -A 300 "Standard NB Results:" | hadoop fs  -copyFromLocal -f - "$HDFS_RESULT_FILE" )
        RETURN_CODE=$?
        if [[ $RETURN_CODE -ne 0 ]] ;  then return $RETURN_CODE; fi

       # echo "----------------------------------------------------------"
       # echo "$QUERY_NAME step 2/3: part 5: dump result to hdfs"
       # echo 'Used Command: mahout seqdumper --tempDir "$MAHOUT_TEMP_DIR" -i "$TEMP_DIR/result/part-m-00000" | hadoop fs -copyFromLocal -f - "$HDFS_RAW_RESULT_FILE"'
       # echo "IN: $TEMP_DIR/result/part-m-00000"
       # echo "OUT: $HDFS_RAW_RESULT_FILE"
       # echo "----------------------------------------------------------"
       # runCmdWithErrorCheck mahout seqdumper --tempDir "$MAHOUT_TEMP_DIR" -i "$TEMP_DIR/result/part-m-00000" | hadoop fs -copyFromLocal -f - "$HDFS_RAW_RESULT_FILE"
       # RETURN_CODE=$?
       # if [[ $RETURN_CODE -ne 0 ]] ;  then return $RETURN_CODE; fi


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
    if [[ $RETURN_CODE -ne 0 ]] ;  then return $RETURN_CODE; fi
  fi

  #echo "========================="
  #echo "to display : hadoop fs -cat $HDFS_RESULT_FILE"
  #echo "to display raw : hadoop fs -cat $HDFS_RAW_RESULT_FILE"
  #echo "========================="
}

query_run_clean_method () {
  runCmdWithErrorCheck runEngineCmd -e "DROP TABLE IF EXISTS $TEMP_TABLE1; DROP TABLE IF EXISTS $TEMP_TABLE2; DROP TABLE IF EXISTS $RESULT_TABLE;"
  runCmdWithErrorCheck hadoop fs -rm -r -f "$HDFS_RESULT_FILE"
  #runCmdWithErrorCheck hadoop fs -rm -r -f "$HDFS_RAW_RESULT_FILE"
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

