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

HDFS_RESULT_FILE="${RESULT_DIR}/logRegResult.txt"

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
  #step 1.  hive q05.sql    :  Run hive querys to extract the input data
  #step 2.  mahout TrainLogistic  :  Train logistic regression model
  #step 3.  mahout calc log_reg   :  Calculating logistic regression for input data
  #step 4.  mahout dump > hdfs/res:  Converting result and copy result do hdfs query result folder
  #step 5.  hive && hdfs     :  cleanup.sql && hadoop fs rm MH

  RETURN_CODE=0
  if [[ -z "$DEBUG_QUERY_PART" || $DEBUG_QUERY_PART -eq 1 ]] ; then
    echo "========================="
    echo "$QUERY_NAME Step 1/3: Executing hive queries"
    echo "tmp output: ${TEMP_DIR}"
    echo "========================="
    # Write input for k-means into ctable
    runCmdWithErrorCheck runEngineCmd -f "$QUERY_SCRIPT"
    RETURN_CODE=$?
    if [[ $RETURN_CODE -ne 0 ]] ;  then return $RETURN_CODE; fi
  fi
  

  if [[ -z "$DEBUG_QUERY_PART" || $DEBUG_QUERY_PART -eq 2 ]] ; then
    ################################
    #run with spark (default) 
    ################################
    if [[ -z "$BIG_BENCH_ENGINE_HIVE_ML_FRAMEWORK" || "$BIG_BENCH_ENGINE_HIVE_ML_FRAMEWORK" == "spark" ]] ; then
      echo "========================="
      echo "$QUERY_NAME Step 2/3: logistic regression with spark-mllib with direct metastore access"
      echo "========================="
      #io.bigdatabenchmark.v1.queries.q05.LogisticRegression 
      #Options:
      #[-i  | --input <input dir> OR <database>.<table>]
      #[-o  | --output output folder]
      #[-d  | --csvInputDelimiter <delimiter> (only used if load from csv)]
      #[--type LBFGS|SGD]
      #[-it | --iterations iterations]
      #[-l  | --lambda regularizationParameter]
      #[-n  | --numClasses ]
      #[-t  | --convergenceTol ]
      #[-c  | --numCorrections (LBFGS only) ]
      #[-s  | --step-size size (SGD only)]
      #[--fromHiveMetastore true=load from hive table | false=load from csv file]
      #[--saveClassificationResult store classification result into HDFS
      #[--saveMetaInfo store metainfo about classification (cluster centers and clustering quality) into HDFS
      #[-v  | --verbose]
      #Defaults:
      #  step size: 1.0 (only used with --type sgd)
      #  type: LBFGS
      #  iterations: 20
      #  lambda: 0
      #  numClasses: 2
      #  numCorrections: 10
      #  convergenceTol: 1e-5.
      #  fromHiveMetastore: true
      #  saveClassificationResult: true
      #  saveMetaInfo: true
      #  verbose: false
   
      input="--fromHiveMetastore true -i ${BIG_BENCH_DATABASE}.${TEMP_TABLE}"
      parameters="--type LBFGS --step-size 1 --iterations 20 --lambda 0 --numClasses 2 --convergenceTol 1e-5 --numCorrections 10 "
      
      output="${RESULT_DIR}/"

                      echo $BIG_BENCH_ENGINE_HIVE_ML_FRAMEWORK_SPARK_BINARY --class io.bigdatabenchmark.v1.queries.q05.LogisticRegression "$BIG_BENCH_QUERIES_DIR/Resources/bigbench-ml-spark.jar" ${input} -o "${output}/" $parameters --saveClassificationResult false --saveMetaInfo true --verbose false 
      runCmdWithErrorCheck $BIG_BENCH_ENGINE_HIVE_ML_FRAMEWORK_SPARK_BINARY --class io.bigdatabenchmark.v1.queries.q05.LogisticRegression "$BIG_BENCH_QUERIES_DIR/Resources/bigbench-ml-spark.jar" ${input} -o "${output}/" $parameters --saveClassificationResult false --saveMetaInfo true --verbose false 
    
      RETURN_CODE=$?
      if [[ $RETURN_CODE -ne 0 ]] ;  then return $RETURN_CODE; fi

    ################################  
    #run with spark, but use CSV as transport from HIVE -> spark (DEPRECATED)
    ################################
    elif [[ "$BIG_BENCH_ENGINE_HIVE_ML_FRAMEWORK" == "spark-csv" ]] ; then
      echo "========================="
      echo "$QUERY_NAME Step 2/3: logistic regression with spark-mllib using csv as intermediate format (DEPRECATED)"
      echo "========================="
   
      input="--fromHiveMetastore false -i ${TEMP_DIR}/"
      output="${RESULT_DIR}/"

                      echo $BIG_BENCH_ENGINE_HIVE_ML_FRAMEWORK_SPARK_BINARY --class io.bigdatabenchmark.v1.queries.q05.LogisticRegression "$BIG_BENCH_QUERIES_DIR/Resources/bigbench-ml-spark.jar" --csvInputDelimiter ',' ${input} -o "${output}/" --type LBFGS --step-size 1 --iterations 20 --lambda 0 --saveClassificationResult true --saveMetaInfo true --verbose false 
      runCmdWithErrorCheck $BIG_BENCH_ENGINE_HIVE_ML_FRAMEWORK_SPARK_BINARY --class io.bigdatabenchmark.v1.queries.q05.LogisticRegression "$BIG_BENCH_QUERIES_DIR/Resources/bigbench-ml-spark.jar" --csvInputDelimiter ',' ${input} -o "${output}/" --type LBFGS --step-size 1 --iterations 20 --lambda 0 --saveClassificationResult true --saveMetaInfo true --verbose false 
    
      RETURN_CODE=$?
      if [[ $RETURN_CODE -ne 0 ]] ;  then return $RETURN_CODE; fi  
      
    ################################
    # run with mahout (DEPRECATED)
    ################################    
    elif [[ $BIG_BENCH_ENGINE_HIVE_ML_FRAMEWORK == 'mahout' ]] ; then
      echo "========================="
      echo "$QUERY_NAME Step 2/3: log regression with mahout (deprecated)"
      echo "========================="

      TMP_LOG_REG_IN_FILE="`mktemp`"
      TMP_LOG_REG_MODEL_FILE="`mktemp`"

      echo "----------------------------------------------------------"
      echo "$QUERY_NAME Step 2/3 Part 1: Copy hive result to local csv file"
      echo "tmp output: ${TMP_LOG_REG_IN_FILE}"
      echo "----------------------------------------------------------"

      echo "streaming result from hive ..."
      #write header    
      runCmdWithErrorCheck echo '"clicks_in_category","college_education","male","clicks_in_1","clicks_in_2","clicks_in_3","clicks_in_4","clicks_in_5","clicks_in_6","clicks_in_7"' > "${TMP_LOG_REG_IN_FILE}"
      # append hive result
      runCmdWithErrorCheck hadoop fs -cat "${TEMP_DIR}"/* >> "${TMP_LOG_REG_IN_FILE}"
      echo "streaming result from hive ... done"
      echo "sample:"
      echo "size: " `du -bh "${TMP_LOG_REG_IN_FILE}"`
      echo "------"
      head "${TMP_LOG_REG_IN_FILE}"
      echo "..." 

      echo "----------------------------------------------------------"
      echo "$QUERY_NAME Step 2/3 Part 2: Train logistic model"
      echo "tmp output: ${TMP_LOG_REG_MODEL_FILE}"
      echo "----------------------------------------------------------"
      echo mahout trainlogistic --input "$TMP_LOG_REG_IN_FILE" --output "$TMP_LOG_REG_MODEL_FILE" --target clicks_in_category --categories 2 --predictors college_education male clicks_in_1 clicks_in_2 clicks_in_3 clicks_in_4 clicks_in_5 clicks_in_6 clicks_in_7  --types n n n n n n n n n --passes 20 --features 20 --rate 1 --lambda 0.5
      runCmdWithErrorCheck mahout trainlogistic --input "$TMP_LOG_REG_IN_FILE" --output "$TMP_LOG_REG_MODEL_FILE" --target clicks_in_category --categories 2 --predictors college_education male clicks_in_1 clicks_in_2 clicks_in_3 clicks_in_4 clicks_in_5 clicks_in_6 clicks_in_7  --types n n n n n n n n n --passes 20 --features 20 --rate 1 --lambda 0.5  
      RETURN_CODE=$?
      if [[ $RETURN_CODE -ne 0 ]] ;  then return $RETURN_CODE; fi
    

      echo "----------------------------------------------------------"
       echo "$QUERY_NAME Step 2/3 Part 3: Calculating Logistic Regression"
      echo "Command: mahout runlogistic --input $TMP_LOG_REG_IN_FILE --model $TMP_LOG_REG_MODEL_FILE --auc --confusion --quiet"
       echo "output: hdfs://"$HDFS_RESULT_FILE
      echo "----------------------------------------------------------"

      runCmdWithErrorCheck mahout runlogistic --input "$TMP_LOG_REG_IN_FILE" --model "$TMP_LOG_REG_MODEL_FILE" --auc --confusion --quiet 2> /dev/null | grep -A 3 "AUC =" | hadoop fs -copyFromLocal -f - "$HDFS_RESULT_FILE"
      RETURN_CODE=$?
      if [[ $RETURN_CODE -ne 0 ]] ;  then return $RETURN_CODE; fi
    
      echo "----------------------------------------------------------"
      echo "$QUERY_NAME Step 2/3 Part 4: Cleanup tmp files"
      echo "----------------------------------------------------------"
      rm -f "$TMP_LOG_REG_IN_FILE"
      rm -f "$TMP_LOG_REG_MODEL_FILE"

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
}

query_run_clean_method () {
  runCmdWithErrorCheck runEngineCmd -e "DROP TABLE IF EXISTS $TEMP_TABLE; DROP TABLE IF EXISTS $RESULT_TABLE;"
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
