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


-- TASK
-- Build text classifier for online review sentiment classification (Positive,
-- Negative, Neutral), using 90% of available reviews for training and the remaining
-- 40% for testing. Display classifier accuracy on testing data 
-- and classification result for the 10% testing data: <reviewSK>,<originalRating>,<classificationResult>

-- IMPLEMENTATION NOTICE:
-- All reviews are split as follows:
-- case (pr_review_sk % 5) in
--   1|2|3) => use for training
--   ;;
--   0|4) => use for testing
--   ;;
-- esac

-- The input format for the clustering is:
-- ID of the review, rating of the review (NEG, NEU, POS), review text
-- Fields are separated by tabs
-- Example:
-- 1\tNEU\tThis is a neutral review text\n

-- Query parameters

-- Resources
--ADD FILE ${hiveconf:QUERY_DIR}/mapper_q28.py;

--Result 1 Training table for mahout--------------------------------------------------------------------
--keep result human readable
set hive.exec.compress.output=false;
set hive.exec.compress.output;

DROP TABLE IF EXISTS ${hiveconf:TEMP_TABLE1};
CREATE TABLE ${hiveconf:TEMP_TABLE1} (
  pr_review_sk      BIGINT,
  pr_rating         STRING,
  pr_review_content STRING
)
-- mahout "ToSequenceFile" requires "TAB_SEPARATED" CSV (unlike Kmeans or LogisticRegressionQueries)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' LINES TERMINATED BY '\n'
STORED AS TEXTFILE LOCATION '${hiveconf:TEMP_DIR1}';

--Result 2 Testing table for mahout --------------------------------------------------------------------
--keep result human readable
set hive.exec.compress.output=false;
set hive.exec.compress.output;

DROP TABLE IF EXISTS ${hiveconf:TEMP_TABLE2};
CREATE TABLE ${hiveconf:TEMP_TABLE2} (
  pr_review_sk      BIGINT,
  pr_rating         STRING,
  pr_review_content STRING
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' LINES TERMINATED BY '\n'
STORED AS TEXTFILE LOCATION '${hiveconf:TEMP_DIR2}';

--Split reviews table into training and testing
FROM (
  SELECT
    pr_review_sk,
    pr_review_rating,
    pr_review_content
  FROM product_reviews
  ORDER BY pr_review_sk
)p
INSERT OVERWRITE TABLE ${hiveconf:TEMP_TABLE1}
  SELECT *
  WHERE pmod(pr_review_sk, 10) IN (1,2,3,4,5,6,7,8,9) -- 90% are training
INSERT OVERWRITE TABLE ${hiveconf:TEMP_TABLE2}
  SELECT *
  WHERE pmod(pr_review_sk, 10) IN (0) -- 10% are testing
;
