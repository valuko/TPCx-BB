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


-- Identify the stores with flat or declining sales in 3 consecutive months,
-- check if there are any negative reviews regarding these stores available online.

-- Resources
ADD JAR ${env:BIG_BENCH_QUERIES_DIR}/Resources/opennlp-maxent-3.0.3.jar;
ADD JAR ${env:BIG_BENCH_QUERIES_DIR}/Resources/opennlp-tools-1.6.0.jar;
ADD JAR ${env:BIG_BENCH_QUERIES_DIR}/Resources/bigbenchqueriesmr.jar;
CREATE TEMPORARY FUNCTION extract_NegSentiment AS 'io.bigdatabenchmark.v1.queries.q18.NegativeSentimentUDF';

--Result  --------------------------------------------------------------------
--keep result human readable
set hive.exec.compress.output=false;
set hive.exec.compress.output;

-- This query requires parallel orderby for fast and deterministic global ordering of final result
set hive.optimize.sampling.orderby=${hiveconf:bigbench.hive.optimize.sampling.orderby};
set hive.optimize.sampling.orderby.number=${hiveconf:bigbench.hive.optimize.sampling.orderby.number};
set hive.optimize.sampling.orderby.percent=${hiveconf:bigbench.hive.optimize.sampling.orderby.percent};
--debug print
set hive.optimize.sampling.orderby;
set hive.optimize.sampling.orderby.number;
set hive.optimize.sampling.orderby.percent;

--STEP 1: calculate and filter stores with linear regression: stores with flat or declining sales in 3 consecutive months
DROP TABLE IF EXISTS ${hiveconf:TEMP_TABLE1};
CREATE TABLE IF NOT EXISTS ${hiveconf:TEMP_TABLE1} AS
  -- join with "store" table to retrieve store name
  SELECT s.s_store_sk, s.s_store_name
  FROM store s,
  (
    --select ss_store_sk's with flat or declining sales in 3 consecutive months.
    -- linear regression part of stores by analysing store_sales
    SELECT
      temp.ss_store_sk,
      ((count(temp.x) * SUM(temp.xy) - SUM(temp.x) * SUM(temp.y)) / (count(temp.x) * SUM(temp.xx) - SUM(temp.x) * SUM(temp.x)) ) AS slope

      -- intercept not required in further query
      --,(SUM(temp.y) - ((count(temp.x) * SUM(temp.xy) - SUM(temp.x) * SUM(temp.y)) / (count(temp.x) * SUM(temp.xx) - SUM(temp.x) * SUM(temp.x)) ) * SUM(temp.x)) / count(temp.x) as intercept

      -- More detailed version of the "allInOne" formulas for slope and intercept:
      --SUM(temp.x)as sumX,
      --SUM(temp.y)as sumY,
      --SUM(temp.xy)as sumXY,
      --SUM(temp.xx)as sumXSquared,
      --count(temp.x) as N,
      --N * sumXY - sumX * sumY AS numerator,
      --N * sumXSquared - sumX*sumX AS denom
      --numerator / denom as slope,
      --(sumY - slope * sumX) / N as intercept
      --(count(temp.x) * SUM(temp.xy) - SUM(temp.x) * SUM(temp.y)) AS numerator,
      --(count(temp.x) * SUM(temp.xx) - SUM(temp.x) * SUM(temp.x)) AS denom
      --numerator / denom as slope,
      --(sumY - slope * sumX) / N as intercept
    FROM
    (
      SELECT
        s.ss_store_sk,
        s.ss_sold_date_sk AS x,
        SUM(s.ss_net_paid) AS y,
        s.ss_sold_date_sk * SUM(s.ss_net_paid) AS xy,
        s.ss_sold_date_sk*s.ss_sold_date_sk AS xx
      FROM store_sales s
      --select date range
      LEFT SEMI JOIN (
        SELECT d_date_sk
        FROM date_dim d
        WHERE d.d_date >= '${hiveconf:q18_startDate}'
        AND   d.d_date <= '${hiveconf:q18_endDate}'
      ) dd ON ( s.ss_sold_date_sk=dd.d_date_sk )
      GROUP BY s.ss_store_sk, s.ss_sold_date_sk
    ) temp
    GROUP BY temp.ss_store_sk
  ) regression_analysis
  WHERE slope <= 0--flat or declining sales
  AND s.s_store_sk = regression_analysis.ss_store_sk
;

-- STEP 2: reviews regarding these stores
DROP TABLE IF EXISTS ${hiveconf:TEMP_TABLE2};
CREATE TABLE IF NOT EXISTS ${hiveconf:TEMP_TABLE2} AS
SELECT
  CONCAT(s_store_sk,"_", s_store_name ) AS store_ID, --this could be any string we want to identify the store. but as store_sk is just a number and store_name is ambiguous we choose the concatenation of both
  pr_review_date,
  pr_review_content
-- THIS IS A CROSS JOIN! but fortunately the "stores_with_regression" table is very small
FROM product_reviews pr, ${hiveconf:TEMP_TABLE1} stores_with_regression
WHERE locate(lower(stores_with_regression.s_store_name), lower(pr.pr_review_content), 1) >= 1 --find store name in reviews
;


--Prepare result storage
DROP TABLE IF EXISTS ${hiveconf:RESULT_TABLE};
CREATE TABLE ${hiveconf:RESULT_TABLE} (
  s_name         STRING,
  r_date         STRING,
  r_sentence     STRING,
  sentiment      STRING,
  sentiment_word STRING
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n'
STORED AS ${env:BIG_BENCH_hive_default_fileformat_result_table} LOCATION '${hiveconf:RESULT_DIR}';

-- the real query - filter
INSERT INTO TABLE ${hiveconf:RESULT_TABLE}
SELECT s_name, r_date, r_sentence, sentiment, sentiment_word
FROM ( --wrap in additional FROM(), because Sorting/distribute by with UDTF in select clause is not allowed
  --negative sentiment analysis of found reviews for stores with declining sales
  SELECT extract_NegSentiment(store_ID, pr_review_date, pr_review_content) AS ( s_name, r_date, r_sentence, sentiment, sentiment_word )
  FROM ${hiveconf:TEMP_TABLE2}
) extracted
ORDER BY s_name, r_date, r_sentence, sentiment,sentiment_word
--CLUSTER BY instead of ORDER BY does not work to achieve global ordering. e.g. 2 reducers: first reducer will write keys 0,2,4,6.. into file 000000_0 and reducer 2 will write keys 1,3,5,7,.. into file 000000_1.concatenating these files does not produces a deterministic result if number of reducer changes.
--Solution: parallel "order by" as non parallel version only uses a single reducer and we cant use "limit
;


-- Cleanup ----------------------------------------
DROP TABLE IF EXISTS ${hiveconf:TEMP_TABLE1};
DROP TABLE IF EXISTS ${hiveconf:TEMP_TABLE2};
