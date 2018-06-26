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


-- Perform category affinity analysis for products viewed together online.
-- Note that the order of products viewed does not matter,
-- and "viewed together" relates to a click_session of a user with a session timeout of 60min.
-- If the duration between two clicks of a user is greater then the session time-out, a new session begins.


-- IMPLEMENTATION NOTICE:
-- "Market basket analysis"
-- First difficult part is to create pairs of "viewed together" items within one sale
-- There are are several ways to to "basketing", implemented is way A)
-- A) collect distinct viewed items per session (same sales_sk) in list and employ a UDTF to produce pairwise combinations of all list elements
-- B) distribute by sales_sk and employ reducer streaming script to aggregate all items per session and produce the pairs
-- C) pure SQL: produce pairings by self joining on sales_sk and filtering out left.item_sk < right.item_sk
--
-- The second difficulty is to reconstruct a users browsing session from the web_clickstreams table
-- There are are several ways to to "sessionize", common to all is the creation of a unique virtual time stamp from the date and time serial
-- key's as we know they are both strictly monotonic increasing in order of time: (wcs_click_date_sk*24*60*60 + wcs_click_time_sk)
-- A) sessionize using SQL-windowing functions => partition by user and sort by virtual time stamp.
--    Step1: compute time difference to preceding click_session
--    Step2: compute session id per user by defining a session as: clicks no father apart then q02_session_timeout_inSec
--    Step3: make unique session identifier <user_sk>_<user_session_ID>
-- B) sessionize by clustering on user_sk and sorting by virtual time stamp then feeding the output through a external reducer script
--    which linearly iterates over the rows,  keeps track of session id's per user based on the specified session time-out and makes the unique session identifier <user_sk>_<user_seesion_ID>


-- Resources
ADD JAR ${env:BIG_BENCH_QUERIES_DIR}/Resources/bigbenchqueriesmr.jar;
CREATE TEMPORARY FUNCTION makePairs AS 'io.bigdatabenchmark.v1.queries.udf.PairwiseUDTF';
ADD FILE ${hiveconf:QUERY_DIR}/q30-sessionize.py;


-- SESSIONZIE by streaming
-- Step1: compute time difference to preceding click_session
-- Step2: compute session id per user by defining a session as: clicks no father appart then q30_session_timeout_inSec
-- Step3: make unique session identifier <user_sk>_<user_seesion_ID>
DROP VIEW IF EXISTS ${hiveconf:TEMP_TABLE};
CREATE VIEW ${hiveconf:TEMP_TABLE} AS
SELECT *
FROM (
  FROM (
    SELECT wcs_user_sk,
      (wcs_click_date_sk*24L*60L*60L + wcs_click_time_sk) AS tstamp_inSec,
      i_category_id
    FROM web_clickstreams wcs, item i
    WHERE wcs.wcs_item_sk = i.i_item_sk
    AND i.i_category_id IS NOT NULL
    AND wcs.wcs_user_sk IS NOT NULL
    --Hive uses the columns in Distribute By to distribute the rows among reducers. All rows with the same Distribute By columns will go to the same reducer. However, Distribute By does not guarantee clustering or sorting properties on the distributed keys.
    DISTRIBUTE BY wcs_user_sk SORT BY wcs_user_sk, tstamp_inSec, i_category_id -- "sessionize" reducer script requires the cluster by uid and sort by tstamp ASCENDING
  ) clicksAnWebPageType
  REDUCE
    wcs_user_sk,
    tstamp_inSec,
    i_category_id
  USING 'python q30-sessionize.py ${hiveconf:q30_session_timeout_inSec}'
  AS (
    category_id BIGINT,
    sessionid STRING
  )
) q02_tmp_sessionize
Cluster by sessionid
;


--Result -------------------------------------------------------------------------
--keep result human readable
set hive.exec.compress.output=false;
set hive.exec.compress.output;
--CREATE RESULT TABLE. Store query result externally in output_dir/qXXresult/
DROP TABLE IF EXISTS ${hiveconf:RESULT_TABLE};
CREATE TABLE ${hiveconf:RESULT_TABLE} (
  category_id_1 BIGINT,
  category_id_2 BIGINT,
  cnt BIGINT
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n'
STORED AS ${env:BIG_BENCH_hive_default_fileformat_result_table} LOCATION '${hiveconf:RESULT_DIR}';


-- the real query part

INSERT INTO TABLE ${hiveconf:RESULT_TABLE}
SELECT  category_id_1, category_id_2, COUNT (*) AS cnt
FROM (
  -- Make category "viewed together" pairs
  -- combining collect_set + sorting + makepairs(array, noSelfParing)
  -- ensures we get no pairs with swapped places like: (12,24),(24,12).
  -- We only produce tuples (12,24) ensuring that the smaller number is always on the left side
  SELECT makePairs(sort_array(itemArray), false) AS (category_id_1,category_id_2)
  FROM (
    SELECT collect_set(category_id) as itemArray --(_list= with duplicates, _set = distinct)
    FROM ${hiveconf:TEMP_TABLE}
    GROUP BY sessionid
  ) collectedList
) pairs
GROUP BY category_id_1, category_id_2
ORDER BY cnt DESC, category_id_1, category_id_2
LIMIT ${hiveconf:q30_limit};

-- cleanup
DROP VIEW IF EXISTS ${hiveconf:TEMP_TABLE};
