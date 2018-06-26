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


--Perform category affinity analysis for products purchased together online.
-- Note that the order of products viewed does not matter,


-- IMPLEMENTATION NOTICE:
-- "Market basket analysis"
-- A difficult part is to create pairs of "purchased together" items within one sale
-- There are are several ways to to "basketing", implemented is way A)
-- A) collect all pairs per purchase (same order_number) in list and employ a UDTF to produce pairwise combinations of all list elements
-- B) distribute by order_number and employ reducer streaming script to aggregate all items per purchase and produce the pairs
-- C) pure SQL: produce pairings by self joining on order_number and filtering out left.item_sk < right.item_sk

-- Resources
ADD JAR ${env:BIG_BENCH_QUERIES_DIR}/Resources/bigbenchqueriesmr.jar;
CREATE TEMPORARY FUNCTION makePairs AS 'io.bigdatabenchmark.v1.queries.udf.PairwiseUDTF';


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
SELECT category_id_1, category_id_2, COUNT (*) AS cnt
FROM (
  -- Make category "purchased together" pairs
  -- combining collect_set + sorting + makepairs(array, noSelfParing)
  -- ensures we get no pairs with swapped places like: (12,24),(24,12).
  -- We only produce tuples (12,24) ensuring that the smaller number is always on the left side
  SELECT makePairs(sort_array(itemArray), false) AS (category_id_1,category_id_2)
  FROM (
    SELECT collect_set(i_category_id) as itemArray --(_list= with duplicates, _set = distinct)
    FROM web_sales ws, item i
    WHERE ws.ws_item_sk = i.i_item_sk
    AND i.i_category_id IS NOT NULL
    GROUP BY ws_order_number
  ) collectedList
) pairs
GROUP BY category_id_1, category_id_2
ORDER BY cnt DESC, category_id_1, category_id_2
LIMIT ${hiveconf:q29_limit};
