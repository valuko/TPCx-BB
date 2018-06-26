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

-- based on tpc-ds q74
-- Display customers with both store and web sales in
-- consecutive years for whom the increase in web sales exceeds the increase in
-- store sales for a specified year.

-- Implementation notice:
-- loosely based on implementation of tpc-ds q74 - Query description in tpcds_1.1.0.pdf does NOT match implementation in tpc-ds qgen\query_templates\query74.tpl
-- This version:
--    * avoids union of 2 sub-queries followed by 4 self joins and replaces them with only one join by creating two distinct views with better pre-filters and aggregations for store/web-sales first and second year
--    * introduces a more logical sorting by reporting the top 100 customers ranked by their web_sales increase ratio instead of just reporting random 100 customers

   
DROP VIEW IF EXISTS ${hiveconf:TEMP_TABLE1};
DROP VIEW IF EXISTS ${hiveconf:TEMP_TABLE2};

--table contains the values of the intersection of customer table and store_sales tables values
--that meet the necessary requirements and whose year value is either 1999 or 2000
CREATE VIEW ${hiveconf:TEMP_TABLE1} AS
SELECT
    ss.ss_customer_sk AS customer_sk,
    sum( case when (d_year = ${hiveconf:q13_Year})   THEN ss_net_paid  ELSE 0 END) first_year_total,
    sum( case when (d_year = ${hiveconf:q13_Year}+1) THEN ss_net_paid  ELSE 0 END) second_year_total
FROM store_sales ss
JOIN (
  SELECT d_date_sk, d_year
  FROM date_dim d
  WHERE d.d_year in (${hiveconf:q13_Year}, (${hiveconf:q13_Year} + 1))
) dd on ( ss.ss_sold_date_sk = dd.d_date_sk )
GROUP BY ss.ss_customer_sk 
HAVING first_year_total > 0  -- required to avoid division by 0, because later we will divide by this value
;

CREATE VIEW ${hiveconf:TEMP_TABLE2} AS
SELECT
       ws.ws_bill_customer_sk AS customer_sk,
       sum( case when (d_year = ${hiveconf:q13_Year})   THEN ws_net_paid  ELSE 0 END) first_year_total,
       sum( case when (d_year = ${hiveconf:q13_Year}+1) THEN ws_net_paid  ELSE 0 END) second_year_total
FROM web_sales ws
JOIN (
  SELECT d_date_sk, d_year
  FROM date_dim d
  WHERE d.d_year in (${hiveconf:q13_Year}, (${hiveconf:q13_Year} + 1) )
) dd ON ( ws.ws_sold_date_sk = dd.d_date_sk )
GROUP BY ws.ws_bill_customer_sk 
HAVING first_year_total > 0  -- required to avoid division by 0, because later we will divide by this value
;


--Result  --------------------------------------------------------------------
--keep result human readable
set hive.exec.compress.output=false;
set hive.exec.compress.output;
--CREATE RESULT TABLE. Store query result externally in output_dir/qXXresult/
DROP TABLE IF EXISTS ${hiveconf:RESULT_TABLE};
CREATE TABLE ${hiveconf:RESULT_TABLE} (
  customer_id             BIGINT,
  customer_first_name     STRING,
  customer_last_name      STRING,
  storeSalesIncreaseRatio decimal(15,2),
  webSalesIncreaseRatio   decimal(15,2)
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n'
STORED AS ${env:BIG_BENCH_hive_default_fileformat_result_table} LOCATION '${hiveconf:RESULT_DIR}';

-- the real query part
INSERT INTO TABLE ${hiveconf:RESULT_TABLE}
SELECT
      c_customer_sk,
      c_first_name,
      c_last_name,
      (store.second_year_total / store.first_year_total) AS storeSalesIncreaseRatio ,
      (web.second_year_total / web.first_year_total) AS webSalesIncreaseRatio 
FROM ${hiveconf:TEMP_TABLE1} store ,
     ${hiveconf:TEMP_TABLE2} web ,
     customer c
WHERE store.customer_sk = web.customer_sk
AND   web.customer_sk = c_customer_sk
-- if customer has sales in first year for both store and websales, select him only if web second_year_total/first_year_total ratio is bigger then his store second_year_total/first_year_total ratio.
AND   (web.second_year_total / web.first_year_total)  >  (store.second_year_total / store.first_year_total) 
ORDER BY
  webSalesIncreaseRatio DESC,
  c_customer_sk,
  c_first_name,
  c_last_name
LIMIT ${hiveconf:q13_limit};


---Cleanup-------------------------------------------------------------------
DROP VIEW IF EXISTS ${hiveconf:TEMP_TABLE1};
DROP VIEW IF EXISTS ${hiveconf:TEMP_TABLE2};

