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

-- TASK:
-- Identifies customers shifting their purchase habit from store to web sales.
-- Find customers who spend in relation more money in the second year following a given year in the web_sales channel then in the store sales channel.
-- Hint: web second_year_total/first_year_total > store second_year_total/first_year_total
-- Report customers details: first name, last name, their country of origin, login name and email address) and identify if they are preferred customer, for the top 100 customers with the highest increase in their second year web purchase ratio.
-- Implementation notice:
-- loosely based on implementation of tpc-ds q4 - Query description in tpcds_1.1.0.pdf does NOT match implementation in tpc-ds qgen\query_templates\query4.tpl
-- This version:
--    * does not have the catalog_sales table (there is none in our dataset). Web_sales plays the role of catalog_sales.
--    * avoids the 4 self joins and replaces them with only one by creating two distinct views with better pre-filters and aggregations for store/web-sales first and second year
--    * introduces a more logical sorting by reporting the top 100 customers ranked by their web_sales increase instead of just reporting random 100 customers

-- Resources


-- Part 1 helper table(s) --------------------------------------------------------------
DROP VIEW IF EXISTS ${hiveconf:TEMP_TABLE1};
DROP VIEW IF EXISTS ${hiveconf:TEMP_TABLE2};

-- customer store sales
CREATE VIEW ${hiveconf:TEMP_TABLE1} AS
SELECT ss_customer_sk AS customer_sk,
       sum( case when (d_year = ${hiveconf:q06_YEAR})   THEN (((ss_ext_list_price-ss_ext_wholesale_cost-ss_ext_discount_amt)+ss_ext_sales_price)/2)  ELSE 0 END) first_year_total,
       sum( case when (d_year = ${hiveconf:q06_YEAR}+1) THEN (((ss_ext_list_price-ss_ext_wholesale_cost-ss_ext_discount_amt)+ss_ext_sales_price)/2)  ELSE 0 END) second_year_total
FROM  store_sales
     ,date_dim
WHERE ss_sold_date_sk = d_date_sk
AND   d_year BETWEEN ${hiveconf:q06_YEAR} AND ${hiveconf:q06_YEAR} +1
GROUP BY ss_customer_sk
HAVING first_year_total > 0  -- required to avoid division by 0, because later we will divide by this value
;

-- customer web sales
CREATE  VIEW ${hiveconf:TEMP_TABLE2} AS
SELECT ws_bill_customer_sk AS customer_sk ,
       sum( case when (d_year = ${hiveconf:q06_YEAR})   THEN (((ws_ext_list_price-ws_ext_wholesale_cost-ws_ext_discount_amt)+ws_ext_sales_price)/2)   ELSE 0 END) first_year_total,
       sum( case when (d_year = ${hiveconf:q06_YEAR}+1) THEN (((ws_ext_list_price-ws_ext_wholesale_cost-ws_ext_discount_amt)+ws_ext_sales_price)/2)   ELSE 0 END) second_year_total
FROM web_sales
    ,date_dim
WHERE ws_sold_date_sk = d_date_sk
AND   d_year BETWEEN ${hiveconf:q06_YEAR} AND ${hiveconf:q06_YEAR} +1
GROUP BY ws_bill_customer_sk
HAVING first_year_total > 0  -- required to avoid division by 0, because later we will divide by this value
;         


--Result  --------------------------------------------------------------------
--keep result human readable
set hive.exec.compress.output=false;
set hive.exec.compress.output;
--CREATE RESULT TABLE. Store query result externally in output_dir/qXXresult/
DROP TABLE IF EXISTS ${hiveconf:RESULT_TABLE};
CREATE TABLE ${hiveconf:RESULT_TABLE} (
    web_sales_increase_ratio  DECIMAL(15,1),
    c_customer_sk             BIGINT,
    c_first_name              STRING,
    c_last_name               STRING,
    c_preferred_cust_flag     STRING,
    c_birth_country           STRING,
    c_login                   STRING,
    c_email_address           STRING
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n'
STORED AS ${env:BIG_BENCH_hive_default_fileformat_result_table} LOCATION '${hiveconf:RESULT_DIR}';

-- the real query part
INSERT INTO TABLE ${hiveconf:RESULT_TABLE}
SELECT
      (web.second_year_total / web.first_year_total) AS web_sales_increase_ratio,
      c_customer_sk,
      c_first_name,
      c_last_name,
      c_preferred_cust_flag,
      c_birth_country,
      c_login,
      c_email_address
FROM ${hiveconf:TEMP_TABLE1} store,
     ${hiveconf:TEMP_TABLE2} web,
     customer c
WHERE store.customer_sk = web.customer_sk
AND   web.customer_sk = c_customer_sk
-- if customer has sales in first year for both store and websales, select him only if web second_year_total/first_year_total ratio is bigger then his store second_year_total/first_year_total ratio.
AND   (web.second_year_total / web.first_year_total)  >  (store.second_year_total / store.first_year_total) 
ORDER BY
  web_sales_increase_ratio DESC,
  c_customer_sk,
  c_first_name,
  c_last_name,
  c_preferred_cust_flag,
  c_birth_country,
  c_login
LIMIT ${hiveconf:q06_LIMIT};

---Cleanup-------------------------------------------------------------------
DROP VIEW IF EXISTS ${hiveconf:TEMP_TABLE1};
DROP VIEW IF EXISTS ${hiveconf:TEMP_TABLE2};




