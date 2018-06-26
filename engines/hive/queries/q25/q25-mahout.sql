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
-- Customer segmentation analysis: Customers are separated along the
-- following key shopping dimensions: recency of last visit, frequency of visits and
-- monetary amount. Use the store and online purchase data during a given year
-- to compute. After model of separation is build, 
-- report for the analysed customers to which "group" they where assigned

-- IMPLEMENTATION NOTICE:
-- hive provides the input for the clustering program
-- The input format for the clustering is:
--   customer ID, 
--   flag if customer bought something within the last 60 days (integer 0 or 1), 
--   number of orders, 
--   total amount spent
-- Fields are separated by a single space
-- Example:
-- 1 1 2 42\n

-- ss_sold_date_sk > 2002-01-02
DROP TABLE IF EXISTS ${hiveconf:TEMP_TABLE};
CREATE TABLE ${hiveconf:TEMP_TABLE} (
  cid     BIGINT,
  oid     BIGINT,
  dateid  BIGINT,
  amount  decimal(15,2)
);

-- Add store sales data
INSERT INTO TABLE ${hiveconf:TEMP_TABLE}
SELECT
  ss_customer_sk AS cid,
  ss_ticket_number AS oid,
  ss_sold_date_sk AS dateid,
  SUM(ss_net_paid) AS amount
FROM store_sales ss
JOIN date_dim d ON ss.ss_sold_date_sk = d.d_date_sk
WHERE d.d_date > '${hiveconf:q25_date}'
AND ss_customer_sk IS NOT NULL
GROUP BY
  ss_customer_sk,
  ss_ticket_number,
  ss_sold_date_sk
;

-- Add web sales data
INSERT INTO TABLE ${hiveconf:TEMP_TABLE}
SELECT
  ws_bill_customer_sk AS cid,
  ws_order_number AS oid,
  ws_sold_date_sk AS dateid,
  SUM(ws_net_paid) AS amount
FROM web_sales ws
JOIN date_dim d ON ws.ws_sold_date_sk = d.d_date_sk
WHERE d.d_date > '${hiveconf:q25_date}'
AND ws_bill_customer_sk IS NOT NULL
GROUP BY
  ws_bill_customer_sk,
  ws_order_number,
  ws_sold_date_sk
;


------ create input table for mahout --------------------------------------
--keep result human readable
set hive.exec.compress.output=false;
set hive.exec.compress.output;

-- This query requires parallel order by for fast and deterministic global ordering of final result
set hive.optimize.sampling.orderby=${hiveconf:bigbench.hive.optimize.sampling.orderby};
set hive.optimize.sampling.orderby.number=${hiveconf:bigbench.hive.optimize.sampling.orderby.number};
set hive.optimize.sampling.orderby.percent=${hiveconf:bigbench.hive.optimize.sampling.orderby.percent};
--debug print
set hive.optimize.sampling.orderby;
set hive.optimize.sampling.orderby.number;
set hive.optimize.sampling.orderby.percent;

DROP TABLE IF EXISTS ${hiveconf:TEMP_RESULT_TABLE};
CREATE TABLE ${hiveconf:TEMP_RESULT_TABLE} (
  cid        BIGINT,--used as "label", all following values are used as Vector for ML-algorithm
  recency    double,
  frequency  double,
  totalspend double
)
-- mahout requires "SPACE_SEPARATED" csv
ROW FORMAT DELIMITED FIELDS TERMINATED BY ' ' LINES TERMINATED BY '\n'
STORED AS TEXTFILE LOCATION '${hiveconf:TEMP_RESULT_DIR}';

INSERT INTO TABLE ${hiveconf:TEMP_RESULT_TABLE}
SELECT
  cid AS id,
  CASE WHEN 37621 - max(dateid) < 60 THEN 1.0 ELSE 0.0 END -- 37621 == 2003-01-02
    AS recency,
  count(oid) AS frequency,
  SUM(amount) AS totalspend
FROM ${hiveconf:TEMP_TABLE}
GROUP BY cid 
--CLUSTER BY cid --cluster by preceeded by group by is silently ignored by hive but fails in spark
--no total ordering with ORDER BY required, further processed by clustering algorithm
ORDER BY cid
;


--- CLEANUP--------------------------------------------
DROP TABLE ${hiveconf:TEMP_TABLE};
