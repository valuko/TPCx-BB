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

-- based on tpc-ds q29
-- Get all items that were sold in stores in a given month
-- and year and which were returned in the next 6 months and re-purchased by
-- the returning customer afterwards through the web sales channel in the following
-- three years. For those items, compute the total quantity sold through the
-- store, the quantity returned and the quantity purchased through the web. Group
-- this information by item and store.


--Result --------------------------------------------------------------------
--keep result human readable
set hive.exec.compress.output=false;
set hive.exec.compress.output;

--CREATE RESULT TABLE. Store query result externally in output_dir/qXXresult/
DROP TABLE IF EXISTS ${hiveconf:RESULT_TABLE};
CREATE TABLE ${hiveconf:RESULT_TABLE} (
  item_id                STRING,
  item_desc              STRING,
  store_id               STRING,
  store_name             STRING,
  store_sales_quantity   BIGINT,
  store_returns_quantity BIGINT,
  web_sales_quantity     BIGINT
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n'
STORED AS ${env:BIG_BENCH_hive_default_fileformat_result_table} LOCATION '${hiveconf:RESULT_DIR}';

-- the real query part
INSERT INTO TABLE ${hiveconf:RESULT_TABLE}
SELECT
  part_i.i_item_id AS i_item_id,
  part_i.i_item_desc AS i_item_desc,
  part_s.s_store_id AS s_store_id,
  part_s.s_store_name AS s_store_name,
  SUM(part_ss.ss_quantity) AS store_sales_quantity,
  SUM(part_sr.sr_return_quantity) AS store_returns_quantity,
  SUM(part_ws.ws_quantity) AS web_sales_quantity
FROM (
	SELECT
	  sr_item_sk,
	  sr_customer_sk,
	  sr_ticket_number,
	  sr_return_quantity
	FROM
	  store_returns sr,
	  date_dim d2
	WHERE d2.d_year = ${hiveconf:q21_year}
	AND d2.d_moy BETWEEN ${hiveconf:q21_month} AND ${hiveconf:q21_month} + 6 --which were returned in the next six months
 	AND sr.sr_returned_date_sk = d2.d_date_sk
) part_sr
INNER JOIN (
  SELECT
    ws_item_sk,
    ws_bill_customer_sk,
    ws_quantity
  FROM
    web_sales ws,
    date_dim d3
  WHERE d3.d_year BETWEEN ${hiveconf:q21_year} AND ${hiveconf:q21_year} + 2 -- in the following three years (re-purchased by the returning customer afterwards through the web sales channel)
  AND ws.ws_sold_date_sk = d3.d_date_sk
) part_ws ON (
  part_sr.sr_item_sk = part_ws.ws_item_sk
  AND part_sr.sr_customer_sk = part_ws.ws_bill_customer_sk
)
INNER JOIN (
  SELECT
    ss_item_sk,
    ss_store_sk,
    ss_customer_sk,
    ss_ticket_number,
    ss_quantity
  FROM
    store_sales ss,
    date_dim d1
  WHERE d1.d_year = ${hiveconf:q21_year}
  AND d1.d_moy = ${hiveconf:q21_month}
  AND ss.ss_sold_date_sk = d1.d_date_sk
) part_ss ON (
  part_ss.ss_ticket_number = part_sr.sr_ticket_number
  AND part_ss.ss_item_sk = part_sr.sr_item_sk
  AND part_ss.ss_customer_sk = part_sr.sr_customer_sk
)
INNER JOIN store part_s ON (
  part_s.s_store_sk = part_ss.ss_store_sk
)
INNER JOIN item part_i ON (
  part_i.i_item_sk = part_ss.ss_item_sk
)
GROUP BY
  part_i.i_item_id,
  part_i.i_item_desc,
  part_s.s_store_id,
  part_s.s_store_name
ORDER BY
  part_i.i_item_id,
  part_i.i_item_desc,
  part_s.s_store_id,
  part_s.s_store_name
LIMIT ${hiveconf:q21_limit};
