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


-- Aggregate total amount of sold items over different given types of combinations of customers based on selected groups of
-- marital status, education status, sales price  and   different combinations of state and sales profit.

-- Resources

--Result  --------------------------------------------------------------------
--keep result human readable
set hive.exec.compress.output=false;
set hive.exec.compress.output;
--CREATE RESULT TABLE. Store query result externally in output_dir/qXXresult/
DROP TABLE IF EXISTS ${hiveconf:RESULT_TABLE};
CREATE TABLE ${hiveconf:RESULT_TABLE} (
  totalSales DECIMAL(15,0)
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n'
STORED AS ${env:BIG_BENCH_hive_default_fileformat_result_table} LOCATION '${hiveconf:RESULT_DIR}';

-- the real query part
INSERT INTO TABLE ${hiveconf:RESULT_TABLE}
SELECT SUM(ss1.ss_quantity)
FROM store_sales ss1, date_dim dd,customer_address ca1 , store s ,customer_demographics cd
-- select date range
WHERE ss1.ss_sold_date_sk = dd.d_date_sk 
AND dd.d_year=${hiveconf:q09_year}
AND ss1.ss_addr_sk = ca1.ca_address_sk
AND s.s_store_sk = ss1.ss_store_sk
AND cd.cd_demo_sk = ss1.ss_cdemo_sk
AND 
(
  (
    cd.cd_marital_status = '${hiveconf:q09_part1_marital_status}'
    AND cd.cd_education_status = '${hiveconf:q09_part1_education_status}'
    AND ${hiveconf:q09_part1_sales_price_min} <= ss1.ss_sales_price
    AND ss1.ss_sales_price <= ${hiveconf:q09_part1_sales_price_max}
  ) 
  OR 
  (
    cd.cd_marital_status = '${hiveconf:q09_part2_marital_status}'
    AND cd.cd_education_status = '${hiveconf:q09_part2_education_status}'
    AND ${hiveconf:q09_part2_sales_price_min} <= ss1.ss_sales_price
    AND ss1.ss_sales_price <= ${hiveconf:q09_part2_sales_price_max}
  ) 
  OR 
  (
    cd.cd_marital_status = '${hiveconf:q09_part3_marital_status}'
    AND cd.cd_education_status = '${hiveconf:q09_part3_education_status}'
    AND ${hiveconf:q09_part3_sales_price_min} <= ss1.ss_sales_price
    AND ss1.ss_sales_price <= ${hiveconf:q09_part3_sales_price_max}
  )
) 
AND 
(
  (
    ca1.ca_country = '${hiveconf:q09_part1_ca_country}'
    AND ca1.ca_state IN (${hiveconf:q09_part1_ca_state_IN})
    AND ${hiveconf:q09_part1_net_profit_min} <= ss1.ss_net_profit
    AND ss1.ss_net_profit <= ${hiveconf:q09_part1_net_profit_max}
  ) 
  OR 
  (
    ca1.ca_country = '${hiveconf:q09_part2_ca_country}'
    AND ca1.ca_state IN (${hiveconf:q09_part2_ca_state_IN})
    AND ${hiveconf:q09_part2_net_profit_min} <= ss1.ss_net_profit
    AND ss1.ss_net_profit <= ${hiveconf:q09_part2_net_profit_max}
  ) 
  OR 
  (
    ca1.ca_country = '${hiveconf:q09_part3_ca_country}'
    AND ca1.ca_state IN (${hiveconf:q09_part3_ca_state_IN})
    AND ${hiveconf:q09_part3_net_profit_min} <= ss1.ss_net_profit
    AND ss1.ss_net_profit <= ${hiveconf:q09_part3_net_profit_max}
  )
)
--no sorting required. output is a single line
;
