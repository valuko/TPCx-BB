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

-- based on tpc-ds q61
-- Find the ratio of items sold with and without promotions
-- in a given month and year. Only items in certain categories sold to customers
-- living in a specific time zone are considered.

-- Resources


--Result  --------------------------------------------------------------------
--keep result human readable
set hive.exec.compress.output=false;
set hive.exec.compress.output;
--CREATE RESULT TABLE. Store query result externally in output_dir/qXXresult/
DROP TABLE IF EXISTS ${hiveconf:RESULT_TABLE};
CREATE TABLE ${hiveconf:RESULT_TABLE} (
  promotions decimal(15,2),
  total      decimal(15,2),
  ratio      decimal(15,2)
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n'
STORED AS ${env:BIG_BENCH_hive_default_fileformat_result_table} LOCATION '${hiveconf:RESULT_DIR}';

-- the real query part
INSERT INTO TABLE ${hiveconf:RESULT_TABLE}
-- no need to cast promotions or total to double: SUM(COL) already returned a DOUBLE
SELECT promotions, total, promotions / total * 100
FROM (
   SELECT 
     SUM(ss_ext_sales_price) AS total,
     SUM( CASE WHEN (p_channel_dmail = 'Y' 
                 OR p_channel_email = 'Y' 
                 OR p_channel_tv = 'Y') 
                 THEN ss_ext_sales_price ELSE 0 END ) AS promotions
  FROM store_sales ss
  -- filter "given month and year"
  LEFT SEMI JOIN date_dim dd 
       ON (ss.ss_sold_date_sk = dd.d_date_sk
       AND d_year = ${hiveconf:q17_year}
       AND d_moy = ${hiveconf:q17_month} )
  -- filter "only items in certain categories"
  LEFT SEMI JOIN item i 
       ON (ss.ss_item_sk = i.i_item_sk
       AND i_category IN (${hiveconf:q17_i_category_IN}))
  -- filter "only" stores "in a specific time zone are considered". This one is not explicitly mentioned in the description, but original TPC-DS q61 has it and it can by considered implicitly logical to add.
  LEFT SEMI JOIN store s 
       ON (ss.ss_store_sk = s.s_store_sk
       AND s_gmt_offset = ${hiveconf:q17_gmt_offset})
  -- filter "only" customers "living in a specific time zone are considered""
  LEFT SEMI JOIN 
      (SELECT c_customer_sk
        FROM customer c
        LEFT SEMI JOIN customer_address ca 
             ON (c.c_current_addr_sk = ca.ca_address_sk
             AND ca_gmt_offset = ${hiveconf:q17_gmt_offset})
       ) cust_in_gmt ON ss.ss_customer_sk = cust_in_gmt.c_customer_sk
   -- filter "with and without promotions"
  JOIN promotion p ON ss.ss_promo_sk = p.p_promo_sk
) filtered
-- we don't need a 'ON' join condition. result is just two numbers.
ORDER BY promotions
LIMIT 100
;