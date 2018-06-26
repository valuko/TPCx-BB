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

-- based on tpc-ds q90
-- What is the ratio between the number of items sold over
-- the internet in the morning (7 to 8am) to the number of items sold in the evening
-- (7 to 8pm) of customers with a specified number of dependents. Consider only
-- websites with a high amount of content.

-- Resources

--Result  --------------------------------------------------------------------
--keep result human readable

set hive.exec.compress.output=false;
set hive.exec.compress.output;



--CREATE RESULT TABLE. Store query result externally in output_dir/qXXresult/
DROP TABLE IF EXISTS ${hiveconf:RESULT_TABLE};
CREATE TABLE ${hiveconf:RESULT_TABLE} (
  am_pm_ratio decimal(15,4)
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n'
STORED AS ${env:BIG_BENCH_hive_default_fileformat_result_table} LOCATION '${hiveconf:RESULT_DIR}';

-- Begin: the real query part
INSERT INTO TABLE ${hiveconf:RESULT_TABLE}
SELECT  
     SUM( CASE WHEN (td.t_hour BETWEEN ${hiveconf:q14_morning_startHour} AND ${hiveconf:q14_morning_endHour} ) THEN 1 ELSE 0 END ) 
     /
     SUM( CASE WHEN (td.t_hour BETWEEN ${hiveconf:q14_evening_startHour} AND ${hiveconf:q14_evening_endHour}) THEN 1 ELSE 0 END ) AS am_pm_ratio
FROM web_sales ws
JOIN household_demographics hd 
     ON (hd.hd_demo_sk = ws.ws_ship_hdemo_sk 
     AND hd.hd_dep_count = ${hiveconf:q14_dependents} )
JOIN web_page wp 
     ON (wp.wp_web_page_sk = ws.ws_web_page_sk
     AND wp.wp_char_count BETWEEN ${hiveconf:q14_content_len_min} AND ${hiveconf:q14_content_len_max} )
JOIN time_dim td ON td.t_time_sk = ws.ws_sold_time_sk


--result is a single line
;
