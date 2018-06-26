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
-- Cluster customers into book buddies/club groups based on their in
-- store book purchasing histories. After model of separation is build, 
-- report for the analysed customers to which "group" they where assigned

-- IMPLEMENTATION NOTICE:
-- hive provides the input for the clustering program
-- The input format for the clustering is:
--   customer ID, 
--   sum of store sales in the item class ids [1,15]
-- Fields are separated by a single space
-- Example:
-- 1 12 3 0 9 5 7 2 9 1 5 9 0 4 3 0\n


-- Resources

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

DROP TABLE IF EXISTS ${hiveconf:TEMP_TABLE};
CREATE TABLE ${hiveconf:TEMP_TABLE} (
  cid  BIGINT,--used as "label", all following values are used as Vector for ML-algorithm
  id1  double,
  id2  double,
  id3  double,
  id4  double,
  id5  double,
  id6  double,
  id7  double,
  id8  double,
  id9  double,
  id10 double,
  id11 double,
  id12 double,
  id13 double,
  id14 double,
  id15 double
)
-- mahout requires "SPACE_SEPARATED" csv
ROW FORMAT DELIMITED FIELDS TERMINATED BY ' ' LINES TERMINATED BY '\n'
STORED AS TEXTFILE LOCATION '${hiveconf:TEMP_DIR}';

INSERT INTO TABLE ${hiveconf:TEMP_TABLE}
SELECT
  ss.ss_customer_sk AS cid,
  count(CASE WHEN i.i_class_id=1  THEN 1 ELSE NULL END) AS id1,
  count(CASE WHEN i.i_class_id=2  THEN 1 ELSE NULL END) AS id2,
  count(CASE WHEN i.i_class_id=3  THEN 1 ELSE NULL END) AS id3,
  count(CASE WHEN i.i_class_id=4  THEN 1 ELSE NULL END) AS id4,
  count(CASE WHEN i.i_class_id=5  THEN 1 ELSE NULL END) AS id5,
  count(CASE WHEN i.i_class_id=6  THEN 1 ELSE NULL END) AS id6,
  count(CASE WHEN i.i_class_id=7  THEN 1 ELSE NULL END) AS id7,
  count(CASE WHEN i.i_class_id=8  THEN 1 ELSE NULL END) AS id8,
  count(CASE WHEN i.i_class_id=9  THEN 1 ELSE NULL END) AS id9,
  count(CASE WHEN i.i_class_id=10 THEN 1 ELSE NULL END) AS id10,
  count(CASE WHEN i.i_class_id=11 THEN 1 ELSE NULL END) AS id11,
  count(CASE WHEN i.i_class_id=12 THEN 1 ELSE NULL END) AS id12,
  count(CASE WHEN i.i_class_id=13 THEN 1 ELSE NULL END) AS id13,
  count(CASE WHEN i.i_class_id=14 THEN 1 ELSE NULL END) AS id14,
  count(CASE WHEN i.i_class_id=15 THEN 1 ELSE NULL END) AS id15
FROM store_sales ss
INNER JOIN item i 
  ON (ss.ss_item_sk = i.i_item_sk
  AND i.i_category IN (${hiveconf:q26_i_category_IN})
  AND ss.ss_customer_sk IS NOT NULL
)
GROUP BY ss.ss_customer_sk
HAVING count(ss.ss_item_sk) > ${hiveconf:q26_count_ss_item_sk}
--CLUSTER BY cid --cluster by preceded by group by is silently ignored by hive but fails in spark
ORDER BY cid
;


-------------------------------------------------------------------------------
