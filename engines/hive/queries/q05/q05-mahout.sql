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
-- Build a model using logistic regression: based on existing users online
-- activities (interest in items of different categories) and demographics, for a visitor to an online store, predict the visitors
-- likelihood to be interested in a given item category.
-- input vectors to the machine learning algorithm are:
--  label             STRING, -- number of clicks in specified category "q05_i_category"
--  college_education STRING, -- has college education [0,1]
--  male              STRING, -- isMale [0,1]
--  clicks_in_1       STRING, -- number of clicks in category id 1
--  clicks_in_2       STRING, -- number of clicks in category id 2
--  clicks_in_7       STRING, -- number of clicks in category id 7
--  clicks_in_4       STRING, -- number of clicks in category id 4
--  clicks_in_5       STRING, -- number of clicks in category id 5
--  clicks_in_6       STRING  -- number of clicks in category id 6
-- TODO: updated this description once improved q5 with more features is merged


-- Resources

--Result  --------------------------------------------------------------------
--keep result human readable
set hive.exec.compress.output=false;
set hive.exec.compress.output;

--CREATE RESULT TABLE. Store query result externally in output_dir/qXXresult/
DROP TABLE IF EXISTS ${hiveconf:TEMP_TABLE};
CREATE TABLE ${hiveconf:TEMP_TABLE} (
 --wcs_user_sk BIGINT,-- column is used to identify prediction
 clicks_in_category BIGINT, --column is used as label, all following columns are used as input vector to the ml algorithm
 college_education BIGINT,
 male BIGINT,
 clicks_in_1 BIGINT,
 clicks_in_2 BIGINT,
 clicks_in_3 BIGINT,
 clicks_in_4 BIGINT,
 clicks_in_5 BIGINT,
 clicks_in_6 BIGINT,
 clicks_in_7 BIGINT
)
-- mahout requires "," separated csv
ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n'
STORED AS TEXTFILE LOCATION '${hiveconf:TEMP_DIR}';
;

INSERT INTO TABLE ${hiveconf:TEMP_TABLE}
SELECT
  --wcs_user_sk,
  clicks_in_category,
  CASE WHEN cd_education_status IN (${hiveconf:q05_cd_education_status_IN}) THEN 1 ELSE 0 END AS college_education,
  CASE WHEN cd_gender = ${hiveconf:q05_cd_gender} THEN 1 ELSE 0 END AS male,
  clicks_in_1,
  clicks_in_2,
  clicks_in_3,
  clicks_in_4,
  clicks_in_5,
  clicks_in_6,
  clicks_in_7
FROM( 
  SELECT 
    wcs_user_sk,
    SUM( CASE WHEN i_category = ${hiveconf:q05_i_category} THEN 1 ELSE 0 END) AS clicks_in_category,
    SUM( CASE WHEN i_category_id = 1 THEN 1 ELSE 0 END) AS clicks_in_1,
    SUM( CASE WHEN i_category_id = 2 THEN 1 ELSE 0 END) AS clicks_in_2,
    SUM( CASE WHEN i_category_id = 3 THEN 1 ELSE 0 END) AS clicks_in_3,
    SUM( CASE WHEN i_category_id = 4 THEN 1 ELSE 0 END) AS clicks_in_4,
    SUM( CASE WHEN i_category_id = 5 THEN 1 ELSE 0 END) AS clicks_in_5,
    SUM( CASE WHEN i_category_id = 6 THEN 1 ELSE 0 END) AS clicks_in_6,
    SUM( CASE WHEN i_category_id = 7 THEN 1 ELSE 0 END) AS clicks_in_7
  FROM web_clickstreams
  INNER JOIN item it ON (wcs_item_sk = i_item_sk
                     AND wcs_user_sk IS NOT NULL)
  GROUP BY  wcs_user_sk
)q05_user_clicks_in_cat
INNER JOIN customer ct ON wcs_user_sk = c_customer_sk
INNER JOIN customer_demographics ON c_current_cdemo_sk = cd_demo_sk
;