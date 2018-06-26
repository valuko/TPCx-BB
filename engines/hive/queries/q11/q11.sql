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


-- For a given product, measure the correlation of sentiments, including
-- the number of reviews and average review ratings, on product monthly revenues
-- within a given time frame.

-- Resources

--Result  --------------------------------------------------------------------
--keep result human readable
set hive.exec.compress.output=false;
set hive.exec.compress.output;

DROP TABLE IF EXISTS ${hiveconf:RESULT_TABLE};
CREATE TABLE ${hiveconf:RESULT_TABLE} (
  correlation decimal(15,7)
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n'
STORED AS ${env:BIG_BENCH_hive_default_fileformat_result_table} LOCATION '${hiveconf:RESULT_DIR}';

-- the real query part
INSERT INTO TABLE ${hiveconf:RESULT_TABLE}
SELECT corr(reviews_count,avg_rating)
FROM (
  SELECT
    p.pr_item_sk AS pid,
    p.r_count    AS reviews_count,
    p.avg_rating AS avg_rating,
    s.revenue    AS m_revenue
  FROM (
    SELECT
      pr_item_sk,
      count(*) AS r_count,
      avg(pr_review_rating) AS avg_rating
    FROM product_reviews
    WHERE pr_item_sk IS NOT NULL
    --this is GROUP BY 1 in original::same as pr_item_sk here::hive complains anyhow
    GROUP BY pr_item_sk
  ) p
  INNER JOIN (
    SELECT
      ws_item_sk,
      SUM(ws_net_paid) AS revenue
    FROM web_sales ws
    -- Select date range of interest
    LEFT SEMI JOIN (
      SELECT d_date_sk
      FROM date_dim d
      WHERE d.d_date >= '${hiveconf:q11_startDate}'
      AND   d.d_date <= '${hiveconf:q11_endDate}'
    ) dd ON ( ws.ws_sold_date_sk = dd.d_date_sk )
    WHERE ws_item_sk IS NOT null
    --this is GROUP BY 1 in original::same as ws_item_sk here::hive complains anyhow
    GROUP BY ws_item_sk
  ) s
  ON p.pr_item_sk = s.ws_item_sk
) q11_review_stats
--no sorting required, output is a single line
;

-- cleanup -------------------------------------------------------------
