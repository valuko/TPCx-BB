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
-- For a given product get a top 30 list sorted by number of views in descending order of the last 5 products that are mostly viewed before the product
-- was purchased online. For the viewed products, consider only products in certain item categories and viewed within 10
-- days before the purchase date.


-- IMPLEMENTATION NOTICE: 
-- The task exceeds "click session" boundaries: all clicks of a user within the 10 days before purchase time frame have to be considered.
-- Theoretically you could view this task as a "market basket analysis" with a very large basket (all clicks of a user for every purchase), which would be inefficient.
-- This is a classic MR filtering job which cannot be easily expressed and executed efficiently in hive/sql.
-- This does not mean you can't express the job purely in HQL. By cleverly employing windowing functions with "preceding" rows and "lag" it can be achieved.
-- However this implementation uses a custom reducer streaming job script, which enforces the "last 10 days" and "last 5 views" constraints in a sequential fashion.
-- The employed python script does not require excessive caching or joining besides buffering the "last 5" in a circular LRU cache.
-- The reduce python script requires the input to be pre-partitioned by user_sk and pre-sorted on virtual timestamp (wcs_click_date_sk*24*60*60 + wcs_click_time_sk) by hive.


-- Resources
ADD FILE ${hiveconf:QUERY_DIR}/q03_filterLast_N_viewedItmes_within_y_days.py;


--Result -------------------------------------------------------------------------
--keep result human readable
set hive.exec.compress.output=false;
set hive.exec.compress.output;
--CREATE RESULT TABLE. Store query result externally in output_dir/qXXresult/
DROP TABLE IF EXISTS ${hiveconf:RESULT_TABLE};
CREATE TABLE ${hiveconf:RESULT_TABLE} (
  lastviewed_item BIGINT,
  purchased_item  BIGINT,
  cnt             BIGINT
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n'
STORED AS ${env:BIG_BENCH_hive_default_fileformat_result_table} LOCATION '${hiveconf:RESULT_DIR}';

-- the real query part

INSERT INTO TABLE ${hiveconf:RESULT_TABLE}
SELECT purchased_item, lastviewed_item, COUNT(*) AS cnt
FROM
(
  SELECT *
  FROM item i,
  ( -- sessionize and filter "last 5 viewed products after purchase of specific item" with reduce script
    FROM 
    (
      SELECT
        wcs_user_sk,
        (wcs_click_date_sk * 24 * 60 * 60 + wcs_click_time_sk) AS tstamp,
        wcs_item_sk,
        wcs_sales_sk
      FROM web_clickstreams w
      WHERE wcs_user_sk IS NOT NULL -- only select clickstreams resulting in a purchase (user_sk != null)
      AND wcs_item_sk IS NOT NULL
      DISTRIBUTE BY wcs_user_sk -- build clickstream per user
      SORT BY wcs_user_sk, tstamp ASC, wcs_sales_sk, wcs_item_sk --order by tstamp ASC => required by python script
    ) q03_map_output
    REDUCE
      q03_map_output.wcs_user_sk,
      q03_map_output.tstamp,
      q03_map_output.wcs_item_sk,
      q03_map_output.wcs_sales_sk
    -- Reducer script logic: iterate through clicks of a user in ascending order (oldest recent click first).
    -- keep a list of the last N clicks and clickdate in a LRU list. if a purchase is found (wcs_sales_sk!=null) display the N previous clicks if they are within the provided date range (max 10 days before purchase)
    -- Reducer script selects only:
    -- * products viewed within 'q03_days_before_purchase' days before the purchase date
    -- * consider only purchase of specific item
    -- * only the last 5 products that where viewed before a sale
    USING 'python q03_filterLast_N_viewedItmes_within_y_days.py ${hiveconf:q03_days_in_sec_before_purchase} ${hiveconf:q03_views_before_purchase} ${hiveconf:q03_purchased_item_IN}'
    AS (purchased_item BIGINT, lastviewed_item BIGINT)
  ) lastViewSessions
  WHERE i.i_item_sk = lastViewSessions.lastviewed_item
  AND i.i_category_id IN (${hiveconf:q03_purchased_item_category_IN}) --Only products in certain categories
  CLUSTER BY lastviewed_item,purchased_item -- pre-cluster to speed up following group by and count()
) distributed
GROUP BY purchased_item,lastviewed_item
ORDER BY cnt DESC, purchased_item, lastviewed_item
--DISTRIBUTE BY lastviewed_item SORT BY cnt DESC, purchased_item, lastviewed_item --cluster parallel sorting
LIMIT 100
;
