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


-- Find the categories with flat or declining sales for in store purchases
-- during a given year for a given store.

-- Resources

--Result  --------------------------------------------------------------------
--keep result human readable
set hive.exec.compress.output=false;
set hive.exec.compress.output;

--CREATE RESULT TABLE. Store query result externally in output_dir/qXXresult/
DROP TABLE IF EXISTS ${hiveconf:RESULT_TABLE};
CREATE TABLE ${hiveconf:RESULT_TABLE} (
  cat       INT,
  slope     decimal(15,7),
  intercept decimal(15,7)
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n'
STORED AS ${env:BIG_BENCH_hive_default_fileformat_result_table} LOCATION '${hiveconf:RESULT_DIR}';

INSERT INTO TABLE ${hiveconf:RESULT_TABLE}
SELECT *
FROM (
  SELECT
    cat,
    --input:
    --SUM(x)as sumX,
    --SUM(y)as sumY,
    --SUM(xy)as sumXY,
    --SUM(xx)as sumXSquared,
    --count(x) as N,

    --formula stage1 (logical):
    --N * sumXY - sumX * sumY AS numerator,
    --N * sumXSquared - sumX*sumX AS denom
    --numerator / denom as slope,
    --(sumY - slope * sumX) / N as intercept
    --
    --formula stage2(inserted hive aggregations): 
    --(count(x) * SUM(xy) - SUM(x) * SUM(y)) AS numerator,
    --(count(x) * SUM(xx) - SUM(x) * SUM(x)) AS denom
    --numerator / denom as slope,
    --(sum(y) - slope * sum(x)) / count(X) as intercept
    --
    --Formula stage 3: (insert numerator and denom into slope and intercept function)
    ((count(x) * SUM(xy) - SUM(x) * SUM(y)) / (count(x) * SUM(xx) - SUM(x) * SUM(x)) ) AS slope,
    (SUM(y) - ((count(x) * SUM(xy) - SUM(x) * SUM(y)) / (count(x) * SUM(xx) - SUM(x)*SUM(x)) ) * SUM(x)) / count(x) AS intercept
  FROM (
    SELECT
      i.i_category_id AS cat, -- ranges from 1 to 10
      s.ss_sold_date_sk AS x,
      SUM(s.ss_net_paid) AS y,
      s.ss_sold_date_sk * SUM(s.ss_net_paid) AS xy,
      s.ss_sold_date_sk * s.ss_sold_date_sk AS xx
    FROM store_sales s
    -- select date range
    LEFT SEMI JOIN (
      SELECT d_date_sk
      FROM date_dim d
      WHERE d.d_date >= '${hiveconf:q15_startDate}'
      AND   d.d_date <= '${hiveconf:q15_endDate}'
    ) dd ON ( s.ss_sold_date_sk=dd.d_date_sk )
    INNER JOIN item i ON s.ss_item_sk = i.i_item_sk
    WHERE i.i_category_id IS NOT NULL
    AND s.ss_store_sk = ${hiveconf:q15_store_sk} -- for a given store ranges from 1 to 12
    GROUP BY i.i_category_id, s.ss_sold_date_sk
  ) temp
  GROUP BY cat
) regression
WHERE slope <= 0
ORDER BY cat
-- limit not required, number of categories is known to be small and of fixed size across scalefactors
;
