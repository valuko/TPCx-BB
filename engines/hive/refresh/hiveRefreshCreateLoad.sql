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

set hdfsDataPath=${env:BIG_BENCH_HDFS_ABSOLUTE_REFRESH_DATA_DIR};
set fieldDelimiter=|;
set temporaryTableSuffix=_temporary;

set customerTableName=customer;
set customerAddressTableName=customer_address;
set itemTableName=item;
set inventoryTableName=inventory;
set storeSalesTableName=store_sales;
set storeReturnsTableName=store_returns;
set webSalesTableName=web_sales;
set webReturnsTableName=web_returns;

set marketPricesTableName=item_marketprices;
set clickstreamsTableName=web_clickstreams;
set reviewsTableName=product_reviews;


!echo Create temporary table: ${hiveconf:customerTableName}${hiveconf:temporaryTableSuffix};
DROP TABLE IF EXISTS ${hiveconf:customerTableName}${hiveconf:temporaryTableSuffix};
CREATE EXTERNAL TABLE ${hiveconf:customerTableName}${hiveconf:temporaryTableSuffix}
  ( c_customer_sk             bigint              --not null
  , c_customer_id             string              --not null
  , c_current_cdemo_sk        bigint
  , c_current_hdemo_sk        bigint
  , c_current_addr_sk         bigint
  , c_first_shipto_date_sk    bigint
  , c_first_sales_date_sk     bigint
  , c_salutation              string
  , c_first_name              string
  , c_last_name               string
  , c_preferred_cust_flag     string
  , c_birth_day               int
  , c_birth_month             int
  , c_birth_year              int
  , c_birth_country           string
  , c_login                   string
  , c_email_address           string
  , c_last_review_date        string
  )
  ROW FORMAT DELIMITED FIELDS TERMINATED BY '${hiveconf:fieldDelimiter}'
  STORED AS TEXTFILE LOCATION '${hiveconf:hdfsDataPath}/${hiveconf:customerTableName}'
;

!echo Load text data into table: ${hiveconf:customerTableName};
INSERT INTO TABLE ${hiveconf:customerTableName}
SELECT * FROM ${hiveconf:customerTableName}${hiveconf:temporaryTableSuffix}
;

!echo Drop temporary table: ${hiveconf:customerTableName}${hiveconf:temporaryTableSuffix};
DROP TABLE IF EXISTS ${hiveconf:customerTableName}${hiveconf:temporaryTableSuffix};


!echo Create temporary table: ${hiveconf:customerAddressTableName}${hiveconf:temporaryTableSuffix};
DROP TABLE IF EXISTS ${hiveconf:customerAddressTableName}${hiveconf:temporaryTableSuffix};
CREATE EXTERNAL TABLE ${hiveconf:customerAddressTableName}${hiveconf:temporaryTableSuffix}
  ( ca_address_sk             bigint              --not null
  , ca_address_id             string              --not null
  , ca_street_number          string
  , ca_street_name            string
  , ca_street_type            string
  , ca_suite_number           string
  , ca_city                   string
  , ca_county                 string
  , ca_state                  string
  , ca_zip                    string
  , ca_country                string
  , ca_gmt_offset             decimal(5,2)
  , ca_location_type          string
  )
  ROW FORMAT DELIMITED FIELDS TERMINATED BY '${hiveconf:fieldDelimiter}'
  STORED AS TEXTFILE LOCATION '${hiveconf:hdfsDataPath}/${hiveconf:customerAddressTableName}'
;

!echo Load text data into table: ${hiveconf:customerAddressTableName};
INSERT INTO TABLE ${hiveconf:customerAddressTableName}
SELECT * FROM ${hiveconf:customerAddressTableName}${hiveconf:temporaryTableSuffix}
;

!echo Drop temporary table: ${hiveconf:customerAddressTableName}${hiveconf:temporaryTableSuffix};
DROP TABLE IF EXISTS ${hiveconf:customerAddressTableName}${hiveconf:temporaryTableSuffix};


!echo Create temporary table: ${hiveconf:itemTableName}${hiveconf:temporaryTableSuffix};
DROP TABLE IF EXISTS ${hiveconf:itemTableName}${hiveconf:temporaryTableSuffix};
CREATE EXTERNAL TABLE ${hiveconf:itemTableName}${hiveconf:temporaryTableSuffix}
  ( i_item_sk                 bigint              --not null
  , i_item_id                 string              --not null
  , i_rec_start_date          string
  , i_rec_end_date            string
  , i_item_desc               string
  , i_current_price           decimal(7,2)
  , i_wholesale_cost          decimal(7,2)
  , i_brand_id                int
  , i_brand                   string
  , i_class_id                int
  , i_class                   string
  , i_category_id             int
  , i_category                string
  , i_manufact_id             int
  , i_manufact                string
  , i_size                    string
  , i_formulation             string
  , i_color                   string
  , i_units                   string
  , i_container               string
  , i_manager_id              int
  , i_product_name            string
  )
  ROW FORMAT DELIMITED FIELDS TERMINATED BY '${hiveconf:fieldDelimiter}'
  STORED AS TEXTFILE LOCATION '${hiveconf:hdfsDataPath}/${hiveconf:itemTableName}'
;

!echo Load text data into table: ${hiveconf:itemTableName};
INSERT INTO TABLE ${hiveconf:itemTableName}
SELECT * FROM ${hiveconf:itemTableName}${hiveconf:temporaryTableSuffix}
;

!echo Drop temporary table: ${hiveconf:itemTableName}${hiveconf:temporaryTableSuffix};
DROP TABLE IF EXISTS ${hiveconf:itemTableName}${hiveconf:temporaryTableSuffix};


!echo Create temporary table: ${hiveconf:inventoryTableName}${hiveconf:temporaryTableSuffix};
DROP TABLE IF EXISTS ${hiveconf:inventoryTableName}${hiveconf:temporaryTableSuffix};
CREATE EXTERNAL TABLE ${hiveconf:inventoryTableName}${hiveconf:temporaryTableSuffix}
  ( inv_date_sk               bigint                --not null
  , inv_item_sk               bigint                --not null
  , inv_warehouse_sk          bigint                --not null
  , inv_quantity_on_hand      int
  )
  ROW FORMAT DELIMITED FIELDS TERMINATED BY '${hiveconf:fieldDelimiter}'
  STORED AS TEXTFILE LOCATION '${hiveconf:hdfsDataPath}/${hiveconf:inventoryTableName}'
;

!echo Load text data into table: ${hiveconf:inventoryTableName};
INSERT INTO TABLE ${hiveconf:inventoryTableName}
SELECT * FROM ${hiveconf:inventoryTableName}${hiveconf:temporaryTableSuffix}
;

!echo Drop temporary table: ${hiveconf:inventoryTableName}${hiveconf:temporaryTableSuffix};
DROP TABLE IF EXISTS ${hiveconf:inventoryTableName}${hiveconf:temporaryTableSuffix};


!echo Create temporary table: ${hiveconf:storeSalesTableName}${hiveconf:temporaryTableSuffix};
DROP TABLE IF EXISTS ${hiveconf:storeSalesTableName}${hiveconf:temporaryTableSuffix};
CREATE EXTERNAL TABLE ${hiveconf:storeSalesTableName}${hiveconf:temporaryTableSuffix}
  ( ss_sold_date_sk           bigint
  , ss_sold_time_sk           bigint
  , ss_item_sk                bigint                --not null
  , ss_customer_sk            bigint
  , ss_cdemo_sk               bigint
  , ss_hdemo_sk               bigint
  , ss_addr_sk                bigint
  , ss_store_sk               bigint
  , ss_promo_sk               bigint
  , ss_ticket_number          bigint                --not null
  , ss_quantity               int
  , ss_wholesale_cost         decimal(7,2)
  , ss_list_price             decimal(7,2)
  , ss_sales_price            decimal(7,2)
  , ss_ext_discount_amt       decimal(7,2)
  , ss_ext_sales_price        decimal(7,2)
  , ss_ext_wholesale_cost     decimal(7,2)
  , ss_ext_list_price         decimal(7,2)
  , ss_ext_tax                decimal(7,2)
  , ss_coupon_amt             decimal(7,2)
  , ss_net_paid               decimal(7,2)
  , ss_net_paid_inc_tax       decimal(7,2)
  , ss_net_profit             decimal(7,2)
  )
  ROW FORMAT DELIMITED FIELDS TERMINATED BY '${hiveconf:fieldDelimiter}'
  STORED AS TEXTFILE LOCATION '${hiveconf:hdfsDataPath}/${hiveconf:storeSalesTableName}'
;

!echo Load text data into table: ${hiveconf:storeSalesTableName};
INSERT INTO TABLE ${hiveconf:storeSalesTableName}
SELECT * FROM ${hiveconf:storeSalesTableName}${hiveconf:temporaryTableSuffix}
;

!echo Drop temporary table: ${hiveconf:storeSalesTableName}${hiveconf:temporaryTableSuffix};
DROP TABLE IF EXISTS ${hiveconf:storeSalesTableName}${hiveconf:temporaryTableSuffix};


!echo Create temporary table: ${hiveconf:storeReturnsTableName}${hiveconf:temporaryTableSuffix};
DROP TABLE IF EXISTS ${hiveconf:storeReturnsTableName}${hiveconf:temporaryTableSuffix};
CREATE EXTERNAL TABLE ${hiveconf:storeReturnsTableName}${hiveconf:temporaryTableSuffix}
  ( sr_returned_date_sk       bigint
  , sr_return_time_sk         bigint
  , sr_item_sk                bigint                --not null
  , sr_customer_sk            bigint
  , sr_cdemo_sk               bigint
  , sr_hdemo_sk               bigint
  , sr_addr_sk                bigint
  , sr_store_sk               bigint
  , sr_reason_sk              bigint
  , sr_ticket_number          bigint                --not null
  , sr_return_quantity        int
 , sr_return_amt              decimal(7,2)
  , sr_return_tax             decimal(7,2)
  , sr_return_amt_inc_tax     decimal(7,2)
  , sr_fee                    decimal(7,2)
  , sr_return_ship_cost       decimal(7,2)
  , sr_refunded_cash          decimal(7,2)
  , sr_reversed_charge        decimal(7,2)
  , sr_store_credit           decimal(7,2)
  , sr_net_loss               decimal(7,2)
  )
  ROW FORMAT DELIMITED FIELDS TERMINATED BY '${hiveconf:fieldDelimiter}'
  STORED AS TEXTFILE LOCATION '${hiveconf:hdfsDataPath}/${hiveconf:storeReturnsTableName}'
;

!echo Load text data into table: ${hiveconf:storeReturnsTableName};
INSERT INTO TABLE ${hiveconf:storeReturnsTableName}
SELECT * FROM ${hiveconf:storeReturnsTableName}${hiveconf:temporaryTableSuffix}
;

!echo Drop temporary table: ${hiveconf:storeReturnsTableName}${hiveconf:temporaryTableSuffix};
DROP TABLE IF EXISTS ${hiveconf:storeReturnsTableName}${hiveconf:temporaryTableSuffix};


!echo Create temporary table: ${hiveconf:webSalesTableName}${hiveconf:temporaryTableSuffix};
DROP TABLE IF EXISTS ${hiveconf:webSalesTableName}${hiveconf:temporaryTableSuffix};
CREATE EXTERNAL TABLE ${hiveconf:webSalesTableName}${hiveconf:temporaryTableSuffix}
  ( ws_sold_date_sk           bigint
  , ws_sold_time_sk           bigint
  , ws_ship_date_sk           bigint
  , ws_item_sk                bigint                --not null
  , ws_bill_customer_sk       bigint
  , ws_bill_cdemo_sk          bigint
  , ws_bill_hdemo_sk          bigint
  , ws_bill_addr_sk           bigint
  , ws_ship_customer_sk       bigint
  , ws_ship_cdemo_sk          bigint
  , ws_ship_hdemo_sk          bigint
  , ws_ship_addr_sk           bigint
  , ws_web_page_sk            bigint
  , ws_web_site_sk            bigint
  , ws_ship_mode_sk           bigint
  , ws_warehouse_sk           bigint
  , ws_promo_sk               bigint
  , ws_order_number           bigint                --not null
  , ws_quantity               int
  , ws_wholesale_cost         decimal(7,2)
  , ws_list_price             decimal(7,2)
  , ws_sales_price            decimal(7,2)
  , ws_ext_discount_amt       decimal(7,2)
  , ws_ext_sales_price        decimal(7,2)
  , ws_ext_wholesale_cost     decimal(7,2)
  , ws_ext_list_price         decimal(7,2)
  , ws_ext_tax                decimal(7,2)
  , ws_coupon_amt             decimal(7,2)
  , ws_ext_ship_cost          decimal(7,2)
  , ws_net_paid               decimal(7,2)
  , ws_net_paid_inc_tax       decimal(7,2)
  , ws_net_paid_inc_ship      decimal(7,2)
  , ws_net_paid_inc_ship_tax  decimal(7,2)
  , ws_net_profit             decimal(7,2)
  )
  ROW FORMAT DELIMITED FIELDS TERMINATED BY '${hiveconf:fieldDelimiter}'
  STORED AS TEXTFILE LOCATION '${hiveconf:hdfsDataPath}/${hiveconf:webSalesTableName}'
;

!echo Load text data into table: ${hiveconf:webSalesTableName};
INSERT INTO TABLE ${hiveconf:webSalesTableName}
SELECT * FROM ${hiveconf:webSalesTableName}${hiveconf:temporaryTableSuffix}
;

!echo Drop temporary table: ${hiveconf:webSalesTableName}${hiveconf:temporaryTableSuffix};
DROP TABLE IF EXISTS ${hiveconf:webSalesTableName}${hiveconf:temporaryTableSuffix};


!echo Create temporary table: ${hiveconf:webReturnsTableName}${hiveconf:temporaryTableSuffix};
DROP TABLE IF EXISTS ${hiveconf:webReturnsTableName}${hiveconf:temporaryTableSuffix};
CREATE EXTERNAL TABLE ${hiveconf:webReturnsTableName}${hiveconf:temporaryTableSuffix}
  ( wr_returned_date_sk       bigint 
  , wr_returned_time_sk       bigint
  , wr_item_sk                bigint                --not null
  , wr_refunded_customer_sk   bigint
  , wr_refunded_cdemo_sk      bigint
  , wr_refunded_hdemo_sk      bigint
  , wr_refunded_addr_sk       bigint
  , wr_returning_customer_sk  bigint
  , wr_returning_cdemo_sk     bigint
  , wr_returning_hdemo_sk     bigint
  , wr_returning_addr_sk      bigint
  , wr_web_page_sk            bigint
  , wr_reason_sk              bigint
  , wr_order_number           bigint                --not null
  , wr_return_quantity        int
  , wr_return_amt             decimal(7,2)
  , wr_return_tax             decimal(7,2)
  , wr_return_amt_inc_tax     decimal(7,2)
  , wr_fee                    decimal(7,2)
  , wr_return_ship_cost       decimal(7,2)
  , wr_refunded_cash          decimal(7,2)
  , wr_reversed_charge        decimal(7,2)
  , wr_account_credit         decimal(7,2)
  , wr_net_loss               decimal(7,2)
  )
  ROW FORMAT DELIMITED FIELDS TERMINATED BY '${hiveconf:fieldDelimiter}'
  STORED AS TEXTFILE LOCATION '${hiveconf:hdfsDataPath}/${hiveconf:webReturnsTableName}'
;

!echo Load text data into table: ${hiveconf:webReturnsTableName};
INSERT INTO TABLE ${hiveconf:webReturnsTableName}
SELECT * FROM ${hiveconf:webReturnsTableName}${hiveconf:temporaryTableSuffix}
;

!echo Drop temporary table: ${hiveconf:webReturnsTableName}${hiveconf:temporaryTableSuffix};
DROP TABLE IF EXISTS ${hiveconf:webReturnsTableName}${hiveconf:temporaryTableSuffix};


!echo Create temporary table: ${hiveconf:marketPricesTableName}${hiveconf:temporaryTableSuffix};
DROP TABLE IF EXISTS ${hiveconf:marketPricesTableName}${hiveconf:temporaryTableSuffix};
CREATE EXTERNAL TABLE ${hiveconf:marketPricesTableName}${hiveconf:temporaryTableSuffix}
  ( imp_sk                  bigint                --not null
  , imp_item_sk             bigint                --not null
  , imp_competitor          string
  , imp_competitor_price    decimal(7,2)
  , imp_start_date          bigint
  , imp_end_date            bigint

  )
  ROW FORMAT DELIMITED FIELDS TERMINATED BY '${hiveconf:fieldDelimiter}'
  STORED AS TEXTFILE LOCATION '${hiveconf:hdfsDataPath}/${hiveconf:marketPricesTableName}'
;

!echo Load text data into table: ${hiveconf:marketPricesTableName};
INSERT INTO TABLE ${hiveconf:marketPricesTableName}
SELECT * FROM ${hiveconf:marketPricesTableName}${hiveconf:temporaryTableSuffix}
;

!echo Drop temporary table: ${hiveconf:marketPricesTableName}${hiveconf:temporaryTableSuffix};
DROP TABLE IF EXISTS ${hiveconf:marketPricesTableName}${hiveconf:temporaryTableSuffix};


!echo Create temporary table: ${hiveconf:clickstreamsTableName}${hiveconf:temporaryTableSuffix};
DROP TABLE IF EXISTS ${hiveconf:clickstreamsTableName}${hiveconf:temporaryTableSuffix};
CREATE EXTERNAL TABLE ${hiveconf:clickstreamsTableName}${hiveconf:temporaryTableSuffix}
(   wcs_click_date_sk       bigint
  , wcs_click_time_sk       bigint
  , wcs_sales_sk            bigint
  , wcs_item_sk             bigint
  , wcs_web_page_sk         bigint
  , wcs_user_sk             bigint
  )
  ROW FORMAT DELIMITED FIELDS TERMINATED BY '${hiveconf:fieldDelimiter}'
  STORED AS TEXTFILE LOCATION '${hiveconf:hdfsDataPath}/${hiveconf:clickstreamsTableName}'
;

!echo Load text data into table: ${hiveconf:clickstreamsTableName};
INSERT INTO TABLE ${hiveconf:clickstreamsTableName}
SELECT * FROM ${hiveconf:clickstreamsTableName}${hiveconf:temporaryTableSuffix}
;

!echo Drop temporary table: ${hiveconf:clickstreamsTableName}${hiveconf:temporaryTableSuffix};
DROP TABLE IF EXISTS ${hiveconf:clickstreamsTableName}${hiveconf:temporaryTableSuffix};


!echo Create temporary table: ${hiveconf:reviewsTableName}${hiveconf:temporaryTableSuffix};
DROP TABLE IF EXISTS ${hiveconf:reviewsTableName}${hiveconf:temporaryTableSuffix};
CREATE EXTERNAL TABLE ${hiveconf:reviewsTableName}${hiveconf:temporaryTableSuffix}
(   pr_review_sk            bigint              --not null
  , pr_review_date          string
  , pr_review_time          string 
  , pr_review_rating        int                 --not null
  , pr_item_sk              bigint              --not null
  , pr_user_sk              bigint
  , pr_order_sk             bigint
  , pr_review_content       string --not null
  )
  ROW FORMAT DELIMITED FIELDS TERMINATED BY '${hiveconf:fieldDelimiter}'
  STORED AS TEXTFILE LOCATION '${hiveconf:hdfsDataPath}/${hiveconf:reviewsTableName}'
;

!echo Load text data into table: ${hiveconf:reviewsTableName};
INSERT INTO TABLE ${hiveconf:reviewsTableName}
SELECT * FROM ${hiveconf:reviewsTableName}${hiveconf:temporaryTableSuffix}
;

!echo Drop temporary table: ${hiveconf:reviewsTableName}${hiveconf:temporaryTableSuffix};
DROP TABLE IF EXISTS ${hiveconf:reviewsTableName}${hiveconf:temporaryTableSuffix};
