{{ config(materialized='view') }}

WITH src AS (
  SELECT * FROM {{ source("raw", "your_table") }}
)
SELECT
    TRY_CAST({ adapter.quote("﻿deed_date") } AS VARCHAR) AS deed_date,
    TRY_CAST({ adapter.quote("location_state") } AS VARCHAR) AS location_state,
    TRY_CAST({ adapter.quote("location_zip") } AS VARCHAR) AS location_zip,
    TRY_CAST({ adapter.quote("biddernbr") } AS VARCHAR) AS biddernbr,
    TRY_CAST({ adapter.quote("deed_zip") } AS VARCHAR) AS deed_zip,
    TRY_CAST({ adapter.quote("deed_city_state") } AS VARCHAR) AS deed_city_state,
    TRY_CAST({ adapter.quote("prop_address") } AS VARCHAR) AS prop_address,
    TRY_CAST({ adapter.quote("deed_address") } AS VARCHAR) AS deed_address,
    TRY_CAST({ adapter.quote("location_city") } AS VARCHAR) AS location_city,
    TRY_CAST({ adapter.quote("deed_name") } AS VARCHAR) AS deed_name,
    TRY_CAST({ adapter.quote("prop_zip") } AS VARCHAR) AS prop_zip,
    TRY_CAST({ adapter.quote("final_bid_amt") } AS VARCHAR) AS final_bid_amt,
    TRY_CAST({ adapter.quote("prop_city") } AS VARCHAR) AS prop_city,
    TRY_CAST({ adapter.quote("parcel_id") } AS VARCHAR) AS parcel_id,
    TRY_CAST({ adapter.quote("sept_starting_bid") } AS VARCHAR) AS sept_starting_bid,
    TRY_CAST({ adapter.quote("bidder_name") } AS VARCHAR) AS bidder_name,
    TRY_CAST({ adapter.quote("bidder_city_state") } AS VARCHAR) AS bidder_city_state,
    TRY_CAST({ adapter.quote("bidder_zip") } AS VARCHAR) AS bidder_zip,
    TRY_CAST({ adapter.quote("bidder_address") } AS VARCHAR) AS bidder_address,
    TRY_CAST({ adapter.quote("ObjectId") } AS VARCHAR) AS ObjectId
FROM src
