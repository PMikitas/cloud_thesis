operational_events_raw AS (
  SELECT
    outbox_id,
    event_id,
    occurred_at,
    table_name,
    entity_id,
    operation,
    TRY_PARSE_JSON(payload) AS payload_json
  FROM {{SNOWFLAKE_SCHEMA}}.{{OPERATIONAL_EVENTS_TABLE}}
  WHERE TRY_PARSE_JSON(payload) IS NOT NULL
),

current_operational_events AS (
  SELECT
    outbox_id,
    event_id,
    occurred_at,
    table_name,
    entity_id,
    operation,
    payload_json:columns AS payload_columns
  FROM operational_events_raw
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY table_name, entity_id
    ORDER BY occurred_at DESC, outbox_id DESC
  ) = 1
  AND operation <> 'DELETE'
),

customer AS (
  SELECT
    TRY_TO_NUMBER(payload_columns:CUSTOMER_ID::string) AS CUSTOMER_ID,
    payload_columns:CUSTOMER_EMAIL_ADDRESS::string AS CUSTOMER_EMAIL_ADDRESS,
    payload_columns:CUSTOMER_NICK::string AS CUSTOMER_NICK,
    payload_columns:CUSTOMER_ANONYMOUS::string AS CUSTOMER_ANONYMOUS,
    TRY_TO_NUMBER(payload_columns:MERCHANT_ID::string) AS MERCHANT_ID,
    TRY_TO_TIMESTAMP_NTZ(payload_columns:DATE_CREATED::string) AS DATE_CREATED,
    TRY_TO_TIMESTAMP_NTZ(payload_columns:DATE_MODIFIED::string) AS DATE_MODIFIED
  FROM current_operational_events
  WHERE table_name = 'CUSTOMER'
),

product AS (
  SELECT
    TRY_TO_NUMBER(payload_columns:PRODUCT_ID::string) AS PRODUCT_ID,
    payload_columns:SKU::string AS SKU,
    payload_columns:AVAILABLE::string AS AVAILABLE,
    TRY_TO_NUMBER(payload_columns:MERCHANT_ID::string) AS MERCHANT_ID,
    TRY_TO_NUMBER(payload_columns:QUANTITY_ORDERED::string) AS QUANTITY_ORDERED,
    TRY_TO_TIMESTAMP_NTZ(payload_columns:DATE_CREATED::string) AS DATE_CREATED,
    TRY_TO_TIMESTAMP_NTZ(payload_columns:DATE_MODIFIED::string) AS DATE_MODIFIED
  FROM current_operational_events
  WHERE table_name = 'PRODUCT'
),

product_description AS (
  SELECT
    TRY_TO_NUMBER(payload_columns:DESCRIPTION_ID::string) AS DESCRIPTION_ID,
    TRY_TO_NUMBER(payload_columns:PRODUCT_ID::string) AS PRODUCT_ID,
    TRY_TO_NUMBER(payload_columns:LANGUAGE_ID::string) AS LANGUAGE_ID,
    payload_columns:NAME::string AS NAME,
    payload_columns:TITLE::string AS TITLE
  FROM current_operational_events
  WHERE table_name = 'PRODUCT_DESCRIPTION'
),

product_availability AS (
  SELECT
    TRY_TO_NUMBER(payload_columns:PRODUCT_AVAIL_ID::string) AS PRODUCT_AVAIL_ID,
    TRY_TO_NUMBER(payload_columns:PRODUCT_ID::string) AS PRODUCT_ID,
    payload_columns:SKU::string AS SKU,
    payload_columns:AVAILABLE::string AS AVAILABLE,
    TRY_TO_NUMBER(payload_columns:QUANTITY::string) AS QUANTITY,
    TRY_TO_NUMBER(payload_columns:QUANTITY_ORD_MIN::string) AS QUANTITY_ORD_MIN,
    TRY_TO_NUMBER(payload_columns:QUANTITY_ORD_MAX::string) AS QUANTITY_ORD_MAX,
    TRY_TO_NUMBER(payload_columns:MERCHANT_ID::string) AS MERCHANT_ID,
    TRY_TO_TIMESTAMP_NTZ(payload_columns:DATE_AVAILABLE::string) AS DATE_AVAILABLE
  FROM current_operational_events
  WHERE table_name = 'PRODUCT_AVAILABILITY'
),

product_price AS (
  SELECT
    TRY_TO_NUMBER(payload_columns:PRODUCT_PRICE_ID::string) AS PRODUCT_PRICE_ID,
    TRY_TO_NUMBER(payload_columns:PRODUCT_AVAIL_ID::string) AS PRODUCT_AVAIL_ID,
    TRY_TO_DECIMAL(payload_columns:PRODUCT_PRICE_AMOUNT::string, 18, 4) AS PRODUCT_PRICE_AMOUNT,
    TRY_TO_DECIMAL(payload_columns:PRODUCT_PRICE_SPECIAL_AMOUNT::string, 18, 4) AS PRODUCT_PRICE_SPECIAL_AMOUNT,
    payload_columns:DEFAULT_PRICE::string AS DEFAULT_PRICE,
    payload_columns:PRODUCT_PRICE_CODE::string AS PRODUCT_PRICE_CODE,
    payload_columns:PRODUCT_PRICE_TYPE::string AS PRODUCT_PRICE_TYPE
  FROM current_operational_events
  WHERE table_name = 'PRODUCT_PRICE'
),

shopping_cart AS (
  SELECT
    TRY_TO_NUMBER(payload_columns:SHP_CART_ID::string) AS SHP_CART_ID,
    payload_columns:SHP_CART_CODE::string AS SHP_CART_CODE,
    TRY_TO_NUMBER(payload_columns:CUSTOMER_ID::string) AS CUSTOMER_ID,
    TRY_TO_NUMBER(payload_columns:ORDER_ID::string) AS ORDER_ID,
    TRY_TO_NUMBER(payload_columns:MERCHANT_ID::string) AS MERCHANT_ID,
    payload_columns:PROMO_CODE::string AS PROMO_CODE,
    payload_columns:PROMO_ADDED::string AS PROMO_ADDED,
    payload_columns:IP_ADDRESS::string AS IP_ADDRESS,
    TRY_TO_TIMESTAMP_NTZ(payload_columns:DATE_CREATED::string) AS DATE_CREATED,
    TRY_TO_TIMESTAMP_NTZ(payload_columns:DATE_MODIFIED::string) AS DATE_MODIFIED
  FROM current_operational_events
  WHERE table_name = 'SHOPPING_CART'
),

shopping_cart_item AS (
  SELECT
    TRY_TO_NUMBER(payload_columns:SHP_CART_ITEM_ID::string) AS SHP_CART_ITEM_ID,
    TRY_TO_NUMBER(payload_columns:SHP_CART_ID::string) AS SHP_CART_ID,
    TRY_TO_NUMBER(payload_columns:PRODUCT_ID::string) AS PRODUCT_ID,
    TRY_TO_NUMBER(payload_columns:QUANTITY::string) AS QUANTITY,
    payload_columns:SKU::string AS SKU,
    payload_columns:PRODUCT_VARIANT::string AS PRODUCT_VARIANT,
    TRY_TO_TIMESTAMP_NTZ(payload_columns:DATE_CREATED::string) AS DATE_CREATED,
    TRY_TO_TIMESTAMP_NTZ(payload_columns:DATE_MODIFIED::string) AS DATE_MODIFIED
  FROM current_operational_events
  WHERE table_name = 'SHOPPING_CART_ITEM'
),

orders AS (
  SELECT
    TRY_TO_NUMBER(payload_columns:ORDER_ID::string) AS ORDER_ID,
    payload_columns:CART_CODE::string AS CART_CODE,
    TRY_TO_NUMBER(payload_columns:CUSTOMER_ID::string) AS CUSTOMER_ID,
    TRY_TO_DECIMAL(payload_columns:ORDER_TOTAL::string, 18, 4) AS ORDER_TOTAL,
    payload_columns:ORDER_STATUS::string AS ORDER_STATUS,
    payload_columns:PAYMENT_TYPE::string AS PAYMENT_TYPE,
    TRY_TO_TIMESTAMP_NTZ(payload_columns:DATE_PURCHASED::string) AS DATE_PURCHASED,
    TRY_TO_TIMESTAMP_NTZ(payload_columns:ORDER_DATE_FINISHED::string) AS ORDER_DATE_FINISHED
  FROM current_operational_events
  WHERE table_name = 'ORDERS'
),

order_product AS (
  SELECT
    TRY_TO_NUMBER(payload_columns:ORDER_PRODUCT_ID::string) AS ORDER_PRODUCT_ID,
    TRY_TO_NUMBER(payload_columns:ORDER_ID::string) AS ORDER_ID,
    payload_columns:PRODUCT_SKU::string AS PRODUCT_SKU,
    payload_columns:PRODUCT_NAME::string AS PRODUCT_NAME,
    TRY_TO_NUMBER(payload_columns:PRODUCT_QUANTITY::string) AS PRODUCT_QUANTITY,
    TRY_TO_DECIMAL(payload_columns:ONETIME_CHARGE::string, 18, 4) AS ONETIME_CHARGE
  FROM current_operational_events
  WHERE table_name = 'ORDER_PRODUCT'
),

order_total AS (
  SELECT
    TRY_TO_NUMBER(payload_columns:ORDER_ACCOUNT_ID::string) AS ORDER_ACCOUNT_ID,
    TRY_TO_NUMBER(payload_columns:ORDER_ID::string) AS ORDER_ID,
    payload_columns:CODE::string AS CODE,
    payload_columns:TITLE::string AS TITLE,
    payload_columns:TEXT::string AS TEXT,
    TRY_TO_DECIMAL(payload_columns:VALUE::string, 18, 4) AS VALUE,
    TRY_TO_NUMBER(payload_columns:SORT_ORDER::string) AS SORT_ORDER,
    payload_columns:MODULE::string AS MODULE
  FROM current_operational_events
  WHERE table_name = 'ORDER_TOTAL'
),

order_status_history AS (
  SELECT
    TRY_TO_NUMBER(payload_columns:ORDER_STATUS_HISTORY_ID::string) AS ORDER_STATUS_HISTORY_ID,
    TRY_TO_NUMBER(payload_columns:ORDER_ID::string) AS ORDER_ID,
    payload_columns:STATUS::string AS STATUS,
    TRY_TO_TIMESTAMP_NTZ(payload_columns:DATE_ADDED::string) AS DATE_ADDED
  FROM current_operational_events
  WHERE table_name = 'ORDER_STATUS_HISTORY'
),

sm_transaction AS (
  SELECT
    TRY_TO_NUMBER(payload_columns:TRANSACTION_ID::string) AS TRANSACTION_ID,
    TRY_TO_NUMBER(payload_columns:ORDER_ID::string) AS ORDER_ID,
    TRY_TO_DECIMAL(payload_columns:AMOUNT::string, 18, 4) AS AMOUNT,
    payload_columns:TRANSACTION_TYPE::string AS TRANSACTION_TYPE,
    payload_columns:PAYMENT_TYPE::string AS PAYMENT_TYPE,
    TRY_TO_TIMESTAMP_NTZ(payload_columns:TRANSACTION_DATE::string) AS TRANSACTION_DATE,
    TRY_TO_TIMESTAMP_NTZ(payload_columns:DATE_CREATED::string) AS DATE_CREATED,
    TRY_TO_TIMESTAMP_NTZ(payload_columns:DATE_MODIFIED::string) AS DATE_MODIFIED
  FROM current_operational_events
  WHERE table_name = 'SM_TRANSACTION'
)
