
[+] - average session duration

with CTE as
(
select session_id, min(event_timestamp) as earliest_action, max(event_timestamp) as latest_action  from performance_tracking.api_performance
group by session_id
)

select avg( TIMESTAMP_DIFF(latest_action,earliest_action, SECOND) / 60 ) as average_session_minutes from CTE

[+] percentage of sessions that resulted in a successful sale

WITH session_steps AS (
  SELECT
    session_id,
    MIN(CASE
          WHEN endpoint = 'POST /api/v1/cart'
          THEN event_timestamp
        END) AS first_cart_ts,
    MIN(CASE
          WHEN REGEXP_CONTAINS(endpoint, r'^POST /api/v1/auth/cart/[^/]+/checkout$')
          THEN event_timestamp
        END) AS first_checkout_ts
  FROM `performance_tracking.api_performance`
  GROUP BY session_id
)

SELECT
  COUNTIF(first_cart_ts IS NOT NULL) AS sessions_with_cart,
  COUNTIF(first_cart_ts IS NOT NULL AND first_checkout_ts IS NOT NULL AND first_checkout_ts >= first_cart_ts) AS sessions_with_sale,
  ROUND(
    100 * SAFE_DIVIDE(
      COUNTIF(first_cart_ts IS NOT NULL AND first_checkout_ts IS NOT NULL AND first_checkout_ts >= first_cart_ts),
      COUNTIF(first_cart_ts IS NOT NULL)
    ),
    2
  ) AS cart_to_sale_conversion_rate_pct
FROM session_steps;

[+] - average session LTV

WITH checkout_carts AS (
  SELECT DISTINCT
    ap.session_id,
    REGEXP_EXTRACT(
      ap.endpoint,
      r'^POST /api/v1/auth/cart/([^/]+)/checkout$'
    ) AS shp_cart_code
  FROM `performance_tracking.api_performance` ap
  WHERE REGEXP_CONTAINS(
    ap.endpoint,
    r'^POST /api/v1/auth/cart/[^/]+/checkout$'
  )
),

session_cart_map AS (
  SELECT DISTINCT
    cc.session_id,
    sc.SHP_CART_ID
  FROM checkout_carts cc
  INNER JOIN `performance_tracking.shopping_cart` sc
    ON cc.shp_cart_code = sc.SHP_CART_CODE
),

product_price_one AS (
  SELECT
    av.PRODUCT_ID,
    MAX(pp.PRODUCT_PRICE_AMOUNT) AS product_price_amount
  FROM `performance_tracking.product_availability` av
  LEFT JOIN `performance_tracking.product_price` pp
    ON av.PRODUCT_AVAIL_ID = pp.PRODUCT_AVAIL_ID
  GROUP BY av.PRODUCT_ID
),

cart_prices AS (
  SELECT
    scm.session_id,
    scm.SHP_CART_ID,
    SUM(COALESCE(ppo.product_price_amount, 0) * COALESCE(cart_item.QUANTITY, 1)) AS cart_value
  FROM session_cart_map scm
  LEFT JOIN `performance_tracking.shopping_cart_item` cart_item
    ON scm.SHP_CART_ID = cart_item.SHP_CART_ID
  LEFT JOIN product_price_one ppo
    ON cart_item.PRODUCT_ID = ppo.PRODUCT_ID
  GROUP BY
    scm.session_id,
    scm.SHP_CART_ID
),

session_values AS (
  SELECT
    session_id,
    SUM(cart_value) AS session_value
  FROM cart_prices
  GROUP BY session_id
),

all_sessions AS (
  SELECT DISTINCT session_id
  FROM `performance_tracking.api_performance`
)

SELECT
  COUNT(DISTINCT a.session_id) AS total_sessions,
  COUNT(DISTINCT sv.session_id) AS converted_sessions,
  ROUND(AVG(sv.session_value), 2) AS avg_session_value_converted_only,
  ROUND(AVG(COALESCE(sv.session_value, 0)), 2) AS avg_session_value_all_sessions
FROM all_sessions a
LEFT JOIN session_values sv
  ON a.session_id = sv.session_id;


[+] Average cart stats

WITH sold_cart_codes AS (
  SELECT DISTINCT
    REGEXP_EXTRACT(
      ap.endpoint,
      r'^POST /api/v1/auth/cart/([^/]+)/checkout$'
    ) AS shp_cart_code
  FROM `performance_tracking.api_performance` ap
  WHERE REGEXP_CONTAINS(
    ap.endpoint,
    r'^POST /api/v1/auth/cart/[^/]+/checkout$'
  )
),

sold_carts AS (
  SELECT DISTINCT
    sc.SHP_CART_ID
  FROM `performance_tracking.shopping_cart` sc
  INNER JOIN sold_cart_codes scc
    ON sc.SHP_CART_CODE = scc.shp_cart_code
),

cart_stats AS (
  SELECT
    sci.SHP_CART_ID,
    COUNT(*) AS item_line_count,
    SUM(COALESCE(sci.QUANTITY, 1)) AS total_item_units
  FROM `performance_tracking.shopping_cart_item` sci
  GROUP BY sci.SHP_CART_ID
),

cart_classification AS (
  SELECT
    cs.SHP_CART_ID,
    cs.item_line_count,
    cs.total_item_units,
    CASE
      WHEN sc.SHP_CART_ID IS NOT NULL THEN 'sale'
      ELSE 'failed'
    END AS cart_status
  FROM cart_stats cs
  LEFT JOIN sold_carts sc
    ON cs.SHP_CART_ID = sc.SHP_CART_ID
)

SELECT
  cart_status,
  COUNT(*) AS carts,
  ROUND(AVG(item_line_count), 2) AS avg_item_count_in_cart,
  ROUND(AVG(total_item_units), 2) AS weighted_avg_items_in_cart
FROM (
  SELECT
    SHP_CART_ID,
    item_line_count,
    total_item_units,
    cart_status
  FROM cart_classification

  UNION ALL

  SELECT
    SHP_CART_ID,
    item_line_count,
    total_item_units,
    'all' AS cart_status
  FROM cart_classification
)
GROUP BY cart_status
ORDER BY
  CASE cart_status
    WHEN 'all' THEN 1
    WHEN 'failed' THEN 2
    WHEN 'sale' THEN 3
  END;


[+] avg item sold price


WITH checkout_carts AS (
  SELECT
    REGEXP_EXTRACT(
      ap.endpoint,
      r'^POST /api/v1/auth/cart/([^/]+)/checkout$'
    ) AS shp_cart_code
  FROM `performance_tracking.api_performance` ap
  WHERE REGEXP_CONTAINS(
    ap.endpoint,
    r'^POST /api/v1/auth/cart/[^/]+/checkout$'
  )
),

sold_carts AS (
  SELECT DISTINCT
    sc.SHP_CART_ID
  FROM checkout_carts cc
  LEFT JOIN `performance_tracking.shopping_cart` sc
    ON cc.shp_cart_code = sc.SHP_CART_CODE
  WHERE sc.SHP_CART_ID IS NOT NULL
),

product_price_one AS (
  SELECT
    av.PRODUCT_ID,
    MAX(pp.PRODUCT_PRICE_AMOUNT) AS product_price_amount
  FROM `performance_tracking.product_availability` av
  LEFT JOIN `performance_tracking.product_price` pp
    ON av.PRODUCT_AVAIL_ID = pp.PRODUCT_AVAIL_ID
  GROUP BY av.PRODUCT_ID
)

SELECT
  ROUND(
    SAFE_DIVIDE(
      SUM(COALESCE(ppo.product_price_amount, 0) * COALESCE(cart_item.QUANTITY, 1)),
      SUM(COALESCE(cart_item.QUANTITY, 1))
    ),
    2
  ) AS avg_item_sold_price
FROM sold_carts sc
LEFT JOIN `performance_tracking.shopping_cart_item` cart_item
  ON sc.SHP_CART_ID = cart_item.SHP_CART_ID
LEFT JOIN product_price_one ppo
  ON cart_item.PRODUCT_ID = ppo.PRODUCT_ID;

[+] - average number of items in the cart

with distinct_products as
(
select SHP_CART_ID, count(PRODUCT_ID) as product_count from performance_tracking.shopping_cart_item
group by SHP_CART_ID
)

select avg(product_count) from distinct_products



with items as
(
select SHP_CART_ID, sum(QUANTITY) as product_count from performance_tracking.shopping_cart_item
group by SHP_CART_ID
)

select avg(product_count) from items


[+] - conversion rate from visits to purchase


WITH sessions AS (
  SELECT DISTINCT session_id
  FROM `performance_tracking.api_performance`
),

sale_sessions AS (
  SELECT DISTINCT session_id
  FROM `performance_tracking.api_performance`
  WHERE endpoint LIKE '%checkout%'
)

SELECT
  COUNT(*) AS total_sessions,
  COUNT(sale_sessions.session_id) AS sale_sessions,
  SAFE_DIVIDE(COUNT(sale_sessions.session_id), COUNT(*)) AS conversion_rate
FROM sessions
LEFT JOIN sale_sessions
  USING (session_id);




[+] sessions → cart sessions → checkout sessions


WITH session_steps AS (
  SELECT
    session_id,

    MIN(event_timestamp) AS first_event_ts,

    MIN(CASE
      WHEN endpoint = 'POST /api/v1/cart'
      THEN event_timestamp
    END) AS first_cart_ts,

    MIN(CASE
      WHEN REGEXP_CONTAINS(endpoint, r'^POST /api/v1/auth/cart/[^/]+/checkout$')
      THEN event_timestamp
    END) AS first_checkout_ts

  FROM `performance_tracking.api_performance`
  GROUP BY session_id
)

SELECT
  COUNT(*) AS total_sessions,

  COUNTIF(first_cart_ts IS NOT NULL) AS sessions_with_cart,

  COUNTIF(
    first_checkout_ts IS NOT NULL
    AND first_cart_ts IS NOT NULL
    AND first_checkout_ts >= first_cart_ts
  ) AS sessions_with_checkout,

  ROUND(100 * SAFE_DIVIDE(COUNTIF(first_cart_ts IS NOT NULL), COUNT(*)), 2)
    AS visit_to_cart_rate_pct,

  ROUND(
    100 * SAFE_DIVIDE(
      COUNTIF(first_checkout_ts IS NOT NULL AND first_checkout_ts >= first_cart_ts),
      COUNTIF(first_cart_ts IS NOT NULL)
    ),
    2
  ) AS cart_to_checkout_rate_pct,

  ROUND(
    100 * SAFE_DIVIDE(
      COUNTIF(first_checkout_ts IS NOT NULL AND first_checkout_ts >= first_cart_ts),
      COUNT(*)
    ),
    2
  ) AS visit_to_checkout_rate_pct

FROM session_steps;




[+] How long does it take users to go from cart creation to purchase?


WITH session_steps AS (
  SELECT
    session_id,

    MIN(CASE
      WHEN endpoint = 'POST /api/v1/cart'
      THEN event_timestamp
    END) AS first_cart_ts,

    MIN(CASE
      WHEN REGEXP_CONTAINS(endpoint, r'^POST /api/v1/auth/cart/[^/]+/checkout$')
      THEN event_timestamp
    END) AS first_checkout_ts

  FROM `performance_tracking.api_performance`
  GROUP BY session_id
)

SELECT
  COUNT(*) AS converted_sessions,

  ROUND(AVG(TIMESTAMP_DIFF(first_checkout_ts, first_cart_ts, SECOND)) / 60, 2)
    AS avg_minutes_from_cart_to_checkout,

  APPROX_QUANTILES(
    TIMESTAMP_DIFF(first_checkout_ts, first_cart_ts, SECOND),
    100
  )[OFFSET(50)] AS median_seconds_from_cart_to_checkout,

  APPROX_QUANTILES(
    TIMESTAMP_DIFF(first_checkout_ts, first_cart_ts, SECOND),
    100
  )[OFFSET(95)] AS p95_seconds_from_cart_to_checkout

FROM session_steps
WHERE first_cart_ts IS NOT NULL
  AND first_checkout_ts IS NOT NULL
  AND first_checkout_ts >= first_cart_ts;


WITH session_steps AS (
  SELECT
    session_id,

    MIN(CASE
      WHEN endpoint = 'POST /api/v1/cart'
      THEN event_timestamp
    END) AS first_cart_ts,

    MIN(CASE
      WHEN REGEXP_CONTAINS(endpoint, r'^POST /api/v1/auth/cart/[^/]+/checkout$')
      THEN event_timestamp
    END) AS first_checkout_ts

  FROM `performance_tracking.api_performance`
  GROUP BY session_id
)

SELECT
  COUNTIF(first_cart_ts IS NOT NULL) AS cart_sessions,

  COUNTIF(first_cart_ts IS NOT NULL AND first_checkout_ts IS NULL)
    AS abandoned_cart_sessions,

  ROUND(
    100 * SAFE_DIVIDE(
      COUNTIF(first_cart_ts IS NOT NULL AND first_checkout_ts IS NULL),
      COUNTIF(first_cart_ts IS NOT NULL)
    ),
    2
  ) AS cart_abandonment_rate_pct

FROM session_steps;




[+] 6. Checkout failure / abandoned checkout sessions

WITH session_steps AS (
  SELECT
    session_id,

    MIN(CASE
      WHEN endpoint = 'POST /api/v1/cart'
      THEN event_timestamp
    END) AS first_cart_ts,

    MIN(CASE
      WHEN REGEXP_CONTAINS(endpoint, r'^POST /api/v1/auth/cart/[^/]+/checkout$')
      THEN event_timestamp
    END) AS first_checkout_ts

  FROM `performance_tracking.api_performance`
  GROUP BY session_id
)

SELECT
  COUNTIF(first_cart_ts IS NOT NULL) AS cart_sessions,

  COUNTIF(first_cart_ts IS NOT NULL AND first_checkout_ts IS NULL)
    AS abandoned_cart_sessions,

  ROUND(
    100 * SAFE_DIVIDE(
      COUNTIF(first_cart_ts IS NOT NULL AND first_checkout_ts IS NULL),
      COUNTIF(first_cart_ts IS NOT NULL)
    ),
    2
  ) AS cart_abandonment_rate_pct

FROM session_steps;


[+] 7. Cart value distribution

WITH checkout_carts AS (
  SELECT DISTINCT
    REGEXP_EXTRACT(
      ap.endpoint,
      r'^POST /api/v1/auth/cart/([^/]+)/checkout$'
    ) AS shp_cart_code
  FROM `performance_tracking.api_performance` ap
  WHERE REGEXP_CONTAINS(
    ap.endpoint,
    r'^POST /api/v1/auth/cart/[^/]+/checkout$'
  )
),

sold_carts AS (
  SELECT DISTINCT
    sc.SHP_CART_ID
  FROM checkout_carts cc
  INNER JOIN `performance_tracking.shopping_cart` sc
    ON cc.shp_cart_code = sc.SHP_CART_CODE
),

product_price_one AS (
  SELECT
    av.PRODUCT_ID,
    MAX(pp.PRODUCT_PRICE_AMOUNT) AS product_price_amount
  FROM `performance_tracking.product_availability` av
  LEFT JOIN `performance_tracking.product_price` pp
    ON av.PRODUCT_AVAIL_ID = pp.PRODUCT_AVAIL_ID
  GROUP BY av.PRODUCT_ID
),

cart_values AS (
  SELECT
    sc.SHP_CART_ID,
    SUM(COALESCE(ppo.product_price_amount, 0) * COALESCE(sci.QUANTITY, 1)) AS cart_value
  FROM sold_carts sc
  LEFT JOIN `performance_tracking.shopping_cart_item` sci
    ON sc.SHP_CART_ID = sci.SHP_CART_ID
  LEFT JOIN product_price_one ppo
    ON sci.PRODUCT_ID = ppo.PRODUCT_ID
  GROUP BY sc.SHP_CART_ID
)

SELECT
  COUNT(*) AS sold_carts,
  ROUND(AVG(cart_value), 2) AS avg_cart_value,
  ROUND(MIN(cart_value), 2) AS min_cart_value,
  ROUND(MAX(cart_value), 2) AS max_cart_value,

  APPROX_QUANTILES(cart_value, 100)[OFFSET(50)] AS median_cart_value,
  APPROX_QUANTILES(cart_value, 100)[OFFSET(75)] AS p75_cart_value,
  APPROX_QUANTILES(cart_value, 100)[OFFSET(90)] AS p90_cart_value,
  APPROX_QUANTILES(cart_value, 100)[OFFSET(95)] AS p95_cart_value

FROM cart_values;


[+] 8. Revenue by hour

WITH checkout_carts AS (
  SELECT DISTINCT
    ap.session_id,
    ap.event_timestamp AS checkout_timestamp,
    REGEXP_EXTRACT(
      ap.endpoint,
      r'^POST /api/v1/auth/cart/([^/]+)/checkout$'
    ) AS shp_cart_code
  FROM `performance_tracking.api_performance` ap
  WHERE REGEXP_CONTAINS(
    ap.endpoint,
    r'^POST /api/v1/auth/cart/[^/]+/checkout$'
  )
),

session_cart_map AS (
  SELECT DISTINCT
    cc.session_id,
    cc.checkout_timestamp,
    sc.SHP_CART_ID
  FROM checkout_carts cc
  INNER JOIN `performance_tracking.shopping_cart` sc
    ON cc.shp_cart_code = sc.SHP_CART_CODE
),

product_price_one AS (
  SELECT
    av.PRODUCT_ID,
    MAX(pp.PRODUCT_PRICE_AMOUNT) AS product_price_amount
  FROM `performance_tracking.product_availability` av
  LEFT JOIN `performance_tracking.product_price` pp
    ON av.PRODUCT_AVAIL_ID = pp.PRODUCT_AVAIL_ID
  GROUP BY av.PRODUCT_ID
),

cart_values AS (
  SELECT
    scm.session_id,
    scm.SHP_CART_ID,
    scm.checkout_timestamp,
    SUM(COALESCE(ppo.product_price_amount, 0) * COALESCE(sci.QUANTITY, 1)) AS cart_value
  FROM session_cart_map scm
  LEFT JOIN `performance_tracking.shopping_cart_item` sci
    ON scm.SHP_CART_ID = sci.SHP_CART_ID
  LEFT JOIN product_price_one ppo
    ON sci.PRODUCT_ID = ppo.PRODUCT_ID
  GROUP BY
    scm.session_id,
    scm.SHP_CART_ID,
    scm.checkout_timestamp
)

SELECT
  TIMESTAMP_TRUNC(checkout_timestamp, HOUR) AS checkout_hour,
  COUNT(DISTINCT session_id) AS sale_sessions,
  COUNT(DISTINCT SHP_CART_ID) AS sold_carts,
  ROUND(SUM(cart_value), 2) AS revenue,
  ROUND(AVG(cart_value), 2) AS avg_cart_value

FROM cart_values
GROUP BY checkout_hour
ORDER BY checkout_hour;


9. Revenue by endpoint session intensity
This checks whether longer/more active sessions produce more value.

WITH session_activity AS (
  SELECT
    session_id,
    COUNT(*) AS request_count,
    TIMESTAMP_DIFF(MAX(event_timestamp), MIN(event_timestamp), SECOND) AS session_duration_seconds
  FROM `performance_tracking.api_performance`
  GROUP BY session_id
),

checkout_carts AS (
  SELECT DISTINCT
    ap.session_id,
    REGEXP_EXTRACT(
      ap.endpoint,
      r'^POST /api/v1/auth/cart/([^/]+)/checkout$'
    ) AS shp_cart_code
  FROM `performance_tracking.api_performance` ap
  WHERE REGEXP_CONTAINS(
    ap.endpoint,
    r'^POST /api/v1/auth/cart/[^/]+/checkout$'
  )
),

session_cart_map AS (
  SELECT DISTINCT
    cc.session_id,
    sc.SHP_CART_ID
  FROM checkout_carts cc
  INNER JOIN `performance_tracking.shopping_cart` sc
    ON cc.shp_cart_code = sc.SHP_CART_CODE
),

product_price_one AS (
  SELECT
    av.PRODUCT_ID,
    MAX(pp.PRODUCT_PRICE_AMOUNT) AS product_price_amount
  FROM `performance_tracking.product_availability` av
  LEFT JOIN `performance_tracking.product_price` pp
    ON av.PRODUCT_AVAIL_ID = pp.PRODUCT_AVAIL_ID
  GROUP BY av.PRODUCT_ID
),

session_values AS (
  SELECT
    scm.session_id,
    SUM(COALESCE(ppo.product_price_amount, 0) * COALESCE(sci.QUANTITY, 1)) AS session_value
  FROM session_cart_map scm
  LEFT JOIN `performance_tracking.shopping_cart_item` sci
    ON scm.SHP_CART_ID = sci.SHP_CART_ID
  LEFT JOIN product_price_one ppo
    ON sci.PRODUCT_ID = ppo.PRODUCT_ID
  GROUP BY scm.session_id
),

bucketed AS (
  SELECT
    sa.session_id,
    sa.request_count,
    sa.session_duration_seconds,
    COALESCE(sv.session_value, 0) AS session_value,

    CASE
      WHEN sa.request_count <= 5 THEN '1-5 requests'
      WHEN sa.request_count <= 10 THEN '6-10 requests'
      WHEN sa.request_count <= 20 THEN '11-20 requests'
      WHEN sa.request_count <= 50 THEN '21-50 requests'
      ELSE '50+ requests'
    END AS request_count_bucket

  FROM session_activity sa
  LEFT JOIN session_values sv
    ON sa.session_id = sv.session_id
)

SELECT
  request_count_bucket,
  COUNT(*) AS sessions,
  COUNTIF(session_value > 0) AS converted_sessions,

  ROUND(100 * SAFE_DIVIDE(COUNTIF(session_value > 0), COUNT(*)), 2)
    AS conversion_rate_pct,

  ROUND(AVG(session_value), 2) AS avg_session_value_all_sessions,

  ROUND(AVG(CASE WHEN session_value > 0 THEN session_value END), 2)
    AS avg_session_value_converted_only,

  ROUND(AVG(session_duration_seconds) / 60, 2)
    AS avg_session_duration_minutes

FROM bucketed
GROUP BY request_count_bucket
ORDER BY
  CASE request_count_bucket
    WHEN '1-5 requests' THEN 1
    WHEN '6-10 requests' THEN 2
    WHEN '11-20 requests' THEN 3
    WHEN '21-50 requests' THEN 4
    WHEN '50+ requests' THEN 5
  END;


10. Top products by sold quantity and revenue

  WITH checkout_carts AS (
  SELECT DISTINCT
    REGEXP_EXTRACT(
      ap.endpoint,
      r'^POST /api/v1/auth/cart/([^/]+)/checkout$'
    ) AS shp_cart_code
  FROM `performance_tracking.api_performance` ap
  WHERE REGEXP_CONTAINS(
    ap.endpoint,
    r'^POST /api/v1/auth/cart/[^/]+/checkout$'
  )
),

sold_carts AS (
  SELECT DISTINCT
    sc.SHP_CART_ID
  FROM checkout_carts cc
  INNER JOIN `performance_tracking.shopping_cart` sc
    ON cc.shp_cart_code = sc.SHP_CART_CODE
),

product_price_one AS (
  SELECT
    av.PRODUCT_ID,
    MAX(pp.PRODUCT_PRICE_AMOUNT) AS product_price_amount
  FROM `performance_tracking.product_availability` av
  LEFT JOIN `performance_tracking.product_price` pp
    ON av.PRODUCT_AVAIL_ID = pp.PRODUCT_AVAIL_ID
  GROUP BY av.PRODUCT_ID
)

SELECT
  sci.PRODUCT_ID,
  SUM(COALESCE(sci.QUANTITY, 1)) AS units_sold,
  ROUND(SUM(COALESCE(ppo.product_price_amount, 0) * COALESCE(sci.QUANTITY, 1)), 2)
    AS product_revenue,
  COUNT(DISTINCT sci.SHP_CART_ID) AS sold_carts_containing_product

FROM sold_carts sc
LEFT JOIN `performance_tracking.shopping_cart_item` sci
  ON sc.SHP_CART_ID = sci.SHP_CART_ID
LEFT JOIN product_price_one ppo
  ON sci.PRODUCT_ID = ppo.PRODUCT_ID

GROUP BY sci.PRODUCT_ID
ORDER BY product_revenue DESC
LIMIT 20;

11. Cart abandonment by cart size

WITH sold_cart_codes AS (
  SELECT DISTINCT
    REGEXP_EXTRACT(
      ap.endpoint,
      r'^POST /api/v1/auth/cart/([^/]+)/checkout$'
    ) AS shp_cart_code
  FROM `performance_tracking.api_performance` ap
  WHERE REGEXP_CONTAINS(
    ap.endpoint,
    r'^POST /api/v1/auth/cart/[^/]+/checkout$'
  )
),

sold_carts AS (
  SELECT DISTINCT
    sc.SHP_CART_ID
  FROM `performance_tracking.shopping_cart` sc
  INNER JOIN sold_cart_codes scc
    ON sc.SHP_CART_CODE = scc.shp_cart_code
),

cart_stats AS (
  SELECT
    sci.SHP_CART_ID,
    SUM(COALESCE(sci.QUANTITY, 1)) AS total_item_units
  FROM `performance_tracking.shopping_cart_item` sci
  GROUP BY sci.SHP_CART_ID
),

cart_classification AS (
  SELECT
    cs.SHP_CART_ID,
    cs.total_item_units,
    CASE
      WHEN sc.SHP_CART_ID IS NOT NULL THEN 'sale'
      ELSE 'failed'
    END AS cart_status,

    CASE
      WHEN cs.total_item_units = 1 THEN '1 item'
      WHEN cs.total_item_units = 2 THEN '2 items'
      WHEN cs.total_item_units BETWEEN 3 AND 5 THEN '3-5 items'
      WHEN cs.total_item_units BETWEEN 6 AND 10 THEN '6-10 items'
      ELSE '10+ items'
    END AS cart_size_bucket

  FROM cart_stats cs
  LEFT JOIN sold_carts sc
    ON cs.SHP_CART_ID = sc.SHP_CART_ID
)

SELECT
  cart_size_bucket,

  COUNT(*) AS carts,

  COUNTIF(cart_status = 'sale') AS sold_carts,

  COUNTIF(cart_status = 'failed') AS failed_carts,

  ROUND(100 * SAFE_DIVIDE(COUNTIF(cart_status = 'sale'), COUNT(*)), 2)
    AS cart_conversion_rate_pct

FROM cart_classification
GROUP BY cart_size_bucket
ORDER BY
  CASE cart_size_bucket
    WHEN '1 item' THEN 1
    WHEN '2 items' THEN 2
    WHEN '3-5 items' THEN 3
    WHEN '6-10 items' THEN 4
    WHEN '10+ items' THEN 5
  END;


  12. Successful carts per successful session


  WITH checkout_carts AS (
  SELECT DISTINCT
    ap.session_id,
    REGEXP_EXTRACT(
      ap.endpoint,
      r'^POST /api/v1/auth/cart/([^/]+)/checkout$'
    ) AS shp_cart_code
  FROM `performance_tracking.api_performance` ap
  WHERE REGEXP_CONTAINS(
    ap.endpoint,
    r'^POST /api/v1/auth/cart/[^/]+/checkout$'
  )
),

session_cart_map AS (
  SELECT DISTINCT
    cc.session_id,
    sc.SHP_CART_ID
  FROM checkout_carts cc
  INNER JOIN `performance_tracking.shopping_cart` sc
    ON cc.shp_cart_code = sc.SHP_CART_CODE
),

session_successful_carts AS (
  SELECT
    session_id,
    COUNT(DISTINCT SHP_CART_ID) AS successful_carts
  FROM session_cart_map
  GROUP BY session_id
)

SELECT
  COUNT(*) AS successful_sessions,
  SUM(successful_carts) AS total_successful_carts,

  ROUND(AVG(successful_carts), 2)
    AS avg_successful_carts_per_successful_session,

  MIN(successful_carts) AS min_successful_carts_per_successful_session,
  MAX(successful_carts) AS max_successful_carts_per_successful_session

FROM session_successful_carts;


13. Revenue lost from abandoned carts

This is a powerful metric for your thesis because it uses your existing logic and gives business meaning


WITH sold_cart_codes AS (
  SELECT DISTINCT
    REGEXP_EXTRACT(
      ap.endpoint,
      r'^POST /api/v1/auth/cart/([^/]+)/checkout$'
    ) AS shp_cart_code
  FROM `performance_tracking.api_performance` ap
  WHERE REGEXP_CONTAINS(
    ap.endpoint,
    r'^POST /api/v1/auth/cart/[^/]+/checkout$'
  )
),

sold_carts AS (
  SELECT DISTINCT
    sc.SHP_CART_ID
  FROM `performance_tracking.shopping_cart` sc
  INNER JOIN sold_cart_codes scc
    ON sc.SHP_CART_CODE = scc.shp_cart_code
),

product_price_one AS (
  SELECT
    av.PRODUCT_ID,
    MAX(pp.PRODUCT_PRICE_AMOUNT) AS product_price_amount
  FROM `performance_tracking.product_availability` av
  LEFT JOIN `performance_tracking.product_price` pp
    ON av.PRODUCT_AVAIL_ID = pp.PRODUCT_AVAIL_ID
  GROUP BY av.PRODUCT_ID
),

cart_values AS (
  SELECT
    sci.SHP_CART_ID,
    SUM(COALESCE(ppo.product_price_amount, 0) * COALESCE(sci.QUANTITY, 1)) AS cart_value
  FROM `performance_tracking.shopping_cart_item` sci
  LEFT JOIN product_price_one ppo
    ON sci.PRODUCT_ID = ppo.PRODUCT_ID
  GROUP BY sci.SHP_CART_ID
),

cart_classification AS (
  SELECT
    cv.SHP_CART_ID,
    cv.cart_value,
    CASE
      WHEN sc.SHP_CART_ID IS NOT NULL THEN 'sale'
      ELSE 'abandoned'
    END AS cart_status
  FROM cart_values cv
  LEFT JOIN sold_carts sc
    ON cv.SHP_CART_ID = sc.SHP_CART_ID
)

SELECT
  cart_status,

  COUNT(*) AS carts,

  ROUND(SUM(cart_value), 2) AS total_cart_value,

  ROUND(AVG(cart_value), 2) AS avg_cart_value,

  APPROX_QUANTILES(cart_value, 100)[OFFSET(50)] AS median_cart_value

FROM cart_classification
GROUP BY cart_status;


14. Session duration by conversion status

You have average session duration. Better version: compare converted vs non-converted sessions.


WITH session_duration AS (
  SELECT
    session_id,
    TIMESTAMP_DIFF(MAX(event_timestamp), MIN(event_timestamp), SECOND) AS session_duration_seconds
  FROM `performance_tracking.api_performance`
  GROUP BY session_id
),

sale_sessions AS (
  SELECT DISTINCT
    session_id
  FROM `performance_tracking.api_performance`
  WHERE REGEXP_CONTAINS(endpoint, r'^POST /api/v1/auth/cart/[^/]+/checkout$')
),

classified AS (
  SELECT
    sd.session_id,
    sd.session_duration_seconds,
    CASE
      WHEN ss.session_id IS NOT NULL THEN 'converted'
      ELSE 'not_converted'
    END AS session_status
  FROM session_duration sd
  LEFT JOIN sale_sessions ss
    ON sd.session_id = ss.session_id
)

SELECT
  session_status,
  COUNT(*) AS sessions,

  ROUND(AVG(session_duration_seconds) / 60, 2)
    AS avg_session_duration_minutes,

  ROUND(APPROX_QUANTILES(session_duration_seconds, 100)[OFFSET(50)] / 60, 2)
    AS median_session_duration_minutes,

  ROUND(APPROX_QUANTILES(session_duration_seconds, 100)[OFFSET(95)] / 60, 2)
    AS p95_session_duration_minutes

FROM classified
GROUP BY session_status;


15. Request count by conversion status

This checks whether converted sessions are more active.

WITH session_requests AS (
  SELECT
    session_id,
    COUNT(*) AS request_count
  FROM `performance_tracking.api_performance`
  GROUP BY session_id
),

sale_sessions AS (
  SELECT DISTINCT
    session_id
  FROM `performance_tracking.api_performance`
  WHERE REGEXP_CONTAINS(endpoint, r'^POST /api/v1/auth/cart/[^/]+/checkout$')
),

classified AS (
  SELECT
    sr.session_id,
    sr.request_count,
    CASE
      WHEN ss.session_id IS NOT NULL THEN 'converted'
      ELSE 'not_converted'
    END AS session_status
  FROM session_requests sr
  LEFT JOIN sale_sessions ss
    ON sr.session_id = ss.session_id
)

SELECT
  session_status,
  COUNT(*) AS sessions,

  ROUND(AVG(request_count), 2) AS avg_requests_per_session,

  APPROX_QUANTILES(request_count, 100)[OFFSET(50)] AS median_requests_per_session,

  APPROX_QUANTILES(request_count, 100)[OFFSET(95)] AS p95_requests_per_session

FROM classified
GROUP BY session_status;


16. Endpoint popularity by converted vs non-converted sessions

This is useful for customer journey analysis.

WITH sale_sessions AS (
  SELECT DISTINCT
    session_id
  FROM `performance_tracking.api_performance`
  WHERE REGEXP_CONTAINS(endpoint, r'^POST /api/v1/auth/cart/[^/]+/checkout$')
),

classified_events AS (
  SELECT
    ap.session_id,
    ap.endpoint,
    CASE
      WHEN ss.session_id IS NOT NULL THEN 'converted'
      ELSE 'not_converted'
    END AS session_status
  FROM `performance_tracking.api_performance` ap
  LEFT JOIN sale_sessions ss
    ON ap.session_id = ss.session_id
)

SELECT
  session_status,
  endpoint,
  COUNT(*) AS request_count,
  COUNT(DISTINCT session_id) AS sessions_using_endpoint

FROM classified_events
where endpoint not like '%/api/v1/cart/%'
and endpoint not like '%/auth/cart/%'
GROUP BY session_status, endpoint
ORDER BY session_status, request_count DESC
limit 10
;

17. First endpoint / entry-point analysis

This gives you the most common starting points of sessions.


WITH ranked_events AS (
  SELECT
    session_id,
    endpoint,
    event_timestamp,

    ROW_NUMBER() OVER (
      PARTITION BY session_id
      ORDER BY event_timestamp
    ) AS rn

  FROM `performance_tracking.api_performance`
)

SELECT
  endpoint AS first_endpoint,

  COUNT(*) AS sessions,

  ROUND(100 * SAFE_DIVIDE(COUNT(*), SUM(COUNT(*)) OVER ()), 2)
    AS share_of_sessions_pct

FROM ranked_events
WHERE rn = 1
GROUP BY first_endpoint
ORDER BY sessions DESC
limit 10;


18. Last endpoint before abandoned session

This is useful for diagnosing where sessions die.

WITH sale_sessions AS (
  SELECT DISTINCT
    session_id
  FROM `performance_tracking.api_performance`
  WHERE REGEXP_CONTAINS(endpoint, r'^POST /api/v1/auth/cart/[^/]+/checkout$')
),

non_converted_events AS (
  SELECT
    ap.session_id,
    ap.endpoint,
    ap.event_timestamp
  FROM `performance_tracking.api_performance` ap
  LEFT JOIN sale_sessions ss
    ON ap.session_id = ss.session_id
  WHERE ss.session_id IS NULL
),

ranked_events AS (
  SELECT
    session_id,
    endpoint,
    event_timestamp,

    ROW_NUMBER() OVER (
      PARTITION BY session_id
      ORDER BY event_timestamp DESC
    ) AS rn

  FROM non_converted_events
)

SELECT
  endpoint AS last_endpoint_before_abandonment,

  COUNT(*) AS abandoned_sessions,

  ROUND(100 * SAFE_DIVIDE(COUNT(*), SUM(COUNT(*)) OVER ()), 2)
    AS share_of_abandoned_sessions_pct

FROM ranked_events
WHERE rn = 1
GROUP BY last_endpoint_before_abandonment
ORDER BY abandoned_sessions DESC
limit 10;