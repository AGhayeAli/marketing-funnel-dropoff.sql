/*
📉 Drop-off Funnel Analysis for Top Landing Pages (Incremental, GA4 + BigQuery)

This query incrementally updates a summary table with daily drop-off data
for key landing pages in your funnel.

🛠️ Before using:
- Replace `your_project.your_dataset.your_target_table` with your table name
- Replace example landing page URLs (e.g., https://x.com/...) with your real landing pages
- Replace event names with your funnel steps:
    'first_visit'     -- stays the same
    'item_view'       -- your first funnel action (e.g., replacing get_started_click)
    'add_to_basket'   -- your second funnel action (e.g., replacing checkout_button_click)
    'checkout'        -- your third funnel action (e.g., replacing add_credit_click)
    'purchase'        -- stays the same
*/

MERGE `your_project.your_dataset.your_target_table` AS T
USING (
  WITH
    -- Step 1: Sessions that began on specified landing pages
    target_sessions AS (
      SELECT DISTINCT
        user_pseudo_id,
        (SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_id') AS ga_session_id
      FROM `your_project.your_dataset.events_*`
      WHERE
        _TABLE_SUFFIX BETWEEN FORMAT_DATE('%Y%m%d', DATE_SUB(CURRENT_DATE(), INTERVAL 3 DAY)) AND FORMAT_DATE('%Y%m%d', CURRENT_DATE())
        AND event_name = 'first_visit'
        AND (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'page_location') IN (
          'https://x.com/', 'https://x.com/your-landing-page-1/', 'https://x.com/your-landing-page-2/',
          'https://x.com/your-landing-page-3/', 'https://x.com/your-landing-page-4/'
        )
    ),

    -- Step 2: All funnel events for those sessions
    events_base AS (
      SELECT
        PARSE_DATE('%Y%m%d', e.event_date) AS event_day,
        e.user_pseudo_id,
        (SELECT value.int_value FROM UNNEST(e.event_params) WHERE key = 'ga_session_id') AS ga_session_id,
        e.event_name,
        e.event_timestamp,
        (SELECT value.string_value FROM UNNEST(e.event_params) WHERE key = 'page_location') AS page_location,
        (SELECT COALESCE(value.double_value, value.int_value) FROM UNNEST(e.event_params) WHERE key = 'value') AS event_value
      FROM `your_project.your_dataset.events_*` AS e
      INNER JOIN target_sessions AS ts
        ON e.user_pseudo_id = ts.user_pseudo_id
        AND (SELECT value.int_value FROM UNNEST(e.event_params) WHERE key = 'ga_session_id') = ts.ga_session_id
      WHERE
        e._TABLE_SUFFIX BETWEEN FORMAT_DATE('%Y%m%d', DATE_SUB(CURRENT_DATE(), INTERVAL 3 DAY)) AND FORMAT_DATE('%Y%m%d', CURRENT_DATE())
        AND e.event_name IN ('first_visit', 'item_view', 'add_to_basket', 'checkout', 'purchase')  -- Customize to match your funnel
    ),

    -- Step 3: One row per session with funnel timestamps
    session_funnel_data AS (
      SELECT
        CONCAT(user_pseudo_id, '.', CAST(ga_session_id AS STRING)) AS unique_session_id,
        MIN(IF(event_name = 'first_visit', event_day, NULL)) AS visit_date,
        MIN(IF(event_name = 'first_visit', page_location, NULL)) AS first_visit_page,
        MIN(IF(event_name = 'first_visit', event_timestamp, NULL)) AS first_visit_ts,
        MIN(IF(event_name = 'item_view', event_timestamp, NULL)) AS item_view_ts,
        MIN(IF(event_name = 'add_to_basket', event_timestamp, NULL)) AS add_to_basket_ts,
        MIN(IF(event_name = 'checkout', event_timestamp, NULL)) AS checkout_ts,
        MIN(IF(event_name = 'purchase', event_timestamp, NULL)) AS purchase_ts,
        SUM(IF(event_name = 'purchase', event_value, 0)) AS purchase_value
      FROM events_base
      WHERE ga_session_id IS NOT NULL
      GROUP BY unique_session_id
    ),

    -- Step 4: Aggregate counts and drop-offs
    funnel_counts_by_page AS (
      SELECT
        visit_date,
        first_visit_page,
        COUNT(DISTINCT IF(first_visit_ts IS NOT NULL, unique_session_id, NULL)) AS First_Visits_Count,
        COUNT(DISTINCT IF(item_view_ts > first_visit_ts, unique_session_id, NULL)) AS Item_View_Count,
        COUNT(DISTINCT IF(add_to_basket_ts > COALESCE(item_view_ts, first_visit_ts), unique_session_id, NULL)) AS Add_To_Basket_Count,
        COUNT(DISTINCT IF(checkout_ts > COALESCE(add_to_basket_ts, item_view_ts, first_visit_ts), unique_session_id, NULL)) AS Checkout_Count,
        COUNT(DISTINCT IF(purchase_ts > COALESCE(checkout_ts, add_to_basket_ts, item_view_ts, first_visit_ts), unique_session_id, NULL)) AS Purchase_Count,
        SUM(purchase_value) AS Total_Purchase_Value
      FROM session_funnel_data
      WHERE first_visit_page IS NOT NULL AND visit_date IS NOT NULL
      GROUP BY visit_date, first_visit_page
    )

  -- Step 5: Final SELECT for source data with drop-off %
  SELECT
    FORMAT_DATE('%m %d %Y', visit_date) AS Date,
    first_visit_page AS Page_URL,
    First_Visits_Count,
    ROUND(IFNULL(SAFE_DIVIDE(First_Visits_Count - Item_View_Count, First_Visits_Count) * 100, 0), 2) AS Dropoff_To_Item_View_Pct,
    Item_View_Count,
    ROUND(IFNULL(SAFE_DIVIDE(Item_View_Count - Add_To_Basket_Count, Item_View_Count) * 100, 0), 2) AS Dropoff_To_Add_To_Basket_Pct,
    Add_To_Basket_Count,
    ROUND(IFNULL(SAFE_DIVIDE(Add_To_Basket_Count - Checkout_Count, Add_To_Basket_Count) * 100, 0), 2) AS Dropoff_To_Checkout_Pct,
    Checkout_Count,
    ROUND(IFNULL(SAFE_DIVIDE(Checkout_Count - Purchase_Count, Checkout_Count) * 100, 0), 2) AS Dropoff_To_Purchase_Pct,
    Purchase_Count,
    Total_Purchase_Value
  FROM funnel_counts_by_page
  WHERE First_Visits_Count > 0
) AS S

-- Matching logic
ON T.Date = S.Date AND T.Page_URL = S.Page_URL

-- Update existing rows
WHEN MATCHED THEN
  UPDATE SET
    T.First_Visits_Count = S.First_Visits_Count,
    T.Dropoff_To_Item_View_Pct = S.Dropoff_To_Item_View_Pct,
    T.Item_View_Count = S.Item_View_Count,
    T.Dropoff_To_Add_To_Basket_Pct = S.Dropoff_To_Add_To_Basket_Pct,
    T.Add_To_Basket_Count = S.Add_To_Basket_Count,
    T.Dropoff_To_Checkout_Pct = S.Dropoff_To_Checkout_Pct,
    T.Checkout_Count = S.Checkout_Count,
    T.Dropoff_To_Purchase_Pct = S.Dropoff_To_Purchase_Pct,
    T.Purchase_Count = S.Purchase_Count,
    T.Total_Purchase_Value = S.Total_Purchase_Value

-- Insert new rows
WHEN NOT MATCHED BY TARGET THEN
  INSERT (
    Date, Page_URL,
    First_Visits_Count, Dropoff_To_Item_View_Pct, Item_View_Count,
    Dropoff_To_Add_To_Basket_Pct, Add_To_Basket_Count,
    Dropoff_To_Checkout_Pct, Checkout_Count,
    Dropoff_To_Purchase_Pct, Purchase_Count, Total_Purchase_Value
  )
  VALUES (
    Date, Page_URL,
    First_Visits_Count, Dropoff_To_Item_View_Pct, Item_View_Count,
    Dropoff_To_Add_To_Basket_Pct, Add_To_Basket_Count,
    Dropoff_To_Checkout_Pct, Checkout_Count,
    Dropoff_To_Purchase_Pct, Purchase_Count, Total_Purchase_Value
  );

-- SQL written by Ali Ahmadi
-- GitHub: https://github.com/AGhayeAli
-- View more: https://github.com/AGhayeAli/funnel-analytics
