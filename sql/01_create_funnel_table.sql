/*
📊 Incremental Funnel Summary Query (BigQuery + GA4)

This query incrementally updates a summary table with daily funnel data.
It's optimized for scheduled use (e.g., daily), reducing costs by processing only recent data.

🛠️ Setup Instructions:
- Replace `your_project.your_dataset.your_funnel_table` with your actual destination table.
- Replace the funnel event names below with your own funnel steps, e.g.:
  'first_visit', 'item_view', 'add_to_basket', 'checkout', 'purchase'
*/

MERGE `your_project.your_dataset.your_funnel_table` AS T
USING (
  -- Step 1: Capture last 3 days of funnel activity from GA4 event data
  WITH
    events_base AS (
      SELECT
        PARSE_DATE('%Y%m%d', event_date) AS event_day,
        user_pseudo_id,
        (SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_id') AS ga_session_id,
        event_name,
        event_timestamp,
        (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'page_location') AS page_location
      FROM
        `your_project.your_dataset.events_*`
      WHERE
        _TABLE_SUFFIX BETWEEN FORMAT_DATE('%Y%m%d', DATE_SUB(CURRENT_DATE(), INTERVAL 3 DAY)) AND FORMAT_DATE('%Y%m%d', CURRENT_DATE())
        AND event_name IN ('first_visit', 'item_view', 'add_to_basket', 'checkout', 'purchase') -- Replace with your funnel events
    ),

    session_funnel_data AS (
      SELECT
        CONCAT(user_pseudo_id, '.', CAST(ga_session_id AS STRING)) AS unique_session_id,
        MIN(IF(event_name = 'first_visit', event_day, NULL)) AS visit_date,
        MIN(IF(event_name = 'first_visit', page_location, NULL)) AS first_visit_page,
        MIN(IF(event_name = 'first_visit', event_timestamp, NULL)) AS first_visit_ts,
        MIN(IF(event_name = 'item_view', event_timestamp, NULL)) AS item_view_ts,
        MIN(IF(event_name = 'add_to_basket', event_timestamp, NULL)) AS add_to_basket_ts,
        MIN(IF(event_name = 'checkout', event_timestamp, NULL)) AS checkout_ts,
        MIN(IF(event_name = 'purchase', event_timestamp, NULL)) AS purchase_ts
      FROM events_base
      WHERE ga_session_id IS NOT NULL
      GROUP BY unique_session_id
    ),

    unpivoted_funnel AS (
        SELECT visit_date, first_visit_page, '1. First_Visit' AS Funnel_Step, COUNT(DISTINCT unique_session_id) AS User_Count
        FROM session_funnel_data
        WHERE visit_date IS NOT NULL AND first_visit_page IS NOT NULL
        GROUP BY 1, 2

        UNION ALL
        SELECT visit_date, first_visit_page, '2. Item_View' AS Funnel_Step, COUNT(DISTINCT IF(item_view_ts > first_visit_ts, unique_session_id, NULL))
        FROM session_funnel_data
        WHERE visit_date IS NOT NULL AND first_visit_page IS NOT NULL
        GROUP BY 1, 2

        UNION ALL
        SELECT visit_date, first_visit_page, '3. Add_to_Basket' AS Funnel_Step, COUNT(DISTINCT IF(add_to_basket_ts > COALESCE(item_view_ts, first_visit_ts), unique_session_id, NULL))
        FROM session_funnel_data
        WHERE visit_date IS NOT NULL AND first_visit_page IS NOT NULL
        GROUP BY 1, 2

        UNION ALL
        SELECT visit_date, first_visit_page, '4. Checkout' AS Funnel_Step, COUNT(DISTINCT IF(checkout_ts > COALESCE(add_to_basket_ts, item_view_ts, first_visit_ts), unique_session_id, NULL))
        FROM session_funnel_data
        WHERE visit_date IS NOT NULL AND first_visit_page IS NOT NULL
        GROUP BY 1, 2

        UNION ALL
        SELECT visit_date, first_visit_page, '5. Purchase' AS Funnel_Step, COUNT(DISTINCT IF(purchase_ts > COALESCE(checkout_ts, add_to_basket_ts, item_view_ts, first_visit_ts), unique_session_id, NULL))
        FROM session_funnel_data
        WHERE visit_date IS NOT NULL AND first_visit_page IS NOT NULL
        GROUP BY 1, 2
    )

  SELECT
    FORMAT_DATE('%m %d %Y', visit_date) AS Date,
    first_visit_page AS Page_URL,
    Funnel_Step,
    User_Count
  FROM unpivoted_funnel
) AS S

ON T.Date = S.Date AND T.Page_URL = S.Page_URL AND T.Funnel_Step = S.Funnel_Step

WHEN MATCHED THEN
  UPDATE SET T.User_Count = S.User_Count

WHEN NOT MATCHED BY TARGET THEN
  INSERT (Date, Page_URL, Funnel_Step, User_Count)
  VALUES (Date, Page_URL, Funnel_Step, User_Count);

-- SQL written by Ali Ahmadi
-- GitHub: https://github.com/AGhayeAli
-- For updates or related queries, visit: https://github.com/AGhayeAli/funnel-analytics

