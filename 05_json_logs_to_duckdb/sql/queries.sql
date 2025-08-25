-- 1) Події по користувачах (перевірка завантаження)
SELECT user_id, event, timestamp
FROM fact_events
ORDER BY timestamp;

-- 2) Частоти подій (core метрика)
SELECT event, COUNT(*) AS events
FROM fact_events
GROUP BY event
ORDER BY events DESC;

-- 3) Daily Active Users (DAU)
SELECT CAST(timestamp AS DATE) AS d, COUNT(DISTINCT user_id) AS dau
FROM fact_events
GROUP BY d
ORDER BY d DESC;

-- 4) Перша подія користувача (onboarding)
WITH first_evt AS (
  SELECT user_id, MIN(timestamp) AS first_ts
  FROM fact_events
  GROUP BY user_id
)
SELECT f.user_id, f.first_ts, e.event
FROM first_evt f
JOIN fact_events e
  ON e.user_id = f.user_id AND e.timestamp = f.first_ts
ORDER BY f.first_ts;

-- 5) Проста ретеншн-градація: чи повертався користувач у той самий день
WITH sessions AS (
  SELECT user_id, DATE_TRUNC('day', timestamp) AS d, COUNT(*) AS hits
  FROM fact_events
  GROUP BY user_id, d
),
retained AS (
  SELECT s1.user_id, s1.d AS day0,
         EXISTS(
           SELECT 1 FROM sessions s2
           WHERE s2.user_id = s1.user_id
             AND s2.d > s1.d
         ) AS returned_later
  FROM sessions s1
  QUALIFY ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY day0) = 1
)
SELECT returned_later, COUNT(*) AS users
FROM retained
GROUP BY returned_later;

-- 6) «Мікро-фанел»: login -> purchase в межах 1 дня
WITH per_user AS (
  SELECT user_id,
         MIN(CASE WHEN event='login' THEN timestamp END) AS first_login,
         MIN(CASE WHEN event='purchase' THEN timestamp END) AS first_purchase
  FROM fact_events
  GROUP BY user_id
)
SELECT
  COUNT(*)                                       AS users_total,
  COUNT(*) FILTER (WHERE first_login IS NOT NULL) AS users_logged,
  COUNT(*) FILTER (WHERE first_login IS NOT NULL AND first_purchase IS NOT NULL
                   AND DATE_TRUNC('day', first_login)=DATE_TRUNC('day', first_purchase)) AS users_purchased_same_day;
