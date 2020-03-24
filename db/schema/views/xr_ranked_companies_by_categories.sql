DROP VIEW IF EXISTS vw_xr_ranked_companies_by_categories_by_day CASCADE;
CREATE VIEW vw_xr_ranked_companies_by_categories_by_day AS
(
  WITH t1 AS
  (
   SELECT
       'day' as time_period_label
       , date_trunc('day', published_at at time zone 'utc' at time zone 'est') as time_period
       , oc.category
       , c.name
       , count(c.feedly_id) as freq
     FROM
     vw_article_nlp_companies c
     JOIN articles a on a.feedly_id = c.feedly_id
     JOIN vw_organization_categories oc ON oc.uuid = c.uuid
     GROUP BY 1, 2, 3, 4
     HAVING count(c.feedly_id) > 1
     ORDER by 1, 2, 5 desc, 3, 4
  ), t2 AS
  (
    SELECT
      t1.*
    , dense_rank() OVER (PARTITION BY time_period_label, time_period, category ORDER BY freq DESC) as rank
    , max(freq) OVER (PARTITION BY time_period_label, time_period, category) as max_freq
    , min(freq) OVER (PARTITION BY time_period_label, time_period, category) as min_freq
    FROM t1
  )

  SELECT
    t2.*
    , freq / max_freq::numeric as pct
    , RPAD ('#'::text, (20 * (freq / max_freq::numeric))::integer, '#'::text) as histogram
  FROM t2
  ORDER BY time_period_label, time_period DESC, category, rank ASC, name
);

---

DROP VIEW IF EXISTS vw_xr_ranked_companies_by_categories_by_week CASCADE;
CREATE VIEW vw_xr_ranked_companies_by_categories_by_week AS
(
  WITH t1 AS
  (
   SELECT
       'week' as time_period_label
       , date_trunc('week', published_at at time zone 'utc' at time zone 'est') as time_period
       , oc.category
       , c.name
       , count(c.feedly_id) as freq
     FROM
     vw_article_nlp_companies c
     JOIN articles a on a.feedly_id = c.feedly_id
     JOIN vw_organization_categories oc ON oc.uuid = c.uuid
     GROUP BY 1, 2, 3, 4
     HAVING count(c.feedly_id) > 1
     ORDER by 1, 2, 5 desc, 3, 4
  ), t2 AS
  (
    SELECT
      t1.*
    , dense_rank() OVER (PARTITION BY time_period_label, time_period, category ORDER BY freq DESC) as rank
    , max(freq) OVER (PARTITION BY time_period_label, time_period, category) as max_freq
    , min(freq) OVER (PARTITION BY time_period_label, time_period, category) as min_freq
    FROM t1
  )

  SELECT
    t2.*
    , freq / max_freq::numeric as pct
    , RPAD ('#'::text, (20 * (freq / max_freq::numeric))::integer, '#'::text) as histogram
  FROM t2
  ORDER BY time_period_label, time_period DESC, category, rank ASC, name
);

---

DROP VIEW IF EXISTS vw_xr_ranked_companies_by_categories_by_month CASCADE;
CREATE VIEW vw_xr_ranked_companies_by_categories_by_month AS
(
  WITH t1 AS
  (
   SELECT
       'month' as time_period_label
       , date_trunc('month', published_at at time zone 'utc' at time zone 'est') as time_period
       , oc.category
       , c.name
       , count(c.feedly_id) as freq
     FROM
     vw_article_nlp_companies c
     JOIN articles a on a.feedly_id = c.feedly_id
     JOIN vw_organization_categories oc ON oc.uuid = c.uuid
     GROUP BY 1, 2, 3, 4
     HAVING count(c.feedly_id) > 1
     ORDER by 1, 2, 5 desc, 3, 4
  ), t2 AS
  (
    SELECT
      t1.*
    , dense_rank() OVER (PARTITION BY time_period_label, time_period, category ORDER BY freq DESC) as rank
    , max(freq) OVER (PARTITION BY time_period_label, time_period, category) as max_freq
    , min(freq) OVER (PARTITION BY time_period_label, time_period, category) as min_freq
    FROM t1
  )

  SELECT
    t2.*
    , freq / max_freq::numeric as pct
    , RPAD ('#'::text, (20 * (freq / max_freq::numeric))::integer, '#'::text) as histogram
  FROM t2
  ORDER BY time_period_label, time_period DESC, category, rank ASC, name
);

---
