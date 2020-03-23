DROP VIEW IF EXISTS vw_xr_ranked_categories_by_day CASCADE;
CREATE VIEW vw_xr_ranked_categories_by_day AS
(
	WITH t1 AS
	(
		SELECT
		    'day' as time_period_label
		    , date_trunc('day', published_at at time zone 'utc' at time zone 'est') as time_period
		    , oc.category
		    , count(c.feedly_id) as freq
		  FROM
		  vw_article_nlp_companies c
		  JOIN articles a on a.feedly_id = c.feedly_id
			JOIN vw_organization_categories oc ON oc.uuid = c.uuid
		  GROUP BY 1, 2, 3
		  HAVING count(c.feedly_id) > 1
		  ORDER by 1, 2, 4 desc, 3
	), t2 AS
	(
		SELECT
		  t1.*
		, dense_rank() OVER (PARTITION BY time_period_label, time_period ORDER BY freq DESC) as rank
		, max(freq) OVER (PARTITION BY time_period_label, time_period) as max_freq
		, min(freq) OVER (PARTITION BY time_period_label, time_period) as min_freq
		FROM t1
	)

	SELECT
	  t2.*
    , freq / max_freq::numeric as pct
    , RPAD ('#'::text, (20 * (freq / max_freq::numeric))::integer, '#'::text) as histogram
	FROM t2
	ORDER BY time_period_label, time_period DESC, rank ASC, category
);

---

DROP VIEW IF EXISTS vw_xr_ranked_categories_by_week CASCADE;
CREATE VIEW vw_xr_ranked_categories_by_week AS
(
	WITH t1 AS
	(
		SELECT
		    'week' as time_period_label
		    , date_trunc('week', published_at at time zone 'utc' at time zone 'est') as time_period
		    , oc.category
		    , count(c.feedly_id) as freq
		  FROM
		  vw_article_nlp_companies c
		  JOIN articles a on a.feedly_id = c.feedly_id
			JOIN vw_organization_categories oc ON oc.uuid = c.uuid
		  GROUP BY 1, 2, 3
		  HAVING count(c.feedly_id) > 1
		  ORDER by 1, 2, 4 desc, 3
	), t2 AS
	(
		SELECT
		  t1.*
		, dense_rank() OVER (PARTITION BY time_period_label, time_period ORDER BY freq DESC) as rank
		, max(freq) OVER (PARTITION BY time_period_label, time_period) as max_freq
		, min(freq) OVER (PARTITION BY time_period_label, time_period) as min_freq
		FROM t1
	)

	SELECT
	  t2.*
    , freq / max_freq::numeric as pct
    , RPAD ('#'::text, (20 * (freq / max_freq::numeric))::integer, '#'::text) as histogram
	FROM t2
	ORDER BY time_period_label, time_period DESC, rank ASC, category
);

---

DROP VIEW IF EXISTS vw_xr_ranked_categories_by_month CASCADE;
CREATE VIEW vw_xr_ranked_categories_by_month AS
(
	WITH t1 AS
	(
		SELECT
		    'month' as time_period_label
		    , date_trunc('month', published_at at time zone 'utc' at time zone 'est') as time_period
		    , oc.category
		    , count(c.feedly_id) as freq
		  FROM
		  vw_article_nlp_companies c
		  JOIN articles a on a.feedly_id = c.feedly_id
			JOIN vw_organization_categories oc ON oc.uuid = c.uuid
		  GROUP BY 1, 2, 3
		  HAVING count(c.feedly_id) > 1
		  ORDER by 1, 2, 4 desc, 3
	), t2 AS
	(
		SELECT
		  t1.*
		, dense_rank() OVER (PARTITION BY time_period_label, time_period ORDER BY freq DESC) as rank
		, max(freq) OVER (PARTITION BY time_period_label, time_period) as max_freq
		, min(freq) OVER (PARTITION BY time_period_label, time_period) as min_freq
		FROM t1
	)

	SELECT
	  t2.*
    , freq / max_freq::numeric as pct
    , RPAD ('#'::text, (20 * (freq / max_freq::numeric))::integer, '#'::text) as histogram
	FROM t2
	ORDER BY time_period_label, time_period DESC, rank ASC, category
);

---
