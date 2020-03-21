---

DROP VIEW IF EXISTS vw_article_topics_by_month CASCADE;
CREATE VIEW vw_article_topics_by_month AS (
  SELECT
    'month' as time_period_label
    , date_trunc('month', published_at) as time_period
    , topic_label
    , count(feedly_id) as count
  FROM
  vw_article_common_topics
  GROUP BY 1, 2, 3
  HAVING count(feedly_id) > 1
  ORDER by 1, 2, 4 desc, 3
);

---

DROP VIEW IF EXISTS vw_article_topics_by_week CASCADE;
CREATE VIEW vw_article_topics_by_week AS (
  SELECT
    'week' as time_period_label
    , date_trunc('week', published_at) as time_period
    , topic_label
    , count(feedly_id) as count
  FROM
  vw_article_common_topics
  GROUP BY 1, 2, 3
  HAVING count(feedly_id) > 1
  ORDER by 1, 2, 4 desc, 3
);

---

DROP VIEW IF EXISTS vw_article_topics_by_day CASCADE;
CREATE VIEW vw_article_topics_by_day AS (
  SELECT
    'day' as time_period_label
    , date_trunc('day', published_at) as time_period
    , topic_label
    , count(feedly_id) as count
  FROM
  vw_article_common_topics
  GROUP BY 1, 2, 3
  HAVING count(feedly_id) > 1
  ORDER by 1, 2, 4 desc, 3
);

---

---

DROP VIEW IF EXISTS vw_article_keywords_by_month CASCADE;
CREATE VIEW vw_article_keywords_by_month AS (
  SELECT
    'month' as time_period_label
    , date_trunc('month', published_at) as time_period
    , keyword
    , count(feedly_id) as count
  FROM
  vw_article_keywords
  GROUP BY 1, 2, 3
  HAVING count(feedly_id) > 1
  ORDER by 1, 2, 4 desc, 3
);

---

DROP VIEW IF EXISTS vw_article_keywords_by_week CASCADE;
CREATE VIEW vw_article_keywords_by_week AS (
  SELECT
    'week' as time_period_label
    , date_trunc('week', published_at) as time_period
    , keyword
    , count(feedly_id) as count
  FROM
  vw_article_keywords
  GROUP BY 1, 2, 3
  HAVING count(feedly_id) > 1
  ORDER by 1, 2, 4 desc, 3
);

---

DROP VIEW IF EXISTS vw_article_keywords_by_day CASCADE;
CREATE VIEW vw_article_keywords_by_day AS (
  SELECT
    'day' as time_period_label
    , date_trunc('day', published_at) as time_period
    , keyword
    , count(feedly_id) as count
  FROM
  vw_article_keywords
  GROUP BY 1, 2, 3
  HAVING count(feedly_id) > 1
  ORDER by 1, 2, 4 desc, 3
);

---

---

DROP VIEW IF EXISTS vw_article_entities_by_month CASCADE;
CREATE VIEW vw_article_entities_by_month AS (
  SELECT
    'month' as time_period_label
    , date_trunc('month', published_at) as time_period
    , entity_label
    , count(feedly_id) as count
  FROM
  vw_article_entities
  GROUP BY 1, 2, 3
  HAVING count(feedly_id) > 1
  ORDER by 1, 2, 4 desc, 3
);

---

DROP VIEW IF EXISTS vw_article_entities_by_week CASCADE;
CREATE VIEW vw_article_entities_by_week AS (
  SELECT
    'week' as time_period_label
    , date_trunc('week', published_at) as time_period
    , entity_label
    , count(feedly_id) as count
  FROM
  vw_article_entities
  GROUP BY 1, 2, 3
  HAVING count(feedly_id) > 1
  ORDER by 1, 2, 4 desc, 3
);

---

DROP VIEW IF EXISTS vw_article_entities_by_day CASCADE;
CREATE VIEW vw_article_entities_by_day AS (
  SELECT
    'day' as time_period_label
    , date_trunc('day', published_at) as time_period
    , entity_label
    , count(feedly_id) as count
  FROM
  vw_article_entities
  GROUP BY 1, 2, 3
  HAVING count(feedly_id) > 1
  ORDER by 1, 2, 4 desc, 3
);

---

DROP VIEW IF EXISTS vw_article_entities_by_topics CASCADE;
CREATE VIEW vw_article_entities_by_topics AS (
SELECT
  e.created_at
, e.updated_at
, e.published_at
, e.feedly_id
, e.title
, e.entity_id
, e.entity_label
, t.topic_id
, t.topic_label
FROM
  vw_article_entities e
LEFT JOIN vw_article_common_topics t on t.feedly_id = e.feedly_id
ORDER BY
  e.feedly_id, e.entity_label, t.topic_label
);

---

DROP VIEW IF EXISTS vw_article_entities_by_keywords CASCADE;
CREATE VIEW vw_article_entities_by_keywords AS (
SELECT
  e.created_at
, e.updated_at
, e.published_at
, e.feedly_id
, e.title
, e.entity_id
, e.entity_label
, k.keyword
FROM
  vw_article_entities e
LEFT JOIN vw_article_keywords k on k.feedly_id = e.feedly_id
ORDER BY
  e.feedly_id, e.entity_label, k.keyword
);

---
