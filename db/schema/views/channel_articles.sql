DROP MATERIALIZED VIEW IF EXISTS vw_xr_channel_articles CASCADE;
CREATE MATERIALIZED VIEW vw_xr_channel_articles AS (
  WITH sentences AS
  (
    SELECT
    a.feedly_id
    , array_agg(s.summary_sentence) as summary_sentences
    FROM articles a
    LEFT JOIN vw_article_summary_sentences s on s.feedly_id = a.feedly_id
    GROUP BY a.feedly_id
  )

  SELECT
  DISTINCT
    ch.id as channel_id
  , ch.name as channel_name
  , ch.emoji_icon channel_emoji_icon
  , ch.target
  , ch.target_id
  , c.name as concept_name
  , c.emoji_icon concept_emoji_icon
  , r.emoji_icon concept_rule_emoji_icon
  , e.entity_type
  , e.entity_id
  , t.topic_label
  , a.feedly_id
  , lower(channel_id || '-' || c.name || '-' || target || '-' || ch.target_id || '-' || ch.name || '-' || a.feedly_id || '-' || e.entity_type ||  '-' || e.entity_id || '-' || t.topic_label) as identifier
  , a.title
  , a.url
  , a.author
  , a.site
  , a.image_url
  , sentences.summary_sentences
  , a.engagement_rate
  , a.engagement
  , a.created_at
  , a.updated_at
  , a.published_at
  , a.published_at at time zone 'utc' at time zone 'est' as published_at_with_tz
  FROM vw_article_nlp_entities e

  JOIN articles a ON a.feedly_id = e.feedly_id
  JOIN vw_article_nlp_topics np ON np.feedly_id = a.feedly_id
  JOIN concept_entity_rules r ON r.entity_type = e.entity_type
                                 AND e.confidence_sore >= r.entity_confidence_score_threshold
                                 AND e.relevance_score >= r.entity_relevance_score_threshold

  JOIN channel_concepts cc ON cc.concept_id = r.concept_id
  JOIN concepts c ON c.id = cc.concept_id AND r.concept_id = c.id
  JOIN channels ch ON ch.id = cc.channel_id
  JOIN vw_xr_topics t ON t.topic_label = np.topic_label
                         AND np.topic_score >= r.topic_score_threshold

  LEFT JOIN sentences on sentences.feedly_id = e.feedly_id

  ORDER BY
    c.name
  , e.entity_type
  , entity_id
  , t.topic_label
  , published_at desc
);

CREATE INDEX xr_channel_articles_channel_id_idx ON vw_xr_channel_articles USING btree (channel_id);
CREATE INDEX xr_channel_articles_channel_name_idx ON vw_xr_channel_articles USING btree (channel_name);

CREATE INDEX xr_channel_articles_entity_type_idx ON vw_xr_channel_articles USING btree (entity_type);
CREATE INDEX xr_channel_articles_entity_id_idx ON vw_xr_channel_articles USING btree (entity_id);

CREATE INDEX xr_channel_articles_feedly_id_idx ON vw_xr_channel_articles USING btree (feedly_id);
-- CREATE INDEX xr_channel_articles_entity_identifier_idx ON vw_xr_channel_articles USING btree (identifier);

CREATE INDEX xr_channel_articles_topic_label_idx ON vw_xr_channel_articles USING btree (topic_label);
CREATE INDEX xr_channel_articles_target_idx ON vw_xr_channel_articles USING btree (target);
CREATE INDEX xr_channel_articles_target_id_idx ON vw_xr_channel_articles USING btree (target_id);

CREATE UNIQUE INDEX vw_xr_channel_articles_uniqueness
  ON vw_xr_channel_articles (identifier);

---

DROP MATERIALIZED VIEW IF EXISTS vw_xr_channel_article_details CASCADE;
CREATE MATERIALIZED VIEW vw_xr_channel_article_details AS (
  SELECT
    ca.channel_id
  , ca.channel_emoji_icon
  , ca.target
  , ca.target_id
  , ca.feedly_id
  , ca.summary_sentences
  , array_agg(DISTINCT ca.concept_name) FILTER (WHERE ca.concept_name IS NOT NULL) as concept_names
  , array_agg(DISTINCT ca.concept_emoji_icon) FILTER (WHERE ca.concept_emoji_icon IS NOT NULL) as concept_emoji_icons
  , array_agg(DISTINCT ca.concept_rule_emoji_icon) FILTER (WHERE ca.concept_rule_emoji_icon IS NOT NULL) as concept_rule_emoji_icons
  , array_agg(DISTINCT ca.entity_type) FILTER (WHERE ca.entity_type IS NOT NULL) as entity_types
  , array_agg(DISTINCT ca.topic_label) FILTER (WHERE ca.topic_label IS NOT NULL) as topic_labels
  , array_agg(DISTINCT c.category) FILTER (WHERE c.category IS NOT NULL) AS categories
  , array_agg(DISTINCT c.category_group) FILTER (WHERE c.category_group IS NOT NULL) AS category_group
  , array_agg(DISTINCT c.name) AS company_names
  FROM vw_xr_channel_articles ca
    LEFT JOIN xr_company_articles_with_sectors c ON c.feedly_id = ca.feedly_id
  GROUP BY 1, 2, 3, 4, 5, 6
);

CREATE INDEX vw_xr_channel_article_details_channel_id_idx ON vw_xr_channel_article_details USING btree (channel_id);
CREATE INDEX vw_xr_channel_article_details_target_idx ON vw_xr_channel_article_details USING btree (target);
CREATE INDEX vw_xr_channel_article_details_target_id_idx ON vw_xr_channel_article_details USING btree (target_id);
CREATE INDEX vw_xr_channel_article_details_feedly_id_idx ON vw_xr_channel_article_details USING btree (feedly_id);

CREATE UNIQUE INDEX vw_xr_channel_article_details_uniqueness
  ON vw_xr_channel_article_details (channel_id, target, target_id, feedly_id);

---

DROP VIEW IF EXISTS vw_xr_channel_articles_unsent_by_channel_and_target CASCADE;
CREATE VIEW vw_xr_channel_articles_unsent_by_channel_and_target AS (
  SELECT
    ch.id as channel_id
  , ch.name as channel_name
  , ch.emoji_icon channel_emoji_icon
  , d.target
  , d.target_id
  , a.feedly_id
  , a.title
  , a.site
  , a.author
  , a.url
  , a.image_url
  , d.summary_sentences
  , d.concept_names
  , d.concept_emoji_icons
  , d.concept_rule_emoji_icons
  , d.entity_types
  , d.topic_labels
  , d.categories
  , d.category_group
  , d.company_names
  , a.engagement_rate
  , a.engagement
  , a.created_at
  , a.updated_at
  , a.published_at
  , timezone('est'::text, timezone('utc'::text, a.published_at)) AS published_at_with_tz
  , ch.last_sent_at
  FROM articles a
  JOIN vw_xr_channel_article_details d on d.feedly_id = a.feedly_id
  JOIN channels ch on ch.id = d.channel_id
  WHERE
    a.created_at > ch.last_sent_at
  ORDER BY 1, 2, a.published_at
);

---
