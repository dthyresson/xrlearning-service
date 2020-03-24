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
  , ch.target
  , ch.target_id
  , c.name as concept_name
  , e.entity_type
  , e.entity_id
  , t.topic_label
  , a.feedly_id
  , lower(target || '-' || ch.target_id || '-' || ch.name || '-' || a.feedly_id || '-' || e.entity_type ||  '-' || e.entity_id || '-' || t.topic_label) as identifier
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
