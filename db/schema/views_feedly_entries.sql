DROP VIEW IF EXISTS vw_feedly_entry_details CASCADE;
CREATE VIEW vw_feedly_entry_details AS (
  WITH alts AS (
    SELECT
      row_number() over (PARTITION BY e.feedly_id) AS rownum,
      e.feedly_id,
      jsonb_array_elements(e.payload -> 'alternate') ->> 'href'::text AS url
      FROM
      feedly_entries e
  ),
   alt_urls AS (
     SELECT feedly_id, url FROM alts WHERE rownum = 1
   ),

   t1 AS (

      SELECT
          e.created_at
          , e.updated_at
          , e.feedly_id

          , (e.payload ->> 'visual'::text)::jsonb ->> 'url'::text AS visual_image_url
          , ((e.payload ->> 'enclosure'::text)::jsonb ->1)::jsonb ->> 'href'::text AS enclosure_image_url

          , e.payload ->> 'title'::text AS title
          , e.payload ->> 'author'::text AS author
          , COALESCE(e.payload ->> 'ampUrl'::text,
                     COALESCE(e.payload ->> 'canonicalUrl'::text,
                              COALESCE(u.url,
                                       e.payload ->> 'originId'::text))
                   ) AS url
          , e.payload ->> 'fingerprint'::text AS fingerprint
          , e.payload ->> 'originId'::text AS origin_url
          , e.payload ->> 'ampUrl'::text AS amp_url
          , e.payload ->> 'canonicalUrl'::text AS canonical_url
          , u.url as alternate_url
          , replace(substring(url from '.*://([^/]*)'),'www.','')  AS site
          , COALESCE((e.payload ->> 'content'::text)::jsonb, jsonb_build_object('content', null))::jsonb ->> 'content'::text AS content
          , COALESCE((e.payload ->> 'summary'::text)::jsonb, jsonb_build_object('content', null))::jsonb ->> 'content'::text AS summary
          , COALESCE((e.payload ->> 'leoSummary'::text)::jsonb, jsonb_build_object('sentences', null))::jsonb ->> 'sentences'::text AS leo_summary
          , e.published_at
          , (e.payload -> 'published'::text)::numeric AS published
          , TIMESTAMP 'epoch' + (e.payload -> 'crawled'::text)::numeric * INTERVAL '1 millisecond' AS crawled_at
          , (e.payload -> 'crawled'::text)::numeric AS crawled

          , COALESCE((e.payload ->> 'engagement'::text)::numeric, 0) AS engagement
          , COALESCE((e.payload ->> 'engagementRate'::text)::numeric, 0) AS engagement_rate
         FROM feedly_entries e
         LEFT JOIN alt_urls u on u.feedly_id = e.feedly_id
    )

    SELECT
      t1.*
    , case visual_image_url
      when 'none' then enclosure_image_url
	      else visual_image_url
      end as image_url
    , date_trunc('day', t1.published_at) as published_on
    , soundex(title) as soundex_title
    FROM t1
    ORDER BY t1.site
    , t1.author desc
    , soundex_title
    , engagement desc
    , t1.fingerprint
    , t1.created_at desc
    , t1.crawled_at desc
);

---

DROP MATERIALIZED VIEW IF EXISTS articles CASCADE;
CREATE MATERIALIZED VIEW articles AS (
  WITH t1 AS
  (
    SELECT
      vw_feedly_entry_details.*
      , row_number() over (
          PARTITION BY
            site
            , soundex_title
          ORDER BY
            author
          , engagement desc
          , engagement_rate desc
          , crawled_at desc
          , published_at desc
          , fingerprint
        ) AS duplicate_index
    FROM
      vw_feedly_entry_details
  )

  SELECT * FROM t1
  WHERE duplicate_index = 1
  ORDER BY
    created_at
  , updated_at
  , site
  , title
);
CREATE UNIQUE INDEX articles_idx ON articles (feedly_id);
CREATE INDEX articles_site_idx ON articles (site);
CREATE INDEX articles_author_idx ON articles (author);


--


DROP VIEW IF EXISTS vw_article_common_topics CASCADE;
CREATE VIEW vw_article_common_topics AS (
  SELECT
    e.created_at
  , e.updated_at
  , e.published_at
  , e.feedly_id
  , e.payload ->> 'title'::text AS title
  , jsonb_array_elements((e.payload ->> 'commonTopics'::text)::jsonb) ->> 'id'::text AS topic_id
  , jsonb_array_elements((e.payload ->> 'commonTopics'::text)::jsonb) ->> 'label'::text AS topic_label
  , (jsonb_array_elements((e.payload ->> 'commonTopics'::text)::jsonb) ->> 'score'::text)::numeric AS topic_score
  , jsonb_array_elements((e.payload ->> 'commonTopics'::text)::jsonb) ->> 'salienceLevel'::text AS topic_salince_level
  FROM feedly_entries e
  JOIN articles a on a.feedly_id = e.feedly_id
  ORDER BY 1
);

--

DROP VIEW IF EXISTS vw_article_common_common_topic_list CASCADE;
CREATE VIEW vw_article_common_common_topic_list AS (
  SELECT DISTINCT
    topic_id
  , topic_label
  FROM vw_article_common_topics e
  JOIN articles a on a.feedly_id = e.feedly_id
  ORDER BY 2
);

---

DROP VIEW IF EXISTS vw_article_entities CASCADE;
CREATE VIEW vw_article_entities AS (
  SELECT
    e.created_at
  , e.updated_at
  , e.published_at
  , e.feedly_id
  , e.payload ->> 'title'::text AS title
  , jsonb_array_elements((e.payload ->> 'entities'::text)::jsonb) ->> 'id'::text AS entity_id
  , jsonb_array_elements((e.payload ->> 'entities'::text)::jsonb) ->> 'label'::text AS entity_label
  , jsonb_array_elements((e.payload ->> 'entities'::text)::jsonb) ->> 'salienceLevel'::text AS entity_salince_level
  FROM feedly_entries e
  JOIN articles a on a.feedly_id = e.feedly_id
  ORDER BY 1
);

---

DROP VIEW IF EXISTS vw_article_entity_list CASCADE;
CREATE VIEW vw_article_entity_list AS (
  SELECT DISTINCT
    entity_id
  , entity_label
  FROM vw_article_entities
  ORDER BY 2
);

---

DROP VIEW IF EXISTS vw_article_keywords CASCADE;
CREATE VIEW vw_article_keywords AS (
  SELECT
    e.created_at
  , e.updated_at
  , e.published_at
  , e.feedly_id
  , e.payload ->> 'title'::text AS title
  , jsonb_array_elements_text(e.payload -> 'keywords'::text) AS keyword
  FROM feedly_entries e
  JOIN articles a on a.feedly_id = e.feedly_id
  ORDER BY 1
);

---

DROP VIEW IF EXISTS vw_article_keyword_list CASCADE;
CREATE VIEW vw_article_keyword_list AS (
  SELECT DISTINCT
    keyword
  FROM vw_article_keywords
  ORDER BY 1
);

---

DROP VIEW IF EXISTS vw_article_summary_sentences CASCADE;
CREATE VIEW vw_article_summary_sentences AS (
SELECT
    e.created_at
    , e.updated_at
    , e.published_at
    , e.feedly_id
    , e.payload ->> 'title'::text AS title
    , jsonb_array_elements((e.payload -> 'leoSummary'::text) -> 'sentences'::text) ->> 'text'::text AS summary_sentence
    , jsonb_array_elements((e.payload -> 'leoSummary'::text) -> 'sentences'::text) -> 'position'::text AS summary_sentence_position
    , jsonb_array_elements((e.payload -> 'leoSummary'::text) -> 'sentences'::text) -> 'score'::text AS summary_sentence_score
   FROM feedly_entries e
   JOIN articles a on a.feedly_id = e.feedly_id
   ORDER BY e.created_at, e.feedly_id
);

---

DROP VIEW IF EXISTS vw_article_priorities;
CREATE VIEW vw_article_priorities AS (
with t1 as (
SELECT
  e.feedly_id
, jsonb_array_elements_text(e.payload -> 'priorities'::text)::jsonb ->> 'id'::text AS priority_id
, jsonb_array_elements_text(e.payload -> 'priorities'::text)::jsonb ->> 'label'::text AS priority_label
, (jsonb_array_elements_text(e.payload -> 'priorities'::text)::jsonb -> 'searchTerms'::text)::jsonb -> 'parts'::text AS parts
FROM feedly_entries e
JOIN articles a on a.feedly_id = e.feedly_id
)

select
  e.created_at
, e.updated_at
, e.published_at
,  e.feedly_id
, t1.priority_id
, t1.priority_label
, jsonb_array_elements_text(t1.parts)::jsonb ->> 'id'::text as search_term_id
, jsonb_array_elements_text(t1.parts)::jsonb ->> 'label'::text as search_term_label
from t1
join feedly_entries e on e.feedly_id = t1.feedly_id
);

---

-- CREATE VIEW vw_xr_company_articles_with_sectors AS (
-- select
--   n.feedly_id
-- , n.topic_label
-- , n.topic_score
-- , c.uuid
-- , c.name
-- , c.relevance_score
-- , c.confidence_sore
-- , c.country_code
-- , c.city
-- , c.region
-- , oc.category
-- , g.category_group
-- , e.title
-- , e.published_at
-- , date_trunc('day', e.published_at) as published_on
-- , date_trunc('week', e.published_at) as published_week
-- , date_trunc('month', e.published_at) as published_month
-- from vw_feedly_entry_nlp_topics n
-- join vw_xr_nlp_topics t on t.topic_label = n.topic_label
-- join vw_feedly_entry_nlp_companies c on c.feedly_id = n.feedly_id
-- join vw_feedly_entry_details e on e.feedly_id = n.feedly_id
-- join vw_organization_categories oc on oc.uuid = c.uuid
-- join vw_organization_category_groups g on g.uuid = c.uuid
-- );




CREATE VIEW vw_xr_article_rankings AS (

with t1 as
(
select
*
, published_at at time zone 'utc' at time zone 'est' as published_at_with_tz
from
articles
where engagement is not null
)
, t2 as
(


select
t1.*
, date_trunc('day', published_at_with_tz) as published_day
, date_trunc('week', published_at_with_tz) as published_week
, date_trunc('month', published_at_with_tz) as published_month
, date_trunc('quarter', published_at_with_tz) as published_quarter
, date_trunc('year', published_at_with_tz) as published_year
from t1

)
, t3 as
(
select
t2.*
, dense_rank() over (partition by published_day order by engagement_rate desc) popularity_by_day
, dense_rank() over (partition by published_week order by engagement_rate desc) popularity_by_week
, dense_rank() over (partition by published_month order by engagement_rate desc) popularity_by_month
from t2
)

select * from t3 where
published_week = '2020-03-16 00:00:00'
and popularity_by_week <= 20
order by popularity_by_week asc
);
