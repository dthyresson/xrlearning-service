DROP VIEW IF EXISTS vw_article_nlp_topics CASCADE;
CREATE VIEW vw_article_nlp_topics AS (
  SELECT
    e.feedly_id
  , jsonb_array_elements((e.payload -> 'topics'::text)) -> 'id'::text AS topic_position
  , jsonb_array_elements((e.payload -> 'topics'::text)) ->> 'label'::text AS topic_label
  , (jsonb_array_elements((e.payload -> 'topics'::text)) -> 'score'::text)::numeric AS topic_score
  , jsonb_array_elements((e.payload -> 'topics'::text)) ->> 'wikiLink'::text AS topic_wiki_link
  , jsonb_array_elements((e.payload -> 'topics'::text)) ->> 'wikidataId'::text AS topic_wikidata_id

  FROM feedly_entry_text_analyses e
  JOIN articles a on a.feedly_id = e.feedly_id
  ORDER BY e.created_at, e.feedly_id
);

---

-- see: https://www.textrazor.com/blog/2019/02/crunchbase-lei-permid-and-openfigi-company-identifiers.html

/*

permid	Links to Thomson Reuters Open PermID. PermID provides comprehensive identification across a wide variety of entity types including organizations, instruments, funds, issuers and people. PermID never changes and is unambiguous, making it ideal as a reference identifier.

lei	A Legal Entity Identifier (or LEI) is a international identifier made up of a 20-character identifier that identifies distinct legal entities that engage in financial transactions. It is defined by ISO 17442. Natural persons are not required to have an LEI; they’re eligible to have one issued, however, but only if they act in an independent business capacity. The LEI is a global standard, designed to be non-proprietary data that is freely accessible to all. As of December 2018, over 1,300,000 legal entities from more than 200 countries have now been issued with LEIs.

crunchbaseId	Crunchbase is a platform for finding business information about private and public companies. Crunchbase information includes investments and funding information, founding members and individuals in leadership positions, mergers and acquisitions, news, and industry trends. Originally built to track startups, the Crunchbase website contains information on public and private companies globally.

figi	The Financial Instrument Global Identifier (FIGI) (formerly Bloomberg Global Identifier (BBGID)) is an open standard, unique identifier of financial instruments that can be assigned to instruments including common stock, options, derivatives, futures, corporate and government bonds, municipals, currencies, and mortgage products.
*/

DROP VIEW IF EXISTS vw_article_nlp_entities CASCADE;
CREATE VIEW vw_article_nlp_entities AS (
SELECT DISTINCT
  e.feedly_id
, jsonb_array_elements(jsonb_array_elements((e.payload -> 'entities'::text)) -> 'type'::text) ->> 0 AS entity_type
, jsonb_array_elements((e.payload -> 'entities'::text)) ->> 'entityId'::text AS entity_id
, jsonb_array_elements((e.payload -> 'entities'::text)) ->> 'unit'::text AS unit
, jsonb_array_elements((e.payload -> 'entities'::text)) ->> 'crunchbaseId'::text AS crunchbase_id
, jsonb_array_elements((e.payload -> 'entities'::text)) ->> 'figi'::text AS figi
, jsonb_array_elements((e.payload -> 'entities'::text)) ->> 'freebaseId'::text AS freebase_id
, jsonb_array_elements((e.payload -> 'entities'::text)) ->> 'lei'::text AS lei
, jsonb_array_elements((e.payload -> 'entities'::text)) ->> 'permid'::text AS permid
, jsonb_array_elements((e.payload -> 'entities'::text)) ->> 'wikidataId'::text AS wikidata_id
, jsonb_array_elements((e.payload -> 'entities'::text)) ->> 'wikiLink'::text AS wiki_link
, (jsonb_array_elements((e.payload -> 'entities'::text)) -> 'relevanceScore'::text)::numeric AS relevance_score
, (jsonb_array_elements((e.payload -> 'entities'::text)) -> 'confidenceScore'::text)::numeric  AS confidence_sore
FROM feedly_entry_text_analyses e
JOIN articles a on a.feedly_id = e.feedly_id
ORDER by e.feedly_id
);

---

DROP VIEW IF EXISTS vw_article_nlp_companies CASCADE;
CREATE VIEW vw_article_nlp_companies AS (
WITH
  t1 as (
    SELECT DISTINCT feedly_id, crunchbase_id, relevance_score, confidence_sore
    FROM vw_article_nlp_entities

    WHERE entity_type in ('Company', 'Organisation')
  )

  SELECT
    t1.feedly_id
  , t1.relevance_score
  , t1.confidence_sore
  , o.uuid
  , o.name
  , o.short_description
  , o.legal_name
  , t1.crunchbase_id
  , o.permalink
  , o.cb_url
  , o.status
  , o.logo_url
  , o.state_code
  , o.country_code
  , o.region
  , o.city
  , o.closed_on
  FROM t1
  JOIN organizations o ON o.permalink = t1.crunchbase_id
);

---

DROP VIEW IF EXISTS vw_article_nlp_text CASCADE;
CREATE VIEW vw_article_nlp_text AS (
SELECT
  e.created_at
, e.updated_at
, e.published_at
, e.feedly_id
, nlp.payload ->> 'cleanedText'::text as full_text
FROM
articles e
JOIN feedly_entry_text_analyses nlp ON nlp.feedly_id = e.feedly_id
);

---

DROP VIEW IF EXISTS vw_article_nlp_categories CASCADE;
CREATE VIEW vw_article_nlp_categories AS (
SELECT
  e.created_at
, e.updated_at
, e.published_at
, e.feedly_id
, (jsonb_array_elements((nlp.payload -> 'categories'::text)) -> 'id'::text)::integer AS category_position
, jsonb_array_elements((nlp.payload -> 'categories'::text)) ->> 'label'::text AS category_label
, (jsonb_array_elements((nlp.payload -> 'categories'::text)) -> 'score'::text)::numeric AS category_score
, jsonb_array_elements((nlp.payload -> 'categories'::text)) ->> 'categoryId'::text AS category_id
, jsonb_array_elements((nlp.payload -> 'categories'::text)) ->> 'classifierId'::text AS classifier_id
FROM
articles e
JOIN feedly_entry_text_analyses nlp ON nlp.feedly_id = e.feedly_id
);

---
