DROP VIEW IF EXISTS vw_xr_nlp_topics CASCADE;
CREATE VIEW vw_xr_nlp_topics AS
  select
  distinct topic_label
  from
  vw_article_nlp_topics nt
  where
  (
  nt.topic_label ilike '%reality%'
  or
  nt.topic_label ilike '%learn%'
  or
  nt.topic_label ilike '%virtual%'
  )
  and
  nt.topic_label not ilike '%reality television%'
  order by topic_label
;

---

DROP VIEW IF EXISTS vw_xr_topics CASCADE;
CREATE VIEW vw_xr_topics AS (
		SELECT
		  CASE topic_label
		    WHEN 'Virtual reality headset'
		    THEN 'Virtual reality headsets'
		    WHEN 'Virtual reality organizations'
		    THEN 'Virtual reality companies'
		    WHEN 'Virtual machine'
		    THEN 'Virtual machines'
		    WHEN 'Virtual economy'
		    THEN 'Virtual economies'
		    WHEN 'Virtual assistant'
		    THEN 'Virtual assistants'
		    WHEN 'Mixed reality game'
		    THEN 'Mixed reality games'
		    ELSE topic_label
		  END AS topic_label
		FROM vw_xr_nlp_topics
	    WHERE
	      topic_label NOT IN ('Alternate reality', 'Virtual reality', 'Mixed reality', 'Reality')
);

---

DROP MATERIALIZED VIEW IF EXISTS xr_company_articles_with_sectors;
CREATE MATERIALIZED VIEW xr_company_articles_with_sectors AS (
select
  n.feedly_id
, n.topic_label
, n.topic_score
, c.uuid
, COALESCE(c.name, c.entity_id) as name
, c.relevance_score
, c.confidence_sore
, c.country_code
, c.city
, c.region
, oc.category
, g.category_group
, a.title
, a.image_url
, a.published_at
, date_trunc('day', a.published_at) as published_on
, date_trunc('week', a.published_at) as published_week
, date_trunc('month', a.published_at) as published_month
from articles a
join vw_article_nlp_topics n on n.feedly_id = a.feedly_id
join vw_xr_nlp_topics t on t.topic_label = n.topic_label
join vw_article_nlp_companies c on c.feedly_id = n.feedly_id
left join vw_organization_categories oc on oc.uuid = c.uuid
left join vw_organization_category_groups g on g.uuid = c.uuid
);

CREATE INDEX xr_company_articles_feedly_id_idx ON xr_company_articles_with_sectors USING btree (feedly_id);
CREATE INDEX xr_company_articles_topic_label_idx ON xr_company_articles_with_sectors USING btree (topic_label);
CREATE INDEX xr_company_articles_category_idx ON xr_company_articles_with_sectors USING btree (category);
CREATE INDEX xr_company_articles_category_group_idx ON xr_company_articles_with_sectors USING btree (category_group);
CREATE INDEX xr_company_articles_country_code_idx ON xr_company_articles_with_sectors USING btree (country_code);
CREATE INDEX xr_company_articles_city_idx ON xr_company_articles_with_sectors USING btree (city);
CREATE INDEX xr_company_articles_region_idx ON xr_company_articles_with_sectors USING btree (region);
CREATE INDEX xr_company_articles_published_on_idx ON xr_company_articles_with_sectors USING btree (published_on);
CREATE INDEX xr_company_articles_published_week_idx ON xr_company_articles_with_sectors USING btree (published_week);
CREATE INDEX xr_company_articles_published_month_idx ON xr_company_articles_with_sectors USING btree (published_month);
CREATE INDEX xr_company_articles_with_sectors_uuid_idx ON xr_company_articles_with_sectors USING btree (uuid);
CREATE INDEX xr_company_articles_with_sectors_name_idx ON xr_company_articles_with_sectors USING btree (name);


CREATE UNIQUE INDEX xr_company_articles_with_sectors_pk
  ON xr_company_articles_with_sectors (feedly_id, topic_label, uuid, category_group, category);

--




-- TODO: count companies per category and category_group
