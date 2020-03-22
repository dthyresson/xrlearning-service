DROP VIEW IF EXISTS vw_latest_articles CASCADE;

CREATE VIEW vw_latest_articles as
(
with t1 as
(
select
 a.feedly_id
, array_agg(DISTINCT c.topic_label) FILTER (WHERE c.topic_label IS NOT NULL) as topic_labels
, array_agg(DISTINCT c.category) FILTER (WHERE c.category IS NOT NULL) AS categories
, array_agg(DISTINCT c.category_group) FILTER (WHERE c.category_group IS NOT NULL)  AS category_group
, array_agg(DISTINCT c.name) as company_names

 from
articles a
join xr_company_articles_with_sectors c on c.feedly_id = a.feedly_id
where
a.created_at between CURRENT_TIMESTAMP - interval '24 hours' and CURRENT_TIMESTAMP
group by a.feedly_id
),
t2 as
(
select
a.feedly_id
, array_agg(s.summary_sentence) as summary_sentences
from articles a
left join vw_article_summary_sentences s on s.feedly_id = a.feedly_id
group by a.feedly_id
)

select
  t1.feedly_id
, a.published_at
, a.published_at at time zone 'utc' at time zone 'est' as published_at_with_tz
, t1.topic_labels
, t1.categories
, t1.category_group
, t1.company_names
, a.title
, a.engagement_rate
, a.engagement
, a.url
, a.author
, a.site
, a.image_url
, t2.summary_sentences
from t1
join articles a on a.feedly_id = t1.feedly_id
left join t2 on t2.feedly_id = t1.feedly_id
order by
a.created_at desc
)
;
