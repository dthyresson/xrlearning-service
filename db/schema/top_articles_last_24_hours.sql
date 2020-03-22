DROP VIEW IF EXISTS vw_top_articles_last_24_hours CASCADE;

CREATE VIEW vw_top_articles_last_24_hours as
(
with t1 as
(
select
 a.feedly_id
, array_agg(distinct c.topic_label) as topic_labels
, array_agg(distinct c.category) as categories
, array_agg(distinct c.category_group) as category_group
, array_agg(distinct c.name) as company_names

 from
articles a
join xr_company_articles_with_sectors c on c.feedly_id = a.feedly_id
where (topic_score >= 0.3 or c.relevance_score >= 0.3)
and (engagement >= 10  or engagement_rate >= 0.1)
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
, t2.summary_sentences
from t1
join articles a on a.feedly_id = t1.feedly_id
left join t2 on t2.feedly_id = t1.feedly_id
where
published_at >= current_date - interval '24 hours'
order by
engagement_rate desc
)
;