DROP VIEW IF EXISTS vw_latest_articles CASCADE;

CREATE VIEW vw_latest_articles as
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
where
a.published_at between current_date - interval '24 hours' and current_date
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
a.published_at desc
)
;
