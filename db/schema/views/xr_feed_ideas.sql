-- select 
-- distinct entity_type from vw_article_nlp_entities
-- order by 1
-- ;

--  sports
select e.entity_type, e.confidence_sore, e.relevance_score, e.entity_id, a.published_on, a.title, a.url
from vw_article_nlp_entities e
join articles a on a.feedly_id = e.feedly_id
join vw_article_nlp_topics np on np.feedly_id = a.feedly_id
join vw_xr_topics t on t.topic_label = np.topic_label
where ((entity_type like '%League' or entity_type like '%Team' or entity_type = 'Tournament' or entity_type = 'Racecourse' or entity_type = 'Stadium' or entity_type like 'Sports%' or entity_type like 'Soccer%')
and (e.confidence_sore > 1
     and e.relevance_score >= 0.2) and np.topic_score >= 0.2)
and created_at > '2020-03-01'
order by entity_id;


--  music, fashion, pop-culture
select e.entity_type, e.confidence_sore, e.relevance_score, e.entity_id, a.published_on, a.title, a.url
from vw_article_nlp_entities e
join articles a on a.feedly_id = e.feedly_id
join vw_article_nlp_topics np on np.feedly_id = a.feedly_id
join vw_xr_topics t on t.topic_label = np.topic_label
where ((entity_type = 'TopicalConcept' or entity_type like '%Show' or entity_type = 'VideoGame' or entity_type in ('Fashion', 'MusicalArtist', 'MusicalWork')) -- aka channel or feed
and (e.confidence_sore > 1
     and e.relevance_score >= 0.2) and np.topic_score >= 0.2)
and created_at > '2020-03-01'
order by entity_id;


--  academia
select e.entity_type, e.confidence_sore, e.relevance_score, e.entity_id, a.published_on, a.title, a.url
from vw_article_nlp_entities e
join articles a on a.feedly_id = e.feedly_id
join vw_article_nlp_topics np on np.feedly_id = a.feedly_id
join vw_xr_topics t on t.topic_label = np.topic_label
where ((entity_type in ('University', 'EducationalInstitution', 'AcademicJournal', 'AcademicConference', 'School')) -- aka channel or feed
and (e.confidence_sore > 1
     and e.relevance_score >= 0.2) and np.topic_score >= 0.2)
and created_at > '2020-03-01'
order by entity_id;


-- events
select e.entity_type, e.confidence_sore, e.relevance_score, e.entity_id, a.published_on, a.title, a.url
from vw_article_nlp_entities e
join articles a on a.feedly_id = e.feedly_id
join vw_article_nlp_topics np on np.feedly_id = a.feedly_id
join vw_xr_topics t on t.topic_label = np.topic_label
where ((entity_type in ('Venue', 'Theatre', 'Event')) -- aka channel or feed
and (e.confidence_sore > 1
     and e.relevance_score >= 0.2) and np.topic_score >= 0.2)
and created_at > '2020-03-01'
order by entity_id;
