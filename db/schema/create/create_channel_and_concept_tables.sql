
CREATE TABLE channels (
    id BIGSERIAL PRIMARY KEY,
    created_at timestamp without time zone NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at timestamp without time zone NOT NULL DEFAULT CURRENT_TIMESTAMP,
    name text NOT NULL UNIQUE,
    description text,
    target text NOT NULL,
    target_id text NOT NULL
);

CREATE INDEX channels_target_target_id_idx ON channels(target text_ops,target_id text_ops);
CREATE UNIQUE INDEX channels_name_target_target_id_idx ON channels(name text_ops,target text_ops,target_id text_ops);

---

CREATE TABLE concepts (
    id BIGSERIAL PRIMARY KEY,
    created_at timestamp without time zone DEFAULT CURRENT_TIMESTAMP,
    updated_at timestamp without time zone DEFAULT CURRENT_TIMESTAMP,
    name text NOT NULL,
    description text
);

CREATE UNIQUE INDEX concepts_name_idx ON concepts(name text_ops);

---

CREATE TABLE channel_concepts (
    id BIGSERIAL PRIMARY KEY,
    created_at timestamp without time zone NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at timestamp without time zone NOT NULL DEFAULT CURRENT_TIMESTAMP,
    channel_id bigint NOT NULL REFERENCES channels(id),
    concept_id bigint NOT NULL REFERENCES concepts(id)
);

---

CREATE TABLE concept_entity_rules (
    id  BIGSERIAL PRIMARY KEY,
    created_at timestamp without time zone NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at timestamp without time zone NOT NULL DEFAULT CURRENT_TIMESTAMP,
    concept_id bigint REFERENCES concepts(id),
    entity_type text NOT NULL,
    entity_confidence_score_threshold numeric(5,3) NOT NULL DEFAULT '0'::numeric,
    entity_relevance_score_threshold numeric(5,3) NOT NULL DEFAULT '0'::numeric,
    topic_score_threshold numeric(5,3) NOT NULL DEFAULT '0'::numeric
);

CREATE UNIQUE INDEX concept_entity_rules_concept_id_entity_type_idx ON concept_entity_rules(concept_id int8_ops,entity_type text_ops);
CREATE INDEX concept_entity_rules_entity_type_idx ON concept_entity_rules(entity_type text_ops);
CREATE INDEX concept_entity_rules_concept_id_idx ON concept_entity_rules(concept_id int8_ops);

---


CREATE TABLE feedly_channel_logs (
    id  BIGSERIAL PRIMARY KEY,
    created_at timestamp without time zone NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at timestamp without time zone NOT NULL DEFAULT CURRENT_TIMESTAMP,
    channel_id bigint NOT NULL REFERENCES channels(id),
    feedly_id text NOT NULL REFERENCES feedly_entries(feedly_id),
    pending_at timestamp without time zone,
    sent_at timestamp without time zone
);

CREATE INDEX feedly_channel_logs_channel_id_feedly_id_idx ON feedly_channel_logs(channel_id int8_ops,feedly_id text_ops);

---
