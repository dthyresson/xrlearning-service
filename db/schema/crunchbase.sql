
CREATE TABLE category_groups (
    id uuid PRIMARY KEY DEFAULT uuid_generate_v4 (),
    name text,
    type text,
    permalink text,
    cb_url text,
    rank bigint,
    created_at timestamp without time zone,
    updated_at timestamp without time zone,
    category_groups_list text[] DEFAULT '{}'::text[]
);

CREATE UNIQUE INDEX category_groups_pkey ON category_groups(id uuid_ops);
CREATE INDEX category_groups_name_idx ON category_groups(name text_ops);
CREATE INDEX category_groups_type_idx ON category_groups(type text_ops);
CREATE INDEX category_groups_permalink_idx ON category_groups(permalink text_ops);
CREATE INDEX category_groups_cb_url_idx ON category_groups(cb_url text_ops);

cat ~/Dropbox\ (Personal)/crunchbase/bulk_export/category_groups.csv | \
psql `heroku config:get DATABASE_URL --app xrinlearning` -c "COPY category_groups FROM STDIN WITH HEADER CSV;"

---

CREATE TABLE organizations (
    uuid uuid PRIMARY KEY DEFAULT uuid_generate_v4 (),
    name text,
    type text,
    permalink text,
    cb_url text,
    rank bigint,
    created_at timestamp without time zone,
    updated_at timestamp without time zone,
    legal_name text,
    roles text,
    domain text,
    homepage_url text,
    country_code text,
    state_code text,
    region text,
    city text,
    address text,
    postal_code text,
    status text,
    short_description text,
    category_list text,
    category_groups_list text,
    num_funding_rounds integer,
    total_funding_usd numeric(18,0),
    total_funding numeric(18,0),
    total_funding_currency_code text,
    founded_on date,
    last_funding_on date,
    closed_on date,
    employee_count text,
    email text,
    phone text,
    facebook_url text,
    linkedin_url text,
    twitter_url text,
    logo_url text,
    alias1 text,
    alias2 text,
    alias3 text,
    primary_role text,
    num_exits integer
);

CREATE UNIQUE INDEX organizations_pkey ON organizations(uuid uuid_ops);
CREATE INDEX organizations_name_idx ON organizations(name text_ops);
CREATE INDEX organizations_permalink_idx ON organizations(permalink text_ops);
CREATE INDEX organizations_city_idx ON organizations(city text_ops);
CREATE INDEX organizations_domain_idx ON organizations(domain text_ops);
CREATE INDEX organizations_cb_url_idx ON organizations(cb_url text_ops);
CREATE INDEX organizations_type_idx ON organizations(type text_ops);

cat ~/Dropbox\ (Personal)/crunchbase/bulk_export/organizations.csv | \
psql `heroku config:get DATABASE_URL --app xrinlearning` -c "COPY organizations FROM STDIN WITH HEADER CSV;"

---


CREATE TABLE organization_descriptions (
    uuid uuid PRIMARY KEY DEFAULT uuid_generate_v4 (),
    name text,
    type text,
    permalink text,
    cb_url text,
    rank bigint,
    created_at timestamp without time zone,
    updated_at timestamp without time zone,
    description text
);

-- Indices -------------------------------------------------------

CREATE UNIQUE INDEX organization_descriptions_pkey ON organization_descriptions(uuid uuid_ops);
CREATE INDEX organization_descriptions_name_idx ON organization_descriptions(name text_ops);
CREATE INDEX organization_descriptions_cb_url_idx ON organization_descriptions(cb_url text_ops);
CREATE INDEX organization_descriptions_permalink_idx ON organization_descriptions(permalink text_ops);
CREATE INDEX organization_descriptions_type_idx ON organization_descriptions(type text_ops);

cat ~/Dropbox\ (Personal)/crunchbase/bulk_export/organization_descriptions.csv | \
psql `heroku config:get DATABASE_URL --app xrinlearning` -c "COPY organization_descriptions FROM STDIN WITH HEADER CSV;"

---

CREATE TABLE organization_categories (
    organization_uuid uuid PRIMARY KEY DEFAULT uuid_generate_v4 (),
    organization_name text,
    category text
);

CREATE INDEX organization_categories_organization_name_idx ON organization_categories(organization_name text_ops);
CREATE INDEX organization_categories_category_idx ON organization_categories(category text_ops);

cat ~/Dropbox\ (Personal)/crunchbase/bulk_export/organization_categories.csv | \
psql `heroku config:get DATABASE_URL --app xrinlearning` -c "COPY organization_categories FROM STDIN WITH HEADER CSV;"

---

CREATE TABLE org_parents (
    uuid uuid PRIMARY KEY DEFAULT uuid_generate_v4 (),
    name text,
    type text,
    permalink text,
    cb_url text,
    rank bigint,
    created_at timestamp without time zone,
    updated_at timestamp without time zone,
    parent_uuid text,
    parent_name text
);

CREATE INDEX org_parents_name_idx ON org_parents(name text_ops);
CREATE INDEX org_parents_cb_url_idx ON org_parents(cb_url text_ops);
CREATE INDEX org_parents_type_idx ON org_parents(type text_ops);
CREATE INDEX org_parents_permalink_idx ON org_parents(permalink text_ops);
CREATE INDEX org_parents_parent_uuid_idx ON org_parents(parent_uuid text_ops);
CREATE INDEX org_parents_parent_name_idx ON org_parents(parent_name text_ops);

cat ~/Dropbox\ (Personal)/crunchbase/bulk_export/org_parents.csv | \
psql `heroku config:get DATABASE_URL --app xrinlearning` -c "COPY org_parents FROM STDIN WITH HEADER CSV;"

---

DROP MATERIALIZED VIEW vw_organization_categories CASCADE;
CREATE MATERIALIZED VIEW vw_organization_categories AS  SELECT o.uuid,
    o.name,
    unnest(string_to_array(o.category_list, ','::text)) AS category
   FROM organizations o
  WHERE o.category_list IS NOT NULL;

CREATE INDEX vw_organization_categories_uuid_idx ON vw_organization_categories(uuid uuid_ops);
CREATE INDEX vw_organization_categories_category_idx ON vw_organization_categories(category text_ops);

--

DROP MATERIALIZED VIEW vw_organization_category_groups CASCADE;
CREATE MATERIALIZED VIEW vw_organization_category_groups AS  SELECT o.uuid,
    o.name,
    unnest(string_to_array(o.category_groups_list, ','::text)) AS category_group
   FROM organizations o
  WHERE o.category_groups_list IS NOT NULL;

CREATE INDEX vw_organization_category_groups_uuid_idx ON vw_organization_category_groups(uuid uuid_ops);
CREATE INDEX vw_organization_category_groups_category_group_idx ON vw_organization_category_groups(category_group text_ops);

---

DROP MATERIALIZED VIEW IF EXISTS vw_categories_with_category_groups CASCADE;
CREATE MATERIALIZED VIEW vw_categories_with_category_groups AS
select
  id
, name as category
, permalink
, unnest(string_to_array(category_groups_list, ',')) as category_group
from category_groups
order by 2, 4;

CREATE INDEX vw_categories_with_category_groups_category_idx ON vw_categories_with_category_groups(category text_ops);
CREATE INDEX vw_categories_with_category_groups_category_group_idx ON vw_categories_with_category_groups(category_group text_ops);

---

DROP MATERIALIZED VIEW IF EXISTS vw_category_list CASCADE;
CREATE MATERIALIZED VIEW vw_category_list AS
select distinct
  id
, name as category
, permalink
from category_groups
order by 2;

CREATE INDEX vw_category_list_uuid_idx ON vw_category_list(id uuid_ops);
CREATE INDEX vw_category_list_category_idx ON vw_category_list(category text_ops);
CREATE INDEX vw_category_list_permalink_idx ON vw_category_list(permalink text_ops);

---

DROP MATERIALIZED VIEW IF EXISTS vw_category_groups_list CASCADE;
CREATE MATERIALIZED VIEW vw_category_groups_list AS
(
  with t1 as (
    select
     unnest(string_to_array(category_groups_list, ',')) as category_group
    from category_groups
  )

  select distinct category_group from t1 order by 1
);

CREATE INDEX vw_category_groups_listy_idx ON vw_category_groups_list(category_group text_ops);

---
