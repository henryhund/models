create or replace view {schema}.snowplow_marketing_aliases as (
  select
    distinct user_id as "@user_id", 
    domain_userid as "@domain_userid"
  from
    atomic.events
  where 
    user_id is not null
);

create or replace view {schema}.snowplow_marketing_users as (
  with first_form_submissions as 
  (
  select 
    a."@user_id", 
    min(collector_tstamp) as first_form_submit
  from 
    atomic.com_snowplowanalytics_snowplow_submit_form_1 f 
  join 
    atomic.events e
    on f.root_id = e.event_id
  join 
    ac_hhund.snowplow_marketing_aliases a
    on e.domain_userid = a."@domain_userid" 
  group by a."@user_id"
  )
  
  select
    a."@user_id" as "@user_id",
    min(e.collector_tstamp) as "@user_created",
    min(f.first_form_submit) as "@first_form_submit"
  from
    atomic.events e
  join
    {schema}.snowplow_marketing_aliases a
    on a."@domain_userid" = e.domain_userid
  join
    first_form_submissions f
    on a."@user_id" = f."@user_id"
  group by
    a."@user_id"
);


create or replace view {schema}.snowplow_marketing_campaign_influence as (
  select
    DISTINCT
    a."@user_id" as "@user_id",
    e.mkt_campaign as "@utm_campaign",
    e.mkt_medium as "@utm_medium",
    e.mkt_source as "@utm_source",
    e.mkt_term as "@utm_term",
    e.mkt_content as "@utm_content",
    min(e.collector_tstamp)::datetime as "@first_event_timestamp"
  from
    atomic.events e
  join
    {schema}.snowplow_marketing_aliases a
    on a."@domain_userid" = e.domain_userid
  where
    e.mkt_campaign is not null
  group by
    a."@user_id",
    e.mkt_campaign,
    e.mkt_medium,
    e.mkt_source,
    e.mkt_term,
    e.mkt_content
  order by
    "@user_id" ASC

);

create or replace view {schema}.snowplow_marketing_campaign_influence_first_forms_submitted as (
  select
    ci.*
  from
    {schema}.snowplow_marketing_campaign_influence ci
  join
    {schema}.snowplow_marketing_users mu
    on mu."@userid" = ci."@userid"
  where
    ci."@first_event_timestamp" < mu."@first_form_submit"
  group by
    a."@user_id",
    e.mkt_campaign,
    e.mkt_medium,
    e.mkt_source,
    e.mkt_term,
    e.mkt_content
  order by
    "@user_id" ASC

);
