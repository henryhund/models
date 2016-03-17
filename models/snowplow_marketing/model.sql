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
    {schema}.snowplow_marketing_aliases a
    on e.domain_userid = a."@domain_userid" 
  where
    f.elements ILIKE '%email%'
  group by a."@user_id"
  ),
  first_utm as
  (
  select 
    a."@user_id", 
    ROW_NUMBER() OVER(PARTITION BY a."@user_id" 
                                 ORDER BY e.collector_tstamp ASC) AS event_no,
    e.mkt_campaign,
    e.mkt_medium,
    e.mkt_source,
    e.mkt_term,
    e.mkt_content
  from
    atomic.events e
  join 
    {schema}.snowplow_marketing_aliases a
    on e.domain_userid = a."@domain_userid" 
  group by 
    a."@user_id",
    e.mkt_campaign,
    e.mkt_medium,
    e.mkt_source,
    e.mkt_term,
    e.mkt_content,
    e.collector_tstamp
  )
  
  select
    a."@user_id" as "@user_id",
    min(e.collector_tstamp) as "@user_first_touch",
    min(f.first_form_submit) as "@first_form_submit",
    fu.mkt_campaign as "@first_touch_campaign",
    fu.mkt_medium as "@first_touch_medium",
    fu.mkt_source as "@first_touch_source",
    fu.mkt_content as "@first_touch_content",
    fu.mkt_term as "@first_touch_term"
  from
    atomic.events e
  join
    {schema}.snowplow_marketing_aliases a
    on a."@domain_userid" = e.domain_userid
  join
    first_utm fu
    on a."@user_id" = fu."@user_id"
  left join
    first_form_submissions f
    on a."@user_id" = f."@user_id"
  where
    fu.event_no = 1
  group by
    a."@user_id",
    fu.mkt_campaign,
    fu.mkt_medium,
    fu.mkt_source,
    fu.mkt_content,
    fu.mkt_term
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
    on a."@domain_user_id" = e.domain_user_id
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

create or replace view {schema}.snowplow_marketing_campaign_influence_first_form_submitted as (
  select
    ci.*,
    mu."@first_form_submit"
  from
    {schema}.snowplow_marketing_campaign_influence ci
  join
    {schema}.snowplow_marketing_users mu
    on mu."@user_id" = ci."@user_id"
  where
    ci."@first_event_timestamp" < mu."@first_form_submit"

);
