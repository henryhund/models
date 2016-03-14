create or replace view {schema}.model_tests
 (name, description, result)
 as (

   select
     'adwords_fresher_than_one_day',
     'Most recent adwords entry is no more than one day old',
  max("@date"::date) > current_date - '1 day'::interval
  from {schema}.adwords_summary

 );
