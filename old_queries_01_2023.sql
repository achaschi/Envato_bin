select *
from elements.dim_elements_license_plans
limit 100;

with visitors as (select distinct a.fullvisitorid,

                                  CASE
                                      WHEN se.geonetwork_country in ('United States', 'United Kingdom', 'Germany',
                                                                     'Canada', 'Australia', 'France', 'Italy', 'Spain',
                                                                     'Netherlands', 'Brazil',
                                                                     'India', 'South Korea', 'Turkey', 'Switzerland',
                                                                     'Japan', 'Spain')
                                          then se.geonetwork_country
                                      when geonetwork_country in ('Argentina', 'Bolivia', 'Chile',
                                                                  'Colombia', 'Costa Rica', 'Cuba', 'Ecuador', 'Mexico',
                                                                  'Paraguay', 'Uruguay',
                                                                  'Venezuela') then 'LATAM'
                                      when geonetwork_country in ('Belarus', 'Kazakhstan', 'Russia', 'Ukraine')
                                          then 'RU'
                                      else 'ROW' end as geonetwork_country,
                                  sc.channel,
                                  se.sessionid,
                                  a.variant,
                                  a.date
                  from ds_bq_abtesting_enrolments_elements a
                           inner join
                       webanalytics.ds_bq_sessions_elements se
                       on a.fullvisitorid = se.fullvisitorid::varchar and a.visitid = se.visitid
                           left join elements.rpt_elements_session_channel sc on se.sessionid = sc.sessionid
                  where experiment_id = 'geInrbFNTa2AZdiRswkP2A' --- change exp and dates
                    and a.date >= '2021-10-25'
                    and se.date >= '20211025'
)
select *
from visitors;


select *
from placeit.view_placeit_signup_attribution
where date_trunc('day', session_date) = '2021-10-01'
limit 100

select *
from elements_users_all

where;

select *

from webanalytics.ds_bq_events_elements
limit 20;

select *
from elements.dim_elements_subscription
limit 100;

-- experiment analysis

with visitors as (select a.fullvisitorid,

                         max(CASE
                                 WHEN se.geonetwork_country in ('United States', 'United Kingdom', 'Germany',
                                                                'Canada', 'Australia', 'France', 'Italy', 'Spain',
                                                                'Netherlands', 'Brazil',
                                                                'India', 'South Korea', 'Turkey', 'Switzerland',
                                                                'Japan', 'Spain')
                                     then se.geonetwork_country
                                 when geonetwork_country in ('Argentina', 'Bolivia', 'Chile',
                                                             'Colombia', 'Costa Rica', 'Cuba', 'Ecuador', 'Mexico',
                                                             'Paraguay', 'Uruguay',
                                                             'Venezuela') then 'LATAM'
                                 when geonetwork_country in ('Belarus', 'Kazakhstan', 'Russia', 'Ukraine')
                                     then 'RU'
                                 else 'ROW' end) as geonetwork_country,
                         --sc.channel as channel,
                         se.sessionid,
                         a.variant, --check if users get assigned to only 1 var
                         a.date                  as edate
                  from ds_bq_abtesting_enrolments_elements a
                           inner join
                       webanalytics.ds_bq_sessions_elements se
                       on a.fullvisitorid = se.fullvisitorid::varchar and a.visitid = se.visitid
                           left join elements.rpt_elements_session_channel sc on se.sessionid = sc.sessionid
                  where experiment_id = 'geInrbFNTa2AZdiRswkP2A' --- change exp and dates
                    -- and a.date >= '2021-10-25'
                    and a.date between '2021-10-25' and '2021-11-10'
                    --and se.date >= '20211025'
                    and se.date between '20211025' and '20211110'
),

     clicks as
         (
             select sessionid,
                    hits_eventinfo_eventcategory,
                    hits_eventinfo_eventlabel,
                    fullvisitorid || '_' || visitid || '_' || hits_hitnumber most_accurate
             from webanalytics.ds_bq_events_elements
             where date >= 20211025
               and hits_eventinfo_eventcategory = 'page-header-top');


select variant,
       country,
       case
           when actions <= 10 then '10 actions or less'
           when actions <= 30 then '30 actions or less'
           else 'more than 30 actions' end                              as activity,
       case when new_user = 1 then 'new_user' else 'returning_user' end as u_age,
       count(*)                                                         as users,
       --avg(days_active * 1.00) as av_days_active,
       --approximate percentile_disc(0.5) within group (order by days_active) as median_days_active,
       count(subbed_on_trial)                                           as subbed_on_trial,
       count(case when login_page > 0 then 1 end)                       as on_login_page,
       count(case when sub_page > 0 then 1 end)                         as on_sub_page,
       count(case when pricing_page > 0 then 1 end)                     as on_pricing_page,
       count(has_paid)                                                  as has_paid,
       count(new_cancelled)                                             as cancelled,
       count(case when has_paid = 1 and new_cancelled = 1 then 1 end)   as paid_and_cancelled,
       count(case when signup > 0 then 1 end)                           as signed_up,
       count(case when old_sub = 1 then 1 end)                          as returning_subscribers,
       count(case when downloads > 0 then 1 end)                        as downloaders,
       sum(downloads)                                                   as downloads

from (
         select variant,
                a.fullvisitorid,
                max(geonetwork_country)                                                         as country,
                max(case when visitnumber = 1 then 1 end)                                       as new_user,
                count(*)                                                                        as actions,
                max(case when subscription_started_on_trial = true then 1 end)                  as subbed_on_trial,
                max(case
                        when first_canceled_at is not null
                            and subscription_start_date :: date between '2021-10-25' and '2021-11-16'
                            then 1 end)                                                         as new_cancelled,
                max(case
                        when has_successful_payment = true
                            and is_first_subscription = true
                            and subscription_start_date :: date between '2021-10-25' and '2021-11-16'
                            then 1 end)                                                         as has_paid,
                max(case
                        when has_successful_payment = true
                            and is_first_subscription = true
                            and subscription_start_date :: date < '2021-10-25'
                            and termination_date isnull
                            then 1 end)                                                         as old_sub,
                count(case when hits_page_pagepath like '%/sign-in%' then 1 end)                as login_page,


                count(case when hits_page_pagepath like '%/subscribe%' then 1 end)              as sub_page,
                count(case when hits_page_pagepath like '%/pricing%' then 1 end)                as pricing_page,
                count(case when hits_eventinfo_eventlabel = 'SubscribedButtonClick' then 1 end) as sub_button_click,
                count(case
                          when hits_eventinfo_eventcategory = 'sign-up' and hits_eventinfo_eventaction = 'submit'
                              then 1 end)                                                       as signup,
                sum(dls)                                                                        as downloads


                -- doesnt work max(case when hits_eventinfo_eventcategory = 'cancellation-form' then 1 end)                                  as cancellation_Form

         from
             -- get experiment users
             (select a.fullvisitorid,

                     CASE
                         WHEN se.geonetwork_country in ('United States', 'United Kingdom', 'Germany',
                                                        'Canada', 'Australia', 'France', 'Italy', 'Spain',
                                                        'Netherlands', 'Brazil',
                                                        'India', 'South Korea', 'Turkey', 'Switzerland',
                                                        'Japan', 'Spain')
                             then se.geonetwork_country
                         when geonetwork_country in ('Argentina', 'Bolivia', 'Chile',
                                                     'Colombia', 'Costa Rica', 'Cuba', 'Ecuador', 'Mexico',
                                                     'Paraguay', 'Uruguay',
                                                     'Venezuela') then 'LATAM'
                         when geonetwork_country in ('Belarus', 'Kazakhstan', 'Russia', 'Ukraine')
                             then 'RU'
                         else 'ROW' end  as geonetwork_country,
                     se.sessionid,
                     a.variant,
                     min(se.visitnumber) as visitnumber
              from ds_bq_abtesting_enrolments_elements a
                       join (select fullvisitorid
                             from ds_bq_abtesting_enrolments_elements
                             where experiment_id = 'geInrbFNTa2AZdiRswkP2A'
                               and date between '2021-10-25' and '2021-11-16'
                               --and date >= '2021-10-25'
                             group by 1
                             having min(variant) = max(variant)) using (fullvisitorid)
                       join webanalytics.ds_bq_sessions_elements se
                            on a.fullvisitorid = se.fullvisitorid::varchar and a.visitid = se.visitid
              where experiment_id = 'geInrbFNTa2AZdiRswkP2A' --- change exp and dates
                and a.date between '2021-10-25' and '2021-11-16'
                -- and a.date >= '2021-10-25'
                and se.date :: int between 20211025 and 20211116
                -- and se.date >= '20211025'
              group by 1, 2, 3, 4) a

                 -- join events table
                 left join webanalytics.ds_bq_events_elements c
                           on a.sessionid = c.sessionid
                               and c.date :: int between 20211025 and 20211116
                 --and c.date >= 20211025

                 --join subs table
                 left join elements.dim_elements_subscription e
                           on c.user_uuid = e.sso_user_id
                 --and subscription_start_date :: date between '2021-10-25' and '2021-11-10'
                 -- and subscription_start_date :: date >= '2021-10-25'

                 left join (select sso_uuid, count(*) as dls
                            from ds_elements_item_downloads id
                                     join dim_users du on id.user_id = du.elements_id
                                and id.download_started_at between '2021-10-25' and '2021-11-16'
                            group by 1) dl on c.user_uuid = dl.sso_uuid

         group by 1, 2)


group by 1, 2, 3, 4;

-- why does SubscribedButtonClick fire for downloads?

select variant,
       'all_users'                   as hits_eventinfo_eventlabel,
       0                             as clicks,
       count(distinct fullvisitorid) as users

from visitors
group by 1, 2, 3


-- if you are in a free trial and you go to my account - it still shows you upgrade button

select split_part(hits_eventinfo_eventcategory, ';', 1) as category,
       split_part(hits_eventinfo_eventlabel, ';', 1)    as label,
       count(*)
from webanalytics.ds_bq_events_elements
where date = 20211025
group by 1, 2
order by 3 desc
;

select *
-- subscription_start_date,
-- sso_user_id,
-- dim_subscription_key,
-- plan_type,
-- has_successful_payment,
-- is_first_subscription,
-- trial_period_started_at_aet,
-- first_canceled_at,
-- termination_date
from elements.dim_elements_subscription
where subscription_start_date::date >= '2021-10-25'
  and plan_change is false
limit 100;


select *
from webanalytics.ds_bq_events_elements
where user_uuid = '00a155d6-58f4-439e-aabf-174566a2b819' --sso_user_id
  and date = 20211029
limit 100


select case
           when minvar != maxvar then 'more_than_1_var'
           when minvar = maxvar then 'only_1_var'
           else 'wtf' end            as allocation_mixing,
       count(*)                      as users,
       count(distinct fullvisitorid) as users_check

from (
         select a.fullvisitorid,
                min(a.variant) as minvar,
                max(a.variant) as maxvar--check if users get assigned to only 1 var
         from ds_bq_abtesting_enrolments_elements a
         where experiment_id = 'geInrbFNTa2AZdiRswkP2A' --- change exp and dates
           and a.date >= '2021-10-25'
         group by 1)
group by 1;


select count(*),
       count(distinct dim_subscription_key) as uusers
from elements.dim_elements_subscription
where termination_date isnull
  and has_successful_payment = true
  and plan_change = false
;

limit 100;

select *
from elements.rpt_elements_user_signup_session
limit 100;

select *
from webanalytics.ds_bq_events_elements
limit 100;
select *
from elements.dim_elements_subscription
limit 100;

select *
from ds_elements_item_downloads id
         join dim_users du on id.user_id = du.elements_id
    and donwload_started_at between '2021-10-25' and '2021-11-16'
limit 100;


-- ft subs for darren

select variant,
       country,
       case when new_user = 1 then 'new_user' else 'returning_user' end as u_age,
       count(*)                                                         as users,
       count(case when new_cancelled > 0 then 1 end)                    as cancellation_for_new_signups,
       count(case when terminated > 0 then 1 end)                       as overall_terminations,
       count(case when trial_sub > 0 then 1 end)                        as free_trials,
       count(case when trial_sub_remaining > 0 then 1 end)              as fre_trials_remaining,
       count(case when total_sub > 0 then 1 end)                        as total_new_subs,
       count(returning_sub)                                             as returning_subscribers

from (
         select variant,
                a.fullvisitorid,
                max(geonetwork_country)                   as country,
                max(case when visitnumber = 1 then 1 end) as new_user,
                max(case
                        when first_canceled_at is not null
                            and subscription_start_date :: date between '2021-10-25' and '2021-11-16'
                            then 1 end)                   as new_cancelled,
                max(case
                        when termination_date :: date between '2021-10-25' and '2021-11-16'
                            then 1 end)                   as terminated,
                max(case
                        when has_successful_payment = true
                            and is_first_subscription = true
                            and subscription_start_date :: date < '2021-10-25'
                            and termination_date isnull
                            then 1 end)                   as returning_sub,

                count(case
                          when hits_eventinfo_eventcategory = 'sign-up' and hits_eventinfo_eventaction = 'submit'
                              then 1 end)                 as signup,
                max(case
                        when trial_period_started_at_aet :: date between '2021-10-25' and '2021-11-16'
                            then 1 end)                   as trial_sub,
                max(case
                        when trial_period_started_at_aet :: date between '2021-10-25' and '2021-11-16' and
                             termination_date isnull and has_successful_payment = true
                            then 1 end)                   as trial_sub_remaining,
                max(case
                        when subscription_start_date :: date between '2021-10-25' and '2021-11-16' and
                             termination_date isnull and has_successful_payment = true
                            then 1 end)                   as total_sub


                -- doesnt work max(case when hits_eventinfo_eventcategory = 'cancellation-form' then 1 end)                                  as cancellation_Form

         from
             -- get experiment users
             (select a.fullvisitorid,

                     CASE
                         WHEN se.geonetwork_country in ('United States', 'United Kingdom', 'Germany',
                                                        'Canada', 'Australia', 'France', 'Italy', 'Spain',
                                                        'Netherlands', 'Brazil',
                                                        'India', 'South Korea', 'Turkey', 'Switzerland',
                                                        'Japan', 'Spain')
                             then se.geonetwork_country
                         when geonetwork_country in ('Argentina', 'Bolivia', 'Chile',
                                                     'Colombia', 'Costa Rica', 'Cuba', 'Ecuador', 'Mexico',
                                                     'Paraguay', 'Uruguay',
                                                     'Venezuela') then 'LATAM'
                         when geonetwork_country in ('Belarus', 'Kazakhstan', 'Russia', 'Ukraine')
                             then 'RU'
                         else 'ROW' end  as geonetwork_country,
                     se.sessionid,
                     a.variant,
                     min(se.visitnumber) as visitnumber
              from ds_bq_abtesting_enrolments_elements a
                       join (select fullvisitorid
                             from ds_bq_abtesting_enrolments_elements
                             where experiment_id = 'geInrbFNTa2AZdiRswkP2A'

                               and date between '2021-10-25' and '2021-11-16'
                               --and date >= '2021-10-25'
                             group by 1
                             having min(variant) = max(variant)) using (fullvisitorid)
                       join webanalytics.ds_bq_sessions_elements se
                            on a.fullvisitorid = se.fullvisitorid::varchar and a.visitid = se.visitid
              where experiment_id = 'geInrbFNTa2AZdiRswkP2A' --- change exp and dates
                and a.date between '2021-10-25' and '2021-11-16'
                -- and a.date >= '2021-10-25'
                and se.date :: int between 20211025 and 20211116
                -- and se.date >= '20211025'
              group by 1, 2, 3, 4) a

                 -- join events table
                 left join webanalytics.ds_bq_events_elements c
                           on a.sessionid = c.sessionid
                               and c.date :: int between 20211025 and 20211116
                 --and c.date >= 20211025

                 --join subs table
                 left join elements.dim_elements_subscription e
                           on c.user_uuid = e.sso_user_id
                 --and subscription_start_date :: date between '2021-10-25' and '2021-11-10'
                 -- and subscription_start_date :: date >= '2021-10-25'

                 left join (select sso_uuid, count(*) as dls
                            from ds_elements_item_downloads id
                                     join dim_users du on id.user_id = du.elements_id
                                and id.download_started_at between '2021-10-25' and '2021-11-16'
                            group by 1) dl on c.user_uuid = dl.sso_uuid

         group by 1, 2)
group by 1, 2, 3;

select 'Nov2021'                                             as experiment,
       uage,
       country,
       case when variant = 0 then 'control' else variant end as variant,
       count(*)                                              as users,
       count(case when trial_sub = 1 then 1 end)             as free_trials,
       count(case when trial_sub_remaining = 1 then 1 end)   as fre_trials_paid_and_retained,
       count(case when total_sub = 1 then 1 end)             as total_new_subs,
       count(case when non_free_trial_subs = 1 then 1 end)   as total_non_trial_new_subs
from (
         select variant,
                a.fullvisitorid,
                max(country)                                                                       as country,
                max(case when visitnumber = 1 then 'new_users' else 'returning' end)               as uage,
                max(case
                        when trial_period_started_at_aet :: date between '2021-10-25' and '2021-11-16'
                            then 1 end)                                                            as trial_sub,
                max(case
                        when trial_period_started_at_aet :: date between '2021-10-25' and '2021-11-16' and
                             termination_date isnull and has_successful_payment = true
                            then 1 end)                                                            as trial_sub_remaining,
                max(case
                        when subscription_start_date :: date between '2021-10-25' and '2021-11-16' and
                             termination_date isnull and has_successful_payment = true then 1 end) as total_sub,
                max(case
                        when subscription_start_date :: date between '2021-10-25' and '2021-11-16' and
                             trial_period_started_at_aet isnull
                            and termination_date isnull and has_successful_payment = true
                            then 1 end)                                                            as non_free_trial_subs

         from
             -- get experiment users
             (select a.fullvisitorid,

                     CASE
                         WHEN se.geonetwork_country in ('United States', 'United Kingdom', 'Germany',
                                                        'Canada', 'Australia', 'France', 'Italy', 'Spain',
                                                        'Netherlands', 'Brazil',
                                                        'India', 'South Korea', 'Turkey', 'Switzerland',
                                                        'Japan', 'Spain')
                             then se.geonetwork_country
                         when geonetwork_country in ('Argentina', 'Bolivia', 'Chile',
                                                     'Colombia', 'Costa Rica', 'Cuba', 'Ecuador', 'Mexico',
                                                     'Paraguay', 'Uruguay',
                                                     'Venezuela') then 'LATAM'
                         when geonetwork_country in ('Belarus', 'Kazakhstan', 'Russia', 'Ukraine')
                             then 'RU'
                         else 'ROW' end  as country,
                     se.sessionid,
                     a.variant,
                     min(se.visitnumber) as visitnumber
              from ds_bq_abtesting_enrolments_elements a
                       -- exclude users in more than one variant
                       join (select fullvisitorid
                             from ds_bq_abtesting_enrolments_elements
                             where experiment_id = 'geInrbFNTa2AZdiRswkP2A'

                               and date between '2021-10-25' and '2021-11-16'
                               --and date >= '2021-10-25'
                             group by 1
                             having min(variant) = max(variant)) using (fullvisitorid)
                       join webanalytics.ds_bq_sessions_elements se
                            on a.fullvisitorid = se.fullvisitorid::varchar and a.visitid = se.visitid
              where experiment_id = 'geInrbFNTa2AZdiRswkP2A'
                and a.date between '2021-10-25' and '2021-11-16'
                and se.date :: int between 20211025 and 20211116
              group by 1, 2, 3, 4) a

                 -- join events table - get user_uuid
                 left join webanalytics.ds_bq_events_elements c
                           on a.sessionid = c.sessionid
                               and c.date :: int between 20211025 and 20211116

                 --join subs table
                 left join elements.dim_elements_subscription e
                           on c.user_uuid = e.sso_user_id

         group by 1, 2)
group by 1, 2, 3, 4

union all

select 'Mar2021'                                             as experiment,
       uage,
       country,
       case when variant = 0 then 'control' else variant end as variant,
       count(*)                                              as users,
       count(case when trial_sub = 1 then 1 end)             as free_trials,
       count(case when trial_sub_remaining = 1 then 1 end)   as fre_trials_paid_and_retained,
       count(case when total_sub = 1 then 1 end)             as total_new_subs,
       count(case when non_free_trial_subs = 1 then 1 end)   as total_non_trial_new_subs
from (
         select variant,
                a.fullvisitorid,
                max(country)                                                                       as country,
                max(case when visitnumber = 1 then 'new_users' else 'returning' end)               as uage,
                max(case
                        when trial_period_started_at_aet :: date between '2021-02-08' and '2021-03-04'
                            then 1 end)                                                            as trial_sub,
                max(case
                        when trial_period_started_at_aet :: date between '2021-02-08' and '2021-03-04' and
                             termination_date isnull and has_successful_payment = true
                            then 1 end)                                                            as trial_sub_remaining,
                max(case
                        when subscription_start_date :: date between '2021-02-08' and '2021-03-04' and
                             termination_date isnull and has_successful_payment = true then 1 end) as total_sub,
                max(case
                        when subscription_start_date :: date between '2021-02-08' and '2021-03-04' and
                             trial_period_started_at_aet isnull
                            and termination_date isnull and has_successful_payment = true
                            then 1 end)                                                            as non_free_trial_subs

         from
             -- get experiment users
             (select a.fullvisitorid,

                     CASE
                         WHEN se.geonetwork_country in ('United States', 'United Kingdom', 'Germany',
                                                        'Canada', 'Australia', 'France', 'Italy', 'Spain',
                                                        'Netherlands', 'Brazil',
                                                        'India', 'South Korea', 'Turkey', 'Switzerland',
                                                        'Japan', 'Spain')
                             then se.geonetwork_country
                         when geonetwork_country in ('Argentina', 'Bolivia', 'Chile',
                                                     'Colombia', 'Costa Rica', 'Cuba', 'Ecuador', 'Mexico',
                                                     'Paraguay', 'Uruguay',
                                                     'Venezuela') then 'LATAM'
                         when geonetwork_country in ('Belarus', 'Kazakhstan', 'Russia', 'Ukraine')
                             then 'RU'
                         else 'ROW' end  as country,
                     se.sessionid,
                     a.variant,
                     min(se.visitnumber) as visitnumber
              from ds_bq_abtesting_enrolments_elements a
                       -- exclude users in more than one variant
                       join (select fullvisitorid
                             from ds_bq_abtesting_enrolments_elements
                             where experiment_id = 'xksjRJFNTeqn3tqpcP_WOg'

                               and date between '2021-02-08' and '2021-03-04'
                               --and date >= '2021-02-08'
                             group by 1
                             having min(variant) = max(variant)) using (fullvisitorid)
                       join webanalytics.ds_bq_sessions_elements se
                            on a.fullvisitorid = se.fullvisitorid::varchar and a.visitid = se.visitid
              where experiment_id = 'xksjRJFNTeqn3tqpcP_WOg'
                and a.date between '2021-02-08' and '2021-03-04'
                and se.date :: int between 20210208 and 20210304
              group by 1, 2, 3, 4) a

                 -- join events table - get user_uuid
                 left join webanalytics.ds_bq_events_elements c
                           on a.sessionid = c.sessionid
                               and c.date :: int between 20210208 and 20210304

                 --join subs table
                 left join elements.dim_elements_subscription e
                           on c.user_uuid = e.sso_user_id

         group by 1, 2)
group by 1, 2, 3, 4
;

--simple data

select case when variant = 0 then 'control' when variant = 1 then 'free_trial' end as variant,
       count(*)                                                                    as users,
       count(distinct fullvisitorid)                                               as user_dist,
       count(case when subs = 1 then 1 end)                                        as Subs,

       count(case when total_sub = 1 then 1 end)                                   as Paid,
       count(case when cancelled = 1 then 1 end)                                   as cancelled

from (
         select variant,
                a.fullvisitorid,
                max(case
                        when trial_period_started_at_aet :: date between '2021-10-25' and '2021-11-18'
                            then 1 end)                                   as trial_sub,
                max(case
                        when subscription_start_date :: date between '2021-10-25' and '2021-11-18' and
                             termination_date :: date between '2021-10-25' and '2021-11-18'
                            then 1 end)                                   as cancelled,
                max(case
                        when subscription_start_date :: date between '2021-10-25' and '2021-11-18'
                            and has_successful_payment = true then 1 end) as total_sub,
                max(case
                        when subscription_start_date :: date between '2021-10-25' and '2021-11-18' and
                             trial_period_started_at_aet isnull
                            and termination_date isnull and has_successful_payment = true
                            then 1 end)                                   as non_free_trial_subs,
                max(case
                        when subscription_start_date :: date between '2021-10-25' and '2021-11-18'
                            then 1 end)                                   as Subs

         from
             -- get experiment users
             (select a.fullvisitorid,
                     se.sessionid,
                     a.variant
              from ds_bq_abtesting_enrolments_elements a
                       -- exclude users in more than one variant
                       join (select fullvisitorid
                             from ds_bq_abtesting_enrolments_elements
                             where experiment_id = 'geInrbFNTa2AZdiRswkP2A'

                               and date between '2021-10-25' and '2021-11-18'
                               --and date >= '2021-10-25'
                             group by 1
                             having min(variant) = max(variant)) using (fullvisitorid)
                       join webanalytics.ds_bq_sessions_elements se
                            on a.fullvisitorid = se.fullvisitorid::varchar and a.visitid = se.visitid
              where experiment_id = 'geInrbFNTa2AZdiRswkP2A'
                and a.date between '2021-10-25' and '2021-11-18'
                and se.date :: int between 20211025 and 20211118
              group by 1, 2, 3) a

                 -- join events table - get user_uuid
                 left join webanalytics.ds_bq_events_elements c
                           on a.sessionid = c.sessionid
                               and c.date :: int between 20211025 and 20211118

                 --join subs table
                 left join elements.dim_elements_subscription e
                           on c.user_uuid = e.sso_user_id

         group by 1, 2)
group by 1;

select fullvisitorid, variant
from (
         select variant,
                a.fullvisitorid,
                max(case
                        when trial_period_started_at_aet :: date between '2021-10-25' and '2021-11-18'
                            then 1 end)                                   as trial_sub,
                max(case
                        when subscription_start_date :: date between '2021-10-25' and '2021-11-18' and
                             termination_date :: date between '2021-10-25' and '2021-11-18'
                            then 1 end)                                   as cancelled,
                max(case
                        when subscription_start_date :: date between '2021-10-25' and '2021-11-18'
                            and has_successful_payment = true then 1 end) as total_sub,
                max(case
                        when subscription_start_date :: date between '2021-10-25' and '2021-11-18' and
                             trial_period_started_at_aet isnull
                            and termination_date isnull and has_successful_payment = true
                            then 1 end)                                   as non_free_trial_subs,
                max(case
                        when subscription_start_date :: date between '2021-10-25' and '2021-11-18'
                            then 1 end)                                   as Subs

         from
             -- get experiment users
             (select a.fullvisitorid,
                     se.sessionid,
                     a.variant
              from ds_bq_abtesting_enrolments_elements a
                       -- exclude users in more than one variant
                       join (select fullvisitorid
                             from ds_bq_abtesting_enrolments_elements
                             where experiment_id = 'geInrbFNTa2AZdiRswkP2A'

                               and date between '2021-10-25' and '2021-11-18'
                               --and date >= '2021-10-25'
                             group by 1
                             having min(variant) = max(variant)) using (fullvisitorid)
                       join webanalytics.ds_bq_sessions_elements se
                            on a.fullvisitorid = se.fullvisitorid::varchar and a.visitid = se.visitid
              where experiment_id = 'geInrbFNTa2AZdiRswkP2A'
                and a.date between '2021-10-25' and '2021-11-18'
                and se.date :: int between 20211025 and 20211118
              group by 1, 2, 3) a

                 -- join events table - get user_uuid
                 left join webanalytics.ds_bq_events_elements c
                           on a.sessionid = c.sessionid
                               and c.date :: int between 20211025 and 20211118

                 --join subs table
                 left join elements.dim_elements_subscription e
                           on c.user_uuid = e.sso_user_id

         group by 1, 2)
where variant = 0
  and trial_sub > 0
limit 20;

select

where = '4555765399829474163';


with trialsubs as
         (select sso_user_id
          from elements.dim_elements_subscription
          where trial_period_started_at_aet :: date between '2021-10-25' and '2021-11-18'
          group by 1),


     experiment_users as
         (select a.fullvisitorid,
                 a.variant
          from ds_bq_abtesting_enrolments_elements a
                   -- exclude users in more than one variant
                   join (select fullvisitorid
                         from ds_bq_abtesting_enrolments_elements
                         where experiment_id = 'geInrbFNTa2AZdiRswkP2A'
                           and date between '2021-10-25' and '2021-11-18'
                         group by 1
                         having min(variant) = max(variant)) b using (fullvisitorid)
          where experiment_id = 'geInrbFNTa2AZdiRswkP2A'
            and a.date between '2021-10-25' and '2021-11-18'
          group by 1, 2
         ),
     mapping_table as
         (select fullvisitorid,
                 user_uuid
          from webanalytics.ds_bq_events_elements c
          where date :: int between 20211025 and 20211118

          group by 1, 2)

select variant,
       count(distinct sso_user_id) free_subscription_users
from experiment_users e
         left join mapping_table m on e.fullvisitorid = m.fullvisitorid :: varchar
         left join trialsubs t on m.user_uuid = t.sso_user_id
group by 1
;
checkc as
         (select
             fullvisitorid, count(*) as ct from mapping_table group by 1)
select *
from mapping_table m
         join checkc c on m.fullvisitorid = c.fullvisitorid and ct > 1
limit 500;

select count(*),
       count(distinct fullvisitorid) as fvid
from mapping_table
;

join experiment_users a
                           on left(a.fullvisitorid, 20) = left(c.fullvisitorid, 20)
                           and c.date :: int between 20211025 and 20211118


select len(fullvisitorid) as len, count(*)
from webanalytics.ds_bq_events_elements
where date :: int between 20211025 and 20211118
group by 1;


with visitors as (select distinct a.fullvisitorid,

                                  CASE
                                      WHEN se.geonetwork_country in ('United States', 'United Kingdom', 'Germany',
                                                                     'Canada', 'Australia', 'France', 'Italy', 'Spain',
                                                                     'Netherlands', 'Brazil',
                                                                     'India', 'South Korea', 'Turkey', 'Switzerland',
                                                                     'Japan', 'Spain')
                                          then se.geonetwork_country
                                      when geonetwork_country in ('Argentina', 'Bolivia', 'Chile',
                                                                  'Colombia', 'Costa Rica', 'Cuba', 'Ecuador', 'Mexico',
                                                                  'Paraguay', 'Uruguay',
                                                                  'Venezuela') then 'LATAM'
                                      when geonetwork_country in ('Belarus', 'Kazakhstan', 'Russia', 'Ukraine')
                                      when geonetwork_country in ('Belarus', 'Kazakhstan', 'Russia', 'Ukraine')
                                          then 'RU'
                                      else 'ROW' end as geonetwork_country,
                                  sc.channel,
                                  se.sessionid,
                                  a.variant,
                                  a.date
                  from ds_bq_abtesting_enrolments_elements a
                           inner join
                       webanalytics.ds_bq_sessions_elements se
                       on a.fullvisitorid = se.fullvisitorid::varchar and a.visitid = se.visitid
                           left join elements.rpt_elements_session_channel sc on se.sessionid = sc.sessionid
                  where experiment_id = 'geInrbFNTa2AZdiRswkP2A' --- change exp and dates
                    and a.date >= '2021-10-25'
                    and se.date >= '20211025'
),

     signup as (
         select sso_user_id,
                sessionid,
                signup_date
         from elements.rpt_elements_user_signup_session
         where session_date::date >= '2021-10-25'
     ),

     subs as
         (select a.dim_subscription_key,
                 a.sso_user_id,
                 a.subscription_start_date,
                 a.last_sessionid,
                 b.plan_type,
                 b.has_successful_payment,
                 b.is_first_subscription,
                 b.trial_period_started_at_aet,
                 b.first_canceled_at,
                 b.termination_date
          from elements.rpt_elements_subscription_session a
                   join elements.dim_elements_subscription b on a.dim_subscription_key = b.dim_subscription_key
          where b.subscription_start_date::date >= '2021-10-25'
            and plan_change is false
         ),


     totals as (
         select a.*,
                b.sso_user_id signup_users,
                c.dim_subscription_key,
                c.plan_type,
                c.has_successful_payment,
                c.trial_period_started_at_aet,
                c.is_first_subscription,
                c.first_canceled_at,
                c.subscription_start_date
         from visitors a
                  left join signup b on a.sessionid = b.sessionid
                  left join subs c on c.last_sessionid = a.sessionid
     ) --select * from totals limit 100

select date,
       geonetwork_country,
       channel,
       variant,
       count(distinct fullvisitorid) visitors,
       null                          signups,
       null                          total_subs,
       null                          subs_started_on_free,
       null                          first_sub,
       null                          total_canceled,
       null                          subs_started_on_free_cancel_same_day,
       null                          subs_started_on_free_cancel_same_day_1,
       null                          subs_started_on_free_cancel_same_day_2,
       null                          subs_started_on_free_cancel_same_day_3,
       null                          subs_started_on_free_cancel_same_day_4,
       null                          subs_started_on_free_cancel_same_day_5,
       null                          subs_started_on_free_cancel_same_day_6,
       null                          subs_started_on_free_cancel_same_day_7,
       null                          total_paid_subs
from totals
group by 1, 2, 3, 4

union all

select date,
       geonetwork_country,
       channel,
       variant,
       null                         visitors,
       count(distinct signup_users) signups,
       null                         total_subs,
       null                         subs_started_on_free,
       null                         first_sub,
       null                         total_canceled,
       null                         subs_started_on_free_cancel_same_day,
       null                         subs_started_on_free_cancel_same_day_1,
       null                         subs_started_on_free_cancel_same_day_2,
       null                         subs_started_on_free_cancel_same_day_3,
       null                         subs_started_on_free_cancel_same_day_4,
       null                         subs_started_on_free_cancel_same_day_5,
       null                         subs_started_on_free_cancel_same_day_6,
       null                         subs_started_on_free_cancel_same_day_7,
       null                         total_paid_subs
from totals
group by 1, 2, 3, 4

union all


select date,
       geonetwork_country,
       channel,
       variant,
       null                                                                                   visitors,
       null                                                                                   signups,
       count(distinct dim_subscription_key)                                                   total_subs,
       count(distinct case
                          when trial_period_started_at_aet::date >= '2021-10-25'
                              then dim_subscription_key end)                                  subs_started_on_free,
       count(distinct case when is_first_subscription is true then dim_subscription_key end)  first_sub,
       count(distinct case when first_canceled_at is not null then dim_subscription_key end)  total_canceled,
       count(distinct case
                          when trial_period_started_at_aet::date >= '2021-10-25' and
                               subscription_start_date::date = first_canceled_at::date
                              then dim_subscription_key end)                                  subs_started_on_free_cancel_same_day,
       count(distinct case
                          when trial_period_started_at_aet::date >= '2021-10-25' and
                               datediff(day, trial_period_started_at_aet::date, first_canceled_at::date) = 1
                              then dim_subscription_key end)                                  subs_started_on_free_cancel_same_day_1,
       count(distinct case
                          when trial_period_started_at_aet::date >= '2021-10-25' and
                               datediff(day, trial_period_started_at_aet::date, first_canceled_at::date) = 2
                              then dim_subscription_key end)                                  subs_started_on_free_cancel_same_day_2,
       count(distinct case
                          when trial_period_started_at_aet::date >= '2021-10-25' and
                               datediff(day, trial_period_started_at_aet::date, first_canceled_at::date) = 3
                              then dim_subscription_key end)                                  subs_started_on_free_cancel_same_day_3,
       count(distinct case
                          when trial_period_started_at_aet::date >= '2021-10-25' and
                               datediff(day, trial_period_started_at_aet::date, first_canceled_at::date) = 4
                              then dim_subscription_key end)                                  subs_started_on_free_cancel_same_day_4,
       count(distinct case
                          when trial_period_started_at_aet::date >= '2021-10-25' and
                               datediff(day, trial_period_started_at_aet::date, first_canceled_at::date) = 5
                              then dim_subscription_key end)                                  subs_started_on_free_cancel_same_day_5,
       count(distinct case
                          when trial_period_started_at_aet::date >= '2021-10-25' and
                               datediff(day, trial_period_started_at_aet::date, first_canceled_at::date) = 6
                              then dim_subscription_key end)                                  subs_started_on_free_cancel_same_day_6,
       count(distinct case
                          when trial_period_started_at_aet::date >= '2021-10-25' and
                               datediff(day, trial_period_started_at_aet::date, first_canceled_at::date) = 7
                              then dim_subscription_key end)                                  subs_started_on_free_cancel_same_day_7,
       count(distinct case when has_successful_payment is true then dim_subscription_key end) total_paid_subs
from totals
group by 1, 2, 3, 4;


select count(case when user_uuid is not null then 1 end) as                        uuid,
       count(case when user_uuid isnull then 1 end)      as                        uuid_null,
       count(case when ds_bq_events_elements.fullvisitorid is not null then 1 end) fvi,
       count(case when ds_bq_events_elements.fullvisitorid isnull then 1 end)      fvi_null

from webanalytics.ds_bq_events_elements

where date = 20211101
;
select count(*), count(r.user_id)
from elements.dim_elements_subscription s
         left join elements.ds_elements_refunds r on s.sso_user_id = r.user_id;
where has_successful_payment = false and termination_date isnull
;

select t.table_name
from information_schema.tables t
where t.table_schema = 'elements' -- put schema name here
  and t.table_type = 'BASE TABLE'
order by t.table_name;


select count(DISTINCT S.sso_user_id) AS paid_subs,
       count(distinct r.user_id)     as refunds
from elements.dim_elements_subscription s
         join dim_users u on s.sso_user_id = u.sso_uuid
         left join elements.ds_elements_refunds r on r.user_id = u.elements_id
where subscription_start_date >= '2021-10-25'
;

select *
from elements.ds_elements_refunds
limit 200 — 2

select *
from elements.rpt_elements_user_signup_session
where signup_date ::date = '2021-11-01'
limit 100;

select trafficsource_medium, count(*)
from webanalytics.ds_bq_sessions_elements
-- where signup_date ::date = '2021-11-01'
group by 1
order by 2 desc;


select *
from elements.rpt_elements_session_channel
limit 150;

select account_code, created_at, *
from ds_elements_recurly_transactions
order by 1, 2
limit 100


select account_code
from (
         select *,
                row_number() over (partition by account_code order by t.created_at asc) as first_payment
         from ds_elements_recurly_transactions t
         WHERe t.transaction_type = 'purchase'
           AND t.transaction_status <> 'success'
           and t.created_at::Date >= '2021-10-25'
     )
where first_payment = 1
group by 1
limit 100

select *
from elements.dim_elements_subscription
where recurly_account_code = '0000640d-d254-4bfd-a0c6-a77866631d0b'
limit 100;


select transaction_type,
       payment_method,
       transaction_status,
       count(*)
from ds_elements_recurly_transactions
WHERe created_at::Date >= '2021-10-25'
group by 1, 2, 3;

with payments as
         (select account_code,
                 max(case
                         when transaction_type = 'verify' and transaction_status = 'success' then 1
                         else null end) as verified_cc,
                 max(case
                         when transaction_type = 'purchase' and transaction_status = 'success' and
                              payment_method = 'credit_card' then 1
                         else null end) as paid_cc,
                 max(case
                         when transaction_type = 'purchase' and transaction_status = 'success' and
                              payment_method = 'paypal' then 1
                         else null end) as paid_paypal
          from (select account_code,
                       transaction_type,
                       payment_method,
                       split_part(max(created_at || '|' || transaction_status), '|', 2) as transaction_status
                from ds_elements_recurly_transactions
                where created_at::Date >= '2021-10-25'
                group by 1, 2, 3)
          group by 1)
select count(case
                 when s.subscription_start_date :: date between '2021-10-25' and '2021-11-18'
                     and termination_date isnull
                     and has_successful_payment = true
                     then 1 end) as total_new_subs,
       count(case
                 when s.subscription_start_date :: date between '2021-10-25' and '2021-11-18'
                     and termination_date isnull
                     and has_successful_payment = true
                     and (paid_cc = 1 or paid_paypal = 1)
                     then 1 end) as total_new_signups_paid,
       count(case
                 when s.subscription_start_date :: date between '2021-10-25' and '2021-11-18'
                     and termination_date isnull
                     --and has_successful_payment = true
                     and (paid_cc = 1 or paid_paypal = 1)
                     then 1 end) as total_new_signups_paid_check,
       count(case
                 when s.subscription_start_date :: date between '2021-10-25' and '2021-11-18'
                     and termination_date :: date >= '2021-10-25'
                     and first_canceled_at isnull
                     then 1 end) as total_new_signups_terminated_not_cancelled,
       count(case
                 when s.subscription_start_date :: date between '2021-10-25' and '2021-11-18'
                     and termination_date :: date >= '2021-10-25'
                     and first_canceled_at isnull
                     and paid_cc isnull
                     and paid_paypal isnull
                     then 1 end) as total_new_signups_terminated_not_cancelled_payment_fail,
       count(case
                 when s.subscription_start_date :: date between '2021-10-25' and '2021-11-18'
                     and termination_date :: date >= '2021-10-25'
                     and first_canceled_at isnull
                     and paid_cc isnull
                     and paid_paypal isnull
                     then 1 end) as total_new_signups_terminated_not_cancelled_payment_fail_test,
       count(case
                 when s.subscription_start_date :: date between '2021-10-25' and '2021-11-18'
                     and termination_date isnull
                     and has_successful_payment = true
                     and paid_cc isnull
                     and paid_paypal isnull
                     then 1 end) as total_new_signups_active_failed_payment,

       count(case
                 when s.subscription_start_date :: date between '2021-10-25' and '2021-11-18'
                     and paid_cc isnull
                     and paid_paypal isnull
                     and termination_date isnull
                     then 1 end) as total_new_signups_active_failed_payment_check,
       count(case
                 when s.subscription_start_date :: date between '2021-10-25' and '2021-11-18' and
                      first_canceled_at isnull
                     and has_successful_payment is true
--                            and nvl(paid_cc, paid_paypal) = 1
                     then 1 end) as minus_failed_payments,
       count(case
                 when s.subscription_start_date :: date between '2021-10-25' and '2021-11-18' and
                      first_canceled_at isnull
                     and has_successful_payment is true
                     and nvl(paid_cc, paid_paypal) = 1
                     then 1 end) as minus_failed_payments_test


from elements.dim_elements_subscription s
         left join payments p on s.recurly_account_code = p.account_code

with payments as
         (select account_code,
                 max(case
                         when transaction_type = 'verify' and transaction_status = 'success' then 1
                         else null end) as verified_cc,
                 max(case
                         when transaction_type = 'purchase' and transaction_status = 'success' and
                              payment_method in ('wire_transfer', 'credit_card') then 1
                         else null end) as paid_cc,
                 max(case
                         when transaction_type = 'purchase' and transaction_status = 'success' and
                              payment_method = 'paypal' then 1
                         else null end) as paid_paypal

          from ds_elements_recurly_transactions
          where created_at::Date >= '2021-09-25'
          group by 1)

select *
from elements.dim_elements_subscription s
         left join payments p on s.recurly_account_code = p.account_code
where (paid_cc = 1 or paid_paypal = 1)
  and subscription_start_date between '2021-10-25' and '2021-11-18'
  and termination_date isnull
  and has_successful_payment = false
-- and account_code = '4e37facf-b1f0-48b9-9122-71cbb19d1dd0'
limit 100;


select *
from ds_elements_recurly_transactions

where account_code = '5d316f76-e175-42ba-9e40-8b8c05b8acc6';

select *
from elements.dim_elements_subscription
where recurly_account_code = '13057025-d8cb-4a55-92db-ef22ab9ee71f';


select count(case
                 when d.subscription_start_date :: date between '2021-10-25' and '2021-11-18' and
                      termination_date isnull and
                      has_successful_payment = true
                     then 1 end) as total_subs_remaining,
       count(case
                 when d.subscription_start_date :: date between '2021-10-25' and '2021-11-18' and
                      termination_date isnull and
                      has_successful_payment = true
                     and first_canceled_at isnull
                     then 1 end) as total_subs_remaining_1
from elements.dim_elements_subscription d
where subscription_start_date >= '2021-10-25';

select min(first_canceled_at)
from elements.dim_elements_subscription
where subscription_start_date :: date between '2021-10-25' and '2021-11-18';

max
(case
    when d.subscription_start_date :: date between '2021-10-25' and '2021-11-18' and
    termination_date isnull and
    has_successful_payment = true
    and first_canceled_at isnull
    then 1 end)
as total_subs_remaining,
                max(case
                        when d.subscription_start_date :: date between '2021-10-25' and '2021-11-18' and
                             first_canceled_at isnull
                            and has_successful_payment = true
                            and nvl(paid_cc, paid_paypal) = 1
                            and rf.sso_user_id isnull then 1 end)                as minus_refunds,


select *
from elements.dim_elements_subscription
where termination_date isnull
limit 100


select channel, count(*)
from elements.rpt_elements_session_channel
where dss_update_time :: date between '2021-10-25' and '2021-11-18'
group by 1;

with clean_enrol as
         (
             select fullvisitorid
             from ds_bq_abtesting_enrolments_elements
             where experiment_id = 'geInrbFNTa2AZdiRswkP2A'
               and date > '2021-10-25'
             group by 1
             having min(variant) = max(variant)
         )
        ,
     all_enrol as
         (
             select fullvisitorid, visitid, variant
             from ds_bq_abtesting_enrolments_elements a
                      join clean_enrol b using (fullvisitorid)
             where experiment_id = 'geInrbFNTa2AZdiRswkP2A'
               and date > '2021-10-25'
         )


select variant,
       count(distinct user_uuid)                                                                total_subs,
       count(distinct fullvisitorid) as                                                         fvisids,
       count(distinct case
                          when trial_period_started_at_aet is not null and date_aest <= subscription_start_date
                              then user_uuid end)                                               free_uuid,
       count(distinct case when trial_period_started_at_aet is not null then fullvisitorid end) free_fvid
from (
         select b.*,
                c.user_uuid,
                d.trial_period_started_at_aet,
                d.subscription_start_date,
                c.date_aest

         from all_enrol b
                  join webanalytics.ds_bq_events_elements c
                       on c.fullvisitorid::varchar = b.fullvisitorid and c.visitid = b.visitid and c.date >= 20211025
                  left join elements.dim_elements_subscription d on c.user_uuid = d.sso_user_id
         where d.subscription_start_date::date >= '2021-10-25'
--and c.date_aest <=d.subscription_start_date
     )
group by 1;

with enrollments as
         (
             select a.fullvisitorid,
                    a.visitid,
                    CASE
                        WHEN se.geonetwork_country in ('United States', 'United Kingdom', 'Germany',
                                                       'Canada', 'Australia', 'France', 'Italy', 'Spain',
                                                       'Netherlands', 'Brazil',
                                                       'India', 'South Korea', 'Turkey', 'Switzerland',
                                                       'Japan', 'Spain')
                            then se.geonetwork_country
                        when geonetwork_country in ('Argentina', 'Bolivia', 'Chile',
                                                    'Colombia', 'Costa Rica', 'Cuba', 'Ecuador', 'Mexico',
                                                    'Paraguay', 'Uruguay',
                                                    'Venezuela') then 'LATAM'
                        when geonetwork_country in ('Belarus', 'Kazakhstan', 'Russia', 'Ukraine')
                            then 'RU'
                        else 'ROW' end                                            as country,
                    variant,
                    split_part(min(sc.dss_update_time || '|' || channel), '|', 2) as referer,
                    min(se.visitnumber)                                           as visitnumber
             from ds_bq_abtesting_enrolments_elements a
                      join webanalytics.ds_bq_sessions_elements se
                           on a.fullvisitorid = se.fullvisitorid::varchar and a.visitid = se.visitid and
                              se.date between 20211025 and 20211118
                      left join elements.rpt_elements_session_channel sc on se.sessionid = sc.sessionid
             where experiment_id = 'geInrbFNTa2AZdiRswkP2A'
               and a.date between '2021-10-25' and '2021-11-18'
             group by 1, 2, 3, 4
         )
        ,
     clean_sso as (
         select user_uuid,
                max(country)                      as country,
                variant,

                count(distinct date_aest :: date) as days_active,
                count(*)                          as actions


         from webanalytics.ds_bq_events_elements c
                  join (select user_uuid
                        from enrollments b
                                 join webanalytics.ds_bq_events_elements c on c.fullvisitorid::varchar = b.fullvisitorid
                            and c.visitid = b.visitid
                            and c.date between 20211025 and 20211118
                        group by 1
                        having min(variant) = max(variant)) using (user_uuid)
                  join enrollments b on c.fullvisitorid::varchar = b.fullvisitorid
             and c.visitid = b.visitid
             and c.date between 20211025 and 20211118

         group by 1, 3),

     other_users as
         (select b.fullvisitorid,
                 max(country)                      as country,
                 variant,

                 count(distinct date_aest :: date) as days_active,
                 count(*)                          as actions

          from enrollments b
                   join (select fullvisitorid
                         from ds_bq_abtesting_enrolments_elements
                         where experiment_id = 'geInrbFNTa2AZdiRswkP2A'
                           and date between '2021-10-25' and '2021-11-18'
                         group by 1
                         having min(variant) = max(variant)) using (fullvisitorid)
                   join webanalytics.ds_bq_events_elements c on c.fullvisitorid::varchar = b.fullvisitorid
              and c.visitid = b.visitid
              and c.date between 20211025 and 20211118
              and c.user_uuid isnull
          group by 1, 3
         )

select variant,
       case when actions > 100 then 'over_hundred' else 'under_hundred' end as acts,
       --country,
       avg(days_active * 1.00)                                              as avg_active,
--        approximate         percentile_disc(0.5) within
-- group (order by days_active) as med_days_active,

       count(*)                                                             as users,
       sum(actions)


from (select *
      from clean_sso
      union all
      select *
      from other_users)

group by 1, 2
;
select *
from elements.ds_elements_recurly_transactions
limit 10;

SELECT s.sso_user_id,
       t.payment_method
FROM elements.dim_elements_subscription s
         INNER JOIN elements.ds_elements_recurly_invoices i ON i.account_code = s.recurly_account_code
         INNER JOIN elements.ds_elements_recurly_line_items l
                    ON l.invoice_number = i.invoice_number AND l.subscription_id = s.recurly_subscription_id
-- get latest payment attempt for this invoice
         LEFT JOIN (SELECT invoice_number,
                           payment_method,
                           created_at,
                           ROW_NUMBER() OVER (PARTITION BY invoice_number ORDER BY created_at DESC) AS latest_transaction
                    FROM elements.ds_elements_recurly_transactions
                    WHERE transaction_type = 'purchase'
                      AND transaction_status = 'declined') t
                   ON t.invoice_number = i.invoice_number
                       AND t.latest_transaction = 1
WHERE s.termination_date IS NOT NULL                                -- terminated subscriptions
  AND NVL(s.churned_date, s.termination_date) <> s.termination_date -- only include failed payments
  AND s.plan_change IS FALSE                                        -- ignore plan changes
  AND i.status = 'failed'                                           -- only included failed payments
group by 1, 2
;

select *
from elements.rpt_elements_user_signup_session
limit 100;


with enrollments as
         (
             select a.fullvisitorid,
                    a.visitid,
                    CASE
                        WHEN se.geonetwork_country in ('United States', 'United Kingdom', 'Germany',
                                                       'Canada', 'Australia', 'France', 'Italy', 'Spain',
                                                       'Netherlands', 'Brazil',
                                                       'India', 'South Korea', 'Turkey', 'Switzerland',
                                                       'Japan', 'Spain')
                            then se.geonetwork_country
                        when geonetwork_country in ('Argentina', 'Bolivia', 'Chile',
                                                    'Colombia', 'Costa Rica', 'Cuba', 'Ecuador', 'Mexico',
                                                    'Paraguay', 'Uruguay',
                                                    'Venezuela') then 'LATAM'
                        when geonetwork_country in ('Belarus', 'Kazakhstan', 'Russia', 'Ukraine')
                            then 'RU'
                        else 'ROW' end                                            as country,
                    variant,
                    split_part(min(sc.dss_update_time || '|' || channel), '|', 2) as referer,
                    min(se.visitnumber)                                           as visitnumber
             from ds_bq_abtesting_enrolments_elements a
                      join webanalytics.ds_bq_sessions_elements se
                           on a.fullvisitorid = se.fullvisitorid::varchar and a.visitid = se.visitid and
                              se.date between 20211025 and 20211118
                      left join elements.rpt_elements_session_channel sc on se.sessionid = sc.sessionid
             where experiment_id = 'geInrbFNTa2AZdiRswkP2A'
               and a.date between '2021-10-25' and '2021-11-18'
             group by 1, 2, 3, 4
         )
        ,
     clean_sso as (
         select c.user_uuid,
                max(country)                         as country,
                variant,
                min(subscription_start_date) :: date as subdate

         from webanalytics.ds_bq_events_elements c
                  join (select user_uuid
                        from enrollments b
                                 join webanalytics.ds_bq_events_elements c on c.fullvisitorid::varchar = b.fullvisitorid
                            and c.visitid = b.visitid
                            and c.date between 20211025 and 20211118
                        group by 1
                        having min(variant) = max(variant)) using (user_uuid)
                  join enrollments b on c.fullvisitorid::varchar = b.fullvisitorid
             and c.visitid = b.visitid
             and c.date between 20211025 and 20211118

                  join elements.dim_elements_subscription d
                       on c.user_uuid = d.sso_user_id and subscription_start_date between '2021-10-25' and '2021-11-18'

         group by 1, 3)

select subsc,
       country,
       case
           when datediff('day', first_download_date, second_download_date) >= 0
               then datediff('day', first_download_date, second_download_date)
           when datediff('day', first_download_date, second_download_date) isnull then null
           else 0 end as dtd,
       count(*)       as users
from clean_sso
group by 1, 2, 3
order by 4 desc;


select *, email, split_part(email, '@', 2) = 'envato.com'
from elements.ds_elements_sso_users
limit 400;


select *
from elements.rpt_elements_user_signup_session
where signup_date :: date = '2021-10-30'
limit 200;


select u.sso_uuid,
       i.content_type,
       count(i.item_id) as q
from elements.ds_elements_item_downloads dl
         join elements.ds_elements_item_licenses l on dl.item_license_id = l.id
         join elements.dim_elements_items i on i.item_id = l.item_id
         join dim_users u on dl.user_id = u.elements_id
group by 1, 2
limit 100;

select *
from elements.dim_elements_items
limit 100
select *
from elements.ds_elements_item_licenses
limit 100;

with content_t as (
    select b.dim_subscription_key,
           b.sso_user_id,
           a.item_id,
           a.licensed_at,
           a.license_type,
           c.content_type
    from elements.dim_elements_license_plans a
             join elements.dim_elements_subscription b on a.dim_subscription_key = b.dim_subscription_key
        and b.subscription_start_date::date between '2021-10-25' and '2021-11-18'
             join elements.dim_elements_items c on c.item_id = a.item_id),;

with enrollments as
         (
             select a.fullvisitorid,
                    a.visitid,
                    CASE
                        WHEN se.geonetwork_country in ('United States', 'United Kingdom', 'Germany',
                                                       'Canada', 'Australia', 'France', 'Italy', 'Spain',
                                                       'Netherlands', 'Brazil',
                                                       'India', 'South Korea', 'Turkey', 'Switzerland',
                                                       'Japan', 'Spain')
                            then se.geonetwork_country
                        when geonetwork_country in ('Argentina', 'Bolivia', 'Chile',
                                                    'Colombia', 'Costa Rica', 'Cuba', 'Ecuador', 'Mexico',
                                                    'Paraguay', 'Uruguay',
                                                    'Venezuela') then 'LATAM'
                        when geonetwork_country in ('Belarus', 'Kazakhstan', 'Russia', 'Ukraine')
                            then 'RU'
                        else 'ROW' end                                            as country,
                    variant,
                    split_part(min(sc.dss_update_time || '|' || channel), '|', 2) as referer,
                    min(se.visitnumber)                                           as visitnumber
             from ds_bq_abtesting_enrolments_elements a
                      join webanalytics.ds_bq_sessions_elements se
                           on a.fullvisitorid = se.fullvisitorid::varchar and a.visitid = se.visitid and
                              se.date between 20211025 and 20211118
                      left join elements.rpt_elements_session_channel sc on se.sessionid = sc.sessionid
             where experiment_id = 'geInrbFNTa2AZdiRswkP2A'
               and a.date between '2021-10-25' and '2021-11-18'
             group by 1, 2, 3, 4
         )
        ,
     clean_sso as (
         select c.user_uuid,
                max(country)                 as country,
                variant,
                plan_type,
                min(subscription_start_date) as subdate,
                max(termination_date)        as ter_date,
                max(last_canceled_at)        as canc_date

         from webanalytics.ds_bq_events_elements c
                  join (select user_uuid
                        from enrollments b
                                 join webanalytics.ds_bq_events_elements c on c.fullvisitorid::varchar = b.fullvisitorid
                            and c.visitid = b.visitid
                            and c.date between 20211025 and 20211118
                        group by 1
                        having min(variant) = max(variant)) using (user_uuid)
                  join enrollments b on c.fullvisitorid::varchar = b.fullvisitorid
             and c.visitid = b.visitid
             and c.date between 20211025 and 20211118

                  join elements.dim_elements_subscription d
                       on c.user_uuid = d.sso_user_id and subscription_start_date between '2021-10-25' and '2021-11-18'

         group by 1, 3, 4),
     dates as (
         select ter_date :: date as date_cal
         from clean_sso
         where ter_date notnull
         group by 1
     )
select subdate :: date || '|' || case when variant = 0 then 'control' when variant = 1 then 'freeTrial' end || '|' ||
       s.plan_type                                                                as cohort,
       subdate :: date,
       date_cal                                                                   as day_date,
       case when variant = 0 then 'control' when variant = 1 then 'freeTrial' end as Var,
       plan_type,
       datediff(day, subdate:: date, date_cal :: date)                            as days,
       count(distinct case
                          when date_cal >= subdate and (least(canc_date, ter_date) >= date_cal or ter_date isnull)
                              then user_uuid end)                                 as remaining_users

from clean_sso s
         cross join dates d
-- join webanalytics.ds_bq_events_elements e on s.user_uuid = e.user_uuid and e.date_aest >= subdate and e.date_aest <= ter_date
where d.date_cal >= subdate
group by 1, 2, 3, 4, 5, 6;


select *
from webanalytics.ds_bq_abtesting_enrolments_elements
select least('2021-10-11', '2021-11-12')

with enrollments as
         (
             select a.fullvisitorid,
                    a.visitid,
                    CASE
                        WHEN se.geonetwork_country in ('United States', 'United Kingdom', 'Germany',
                                                       'Canada', 'Australia', 'France', 'Italy', 'Spain',
                                                       'Netherlands', 'Brazil',
                                                       'India', 'South Korea', 'Turkey', 'Switzerland',
                                                       'Japan', 'Spain')
                            then se.geonetwork_country
                        when geonetwork_country in ('Argentina', 'Bolivia', 'Chile',
                                                    'Colombia', 'Costa Rica', 'Cuba', 'Ecuador', 'Mexico',
                                                    'Paraguay', 'Uruguay',
                                                    'Venezuela') then 'LATAM'
                        when geonetwork_country in ('Belarus', 'Kazakhstan', 'Russia', 'Ukraine')
                            then 'RU'
                        else 'ROW' end                                            as country,
                    variant,
                    split_part(min(sc.dss_update_time || '|' || channel), '|', 2) as referer,
                    min(se.visitnumber)                                           as visitnumber
             from ds_bq_abtesting_enrolments_elements a
                      join webanalytics.ds_bq_sessions_elements se
                           on a.fullvisitorid = se.fullvisitorid::varchar and a.visitid = se.visitid and
                              se.date between 20211025 and 20211118
                      left join elements.rpt_elements_session_channel sc on se.sessionid = sc.sessionid
             where experiment_id = 'geInrbFNTa2AZdiRswkP2A'
               and a.date between '2021-10-25' and '2021-11-18'
             group by 1, 2, 3, 4
         )
        ,
     clean_sso as (
         select c.user_uuid,
                max(country)                                                                as country,
                variant,
                split_part(current_plan, '_', 2) || '_' || split_part(current_plan, '_', 3) as plan_type,
                min(subscription_start_date)                                                as subdate,
                max(termination_date)                                                       as ter_date,
                max(last_canceled_at)                                                       as canc_date

         from webanalytics.ds_bq_events_elements c
                  join (select user_uuid
                        from enrollments b
                                 join webanalytics.ds_bq_events_elements c on c.fullvisitorid::varchar = b.fullvisitorid
                            and c.visitid = b.visitid
                            and c.date between 20211025 and 20211118
                        group by 1
                        having min(variant) = max(variant)) using (user_uuid)
                  join enrollments b on c.fullvisitorid::varchar = b.fullvisitorid
             and c.visitid = b.visitid
             and c.date between 20211025 and 20211118

                  join elements.dim_elements_subscription d
                       on c.user_uuid = d.sso_user_id and subscription_start_date between '2021-10-25' and '2021-11-18'
         where termination_date isnull

         group by 1, 3, 4)
select country,
       case when variant = 0 then 'control' when variant = 1 then 'freeTrial' end as Var,
       plan_type,
       count(distinct user_uuid)                                                  as remaining_users

from clean_sso s

group by 1, 2, 3;


--oct 2021 exp
with enrollments as
         (
             select a.fullvisitorid,
                    a.visitid,
                    CASE
                        WHEN se.geonetwork_country in ('United States', 'United Kingdom', 'Germany',
                                                       'Canada', 'Australia', 'France', 'Italy', 'Spain',
                                                       'Netherlands', 'Brazil',
                                                       'India', 'South Korea', 'Turkey', 'Switzerland',
                                                       'Japan', 'Spain')
                            then se.geonetwork_country
                        when geonetwork_country in ('Argentina', 'Bolivia', 'Chile',
                                                    'Colombia', 'Costa Rica', 'Cuba', 'Ecuador', 'Mexico',
                                                    'Paraguay', 'Uruguay',
                                                    'Venezuela') then 'LATAM'
                        when geonetwork_country in ('Belarus', 'Kazakhstan', 'Russia', 'Ukraine')
                            then 'RU'
                        else 'ROW' end                                        as country,
                    variant,
                    max(case when su.dim_subscription_key notnull then 1 end) as signup
             from ds_bq_abtesting_enrolments_elements a
                      join webanalytics.ds_bq_sessions_elements se
                           on a.fullvisitorid = se.fullvisitorid::varchar and a.visitid = se.visitid and
                              se.date between 20211025 and 20211118
--                       left join elements.rpt_elements_session_channel sc on se.sessionid = sc.sessionid
                      left join elements.rpt_elements_subscription_session su on su.sessionid = se.sessionid
                 and signup_date >= '2021-10-25'
             where experiment_id = 'geInrbFNTa2AZdiRswkP2A'
               and a.date between '2021-10-25' and '2021-11-18'
               and variant :: varchar in ('0', '1')
             group by 1, 2, 3, 4 --, 5 --limit 500
         ),
--      uq as
--          (select user_uuid
--           from enrollments b
--                    join webanalytics.ds_bq_events_elements c on c.fullvisitorid::varchar = b.fullvisitorid
--               and c.visitid = b.visitid
--               and c.date between 20211025 and 20211118
--           group by 1
--           having min(variant) = max(variant)),

     allocations as (select user_uuid,
                            max(split_part(signup || '|' || variant, '|', 2),
                                max(country) as country
                                from webanalytics.ds_bq_events_elements c
--                               join uq using (user_uuid)
                                join enrollments b on c.fullvisitorid::varchar = b.fullvisitorid
                                    and c.visitid = b.visitid
                                    and c.date between 20211025 and 20211118
                                group by 1),
                            trials as
         (select user_uuid,
                 country,
                 variant,
                 max(case
                         when trial_period_started_at_aet :: date between '2021-10-25' and '2021-11-18'
                             then 'trial' end) as trialU

          from allocations a
                   left join elements.dim_elements_subscription s on a.user_uuid = s.sso_user_id
          group by 1, 2, 3) select
     count(distinct user_uuid),
            count(distinct  sso_user_id)
                     from trials a
                         left join elements.dim_elements_subscription s
                     on a.user_uuid = s.sso_user_id;
,
     content_t as (select u.sso_uuid,
                          -- ,i.content_type,
                          date_trunc('week', download_started_at) as date_dl
                   from elements.ds_elements_item_downloads dl
                            --                            join elements.ds_elements_item_licenses l on dl.item_license_id = l.id
--                       and download_started_at :: date > '2022-01-01'
--                            join elements.dim_elements_items i on i.item_id = l.item_id
                            join dim_users u
                                 on dl.user_id = u.elements_id and dl.download_started_at :: date > '2020-01-01'
                   group by 1, 2),

     dates as (
         select date_trunc('week', date_dl) as date_cal
         from content_t
         where date_dl notnull
         group by 1
     ),
     subs as (
         select CASE
                    WHEN country in ('United States', 'United Kingdom', 'Germany',
                                     'Canada', 'Australia', 'France', 'Italy', 'Spain',
                                     'Netherlands', 'Brazil',
                                     'India', 'South Korea', 'Turkey', 'Switzerland',
                                     'Japan', 'Spain')
                        then country
                    when country in ('Argentina', 'Bolivia', 'Chile',
                                     'Colombia', 'Costa Rica', 'Cuba', 'Ecuador', 'Mexico',
                                     'Paraguay', 'Uruguay',
                                     'Venezuela') then 'LATAM'
                    when country in ('Belarus', 'Kazakhstan', 'Russia', 'Ukraine')
                        then 'RU'
                    else 'ROW' end                                                                                           as country,

                sso_user_id || '|' || dim_subscription_key || '|' || case
                                                                         when current_plan like '%enterprise%'
                                                                             then 'enterprise'
                                                                         when current_plan like '%team%' then 'team'
                                                                         when current_plan like '%student_annual%'
                                                                             then 'student_annual'
                                                                         when current_plan like '%student_monthly%'
                                                                             then 'student_monthly'
                                                                         else plan_type end || '|' || case
                                                                                                          when
--                                                                                                               trial_period_ends_at_aet between '2021-02-08' and '2021-03-01' or
                                                                                                              trial_period_ends_at_aet between '2021-10-25' and '2021-11-18'
                                                                                                              then 'trial'
                                                                                                          else 'regular' end as sso_key,
                variant || '|' || nvl(trialu, 'regular')                                                                     as Variant,


                date_trunc('week', min(subscription_start_date :: date))                                                     as subdate,
                date_trunc('week', max(termination_date :: date))                                                            as ter_date


         from trials a
                 left join elements.dim_elements_subscription s on a.user_uuid = s.sso_user_id

         group by 1, 2, 3
     )

select 'allocations' || variant,
       count(distinct user_uuid) as users

from allocations
group by 1

union all

select 'trials' || variant,
       count(distinct user_uuid) as users
from trials
group by 1

union all

select 'subs' || split_part(Variant, '|', 1),
       count(distinct split_part(sso_key, '|', 1)) as users
from subs
group by 1
;

select subdate :: date || '|' || split_part(sso_key, '|', 3) || '|' || split_part(sso_key, '|', 4) as cohort,
       variant,
       country,

       date_cal :: date                                                                            as day_date,
       datediff(week, subdate:: date, date_cal :: date)                                            as weeks_since_sub,
       count(distinct case
                          when date_cal >= subdate and (ter_date >= date_cal or ter_date isnull)
                              then
                              split_part(sso_key, '|', 1)
           end)                                                                                    as remaining_users,
       count(distinct case
                          when date_cal >= subdate and (ter_date >= date_cal or ter_date isnull)
                              then
                              sso_key
           end)
                                                                                                   as rengaged_users
--        ,count(distinct case
--                           when date_cal >= subdate and (ter_date >= date_cal or ter_date isnull) and c.sso_uuid notnull
--                               then
-- --                               sso_key
--                               split_part(sso_key, '|', 1)
--            end)                                                                                    as downloading_users
from subs s


         cross join dates d

--          left join content_t c on sso_uuid = split_part(sso_key, '|', 1) and date_dl = date_cal
where d.date_cal >= subdate

group by 1, 2, 3, 4, 5;

--oct 2021 exp

with enrollments as
         (
             select a.fullvisitorid,
                    a.visitid,
                    CASE
                        WHEN se.geonetwork_country in ('United States', 'United Kingdom', 'Germany',
                                                       'Canada', 'Australia', 'France', 'Italy', 'Spain',
                                                       'Netherlands', 'Brazil',
                                                       'India', 'South Korea', 'Turkey', 'Switzerland',
                                                       'Japan', 'Spain')
                            then se.geonetwork_country
                        when geonetwork_country in ('Argentina', 'Bolivia', 'Chile',
                                                    'Colombia', 'Costa Rica', 'Cuba', 'Ecuador', 'Mexico',
                                                    'Paraguay', 'Uruguay',
                                                    'Venezuela') then 'LATAM'
                        when geonetwork_country in ('Belarus', 'Kazakhstan', 'Russia', 'Ukraine')
                            then 'RU'
                        else 'ROW' end as country,
                    variant
             from ds_bq_abtesting_enrolments_elements a
                      join webanalytics.ds_bq_sessions_elements se
                           on a.fullvisitorid = se.fullvisitorid::varchar and a.visitid = se.visitid and
                              se.date between 20210208 and 20210304
                      left join elements.rpt_elements_session_channel sc on se.sessionid = sc.sessionid


             where experiment_id = 'xksjRJFNTeqn3tqpcP_WOg'
               and a.date between '2021-02-08' and '2021-03-04'
             group by 1, 2, 3, 4
         ),
     uq as
         (select user_uuid
          from enrollments b
                   join webanalytics.ds_bq_events_elements c on c.fullvisitorid::varchar = b.fullvisitorid
              and c.visitid = b.visitid
              and c.date between 20210208 and 20210304
          group by 1
          having min(variant) = max(variant)),

     allocations as (select user_uuid,
                            variant,
                            country
                     from webanalytics.ds_bq_events_elements c
                              join uq using (user_uuid)
                              join enrollments b on c.fullvisitorid::varchar = b.fullvisitorid
                         and c.visitid = b.visitid
                         and c.date between 20210208 and 20210304
                     group by 1, 2, 3),
     trials as
         (select user_uuid,
                 country,
                 variant,
                 max(case
                         when trial_period_started_at_aet :: date between '2021-02-08' and '2021-03-04'
                             then 'trial' end)         as trialU,
                 max(case
                         when subscription_start_date :: date between '2021-02-08' and '2021-03-04'
                             and is_first_subscription = true
                             then 'new_non_trial' end) as newU,
                 max(case
                         when subscription_start_date :: date between '2021-02-08' and '2021-03-04'
                             and
                              (trial_period_started_at_aet isnull or trial_period_started_at_aet :: date < '2021-02-08')
                             and is_first_subscription = false
                             then 'resub' end)         as resub

          from allocations a
                   join elements.dim_elements_subscription s on a.user_uuid = s.sso_user_id
          group by 1, 2, 3),
     content_t as (select u.sso_uuid,
                          -- ,i.content_type,
                          date_trunc('week', download_started_at) as date_dl
                   from elements.ds_elements_item_downloads dl
                            --                            join elements.ds_elements_item_licenses l on dl.item_license_id = l.id
--                       and download_started_at :: date > '2022-01-01'
--                            join elements.dim_elements_items i on i.item_id = l.item_id
                            join dim_users u
                                 on dl.user_id = u.elements_id and dl.download_started_at :: date > '2021-01-01'
                   group by 1, 2),

     dates as (
         select date_trunc('week', date_dl) as date_cal
         from content_t
         where date_dl notnull
         group by 1
     ),
     subs as (
         select CASE
                    WHEN country in ('United States', 'United Kingdom', 'Germany',
                                     'Canada', 'Australia', 'France', 'Italy', 'Spain',
                                     'Netherlands', 'Brazil',
                                     'India', 'South Korea', 'Turkey', 'Switzerland',
                                     'Japan', 'Spain')
                        then country
                    when country in ('Argentina', 'Bolivia', 'Chile',
                                     'Colombia', 'Costa Rica', 'Cuba', 'Ecuador', 'Mexico',
                                     'Paraguay', 'Uruguay',
                                     'Venezuela') then 'LATAM'
                    when country in ('Belarus', 'Kazakhstan', 'Russia', 'Ukraine')
                        then 'RU'
                    else 'ROW' end                                                          as country,

                sso_user_id || '|' || dim_subscription_key || '|' || case
                                                                         when current_plan like '%enterprise%'
                                                                             then 'enterprise'
                                                                         when current_plan like '%team%' then 'team'
                                                                         when current_plan like '%student_annual%'
                                                                             then 'student_annual'
                                                                         when current_plan like '%student_monthly%'
                                                                             then 'student_monthly'
                                                                         else plan_type end as sso_key,
                variant                                                                     as Variant,
                nvl(trialu, newU, resub, case
                                             when subscription_start_date < '2019-02-08' then 'returning_2y'
                                             when subscription_start_date < '2020-02-08' then 'returning_1y'
                                             when subscription_start_date < '2020-08-08' then 'returning_6m'
                                             when subscription_start_date < '2020-11-08' then 'returning_3m'
                                             else 'returning_3morless' end)                 as returns,


                case
                    when date_trunc('week', min(subscription_start_date :: date)) < '2021-02-08' then '2021-02-08'
                    else date_trunc('week', min(subscription_start_date :: date)) end       as subdate,
                date_trunc('week', max(termination_date :: date))                           as ter_date


         from elements.dim_elements_subscription s
                  join trials a on a.user_uuid = s.sso_user_id

         group by 1, 2, 3, 4
     ),

     paid as
         (select s.sso_user_id,
                 date_trunc('week', date(dim_date_key))           as paydate,
                 sum(total_amount)                                as paym,
                 sum(total_amount - tax_amount - discount_amount) as rev
          from elements.fact_elements_subscription_transactions t
                   join elements.dim_elements_subscription s on t.dim_subscription_key = s.dim_subscription_key
              and date(dim_date_key) >= '2021-02-08'

          group by 1, 2)

select subdate :: date || '|' || split_part(sso_key, '|', 3) as cohort,
       variant,
       returns                                               as returning_group,
--        discount as discount,
       country,

       date_cal :: date                                      as day_date,
       datediff(week, subdate:: date, date_cal :: date)      as weeks_since_sub,
       count(distinct case
                          when date_cal >= subdate and (ter_date >= date_cal or ter_date isnull)
                              then
--                               sso_key
                              split_part(sso_key, '|', 1)
           end)                                              as remaining_users,
       sum(case when date_cal = paydate then paym end)       as cohort_payments,
       sum(case when date_cal = paydate then rev end)        as cohort_revenue

from subs s


         cross join dates da
    --          left join resubs r on split_part(sso_key, '|', 1) = uid and submax > subdate
--          left join content_t c on sso_uuid = split_part(sso_key, '|', 1) and date_dl = date_cal
         left join paid p on split_part(sso_key, '|', 1) = p.sso_user_id
    and paydate >= subdate
where date_cal >= subdate

group by 1, 2, 3, 4, 5, 6

--oct 2021 exp

create temporary table sesh as (
    with sesh as
             (
                 select a.fullvisitorid || a.visitid as vid,
-- user_uuid,
                        CASE
                            WHEN se.geonetwork_country in ('United States', 'United Kingdom', 'Germany',
                                                           'Canada', 'Australia', 'France', 'Italy', 'Spain',
                                                           'Netherlands', 'Brazil',
                                                           'India', 'South Korea', 'Turkey', 'Switzerland',
                                                           'Japan', 'Spain')
                                then se.geonetwork_country
                            when geonetwork_country in ('Argentina', 'Bolivia', 'Chile',
                                                        'Colombia', 'Costa Rica', 'Cuba', 'Ecuador', 'Mexico',
                                                        'Paraguay', 'Uruguay',
                                                        'Venezuela') then 'LATAM'
                            when geonetwork_country in ('Belarus', 'Kazakhstan', 'Russia', 'Ukraine')
                                then 'RU'
                            else 'ROW' end           as country,
                        variant,
-- case when sc.sessionid notnull then variant end as sub_var,
                        min(a.dss_update_time)       as edate

                 from webanalytics.ds_bq_abtesting_enrolments_elements a
                          join webanalytics.ds_bq_sessions_elements se
                               on a.fullvisitorid = se.fullvisitorid::varchar and a.visitid = se.visitid and
                                  se.date between 20211025 and 20211118
                          left join elements.rpt_elements_subscription_session sc on se.sessionid = sc.sessionid
                 where experiment_id = 'geInrbFNTa2AZdiRswkP2A'
                   and a.date between '2021-10-25' and '2021-11-18'
                 group by 1, 2, 3
             ),
         uq as
             (select vid
              from sesh b
--                    join webanalytics.ds_bq_events_elements c on c.fullvisitorid::varchar = b.fullvisitorid
--               and c.visitid = b.visitid
--               and c.date between 20211025 and 20211118
              group by 1
              having min(variant) != max(variant)),

         allocations as (select e.vid,
                                split_part(min(edate || '|' || variant), '|', 2) as variant,
--                             split_part(min(edate || '|' || sub_var), '|', 2)    as sub_var,
                                max(case when uq.vid notnull then 'both' end)    as reallocated,
                                min(edate :: date)                               as date
                                -- max(country)                                        as country
                         from sesh e
--                      from webanalytics.ds_bq_events_elements c
                                  left join uq on e.vid = uq.vid
--                               join enrollments b on c.fullvisitorid::varchar = b.fullvisitorid
--                          and c.visitid = b.visitid
--                          and c.date between 20211025 and 20211118
                         group by 1)
    select date,
           Variant,
           case
               when reallocated notnull then 'reallocated'
               when reallocated isnull then 'not_reallocated'
               else 'wtf'
               end             as reallocation,
           count(distinct vid) as useruuids
    from allocations a

    group by 1, 2, 3);
;

with enrollments as
         (
             select
--                     a.fullvisitorid ||   a.visitid as vid,
user_uuid,
CASE
    WHEN se.geonetwork_country in ('United States', 'United Kingdom', 'Germany',
                                   'Canada', 'Australia', 'France', 'Italy', 'Spain',
                                   'Netherlands', 'Brazil',
                                   'India', 'South Korea', 'Turkey', 'Switzerland',
                                   'Japan', 'Spain')
        then se.geonetwork_country
    when geonetwork_country in ('Argentina', 'Bolivia', 'Chile',
                                'Colombia', 'Costa Rica', 'Cuba', 'Ecuador', 'Mexico',
                                'Paraguay', 'Uruguay',
                                'Venezuela') then 'LATAM'
    when geonetwork_country in ('Belarus', 'Kazakhstan', 'Russia', 'Ukraine')
        then 'RU'
    else 'ROW' end                              as country,
variant,
case when sc.sessionid notnull then variant end as sub_var,
a.dss_update_time                               as edate

             from webanalytics.ds_bq_abtesting_enrolments_elements a
                      join webanalytics.ds_bq_sessions_elements se
                           on a.fullvisitorid = se.fullvisitorid::varchar and a.visitid = se.visitid and
                              se.date between 20211025 and 20211118
                      left join elements.rpt_elements_subscription_session sc on se.sessionid = sc.sessionid
             where experiment_id = 'geInrbFNTa2AZdiRswkP2A'
               and a.date between '2021-10-25' and '2021-11-18'
             group by 1, 2, 3, 4, 5
         )
        ,
     uq as
         (select user_uuid
          from enrollments b
--                    join webanalytics.ds_bq_events_elements c on c.fullvisitorid::varchar = b.fullvisitorid
--               and c.visitid = b.visitid
--               and c.date between 20211025 and 20211118
          group by 1
          having min(variant) != max(variant)),

     allocations as (select e.user_uuid,
                            variant                                             as variant_float,
                            edate :: date                                       as date,
--                             split_part(min(edate || '|' || variant), '|', 2)    as first_variant,
--                             split_part(min(edate || '|' || sub_var), '|', 2)    as sub_var,
                            max(case when uq.user_uuid notnull then 'both' end) as reallocated
--                             min(edate :: date) as date
                            -- max(country)                                        as country
                     from enrollments e
--                      from webanalytics.ds_bq_events_elements c
                              left join uq on e.user_uuid = uq.user_uuid
--                               join enrollments b on c.fullvisitorid::varchar = b.fullvisitorid
--                          and c.visitid = b.visitid
--                          and c.date between 20211025 and 20211118
                     group by 1, 2, 3)


select date,
       Variant_float,
--        first_variant,
       case
--            when sub_var notnull and reallocated notnull then 'reallocated_subbed'
           when reallocated notnull then 'reallocated'
           when reallocated isnull then 'not_reallocated'
           else 'wtf'
           end                                      as reallocation,
       count(distinct user_uuid)                    as useruuids,
       count(distinct case
                          when date_trunc('day', subscription_start_date) <= '2021-10-25'
                              and (date_trunc('day', termination_date) >= '2021-10-25' or termination_date isnull)
                              then sso_user_id end) as subs_returning_all,
       count(distinct case
                          when date_trunc('day', subscription_start_date) <= '2021-07-25'
                              and (date_trunc('day', termination_date) >= '2021-10-25' or termination_date isnull)
                              then sso_user_id end) as subs_returning_3mo,
       count(distinct case
                          when date_trunc('day', subscription_start_date) <= '2021-04-25'
                              and (date_trunc('day', termination_date) >= '2021-10-25' or termination_date isnull)
                              then sso_user_id end) as subs_returning_6mo,
       count(distinct case
                          when date_trunc('day', subscription_start_date) >= '2021-10-25'
                              then sso_user_id end) as subs_new
from allocations a
         left join elements.dim_elements_subscription s on a.user_uuid = s.sso_user_id

group by 1, 2, 3;

select a.date,
       a.variant,
       a.reallocation,
       a.useruuids,
       subs_returning_all,
       subs_returning_3mo,
       subs_returning_6mo,
       subs_new,
       s.useruuids as fvids
from a
         left join sesh s on a.date = s.date and a.variant = s.Variant and a.reallocation = s.reallocation;

,
     trials as
         (select user_uuid,
                 a.country,
                 sub_var,
                 variant,
                 reallocated,
                 max(case
                         when trial_period_started_at_aet :: date between '2021-10-25' and '2021-11-18'
                             then 'trial' end)         as trialU,
                 max(case
                         when s.subscription_start_date :: date between '2021-10-25' and '2021-11-18'
                             and is_first_subscription = true
                             then 'new_non_trial' end) as newU,
                 max(case
                         when subscription_start_date :: date between '2021-10-25' and '2021-11-18'
                             and
                              (trial_period_started_at_aet isnull or trial_period_started_at_aet :: date < '2021-02-08')
                             and is_first_subscription = false
                             then 'resub' end)         as resub


          from allocations a
                   join elements.dim_elements_subscription s on a.user_uuid = s.sso_user_id

          group by 1, 2, 3, 4, 5)
        ,
     content_t as (select u.sso_uuid,
                          -- ,i.content_type,
                          date_trunc('week', download_started_at) as date_dl
                   from elements.ds_elements_item_downloads dl
                            join dim_users u
                                 on dl.user_id = u.elements_id and dl.download_started_at :: date > '2021-01-01'
                   group by 1, 2),

     dates as (
         select date_trunc('week', date_dl) as date_cal
         from content_t
         where date_dl notnull
         group by 1
     ),
--      payments as
--          (select s.sso_user_id
--           from elements.fact_elements_subscription_transactions t
--                    join elements.dim_elements_subscription s on t.dim_subscription_key = s.dim_subscription_key
--                                                                     AND (termination_date isnull or
--                                                                 termination_date >= '2021-10-25')
--                    where discount_amount > 0 and date(dim_date_key) >= '2021-10-25'
--           group by 1),

     subs as (

         select country,
                s.sso_user_id || '|' || s.dim_subscription_key || '|' || case
                                                                             when current_plan like '%enterprise%'
                                                                                 then 'enterprise'
                                                                             when current_plan like '%team%' then 'team'
                                                                             when current_plan like '%student_annual%'
                                                                                 then 'student_annual'
                                                                             when current_plan like '%student_monthly%'
                                                                                 then 'student_monthly'
                                                                             else s.plan_type end as sso_key,
                nvl(sub_var, variant)                                                             as Variant,

                nvl(trialu, newU, resub, case
                                             when subscription_start_date < '2019-10-25' then 'returning_2y'
                                             when subscription_start_date < '2020-10-25' then 'returning_1y'
                                             when subscription_start_date < '2021-04-25' then 'returning_6m'
                                             when subscription_start_date < '2021-07-25' then 'returning_3m'
                                             else 'returning_3morless' end)                       as returns,


                case
                    when date_trunc('week', min(s.subscription_start_date :: date)) < '2021-10-25' then '2021-10-25'
                    else date_trunc('week', min(s.subscription_start_date :: date)) end           as subdate,
                date_trunc('week', max(termination_date :: date))                                 as ter_date,
--                 ,
                max(case
                        when sub_var notnull and reallocated notnull then 'reallocated_subbed'
                        when reallocated notnull then 'reallocated'
                        else 'not_reallocated'
                    end)                                                                          as discount


         from elements.dim_elements_subscription s
                  join trials a on a.user_uuid = s.sso_user_id
         group by 1, 2, 3, 4
     ),
     paid as
         (select s.sso_user_id,
                 date_trunc('week', date(dim_date_key))           as paydate,
                 sum(total_amount)                                as paym,
                 sum(total_amount - tax_amount - discount_amount) as rev
          from elements.fact_elements_subscription_transactions t
                   join elements.dim_elements_subscription s on t.dim_subscription_key = s.dim_subscription_key
              and date(dim_date_key) >= '2021-02-28'

          group by 1, 2)

select subdate :: date || '|' || split_part(sso_key, '|', 3) as cohort,
       variant,
       returns                                               as returning_group,
       discount                                              as discount,
       country,

       date_cal :: date                                      as day_date,
       datediff(week, subdate:: date, date_cal :: date)      as weeks_since_sub,
       count(distinct case
                          when date_cal >= subdate and (ter_date >= date_cal or ter_date isnull)
                              then
--                               sso_key
                              split_part(sso_key, '|', 1)
           end)                                              as remaining_users,
       sum(case when date_cal = paydate then paym end)       as cohort_payments,
       sum(case when date_cal = paydate then rev end)        as cohort_revenue

from subs s


         cross join dates da
    --          left join resubs r on split_part(sso_key, '|', 1) = uid and submax > subdate
--          left join content_t c on sso_uuid = split_part(sso_key, '|', 1) and date_dl = date_cal
         left join paid p on split_part(sso_key, '|', 1) = p.sso_user_id
    and paydate >= subdate
where date_cal >= subdate


group by 1, 2, 3, 4, 5, 6, 7
;


--sss enrollments

drop table if exists cookies;
create temporary table cookies as
    (with cookies as (select a.fullvisitorid,
                             variant,
                             max(CASE
                                     WHEN se.geonetwork_country in ('United States', 'United Kingdom', 'Germany',
                                                                    'Canada', 'Australia', 'France', 'Italy', 'Spain',
                                                                    'Netherlands', 'Brazil',
                                                                    'India', 'South Korea', 'Turkey', 'Switzerland',
                                                                    'Japan', 'Spain')
                                         then se.geonetwork_country
                                     when geonetwork_country in ('Argentina', 'Bolivia', 'Chile',
                                                                 'Colombia', 'Costa Rica', 'Cuba', 'Ecuador', 'Mexico',
                                                                 'Paraguay', 'Uruguay',
                                                                 'Venezuela') then 'LATAM'
                                     when geonetwork_country in ('Belarus', 'Kazakhstan', 'Russia', 'Ukraine')
                                         then 'RU'
                                     else 'ROW' end)                                       as country,
                             min(timestamp 'epoch' + visitstarttime * interval '1 second') as exp_date


                      from webanalytics.ds_bq_abtesting_enrolments_elements a
                               join webanalytics.ds_bq_sessions_elements se
                                    on a.fullvisitorid = se.fullvisitorid::varchar and a.visitid = se.visitid
                                        and a.date between '2021-10-28' and '2021-11-18'
                                        and se.date between 20211028 and 20211118
                      where experiment_id = 'geInrbFNTa2AZdiRswkP2A'
                      group by 1, 2),
          multiple_cookies as
              (select fullvisitorid
               from cookies
               group by 1
               having min(variant) != max(variant)),

          cookies_allocations as
              (select a.fullvisitorid,
                      max(country)                                        as country,

                      split_part(min(exp_date || '|' || variant), '|', 2) as var,
                      max(case
                              when m.fullvisitorid is not null then 'reallocated'
                              else 'not_reallocated' end)                 as reallocation,
                      min(CAST(DATE(exp_date) AS DATE))                   as exp_day_date
               from cookies a
                        left join multiple_cookies m on a.fullvisitorid = m.fullvisitorid
               group by 1)

     select country,
            var                           as variant,
            reallocation,
            null                          as plan_type,
            count(distinct fullvisitorid) as cookies
     from cookies_allocations
     group by 1, 2, 3
    );

drop table if exists allocations;
create temporary table allocations as (
    with enrollments as
             (
                 select sso_user_id,
                        variant,
                        max(CASE
                                WHEN se.geonetwork_country in ('United States', 'United Kingdom', 'Germany',
                                                               'Canada', 'Australia', 'France', 'Italy', 'Spain',
                                                               'Netherlands', 'Brazil',
                                                               'India', 'South Korea', 'Turkey', 'Switzerland',
                                                               'Japan', 'Spain')
                                    then se.geonetwork_country
                                when geonetwork_country in ('Argentina', 'Bolivia', 'Chile',
                                                            'Colombia', 'Costa Rica', 'Cuba', 'Ecuador', 'Mexico',
                                                            'Paraguay', 'Uruguay',
                                                            'Venezuela') then 'LATAM'
                                when geonetwork_country in ('Belarus', 'Kazakhstan', 'Russia', 'Ukraine')
                                    then 'RU'
                                else 'ROW' end)                                       as country,
                        min(timestamp 'epoch' + visitstarttime * interval '1 second') as exp_date,
                        split_part(min(sc.dss_update_time || '|' || channel), '|', 2) as referer
                 from webanalytics.ds_bq_abtesting_enrolments_elements a
                          join webanalytics.ds_bq_sessions_elements se
                               on a.fullvisitorid = se.fullvisitorid::varchar and a.visitid = se.visitid
                                   and a.date between '2021-10-28' and '2021-11-18'
                                   and se.date between 20211028 and 20211118
--
                          join elements.dim_elements_subscription s on se.user_uuid = s.sso_user_id
                          left join elements.rpt_elements_session_channel sc on se.sessionid = sc.sessionid

                 where experiment_id = 'geInrbFNTa2AZdiRswkP2A'
                 group by 1, 2
             ),
         multiple_allocations as
             (select sso_user_id
              from enrollments
              group by 1
              having min(variant) != max(variant))
    select a.sso_user_id,
           max(referer)                                                                           as referer,
           max(country)                                                                           as country,
           split_part(min(exp_date || '|' || variant), '|', 2)                                    as var,
           max(case when m.sso_user_id is not null then 'reallocated' else 'not_reallocated' end) as reallocation,
           min(CAST(DATE(exp_date) AS DATE))                                                      as exp_day_date
    from enrollments a
             left join multiple_allocations m on a.sso_user_id = m.sso_user_id
    group by 1);

with content_t as (select u.sso_uuid,
                          -- ,i.content_type,
                          date_trunc('week', download_started_at) as date_dl,
                          count(*)                                as q
                   from elements.ds_elements_item_downloads dl
                            join dim_users u
                                 on dl.user_id = u.elements_id and dl.download_started_at :: date > '2021-01-01'
                   group by 1, 2),
     paid as
         (select s.sso_user_id,
                 sum(total_amount)                                as paym,
                 sum(total_amount - tax_amount - discount_amount) as rev
          from elements.fact_elements_subscription_transactions t
                   join elements.dim_elements_subscription s on t.dim_subscription_key = s.dim_subscription_key
              and date(dim_date_key) >= '2021-10-27'
          group by 1),

     failed_payments
         as (SELECT s.sso_user_id
             FROM elements.dim_elements_subscription s
                      INNER JOIN elements.ds_elements_recurly_invoices i ON i.account_code = s.recurly_account_code
                      INNER JOIN elements.ds_elements_recurly_line_items l
                                 ON l.invoice_number = i.invoice_number AND
                                    l.subscription_id = s.recurly_subscription_id
-- get latest payment attempt for this invoice
                      LEFT JOIN (SELECT invoice_number,
                                        payment_method,
                                        created_at,
                                        ROW_NUMBER() OVER (PARTITION BY invoice_number ORDER BY created_at DESC) AS latest_transaction
                                 FROM elements.ds_elements_recurly_transactions
                                 WHERE transaction_type = 'purchase'
                                   AND transaction_status = 'declined') t
                                ON t.invoice_number = i.invoice_number
                                    AND t.latest_transaction = 1
             WHERE s.termination_date IS NOT NULL                                -- terminated subscriptions
               AND NVL(s.churned_date, s.termination_date) <> s.termination_date -- only include failed payments
               AND s.plan_change IS FALSE                                        -- ignore plan changes
               AND i.status = 'failed'                                           -- only included failed payments
               AND subscription_start_date :: date >= '2021-10-27'
             group by 1),

     refunds as
         (select sso_user_id, fact.dim_subscription_key
          from elements.fact_elements_subscription_transactions fact
                   inner join elements.dim_elements_transaction_attributes ta
                              on fact.dim_elements_transaction_key = ta.dim_elements_transaction_key
                   inner join elements.dim_elements_subscription sub
                              on fact.dim_subscription_key = sub.dim_subscription_key

          where ta.transaction_type = 'Refund'
            and fact.dim_date_key > 20211027
          group by 1, 2),

     clean_sso as (
         select c.sso_user_id
                                                                                                   as sso_key,
                max(country)                                                                       as country,
                var                                                                                as variant,
                reallocation,
                referer,
                max(case
                        when d.subscription_start_date :: date < '2021-10-27' and
                             (termination_date isnull or termination_date >= '2021-01-27')
                            then 1 end)                                                            as already_subscribed,
                max(case
                        when d.subscription_start_date between '2021-10-27' and '2021-11-18'
                            and is_first_subscription is true then 1 end)                          as new_sub,
                max(case
                        when d.subscription_start_date > '2021-11-18'
                            and is_first_subscription is false then 1 end)                         as new_re_sub,
                split_part(max(d.dim_subscription_key || '|' || case
                                                                    when d.current_plan like '%enterprise%'
                                                                        then 'enterprise'
                                                                    when d.current_plan like '%team%' then 'team'
                                                                    when d.current_plan like '%student_annual%'
                                                                        then 'student_annual'
                                                                    when d.current_plan like '%student_monthly%'
                                                                        then 'student_monthly'
                                                                    else d.plan_type end), '|', 2) as plan_type,
                max(case
                        when d.subscription_start_date :: date between '2021-10-27' and '2021-11-02' and
                             d.trial_period_started_at_aet is not null
                            then 1 end)                                                            as non_free_trial_signups,
                max(case
                        when d.subscription_start_date :: date between '2021-10-27' and '2021-11-18' and
                             d.trial_period_started_at_aet is not null
                            then 1 end)                                                            as free_trial_signups,
                max(case
                        when d.subscription_start_date :: date between '2021-10-27' and '2021-11-18'
                            then 1 end)                                                            as total_new_signups,
                max(case
                        when d.trial_period_started_at_aet :: date between '2021-10-27' and '2021-11-18' and
                             termination_date isnull and has_successful_payment = true
                            then 1 end)                                                            as trial_sub_remaining,
                max(case
                        when d.subscription_start_date :: date between '2021-10-27' and '2021-11-18' and
                             termination_date isnull and
                             has_successful_payment = true
                            then 1 end)                                                            as total_subs_remaining,
                max(case
                        when d.subscription_start_date :: date between '2021-10-27' and '2021-11-18' and
                             (termination_date isnull or termination_date >= '2021-12-27')
                            and has_successful_payment = true
                            then 1 end)                                                            as total_subs_remaining_27dec,
                max(case
                        when d.subscription_start_date :: date between '2021-10-27' and '2021-11-18' and
                             (termination_date isnull or termination_date >= '2022-01-27')
                            and has_successful_payment = true
                            then 1 end)                                                            as total_subs_remaining_27jan,
                max(case
                        when d.subscription_start_date :: date between '2021-10-27' and '2021-11-18' and
                             termination_date :: date >= '2021-10-27'
                            then 1 end)                                                            as total_new_subs_terminated,
                max(case
                        when d.subscription_start_date :: date < '2021-10-27' and
                             termination_date :: date >= '2021-10-27'
                            then 1 end)                                                            as total_returning_subs_terminated,
                max(case
                        when d.subscription_start_date :: date < '2021-10-27' and termination_date isnull
                            then 1 end)                                                            as returning_subs_retained,
                max(case
                        when d.subscription_start_date :: date between '2021-10-27' and '2021-11-18' and
                             has_successful_payment = false
                            and f.sso_user_id is not null
                            then 1 end)                                                            as failed_payments,
                max(case
                        when d.trial_period_started_at_aet :: date between '2021-10-27' and '2021-11-18' and
                             last_canceled_at >= '2021-10-27'
                            and has_successful_payment = false
                            then 1 end)                                                            as trials_cancellation_unpaid,
                max(case
                        when d.trial_period_started_at_aet :: date between '2021-10-27' and '2021-11-18' and
                             last_canceled_at >= '2021-10-27'
                            and has_successful_payment = true
                            then 1 end)                                                            as trials_cancellation_paid,
                max(case
                        when last_canceled_at >= '2021-10-27'
                            then 1 end)                                                            as all_cancellation,
                max(q)                                                                             as downloads,
                max(rev)                                                                           as revenue,
                max(case
                        when rf.sso_user_id notnull then 1 end)                                    as refunds_trials
         from allocations c


                  --              --remove envato users
                  join elements.ds_elements_sso_users sso
                       on sso.id = c.sso_user_id and split_part(email, '@', 2) != 'envato.com'
                  join elements.dim_elements_subscription d
                       on c.sso_user_id = d.sso_user_id
                           and (termination_date isnull or termination_date >= '2021-10-27')
                  left join content_t dl on c.sso_user_id = dl.sso_uuid
                  left join refunds rf
                            on d.sso_user_id = rf.sso_user_id and d.dim_subscription_key = rf.dim_subscription_key
--             and date
                  left join failed_payments f on c.sso_user_id = f.sso_user_id
                  left join paid p on c.sso_user_id = p.sso_user_id
         group by 1, 3, 4, 5
     )

select variant                                                                   as variant,
       country,
       plan_type,
       reallocation,
       referer,
       case when already_subscribed = 1 then 'continuing_sub' else 'new_sub' end as returning,
       case when new_sub = 1 then 'first_timer' else 'resubscriber' end          as experiment_new_sub,
       count(new_re_sub)                                                         as post_exp_returning_subs,
       count(*)                                                                  as subscribers,
       count(free_trial_signups)                                                 as free_trial_signups,
       count(non_free_trial_signups)                                             as non_free_trial_signups,
       count(total_new_signups)                                                  as total_new_signups,
       count(trial_sub_remaining)                                                as trial_sub_remaining,
       count(total_subs_remaining)                                               as total_subs_remaining,
       count(total_subs_remaining_27dec)                                         as total_subs_remaining_27dec,
       count(total_subs_remaining_27jan)                                         as total_subs_remaining_27jan,
       count(total_new_subs_terminated)                                          as total_new_subs_terminated,
       count(total_returning_subs_terminated)                                    as total_returning_subs_terminated,
       count(returning_subs_retained)                                            as returning_subs_retained,
       count(failed_payments)                                                    as failed_payments,
       count(all_cancellation)                                                   as all_cancellations,
       count(trials_cancellation_unpaid)                                         as cancelled_trial_users_unpaid,
       count(trials_cancellation_paid)                                           as cancelled_trial_users_paid,
       sum(downloads)                                                            as downloads,
       sum(revenue)                                                              as revenue_usd,
       count(refunds_trials)                                                     as refunds
from clean_sso
group by 1, 2, 3, 4, 5, 6, 7;

--feb 22 experiment

with enrollments as
         (
             select sso_user_id,
                    variant,
                    max(CASE
                            WHEN se.geonetwork_country in ('United States', 'United Kingdom', 'Germany',
                                                           'Canada', 'Australia', 'France', 'Italy', 'Spain',
                                                           'Netherlands', 'Brazil',
                                                           'India', 'South Korea', 'Turkey', 'Switzerland',
                                                           'Japan', 'Spain')
                                then se.geonetwork_country
                            when geonetwork_country in ('Argentina', 'Bolivia', 'Chile',
                                                        'Colombia', 'Costa Rica', 'Cuba', 'Ecuador', 'Mexico',
                                                        'Paraguay', 'Uruguay',
                                                        'Venezuela') then 'LATAM'
                            when geonetwork_country in ('Belarus', 'Kazakhstan', 'Russia', 'Ukraine')
                                then 'RU'
                            else 'ROW' end)                                       as country,
                    min(timestamp 'epoch' + visitstarttime * interval '1 second') as exp_date,
                    split_part(min(sc.dss_update_time || '|' || channel), '|', 2) as referer
             from webanalytics.ds_bq_abtesting_enrolments_elements a
                      join webanalytics.ds_bq_sessions_elements se
                           on a.fullvisitorid = se.fullvisitorid::varchar and a.visitid = se.visitid
                               and a.date >= '2021-02-20'
                               and se.date > 20220220
                      join elements.dim_elements_subscription s on se.user_uuid = s.sso_user_id
                      left join elements.rpt_elements_session_channel sc on se.sessionid = sc.sessionid

             where experiment_id = 'FTr-7_qaRO-UzNOru4HW1Q'
             group by 1, 2
         ),
     multiple_allocations as
         (select sso_user_id
          from enrollments
          group by 1
          having min(variant) != max(variant)),
     allocations as (
         select a.sso_user_id,
                max(referer)                                                                           as referer,
                max(country)                                                                           as country,
                split_part(min(exp_date || '|' || variant), '|', 2)                                    as var,
                max(case when m.sso_user_id is not null then 'reallocated' else 'not_reallocated' end) as reallocation,
                min(CAST(DATE(exp_date) AS DATE))                                                      as exp_day_date
         from enrollments a
                  left join multiple_allocations m on a.sso_user_id = m.sso_user_id
         group by 1),

     content_t as (select u.sso_uuid,
                          -- ,i.content_type,
--                           date_trunc('week', download_started_at) as date_dl,
                          count(distinct item_license_id) as q
                   from elements.ds_elements_item_downloads dl
                            join dim_users u
                                 on dl.user_id = u.elements_id and dl.download_started_at :: date >= '2022-02-20'
                   group by 1, 2),
     paid as
         (select s.sso_user_id,
                 sum(total_amount)                                as paym,
                 sum(total_amount - tax_amount - discount_amount) as rev
          from elements.fact_elements_subscription_transactions t
                   join elements.dim_elements_subscription s on t.dim_subscription_key = s.dim_subscription_key
              and date(dim_date_key) >= '2022-02-20'
          group by 1),

     failed_payments
         as (SELECT s.dim_subscription_key,
                    split_part(max(t.created_at || '|' || payment_method), '|', 2) as last_payment_method
             FROM elements.dim_elements_subscription s
                      INNER JOIN elements.ds_elements_recurly_invoices i ON i.account_code = s.recurly_account_code
                      INNER JOIN elements.ds_elements_recurly_line_items l
                                 ON l.invoice_number = i.invoice_number AND
                                    l.subscription_id = s.recurly_subscription_id
-- get latest payment attempt for this invoice
                      LEFT JOIN (SELECT invoice_number,
                                        payment_method,
                                        created_at,
                                        ROW_NUMBER() OVER (PARTITION BY invoice_number ORDER BY created_at DESC) AS latest_transaction
                                 FROM elements.ds_elements_recurly_transactions
                                 WHERE transaction_type = 'purchase'
                                   AND transaction_status = 'declined') t
                                ON t.invoice_number = i.invoice_number
                                    AND t.latest_transaction = 1
             WHERE s.termination_date IS NOT NULL                                -- terminated subscriptions
               AND NVL(s.churned_date, s.termination_date) <> s.termination_date -- only include failed payments
               AND s.plan_change IS FALSE                                        -- ignore plan changes
               AND i.status = 'failed'                                           -- only included failed payments
               AND subscription_start_date :: date >= '2022-02-20'
             group by 1
             limit 100),

     refunds as
         (select sso_user_id, fact.dim_subscription_key
          from elements.fact_elements_subscription_transactions fact
                   inner join elements.dim_elements_transaction_attributes ta
                              on fact.dim_elements_transaction_key = ta.dim_elements_transaction_key
                   inner join elements.dim_elements_subscription sub
                              on fact.dim_subscription_key = sub.dim_subscription_key
                                  and ta.dss_update_time_aet > '2022-02-20'
          where ta.transaction_type = 'Refund'
          group by 1, 2),

     clean_sso as (
         select c.sso_user_id
                                                                                                   as sso_key,
                max(country)                                                                       as country,
                var                                                                                as variant,
                reallocation,
                referer,
                max(case
                        when d.subscription_start_date :: date <= '2022-02-20' and
                             (termination_date isnull or termination_date > '2022-02-20')
                            then 1 end)                                                            as already_subscribed,
                max(case
                        when d.subscription_start_date > '2022-02-20'
                            and is_first_subscription is true then 1 end)                          as new_sub,
--                 max(case
--                         when d.subscription_start_date > '2021-11-18'
--                             and is_first_subscription is false then 1 end)                         as new_re_sub,
                split_part(max(d.dim_subscription_key || '|' || case
                                                                    when d.current_plan like '%enterprise%'
                                                                        then 'enterprise'
                                                                    when d.current_plan like '%team%' then 'team'
                                                                    when d.current_plan like '%student_annual%'
                                                                        then 'student_annual'
                                                                    when d.current_plan like '%student_monthly%'
                                                                        then 'student_monthly'
                                                                    else d.plan_type end), '|', 2) as plan_type,
                max(case
                        when d.subscription_start_date :: date > '2022-02-20' and
                             d.trial_period_started_at_aet isnull
                            then 1 end)                                                            as non_free_trial_signups,
                max(case
                        when d.subscription_start_date :: date > '2022-02-20' and
                             d.trial_period_started_at_aet is not null
                            then 1 end)                                                            as free_trial_signups,
                max(case
                        when d.subscription_start_date :: date > '2022-02-20'
                            then 1 end)                                                            as total_new_signups,
                max(case
                        when d.trial_period_started_at_aet :: date > '2022-02-20' and
                             termination_date isnull and has_successful_payment = true
                            then 1 end)                                                            as trial_sub_remaining,
                max(case
                        when d.subscription_start_date :: date > '2022-02-20' and
                             termination_date isnull and
                             has_successful_payment = true
                            then 1 end)                                                            as total_subs_remaining,
                max(case
                        when d.subscription_start_date :: date > '2022-02-20' and
                             termination_date isnull and
                             has_successful_payment = true
                            and datediff(day, exp_day_date, current_date) >= 11
                            then 1 end)                                                            as true_converted_subs,
                max(case
                        when d.subscription_start_date :: date > '2022-02-20' and
                             termination_date :: date >= '2022-02-20'
                            then 1 end)                                                            as total_new_subs_terminated,
                max(case
                        when d.subscription_start_date :: date <= '2022-02-20' and
                             termination_date :: date > '2022-02-20'
                            then 1 end)                                                            as total_returning_subs_terminated,
                max(case
                        when d.subscription_start_date :: date <= '2022-02-20' and termination_date isnull
                            then 1 end)                                                            as returning_subs_retained,
                max(case
                        when d.subscription_start_date :: date > '2022-02-20' and
                             has_successful_payment = false
                            and f.sso_user_id is not null
                            then 1 end)                                                            as failed_payments,
                max(case
                        when d.trial_period_started_at_aet :: date > '2022-02-20' and
                             last_canceled_at > '2022-02-20'
                            and has_successful_payment = false
                            then 1 end)                                                            as trials_cancellation_unpaid,
                max(case
                        when d.trial_period_started_at_aet :: date > '2022-02-20' and
                             last_canceled_at > '2022-02-20'
                            and has_successful_payment = true
                            then 1 end)                                                            as trials_cancellation_paid,
                max(case
                        when last_canceled_at > '2022-02-20'
                            then 1 end)                                                            as all_cancellation,
                max(q)                                                                             as downloads,
                max(rev)                                                                           as revenue,
                max(case
                        when rf.sso_user_id notnull then 1 end)                                    as refunds
         from allocations c


                  --              --remove envato users
                  join elements.ds_elements_sso_users sso
                       on sso.id = c.sso_user_id and split_part(email, '@', 2) != 'envato.com'
                  join elements.dim_elements_subscription d
                       on c.sso_user_id = d.sso_user_id
                           and (termination_date isnull or termination_date > '2022-02-20')
                  left join content_t dl on c.sso_user_id = dl.sso_uuid
                  left join refunds rf
                            on d.sso_user_id = rf.sso_user_id and d.dim_subscription_key = rf.dim_subscription_key
--             and date
                  left join failed_payments f on c.sso_user_id = f.sso_user_id
                  left join paid p on c.sso_user_id = p.sso_user_id
         group by 1, 3, 4, 5
     )

select variant                                                                   as variant,
       country,
       plan_type,
       reallocation,
       referer,
       case when already_subscribed = 1 then 'continuing_sub' else 'new_sub' end as returning,
       case when new_sub = 1 then 'first_timer' else 'resubscriber' end          as experiment_new_sub,
       count(*)                                                                  as subscribers,
       count(free_trial_signups)                                                 as free_trial_signups,
       count(non_free_trial_signups)                                             as non_free_trial_signups,
       count(total_new_signups)                                                  as total_new_signups,
       count(trial_sub_remaining)                                                as trial_sub_remaining,
       count(total_subs_remaining)                                               as total_subs_remaining,

       count(total_new_subs_terminated)                                          as total_new_subs_terminated,
       count(total_returning_subs_terminated)                                    as total_returning_subs_terminated,
       count(returning_subs_retained)                                            as returning_subs_retained,
       count(failed_payments)                                                    as failed_payments,
       count(all_cancellation)                                                   as all_cancellations,
       count(trials_cancellation_unpaid)                                         as cancelled_trial_users_unpaid,
       count(trials_cancellation_paid)                                           as cancelled_trial_users_paid,
       sum(downloads)                                                            as downloads,
       sum(revenue)                                                              as revenue_usd,
       count(refunds)                                                            as refunds
       --fix it
from clean_sso
group by 1, 2, 3, 4, 5, 6, 7;

select sso_user_id, fact.dim_subscription_key, dim_date_key
from elements.fact_elements_subscription_transactions fact
         inner join elements.dim_elements_transaction_attributes ta
                    on fact.dim_elements_transaction_key = ta.dim_elements_transaction_key
         inner join elements.dim_elements_subscription sub on fact.dim_subscription_key = sub.dim_subscription_key
    and fact.dim_date_key > 20220101
where ta.transaction_type = 'Refund'
;


with enrollments as
         (
             select c.sessionid,
                    a.fullvisitorid,
                    variant,
                    -- min(a.date_time_aest)     as                                                        exp_date,
                    -- count(distinct c.sessionid) as sessions,
                    --   max(s.sso_user_id)                                               as sso_uid,
--                     max(CASE
--                             WHEN se.geonetwork_country in ('United States', 'United Kingdom', 'Germany',
--                                                            'Canada', 'Australia', 'France', 'Italy', 'Spain',
--                                                            'Netherlands', 'Brazil',
--                                                            'India', 'South Korea', 'Turkey', 'Switzerland',
--                                                            'Japan', 'Spain')
--                                 then se.geonetwork_country
--                             when geonetwork_country in ('Argentina', 'Bolivia', 'Chile',
--                                                         'Colombia', 'Costa Rica', 'Cuba', 'Ecuador', 'Mexico',
--                                                         'Paraguay', 'Uruguay',
--                                                         'Venezuela') then 'LATAM'
--                             when geonetwork_country in ('Belarus', 'Kazakhstan', 'Russia', 'Ukraine')
--                                 then 'RU'
--                             else 'ROW' end)                                          as country,
--
--                     split_part(min(sc.dss_update_time || '|' || channel), '|', 2)    as referer,
                    max(case when hits_page_pagepath like '%/pricing%' then a.fullvisitorid end)   as pricing_page,
                    max(case when hits_page_pagepath like '%/subscribe%' then a.fullvisitorid end) as sub_page,
                    max(case
                            when hits_eventinfo_eventaction = 'Technical: Subscription Complete'
                                then a.fullvisitorid end)                                          as subscribe,
                    max(case
                            when hits_eventinfo_eventaction = 'license with download'
                                then a.fullvisitorid end)                                          as download_success

             from webanalytics.ds_bq_abtesting_enrolments_elements a
                      join webanalytics.ds_bq_sessions_elements se
                           on a.fullvisitorid = se.fullvisitorid::varchar and a.visitid = se.visitid
                               and a.date >= '2022-02-20'
                               and se.date > 20220220
                 --                       left join elements.dim_elements_subscription s on se.user_uuid = s.sso_user_id
--                       left join elements.rpt_elements_session_channel sc on se.sessionid = sc.sessionid
                      join webanalytics.ds_bq_events_elements c on c.fullvisitorid::varchar = a.fullvisitorid
                 and c.date > 20220220
                 and c.visitid = a.visitid

             where experiment_id = 'FTr-7_qaRO-UzNOru4HW1Q'
               and variant :: varchar in ('0', '1', '2', '3')
             group by 1, 2, 3
         ),
     multiple_allocations as
         (select fullvisitorid
          from enrollments
          group by 1
          having min(variant) = max(variant))
-- select
-- variant,
--        case when m.sso_uid is not null then 'reallocated' else 'not_reallocated' end as reallocation,
--        count(*) as cookies,
--        sum(sessions) as sessions
-- from enrollments a
-- left join multiple_allocations m on a.sso_uid = m.sso_uid
-- group by 1,2
--
-- ;


select variant,

--        country,
--        referer,
       max(case when m.fullvisitorid isnull then 'not_reallocated' else 'reallocated' end) as reallocation,
       count(*)                                                                            as sessions,
       count(distinct a.fullvisitorid)                                                     as cookies,
       count(pricing_page)                                                                 as pricing_page,
       count(sub_page)                                                                     as sub_page,
       count(download_success)                                                             as downloaders,
       count(distinct subscribe)                                                           as subscriptions
from enrollments a
         left join multiple_allocations m on a.fullvisitorid = m.fullvisitorid
group by 1;

--oct test
with enrollments as
         (
             select c.sessionid,
                    a.fullvisitorid,
                    variant,
                    -- min(a.date_time_aest)     as                                                        exp_date,
                    -- count(distinct c.sessionid) as sessions,
                    --   max(s.sso_user_id)                                               as sso_uid,
--                     max(CASE
--                             WHEN se.geonetwork_country in ('United States', 'United Kingdom', 'Germany',
--                                                            'Canada', 'Australia', 'France', 'Italy', 'Spain',
--                                                            'Netherlands', 'Brazil',
--                                                            'India', 'South Korea', 'Turkey', 'Switzerland',
--                                                            'Japan', 'Spain')
--                                 then se.geonetwork_country
--                             when geonetwork_country in ('Argentina', 'Bolivia', 'Chile',
--                                                         'Colombia', 'Costa Rica', 'Cuba', 'Ecuador', 'Mexico',
--                                                         'Paraguay', 'Uruguay',
--                                                         'Venezuela') then 'LATAM'
--                             when geonetwork_country in ('Belarus', 'Kazakhstan', 'Russia', 'Ukraine')
--                                 then 'RU'
--                             else 'ROW' end)                                          as country,
--
--                     split_part(min(sc.dss_update_time || '|' || channel), '|', 2)    as referer,
                    max(case
                            when hits_page_pagepath like '%/pricing'
                                or hits_page_pagepath like '%/pricing/%'
                                then a.fullvisitorid end)                                            as pricing_page,
                    max(case
                            when hits_page_pagepath like '%/subscribe'
                                or hits_page_pagepath like '%/subscribe/%'
                                or hits_page_pagepath like '%/subscribe?%' then a.fullvisitorid end) as sub_page,
                    max(case
                            when hits_eventinfo_eventaction = 'Technical: Subscription Complete'
                                then a.fullvisitorid end)                                            as subscribe,
                    max(case
                            when hits_eventinfo_eventaction = 'license with download'
                                then a.fullvisitorid end)                                            as download_success

             from webanalytics.ds_bq_abtesting_enrolments_elements a
                      join webanalytics.ds_bq_sessions_elements se
                           on a.fullvisitorid = se.fullvisitorid::varchar and a.visitid = se.visitid
                               and a.date
                                  --between '2021-10-25' and '2021-11-04'
                                  >= '2022-02-20'
                               and se.date
                                  --between 20211025 and 20211104
                                  >= 20220220
--                       left join elements.dim_elements_subscription s on se.user_uuid = s.sso_user_id
                      left join elements.rpt_elements_session_channel sc on se.sessionid = sc.sessionid
                      join webanalytics.ds_bq_events_elements c on c.fullvisitorid::varchar = a.fullvisitorid
                 and c.date
--                                 between 20211025 and 20211104
                                                                       >= 20220220
                 and c.visitid = a.visitid

             where
               --experiment_id = 'geInrbFNTa2AZdiRswkP2A'

                 experiment_id = 'FTr-7_qaRO-UzNOru4HW1Q'
               and variant :: varchar in ('0', '1', '2', '3')
             group by 1, 2, 3)
--          ,
--      multiple_allocations as
--          (select fullvisitorid
--           from enrollments
--           group by 1
--              having min(variant) = max(variant))


select variant,

--        country,
--        referer,
--        max(case when m.fullvisitorid isnull then 'not_reallocated' else 'reallocated' end) as reallocation,
       count(*)                        as sessions,
       count(distinct a.fullvisitorid) as cookies,
       count(pricing_page)             as pricing_page,
       count(distinct pricing_page)    as pricing_page_users,
       count(sub_page)                 as sub_page,
       count(distinct sub_page)        as sub_page_users,
       count(download_success)         as downloaders,
       count(distinct subscribe)       as subscriptions
from enrollments a
--          left join multiple_allocations m on a.fullvisitorid = m.fullvisitorid
group by 1;
,2,3 ;

experiment_id = 'geInrbFNTa2AZdiRswkP2A'


with pric as (
    select se.visitid,
           se.fullvisitorid,
           variant
            ,
           min(hits_hitnumber) as hn
    from webanalytics.ds_bq_events_elements se
             join webanalytics.ds_bq_abtesting_enrolments_elements a
                  on a.fullvisitorid = se.fullvisitorid::varchar and a.visitid = se.visitid
                      and se.date >= 20220220

    where experiment_id = 'FTr-7_qaRO-UzNOru4HW1Q'
        and hits_page_pagepath like '%/pricing'
       or hits_page_pagepath like '%/pricing/%'
    group by 1, 2, 3
)

select variant,
       case when right(category_p, 8) ~ '[A-Z0-9]' then 'item_page' else category_p end as category,
       sum(ct)                                                                          as ct
from (
         select variant,
                split_part(case
                               when split_part(hits_page_pagepath, '/', 2) in ('fr', 'pt-br', 'de', 'es', 'ru')
                                   then split_part(hits_page_pagepath, '/', 3)
                               else split_part(hits_page_pagepath, '/', 2) end, '?', 1) as category_p,
                count(*)                                                                as ct
         from webanalytics.ds_bq_events_elements w
                  join pric p on p.visitid ::varchar = w.visitid ::varchar
             and p.fullvisitorid ::varchar = w.fullvisitorid ::varchar
             and (hn - 1 = hits_hitnumber)
             and date >= 20220220
-- left join user_uu
         group by 1, 2)
group by 1, 2
order by 3 desc
--and hn :: varchar = hits_hitnumber :: varchar

-- group by 1 order by 2 desc
-- limit 100
select *
from webanalytics.ds_bq_events_elements w
where hits_page_pagepath like '%lp%'
  and date = 20220228
limit 100
;

select split_part(hits_page_pagepath, '/', 2),
       right(split_part(hits_page_pagepath, '/', 2), 8),
       case when right(split_part(hits_page_pagepath, '/', 2), 8) ~ '[A-Z0-9]' then 1 end

from webanalytics.ds_bq_events_elements w
where date = 20220228
limit 300;

with pric as (
    select se.visitid,
           se.fullvisitorid,
           variant
            ,
           min(hits_hitnumber) as hn
    from webanalytics.ds_bq_events_elements se
             join webanalytics.ds_bq_abtesting_enrolments_elements a
                  on a.fullvisitorid = se.fullvisitorid::varchar and a.visitid = se.visitid
                      and se.date >= 20220220

    where experiment_id = 'FTr-7_qaRO-UzNOru4HW1Q'
      and (hits_page_pagepath like '%/subscribe'
        or hits_page_pagepath like '%/subscribe/%'
        or hits_page_pagepath like '%/subscribe?%')
    group by 1, 2, 3
)
        ,
     contra as
         (select p.visitid,
                 p.fullvisitorid,
                 variant,

                 split_part(max(hits_hitnumber || '|' || split_part(case
                                                                        when hits_page_pagepath isnull then 'missing'
                                                                        when split_part(hits_page_pagepath, '/', 2) in
                                                                             ('fr', 'pt-br', 'de', 'es', 'ru')
                                                                            then split_part(hits_page_pagepath, '/', 3)
                                                                        else split_part(hits_page_pagepath, '/', 2) end,
                                                                    '?', 1)), '|', 2) as referer

          from pric p
                   join webanalytics.ds_bq_events_elements w on p.visitid ::varchar = w.visitid ::varchar
              and p.fullvisitorid ::varchar = w.fullvisitorid ::varchar
              and hn - 1 > hits_hitnumber
              and date >= 20220220
          group by 1, 2, 3)


select variant,
       case when right(referer, 8) ~ '[A-Z0-9]' then 'item_page' else referer end as category,
       count(distinct fullvisitorid || visitid)                                   as sessions,
       count(distinct fullvisitorid)                                              as cookies
from contra
group by 1, 2
order by 3 desc;

with a as (
    select variant,
           a.fullvisitorid,
           a.visitid,
           min(hits_hitnumber) as hn
    from webanalytics.ds_bq_events_elements se
             join webanalytics.ds_bq_abtesting_enrolments_elements a
                  on a.fullvisitorid = se.fullvisitorid::varchar and a.visitid = se.visitid
                      and se.date >= 20220220

    where experiment_id = 'FTr-7_qaRO-UzNOru4HW1Q'
      and (hits_page_pagepath like '%/pricing'
        or hits_page_pagepath like '%/pricing/%')
    group by 1, 2, 3)
select se.fullvisitorid,
       se.visitid,
       hn,
       hits_hitnumber,
       hits_page_pagepath,
       split_part(case
                      when split_part(hits_page_pagepath, '/', 2) in ('fr', 'pt-br', 'de', 'es', 'ru')
                          then split_part(hits_page_pagepath, '/', 3)
                      else split_part(hits_page_pagepath, '/', 2) end, '?', 1) as page_q
from webanalytics.ds_bq_events_elements se
         join a
              on a.fullvisitorid = se.fullvisitorid::varchar and a.visitid = se.visitid
                  and se.date >= 20220220
                  and hn - hits_hitnumber < 5
                  and hn - hits_hitnumber >= 0
order by 1, 2, 4 asc
limit 500


select *
from webanalytics.ds_bq_events_elements se
where fullvisitorid = '459984053009482'
  and visitid = '1645597230';

fullvisitorid,visitid
459984053009482,1645597230

select se.visitid,
       se.fullvisitorid,
       variant
        ,
       min(hits_hitnumber) as hn
from webanalytics.ds_bq_events_elements se
         join webanalytics.ds_bq_abtesting_enrolments_elements a
         join webanalytics.ds_bq_hits_elements
              on a.fullvisitorid = se.fullvisitorid::varchar and a.visitid = se.visitid
                  and se.date >= 20220220

where experiment_id = 'FTr-7_qaRO-UzNOru4HW1Q'
  and (hits_page_pagepath like '%/subscribe'
    or hits_page_pagepath like '%/subscribe/%'
    or hits_page_pagepath like '%/subscribe?%')
group by 1, 2, 3;


select fullvisitorid,
       visitid,
       hits_hitnumber,
       hits_page_pagepath,
       null
from webanalytics.ds_bq_hits_elements
where fullvisitorid = '459984053009482'
  and visitid = '1645597230'

union all
select fullvisitorid,
       visitid,
       hits_hitnumber,
       hits_page_pagepath,
       hits_eventinfo_eventaction
from webanalytics.ds_bq_events_elements
where fullvisitorid = '459984053009482'
  and visitid = '1645597230';


with pric as (
    select ev.fullvisitorid,
           ev.visitid,
           variant,
           max(case
                   when (hits_page_pagePath like '%/pricing'
                       or hits_page_pagePath like '%/pricing/%') then 1 end)           as pricing,
           max(case
                   when (hits_page_pagePath like '%/subscribe'
                       or hits_page_pagePath like '%/subscribe/%'
                       or hits_page_pagePath like '%/subscribe?%') then 1 end)         as sub_page,
           max(case
                   when hits_eventinfo_eventaction = 'Focus On: Create Your Account'
                       or (hits_eventinfo_eventcategory = 'Google Auth' and
                           hits_eventinfo_eventaction = 'Sign Up Success') then 1 end) as signup,
           max(case
                   when hits_eventinfo_eventaction = 'Technical: Subscription Complete'
                       then 1 end)                                                     as subscribe

    from webanalytics.ds_bq_events_elements ev
             join webanalytics.ds_bq_abtesting_enrolments_elements a
                  on a.fullvisitorid::varchar = ev.fullvisitorid::varchar
                      and a.visitid::varchar = ev.visitid::varchar
                      and ev.date >= 20220220
                      and experiment_id = 'FTr-7_qaRO-UzNOru4HW1Q'
    group by 1, 2, 3

    union all

    select ev.fullvisitorid,
           ev.visitid,
           variant,
           max(case
                   when (hits_page_pagePath like '%/pricing'
                       or hits_page_pagePath like '%/pricing/%') then 1 end)   as pricing,
           max(case
                   when (hits_page_pagePath like '%/subscribe'
                       or hits_page_pagePath like '%/subscribe/%'
                       or hits_page_pagePath like '%/subscribe?%') then 1 end) as sub_page,
           null                                                                as signup,
           null                                                                as subscribe

    from webanalytics.ds_bq_hits_elements ev

             join webanalytics.ds_bq_abtesting_enrolments_elements a
                  on a.fullvisitorid::varchar = ev.fullvisitorid::varchar
                      and a.visitid::varchar = ev.visitid::varchar
                      and ev.date >= 20220220
                      and experiment_id = 'FTr-7_qaRO-UzNOru4HW1Q'
    group by 1, 2, 3
),
     fv as
         (
             select ev.fullvisitorid,
                    max(CASE
                            WHEN se.geonetwork_country in ('United States', 'United Kingdom', 'Germany',
                                                           'Canada', 'Australia', 'France', 'Italy', 'Spain',
                                                           'Netherlands', 'Brazil',
                                                           'India', 'South Korea', 'Turkey', 'Switzerland',
                                                           'Japan', 'Spain')
                                then se.geonetwork_country
                            when geonetwork_country in ('Argentina', 'Bolivia', 'Chile',
                                                        'Colombia', 'Costa Rica', 'Cuba', 'Ecuador', 'Mexico',
                                                        'Paraguay', 'Uruguay',
                                                        'Venezuela') then 'LATAM'
                            when geonetwork_country in ('Belarus', 'Kazakhstan', 'Russia', 'Ukraine')
                                then 'RU'
                            else 'ROW' end)                                       as country,
                    split_part(min(sc.dss_update_time || '|' || channel), '|', 2) as referer
             from pric ev
                      join webanalytics.ds_bq_sessions_elements se
                           on ev.fullvisitorid::varchar = se.fullvisitorid::varchar
                               and ev.visitid::varchar = se.visitid::varchar
                      left join elements.rpt_elements_session_channel sc on sc.sessionid = se.sessionid
             group by 1
         ),

     a as (
         select variant,
                country,
                referer,
                count(distinct case
                                   when pricing = 1
                                       then p.fullvisitorid end) as pricing_page,
                count(distinct case
                                   when sub_page = 1
                                       then p.fullvisitorid end) as sub_page,
                count(distinct case
                                   when subscribe = 1
                                       then p.fullvisitorid end) as subscription,
                count(distinct case
                                   when signup = 1
                                       then p.fullvisitorid end) as signup,
                count(distinct p.fullvisitorid)                  as cookies
         from pric p
                  join fv on p.fullvisitorid = fv.fullvisitorid
         group by 1, 2, 3)
select *
from a UNPIVOT (
                hits FOR stage IN (pricing_page, sub_page, subscription, signup, cookies)
    )
;
,
           min(hits_hitnumber) as hn,
               where experiment_id = 'FTr-7_qaRO-UzNOru4HW1Q'
      and (hits_page_pagepath like '%/subscribe'
        or hits_page_pagepath like '%/subscribe/%'
        or hits_page_pagepath like '%/subscribe?%')
     ,
     contra as
         (select p.visitid,
                 p.fullvisitorid,
                 variant,

                  split_part(max(hits_hitnumber || '|' || split_part(case when hits_page_pagepath isnull then 'missing'
                                when split_part(hits_page_pagepath, '/', 2) in ('fr', 'pt-br', 'de', 'es', 'ru')
                                    then split_part(hits_page_pagepath, '/', 3)
                                else split_part(hits_page_pagepath, '/', 2) end, '?', 1)), '|', 2) as referer

          from pric p
                   join webanalytics.ds_bq_events_elements w on p.visitid ::varchar = w.visitid ::varchar
              and p.fullvisitorid ::varchar = w.fullvisitorid ::varchar
              and hn - 1 > hits_hitnumber
              and date >= 20220220
          group by 1, 2, 3)


select variant,
       case when right(referer, 8) ~ '[A-Z0-9]' then 'item_page' else referer end as category,
       count(distinct fullvisitorid || visitid)                                   as sessions,
       count(distinct fullvisitorid)                                              as cookies
from contra
group by 1, 2
order by 3 desc;

select date(date)                    as date_day,
       count(*)                      as ct,
       count(distinct fullvisitorid) as cookies
from webanalytics.ds_bq_events_elements

where hits_eventinfo_eventaction = 'Technical: Subscription Complete'
  and date > 20220220
group by 1;



with pric as (
    select ev.fullvisitorid,
           ev.visitid,
           variant,
           max(case
                   when (hits_page_pagePath like '%/pricing'
                       or hits_page_pagePath like '%/pricing/%') then 1 end)           as pricing,
           max(case
                   when (hits_page_pagePath like '%/subscribe'
                       or hits_page_pagePath like '%/subscribe/%'
                       or hits_page_pagePath like '%/subscribe?%') then 1 end)         as sub_page,
           max(case
                   when hits_eventinfo_eventaction = 'Focus On: Create Your Account'
                       or (hits_eventinfo_eventcategory = 'Google Auth' and
                           hits_eventinfo_eventaction = 'Sign Up Success') then 1 end) as signup,
           max(case
                   when hits_eventinfo_eventaction = 'Technical: Subscription Complete'
                       then 1 end)                                                     as subscribe,
           max(case
                   when (hits_page_pagePath like '%/photos'
                       or hits_page_pagePath like '%/photos/%') then 1 end)            as photos

    from webanalytics.ds_bq_events_elements ev
             join webanalytics.ds_bq_abtesting_enrolments_elements a
                  on a.fullvisitorid::varchar = ev.fullvisitorid::varchar
                      and a.visitid::varchar = ev.visitid::varchar
                      and ev.date between 20220215 and 20220314
                      and experiment_id = 'mtlC1-cIRtq_PTt60IXstw'
    group by 1, 2, 3

    union all

    select ev.fullvisitorid,
           ev.visitid,
           variant,
           max(case
                   when (hits_page_pagePath like '%/pricing'
                       or hits_page_pagePath like '%/pricing/%') then 1 end)   as pricing,
           max(case
                   when (hits_page_pagePath like '%/subscribe'
                       or hits_page_pagePath like '%/subscribe/%'
                       or hits_page_pagePath like '%/subscribe?%') then 1 end) as sub_page,
           null                                                                as signup,
           null                                                                as subscribe,
           max(case
                   when (hits_page_pagePath like '%/photos'
                       or hits_page_pagePath like '%/photos/%') then 1 end)    as photos

    from webanalytics.ds_bq_hits_elements ev

             join webanalytics.ds_bq_abtesting_enrolments_elements a
                  on a.fullvisitorid::varchar = ev.fullvisitorid::varchar
                      and a.visitid::varchar = ev.visitid::varchar
                      and ev.date between 20220215 and 20220314
                      and experiment_id = 'mtlC1-cIRtq_PTt60IXstw'
    group by 1, 2, 3
),
--      fv as
--          (
--              select ev.fullvisitorid
--                     ,max(CASE
--                             WHEN se.geonetwork_country in ('United States', 'United Kingdom', 'Germany',
--                                                            'Canada', 'Australia', 'France', 'Italy', 'Spain',
--                                                            'Netherlands', 'Brazil',
--                                                            'India', 'South Korea', 'Turkey', 'Switzerland',
--                                                            'Japan', 'Spain')
--                                 then se.geonetwork_country
--                             when geonetwork_country in ('Argentina', 'Bolivia', 'Chile',
--                                                         'Colombia', 'Costa Rica', 'Cuba', 'Ecuador', 'Mexico',
--                                                         'Paraguay', 'Uruguay',
--                                                         'Venezuela') then 'LATAM'
--                             when geonetwork_country in ('Belarus', 'Kazakhstan', 'Russia', 'Ukraine')
--                                 then 'RU'
--                             else 'ROW' end)                                       as country
-- --                     ,split_part(min(sc.dss_update_time || '|' || channel), '|', 2) as referer
--              from pric ev
--                       join webanalytics.ds_bq_sessions_elements se
--                            on ev.fullvisitorid::varchar = se.fullvisitorid::varchar
--                                and ev.visitid::varchar = se.visitid::varchar
-- --                       left join elements.rpt_elements_session_channel sc on sc.sessionid = se.sessionid
--              group by 1
--          ),

     a as (
         select variant,
                photos,
--                 country,
--                 referer,
                count(distinct case
                                   when pricing = 1
                                       then p.fullvisitorid end) as pricing_page,
                count(distinct case
                                   when sub_page = 1
                                       then p.fullvisitorid end) as sub_page,
                count(distinct case
                                   when subscribe = 1
                                       then p.fullvisitorid end) as subscription,
                count(distinct case
                                   when signup = 1
                                       then p.fullvisitorid end) as signup,
                count(distinct p.fullvisitorid)                  as cookies
         from pric p
--                   join fv on p.fullvisitorid = fv.fullvisitorid
         group by 1, 2)
select *
from a UNPIVOT (
                hits FOR stage IN (pricing_page, sub_page, subscription, signup, cookies)
    );


Event Action
click;
a
Event Category
sidebar-refinement-group;
properties
Event Label
refinement-selected;
isAuthentic
Event Value
1


https://elements.envato.com/photos/orientation-landscape/properties-authentic/pg-4


with pric as (
    select ev.fullvisitorid,
           ev.visitid,
           variant,
           count(case
                     when (hits_page_pagePath like '%/properties-authentic'
                         or hits_page_pagePath like '%/properties-authentic/%') then 1 end) as auth_filter,
           count(case
                     when (hits_page_pagePath like '%/photos'
                         or hits_page_pagePath like '%/photos/%') then 1 end)               as photo,
           count(case
                     when hits_eventinfo_eventaction = 'license with download'
                         then 1 end)                                                        as download_success,
           count(case
                     when hits_eventinfo_eventaction = 'click;a'
                         and hits_eventinfo_eventcategory = 'item-results;photos'
                         then 1 end)                                                        as photos_av,
           count(case
                     when hits_eventinfo_eventaction = 'click;a'
                         and split_part(hits_eventinfo_eventcategory, ';', 1) = 'item-results'
                         then 1 end)                                                        as all_av

    from webanalytics.ds_bq_events_elements ev
             join webanalytics.ds_bq_abtesting_enrolments_elements a
                  on a.fullvisitorid :: varchar = ev.fullvisitorid::varchar
                      and a.visitid ::varchar = ev.visitid ::varchar
                      and ev.date between 20220215 and 20220314
                      and experiment_id = 'mtlC1-cIRtq_PTt60IXstw'
    group by 1, 2, 3

    union all

    select ev.fullvisitorid,
           ev.visitid,
           variant,
           max(case
                   when (hits_page_pagePath like '%/properties-authentic'
                       or hits_page_pagePath like '%/properties-authentic/%') then 1 end) as auth_filter,
           max(case
                   when (hits_page_pagePath like '%/photos'
                       or hits_page_pagePath like '%/photos/%') then 1 end)               as photo,
           null                                                                           as a,
           null                                                                           as b,
           null                                                                           as c

    from webanalytics.ds_bq_hits_elements ev

             join webanalytics.ds_bq_abtesting_enrolments_elements a
                  on a.fullvisitorid::varchar = ev.fullvisitorid::varchar
                      and a.visitid::varchar = ev.visitid::varchar
                      and ev.date between 20220215 and 20220314
                      and experiment_id = 'mtlC1-cIRtq_PTt60IXstw'
    group by 1, 2, 3
),
     a as (
         select variant,
                case
                    when photo isnull then 'no_viewer'
                    when photo = 0 then 'not_photo_viewer'
                    when photo > 0 then 'photo viewer' end       as photo_user,

                count(distinct case
                                   when photo > 0
                                       then p.fullvisitorid end) as photos_users,
                count(distinct case
                                   when auth_filter > 0
                                       then p.fullvisitorid end) as auth_filter_users,
                count(distinct case
                                   when photos_av = 1
                                       then p.fullvisitorid end) as photos_adviewers,
                sum(download_success)                            as overall_downloads,
                sum(photos_av)                                   as photos_item_viewed,
                sum(all_av)                                      as all_item_viewed,
                sum(photo)                                       as photos_results_pages,
                count(distinct p.fullvisitorid)                  as cookies
         from pric p
         group by 1, 2)
select *
from a UNPIVOT (
                hits FOR stage IN (auth_filter_users, photos_users, photos_adviewers, overall_downloads, photos_item_viewed, all_item_viewed, photos_results_pages, cookies)
    );

select date_aest :: date           as date_day,
       geonetwork_country          as country,
       count(distinct sso_user_id) as signups
from elements.rpt_elements_user_signup_session ss
         join webanalytics.ds_bq_sessions_elements se on ss.sessionid = se.sessionid
    and date_aest :: date >= '2022-03-10'
    and signup_date :: date >= '2022-03-10'
group by 1, 2;



with pric as (
    select se.visitid,
           se.fullvisitorid,
           variant
            ,
           min(hits_hitnumber) as hn
    from webanalytics.ds_bq_events_elements se
             join webanalytics.ds_bq_abtesting_enrolments_elements a
                  on a.fullvisitorid = se.fullvisitorid::varchar and a.visitid = se.visitid
                      and se.date >= 20220404

    where experiment_id = '4C6lkqcfQZ2cNG-QhjT7yA'
        and hits_page_pagepath like '%/pricing'
        and hits_page_pagepath like '%/pricing'
       or hits_page_pagepath like '%/pricing/%'
    group by 1, 2, 3
)

select variant,
       case when right(category_p, 8) ~ '[A-Z0-9]' then 'item_page' else category_p end as category,
       sum(ct)                                                                          as ct
from (
         select variant,
                split_part(case
                               when split_part(hits_page_pagepath, '/', 2) in ('fr', 'pt-br', 'de', 'es', 'ru')
                                   then split_part(hits_page_pagepath, '/', 3)
                               else split_part(hits_page_pagepath, '/', 2) end, '?', 1) as category_p,
                count(*)                                                                as ct
         from webanalytics.ds_bq_events_elements w
                  join pric p on p.visitid ::varchar = w.visitid ::varchar
             and p.fullvisitorid ::varchar = w.fullvisitorid ::varchar
             and (hn - 1 = hits_hitnumber)
             and date >= 20220220
-- left join user_uu
         group by 1, 2)
group by 1, 2
order by 3 desc;


with pric as
         (
             select a.fullvisitorid,
--                     a.visitid :: varchar as visitid,
                    variant,
--                     hits_page_pagepath as pagepath,
                    max(case
                            when hits_eventinfo_eventaction = 'Technical: Subscription Complete'
                                then 1
                            else 0 end)   as subscribe,
--                     count(case
--         when hits_eventinfo_eventaction = 'click;a'
--             and split_part(hits_eventinfo_eventcategory, ';', 1) = 'item-results' then 1
--         else 0 end)                                     as item_page,
                    count(case
                              when hits_eventinfo_eventaction = 'license with download'
                                  then 1
                              else 0 end) as download_success

             from webanalytics.ds_bq_abtesting_enrolments_elements a
                      join webanalytics.ds_bq_events_elements c
                           on a.fullvisitorid = c.fullvisitorid::varchar and a.visitid = c.visitid
--                  and c.date = 20220404 -- date(c.date) = '2022-04-04'
                               and c.date > 20220404 --date(a.date) = '2022-04-04'
                               and experiment_id = '4C6lkqcfQZ2cNG-QhjT7yA'
             where hits_eventinfo_eventaction in ('license with download', 'Technical: Subscription Complete')

             group by 1, 2 --, 3
--              limit 100
         )

select variant,
       count(distinct fullvisitorid) as cookies,
--                 split_part(case
--                                when split_part(pagepath, '/', 2) in ('fr', 'pt-br', 'de', 'es', 'ru')
--                                    then split_part(pagepath, '/', 3)
--                                else split_part(pagepath, '/', 2) end, '?', 1) as category_p,
       sum(subscribe)                as new_subs,
       sum(download_success)         as downloads,
       count(download_success)       as downloaders

from pric
--              p on p.visitid ::varchar = w.visitid ::varchar
--              and p.fullvisitorid ::varchar = w.fullvisitorid ::varchar
--
--              and date >= 20220404
-- left join user_uu
group by 1 --, 2
;

with pric as
         (
             select a.fullvisitorid,
--                     a.visitid :: varchar as visitid,
                    variant,
                    split_part(hits_eventinfo_eventcategory, ';', 2) as category,
                    count(*)                                         as ct

             from webanalytics.ds_bq_abtesting_enrolments_elements a
                      join webanalytics.ds_bq_events_elements c
                           on a.fullvisitorid = c.fullvisitorid::varchar and a.visitid = c.visitid
--                  and c.date = 20220404 -- date(c.date) = '2022-04-04'
                               and c.date > 20220404 --date(a.date) = '2022-04-04'
                               and experiment_id = '4C6lkqcfQZ2cNG-QhjT7yA'
             where (hits_eventinfo_eventaction = 'click;a'
                 and split_part(hits_eventinfo_eventcategory, ';', 1) = 'item-results')

             group by 1, 2, 3
--              limit 100
         )

select variant,
       count(distinct fullvisitorid) as cookies,
       category,
--                 split_part(case
--                                when split_part(pagepath, '/', 2) in ('fr', 'pt-br', 'de', 'es', 'ru')
--                                    then split_part(pagepath, '/', 3)
--                                else split_part(pagepath, '/', 2) end, '?', 1) as category_p,
       sum(ct)                       as item_pages,
       count(ct)                     as item_pagers
from pric
--              p on p.visitid ::varchar = w.visitid ::varchar
--              and p.fullvisitorid ::varchar = w.fullvisitorid ::varchar
--
--              and date >= 20220404
-- left join user_uu
group by 1, 3 --, 2
;


select date_trunc('hour', date_time_aest :: timestamp) as date_hour,
--        case when replace(category,'?adposition=', '') ~ '[A-Z0-9]' then 'item_page' else replace(category,'?adposition=', '') end as category,
       count(*)                                        as searches,
       sum(impressions)                                as impressions,
       sum(dls)                                        as downloads,
       sum(item_clicks)                                as item_clicks,
       sum(sub_from_search)                            as subs
from google_analytics.raw_elements_search_analysis
where date_time_aest :: date between '2022-03-30' and '2022-04-03'
  and category <> 'user-portfolio'
group by 1;
--,2


select *
from google_analytics.raw_elements_search_analysis
limit 100


select date_trunc('hour', '2020-09-04 01:43:15.051' :: timestamp);



select date_aest :: date    as date_day,
       case
           when split_part(hits_page_pagepath, '-',
                           len(hits_page_pagepath) - len(replace(hits_page_pagepath, '-', '')) + 1) ~ '[A-Z0-9]'
               then 'item_page'
           else 'other' end as source,
       count(*)             as ct
from webanalytics.ds_bq_events_elements
where hits_eventinfo_eventaction = 'license with download'
  and date_aest :: date between '2022-03-21' and '2022-04-10'
group by 1, 2


select hits_page_pagepath,
       split_part(hits_page_pagepath, '-', len(hits_page_pagepath) - len(replace(hits_page_pagepath, '-', '')) + 1),
       case
           when split_part(hits_page_pagepath, '-',
                           len(hits_page_pagepath) - len(replace(hits_page_pagepath, '-', '')) + 1) ~ '[A-Z0-9]'
               then 'item_page'
           else 'other' end as source
from (
         select 'https://elements.envato.com/view-on-the-countryside-YRT2777' as hits_page_pagepath
         union all
         select 'https://elements.envato.com/audio/royalty-free-music');



with pric as
         (
             select a.fullvisitorid,
--                     a.visitid :: varchar as visitid,
                    variant,
                    count(case
                              when hits_eventinfo_eventcategory = 'header-search-form' and
                                   hits_eventinfo_eventaction = 'submit'
                                  then 1 end) as searches,
                    max(case
                            when hits_eventinfo_eventaction = 'Technical: Subscription Complete'
                                then 1
                            else 0 end)       as subscribe,
                    count(case
                              when hits_eventinfo_eventaction = 'click;a'
                                  and split_part(hits_eventinfo_eventcategory, ';', 1) = 'item-results' then 1
                              else 0 end)     as item_page,
                    count(case
                              when hits_eventinfo_eventaction = 'license with download'
                                  and hits_eventinfo_eventlabel = 'licensing'
                                  then 1 end) as download_success

             from webanalytics.ds_bq_abtesting_enrolments_elements a
                      join webanalytics.ds_bq_events_elements c
                           on a.fullvisitorid = c.fullvisitorid::varchar and a.visitid = c.visitid
--                  and c.date = 20220404 -- date(c.date) = '2022-04-04'
                               and c.date > 20220516 --date(a.date) = '2022-04-04'
                               and experiment_id = 'rGLaPSUdRpy-5RkCtcBGiw'
                  --where hits_eventinfo_eventaction in ('license with download', 'Technical: Subscription Complete')

             group by 1, 2 --, 3
--              limit 100
         )

select variant,
       count(distinct fullvisitorid)                    as cookies,

       sum(searches)                                    as searches,
       sum(item_page)                                   as item_pages,
       sum(subscribe)                                   as new_subs,
       sum(download_success)                            as downloads,
       count(case when download_success = 1 then 1 end) as downloaders

from pric

group by 1 --, 2
;


select *
from elements.rpt_elements_session_channel
limit 100;


SELECT dim.*, rpt.invoice_number, rpt.transaction_type, rpt.transaction_status, paypal.*
FROM elements.dim_elements_subscription dim
         INNER JOIN elements.rpt_elements_recurly_subscription_transactions rpt
                    ON rpt.dw_recurly_subscription_key = dim.dw_recurly_subscription_key
                        AND rpt.payment_method = 'paypal'
         INNER JOIN paypal.ds_paypal_au_elements paypal
                    ON paypal.invoice_id = rpt.invoice_number
where subscription_start_date = '2022-06-12'
ORDER BY dim.subscription_start_date DESC
LIMIT 100
;

select count(paypal_reference_id)          as ct,
       count(distinct paypal_reference_id) as pref
from paypal.ds_paypal_au_elements
where transaction_initiation_date :: date = '2022-06-12'
--limit 100


select *

from paypal.ds_paypal_transaction_attributes
limit 100;



with a as (
    select fullvisitorid :: varchar                                                      as fullvisitorid,

           max(country)                                                                  as cc,
           case
               when max(country) in
                    (
                     'United States',
                     'United Kingdom',
                     'Germany',
                     'Canada',
                     'France',
                     'Australia',
                     'Spain',
                     'South Korea',
                     'Italy',
                     'Netherlands',
                     'Japan',
                     'Switzerland',
                     'Austria',
                     'Sweden',
                     'Belgium',
                     'Portugal',
                     'New Zealand',
                     'Denmark',
                     'Norway',
                     'Ireland',
                     'Greece',
                     'Finland',
                     'Luxembourg',
                     'Liechtenstein')
                   then true
               else false end                                                            as free_trial_countries,
--max(date_aest :: date)                                                        as date_day,
           count(distinct user_uuid)                                                     as users,
           count(distinct case when has_successful_payment = false then sso_user_id end) as unpaid_trials,
           count(distinct case when has_successful_payment = true then sso_user_id end)  as converted_trials
    from webanalytics.ds_bq_sessions_elements e
             left join elements.dim_elements_subscription s on user_uuid = s.sso_user_id
        and trial_period_started_at_aet :: date > '2022-01-01'
             left join dim_geo_network dgn on s.dim_geo_network_key = dgn.dim_geo_network_key
    where date_aest :: date > '2022-05-01'
      and user_uuid notnull
    group by 1)

select date(date_aest) as              date_day,
       cc,
       free_trial_countries,
       case
           when unpaid_trials > 1 then 'multiple_free_trials'

           when converted_trials > 0 and unpaid_trials = 1 then 'converted_after_one_trial'
           when unpaid_trials = 1 then 'one_free_trial'
           when unpaid_trials = 0 then 'no_trial'
           end         as              type,
       count(distinct a.fullvisitorid) ct
from a
         join webanalytics.ds_bq_sessions_elements s on a.fullvisitorid = s.fullvisitorid :: varchar
    and date_aest :: date > '2022-05-01'
group by 1, 2, 3, 4
;


select *,
       ratio_to_report(cookies) over ()
from (;

select variant,
       -- case when hits_eventinfo_eventcategory = 'sidebar-refinement-group;numberOfPeople' then 1 end as used_filter,
       count(distinct a.fullvisitorid)                                                as cookies,
       count(distinct case
                          when hits_eventinfo_eventcategory = 'item-results;photos'
                              then a.fullvisitorid end)                               as photo_users,
       sum(case when hits_eventinfo_eventcategory = 'item-results;photos' then 1 end) as photo_clicks

from webanalytics.ds_bq_events_elements ev
         join webanalytics.ds_bq_abtesting_enrolments_elements a
              on a.fullvisitorid :: varchar = ev.fullvisitorid::varchar
                  and a.visitid ::varchar = ev.visitid ::varchar
                  and ev.date between 20220712 and 20220817
                  and experiment_id = 'ctaKgiPdTZWoMt1C-VUV-g'
group by 1;


select country_group,
       country,
       case
           when current_plan like '%enterprise%'
               then 'enterprise'
           when current_plan like '%team%' then 'team'
           when current_plan like '%student_annual%'
               then 'student_annual'
           when current_plan like '%student_monthly%'
               then 'student_monthly'
           else plan_type end,
       count(distinct sso_user_id) as active_subscribers

from elements.dim_elements_subscription s
         left join dim_geo_network dgn on s.dim_geo_network_key = dgn.dim_geo_network_key

where termination_date isnull
  and has_successful_payment = true

group by 1, 2, 3;



select is_first_subscription
from elements.dim_elements_subscription;


select *
from webanalytics.ds_bq_sessions_elements
where date = 20220810
limit 100

select *
--        category,
--        count(*) as ct
from google_analytics.raw_elements_search_analysis

where date > '2022-08-01'
  and category isnull
limit 200;



select category,
       search_terms,
       count(*) as ct
from webanalytics.rpt_google_search_terms_elements
where date_aet >= '2022-07-25'
group by 1, 2
order by 3 desc
limit 500

WITH elements_teams AS (
    SELECT t.humane_id,
           tm.user_id    AS elements_user_uuid,
           tm.role_name,
           t.seats,
           s.sso_user_id,
           sso.username,
           sso.email,
           du.dim_users_key,
           tm.created_at AS member_added_on,
           tm.removed_at AS member_removed_on
    FROM elements.ds_elements_team_memberships tm
             INNER JOIN elements.ds_elements_teams t
                        ON tm.team_id = t.id
             INNER JOIN elements.ds_elements_users s
                        ON tm.user_id = s.id
             INNER JOIN elements.ds_elements_sso_users sso
                        ON sso.id = s.sso_user_id
             INNER JOIN envato.dim_users du
                        ON du.sso_uuid = s.sso_user_id
    where tm.removed_at isnull
),
     elements_teams_with_billing_contacts AS (
         SELECT members.humane_id,
                members.sso_user_id
         FROM elements_teams AS members
                  INNER JOIN (SELECT humane_id,
                                     elements_user_uuid                                                       AS billing_contact_elements_uuid,
                                     sso_user_id                                                              AS billing_contact_sso_uuid,
                                     username                                                                 AS billing_contact_username,
                                     email                                                                    AS billing_contact_email,
                                     ROW_NUMBER() OVER (PARTITION BY humane_id ORDER BY member_added_on DESC) AS rn
                              FROM elements_teams
                              WHERE role_name NOT IN ('team_member', 'disabled_team_member')) AS contact
                             ON members.humane_id = contact.humane_id
                                 AND contact.rn = 1
     ),
     esub as (
         select humane_id,
                split_part(max(subscription_start_date || '|' || case
                                                                     when current_plan like '%enterprise%'
                                                                         then 'enterprise'
                                                                     when current_plan like '%team%' then 'team' end),
                           '|', 2) as plan_type
         from elements_teams_with_billing_contacts e
                  join elements.dim_elements_subscription s on e.sso_user_id = s.sso_user_id
         group by 1)
        ,
     a as (
         SELECT e.humane_id                        as team_name,
                MAX(seats)                         AS seats,
                count(distinct elements_user_uuid) AS active_members
         FROM elements_teams e
         GROUP BY 1)
select plan_type,
       seats,
       active_members,
       count(*) as teams
from a
         join esub on a.team_name = esub.humane_id
group by 1, 2, 3;

(with sigs as (
    select created_at_utc :: date                      as date_day,
           si.sso_user_id,
           case when su.sso_user_id notnull then 1 end as paid_sub

    from elements.rpt_elements_user_signup_session si
             left join webanalytics.ds_bq_sessions_elements se on si.sessionid = se.sessionid
             left join elements.dim_elements_subscription su
                       on si.sso_user_id = su.sso_user_id and has_successful_payment = true
    where created_at_utc :: date between '2022-07-26' and '2022-08-24'
      and date(date) between '2022-07-26' and '2022-08-24'
      and device_devicecategory = 'mobile'
    group by 1, 2, 3
)
 select 'mobile'                                                    as platform,
        count(distinct sso_user_id)                                 as signups,
        count(distinct case when paid_sub = 1 then sso_user_id end) as paid_subs,
        count(distinct user_uuid)                                   as also_active_on_mobile_and_desktop,
        count(distinct case when paid_sub = 1 then user_uuid end)   as also_active_on_mobile_and_desktop_paid_sub

 from sigs s
          left join webanalytics.ds_bq_sessions_elements se
                    on se.user_uuid = s.sso_user_id
                        and date_day <= date(se.date)
                        and device_devicecategory <> 'mobile'
                        and date(date) between '2022-07-26' and '2022-08-24')

union all

(with sigs as (
    select created_at_utc :: date                      as date_day,
           si.sso_user_id,
           case when su.sso_user_id notnull then 1 end as paid_sub

    from elements.rpt_elements_user_signup_session si
             left join webanalytics.ds_bq_sessions_elements se on si.sessionid = se.sessionid
             left join elements.dim_elements_subscription su
                       on si.sso_user_id = su.sso_user_id and has_successful_payment = true
    where created_at_utc :: date between '2022-07-26' and '2022-08-24'
      and date(date) between '2022-07-26' and '2022-08-24'
      and device_devicecategory = 'desktop'
    group by 1, 2, 3
)
 select 'desktop'                                                   as platform,
        count(distinct sso_user_id)                                 as signups,
        count(distinct case when paid_sub = 1 then sso_user_id end) as paid_subs,
        count(distinct user_uuid)                                   as also_active_on_mobile_and_desktop,
        count(distinct case when paid_sub = 1 then user_uuid end)   as also_active_on_mobile_and_desktop_paid_sub

 from sigs s
          left join webanalytics.ds_bq_sessions_elements se
                    on se.user_uuid = s.sso_user_id
                        and date_day <= date(se.date)
                        and device_devicecategory <> 'desktop'
                        and date(date) between '2022-07-26' and '2022-08-24');



select *
from elements.fact_elements_active_subscriptions
limit 100;



)
SELECT humane_id,

       e.sso_user_id,
       e.dim_users_key,
       e.username,
       su.current_plan,

FROM elements_teams et
         join elements_teams_with_billing_contacts e on et.humane_id = e.humane_id
         INNER JOIN elements.dim_elements_subscription su
                    ON e.billing_contact_sso_uuid = su.sso_user_id
                           AND su.current_plan LIKE '%_enterprise%' or
                       su.current_plan LIKE '%team%' -- only interested in downloads during an Enterprise subscription

GROUP BY e.sso_user_id, e.dim_users_key, e.username, su.current_plan;


select *

from elements.dim_elements_subscription
where dim_elements_subscription.current_plan like '%team%'
  and subscription_start_date :: date between '2022-08-23' and '2022-08-29';


select distinct sso_user_id,
                full_name,
                split_part(full_name, ' ', 1)
from elements.dim_elements_subscription s

         join dim_users u on s.sso_user_id = sso_uuid
where termination_date notnull
  and has_successful_payment = true
limit 100;


select date_day,
       merchantaccount,
--        billingcountry,
       transactionstatus,
       processorresponsetext,
       sum(ct) as ct
from (
         select date(createddatetime)                                                       as date_day,
                merchantaccount,
--        billingcountry,
--        transactionstatus,

--        issuingbank,
                orderid,
                lower(split_part(max(createddatetime || '|' || transactionstatus), '|', 2)) as transactionstatus,
--        split_part(max(createddatetime || '|' || issuingbank), '|', 2) as issuingbank,
                lower(split_part(max(createddatetime || '|' || processorresponsetext), '|',
                                 2))                                                        as processorresponsetext,
                count(*)                                                                    as ct

         from braintree.view_braintree_transactions_bank
         where billingcountry in ('India') --, 'Indonesia', 'Australia')
           and createddatetime > '2021-01-01'
           and amountauthorized != 1.0000
--     and merchantaccount = 'Envato_elements'
         group by 1, 2, 3)
group by 1, 2, 3, 4
;

select date(createddatetime) as date_day,
       merchantaccount,
       transactionstatus     as transactionstatus,
       processorresponsetext as processorresponsetext,
       count(*)              as ct

from braintree.view_braintree_transactions_bank
where billingcountry in ('India') --, 'Indonesia', 'Australia')
  and createddatetime > '2021-01-01'
group by 1, 2, 3, 4


select *
from zendesk.ds_zendesk_tickets
where created_at :: date = '2022-09-01'

limit 100

with a as (
    select createddatetime :: date                                              as date_day,
           orderid,

           split_part(max(createddatetime || '|' || transactionstatus), '|', 2) as transactionstatus,
           split_part(max(createddatetime || '|' || issuingbank), '|', 2)       as issuingbank,
           count(*)                                                                co

    from braintree.view_braintree_transactions_bank
    where createddatetime :: date > '2022-01-01'
      and billingcountry in ('India')
    group by 1, 2)
select date_day,
       transactionstatus,
       issuingbank,
       count(*) as ct,
       sum(co)  as recs

from a
group by 1, 2, 3;



select *
from braintree.limit 100

with a as (
    select 1 as ab
    union all
    select 2
)
select max_by()


select date(createddatetime) as date_day,
       billingcountry,
       transactionstatus,
       processorresponsetext,
       issuingbank,
       count(*)              as ct

from braintree.view_braintree_transactions_bank
where billingcountry in ('India')
  and merchantaccount = 'Envato_elements'
  and createddatetime > '2021-01-01'
group by 1, 2, 3, 4, 5;


with a as (
    select orderid,
           transactionid,
           createddatetime,
           transactionstatus,
           issuingbank
    from braintree.view_braintree_transactions_bank
    where createddatetime :: date = '2022-09-01'
      and billingcountry in ('India')
),
     b as (
         select orderid,
                split_part(min(createddatetime || '|' || transactionstatus), '|', 2) as transactionstatus,
                split_part(max(createddatetime || '|' || issuingbank), '|', 2)       as issuingbank
         from a
         group by 1)
select 'raw'    as t,
       transactionstatus,
       count(*) as ct
from a
group by 1, 2

union all

select 'agg',
       transactionstatus,
       count(*)

from b
group by 1, 2;

select
-- merchantaccount, count(*) as cr
*
from braintree.view_braintree_transactions_bank
where createddatetime :: date = '2022-09-01'
  and billingcountry in ('India')
  and merchantaccount = 'Envato_elements'
  and amountauthorized != 1.0000
-- group by 1
order by orderid, createddatetime asc

select
from webanalytics.ds_bq_events_elements

select
from elements.rpt_elements_user_signup_session;


select date(date)                as date_day,
       hits_eventinfo_eventcategory,
       hits_eventinfo_eventaction,
       hits_eventinfo_eventlabel,
       hits_eventinfo_eventvalue,
       count(distinct user_uuid) as ct
from webanalytics.ds_bq_events_elements
where hits_eventinfo_eventaction = 'Sign Up Success'
  -- hits_eventinfo_eventcategory in ('Google Auth', 'Facebook Auth', 'Sign In With Apple')
  and date > 20220720
group by 1, 2, 3, 4, 5
union all
(
    select date(created_at) as date_day,
           'users_created_all',
           null,
           null,
           null,
           count(distinct sso_user_id)
    from elements.ds_elements_users
    where date(created_at) > '2022-07-20'
    group by 1, 2, 3, 4, 5);

select *
from elements.ds_elements_users u
         left join elements.rpt_elements_user_signup_session s on u.id = s.user_id
where created_at :: date = '2022-09-01'
limit 400;


select termination_date :: date as date_day,
       plan_type,
       has_successful_payment,
       count(*)                 as term

from elements.dim_elements_subscription
where date_trunc('month', termination_date) > '2022-11-01'
group by 1, 2, 3

select *
from elements.rpt_elements_funnel_attribution
limit 100;


with refunds as
         (select fact.dim_subscription_key
          from elements.fact_elements_subscription_transactions fact
                   inner join elements.dim_elements_transaction_attributes ta
                              on fact.dim_elements_transaction_key = ta.dim_elements_transaction_key
                   inner join elements.dim_elements_subscription sub
                              on fact.dim_subscription_key = sub.dim_subscription_key
                                  and fact.dim_date_key >= 20220220
          where ta.transaction_type = 'Refund'
          group by 1),
     failed_payments as
         (select dim_subscription_key,
                 split_part(max(i.created_at || '|' || payment_method), '|', 2) as failed_payment_method
          from elements.dim_elements_subscription s
                   left JOIN elements.ds_elements_recurly_invoices i ON i.account_code = s.recurly_account_code
                   left JOIN elements.ds_elements_recurly_line_items l
                             ON l.invoice_number = i.invoice_number AND
                                l.subscription_id = s.recurly_subscription_id
-- get latest payment attempt for this invoice
                   LEFT JOIN (SELECT invoice_number,
                                     payment_method,
                                     created_at,
                                     ROW_NUMBER() OVER (PARTITION BY invoice_number ORDER BY created_at DESC) AS latest_transaction
                              FROM elements.ds_elements_recurly_transactions
                              WHERE transaction_type = 'purchase'
                                AND transaction_status = 'declined') t
                             ON t.invoice_number = i.invoice_number
                                 AND t.latest_transaction = 1
          where s.termination_date IS NOT NULL -- terminated subscriptions
            AND NVL(s.churned_date, s.termination_date) <>
                s.termination_date             -- only include failed payments
            AND s.plan_change IS FALSE         -- ignore plan changes
            AND i.status = 'failed'            -- only included failed payments
            and subscription_start_date >= '2022-02-20'
          group by 1),

     payment_method as (
         select s.dim_subscription_key,
                payment_method

         from elements.fact_elements_subscription_transactions t
                  join dim_time d on t.dim_time_key = d.dim_time_key
                  join elements.dim_elements_subscription s on t.dim_subscription_key = s.dim_subscription_key
             and first_successful_non_trivial_payment_date_aet =
                 date(dim_date_key) || ' ' || to_char(time_id, 'HH24:MI:SS')
                  join elements.dim_elements_transaction_attributes ta
                       on t.dim_elements_transaction_key = ta.dim_elements_transaction_key
         where subscription_start_date >= '2022-02-20'
     ),

     prev as (
         select sso_user_id,
                case
                    when s.current_plan like '%enterprise%'
                        then 'enterprise'
                    when s.current_plan like '%team%' then 'team'
                    when s.current_plan like '%student_annual%'
                        then 'student_annual'
                    when s.current_plan like '%student_monthly%'
                        then 'student_monthly'
                    else s.plan_type end                                                         as plan_type,
                case
                    when sub_channel = 'Mixkit'
                        then 'mixkit'
                    when sub_channel like 'Market%'
                        then 'market'
                    when channel in
                         ('Tuts',
                          'Direct',
                          'Email',
                          'Referral',
                          'Social'
                             ) then lower(channel)
                    when channel = 'Affiliates' then 'affiliate'
                    when channel = 'Organic Search' then 'organic'
                    when channel in
                         (
                          'Paid Display',
                          'Paid Search',
                          'Paid Video',
                          'External Paid Search') then 'cpc'
                    when channel in ('Internal Promotion',
                                     'Sponsored Content') then 'promos'
                    else 'other'
                    end                                                                          as traffic_source,
                country_group,
                case
                    when country in
                         (
                          'United States',
                          'United Kingdom',
                          'Germany',
                          'Canada',
                          'France',
                          'Australia',
                          'Spain',
                          'South Korea',
                          'Italy',
                          'Netherlands',
                          'Japan',
                          'Switzerland',
                          'Austria',
                          'Sweden',
                          'Belgium',
                          'Portugal',
                          'New Zealand',
                          'Denmark',
                          'Norway',
                          'Ireland',
                          'Greece',
                          'Finland',
                          'Luxembourg',
                          'Liechtenstein')
                        then true
                    else false end                                                               as free_trial_countries,
                s.dim_subscription_key,
                has_successful_payment,
                is_first_subscription,
                subscription_started_on_trial,
                nvl(trial_period_started_at_aet + 7, subscription_start_date)                    as start_Date,
                termination_date,
                failed_payment_method                                                            as failed_payment_method,
                payment_method,
                case when r.dim_subscription_key notnull then 'refund' end                       as refund,
                case
                    when has_successful_payment = false and trial_period_started_at_aet notnull
                        and termination_date notnull
                        then nvl(datediff(day, subscription_start_date, last_canceled_at) :: varchar,
                                 '11_days_terminated')
                    when has_successful_payment = false then 'current_trial'
                    else 'paid' end                                                              as cancellation_days,
                case
                    when has_successful_payment = false and trial_period_started_at_aet notnull
                        and datediff(day, subscription_start_date, last_canceled_at) = 0
                        then datediff(hour, subscription_start_date, last_canceled_at)
                    end                                                                          as cancellation_hours,
                row_number() over (partition by sso_user_id order by s.dim_subscription_key asc) as rn

         from elements.dim_elements_subscription s
                  left join dim_geo_network dgn on s.dim_geo_network_key = dgn.dim_geo_network_key
                  left join elements.dim_elements_channel dc on s.dim_elements_channel_key = dc.dim_elements_channel_key
                  left join refunds r on s.dim_subscription_key = r.dim_subscription_key
                  left join failed_payments fp on fp.dim_subscription_key = s.dim_subscription_key
                  left join payment_method pm on pm.dim_subscription_key = s.dim_subscription_key

         where plan_change = false
     ),


     elements_coupons_prep as (
         SELECT a.dim_subscription_key,
                a.dim_elements_coupon_key,
                case
                    when b.discount_percent = 100 then 'full free'
                    when lower(b.name) like '%free%' then 'full free'
                    when b.dim_elements_coupon_key = 0 then 'no coupon'
                    else 'partial free'
                    end || '|' ||
                case
                    when b.coupon_code like '%1first%'
                        then '$1 coupon' --all the 1 dollar coupons have this string "1first" in the coupon_code
                    when b.coupon_code like '%9first%'
                        then '$9 coupon' --all the 1 dollar coupons have this string "9first" in the coupon_code
                    else 'other coupon'
                    end                                                                           as coupon_type,
                row_number() over (partition by a.dim_subscription_key order by dim_date_key asc) as invoice_number
         FROM elements.fact_elements_subscription_transactions a
                  join elements.dim_elements_coupon b on (a.dim_elements_coupon_key = b.dim_elements_coupon_key)
         WHERE a.dim_elements_coupon_key > 0
     )
        ,
     p as
         (select po.sso_user_id,
                 po.dim_subscription_key,
                 po.country_group,
                 po.traffic_source,
                 po.free_trial_countries,
                 po.plan_type,
                 nvl(e.coupon_type, 'no_coupon') as coupon_type,
                 po.subscription_started_on_trial,
                 po.start_Date,
                 po.termination_date,
                 po.has_successful_payment,
                 po.is_first_subscription,
                 pt.start_Date                      prev_start,
                 pt.dim_subscription_key         as prev_skey,
                 pt.has_successful_payment       as prev_paid,
                 po.cancellation_days,
                 po.cancellation_hours,
                 po.refund,
                 po.failed_payment_method,
                 po.payment_method

          from prev po
                   left join prev pt on po.sso_user_id = pt.sso_user_id and po.rn = pt.rn + 1
                   left join elements_coupons_prep e
                             on po.dim_subscription_key = e.dim_subscription_key and invoice_number = 1
         )

select termination_date :: date                                  as date_week,
       country_group,
       traffic_source,
       free_trial_countries,
       plan_type,
       cancellation_days,
       cancellation_hours,
       refund,
       failed_payment_method,
       payment_method,
       case
           when has_successful_payment = true and prev_paid = false then 'converted_from_cancelled_trial'
           when has_successful_payment = true and subscription_started_on_trial = true
               then 'converted_during_free_trial'
           when has_successful_payment = true and is_first_subscription = true then 'paid first sub'
           when has_successful_payment = true and is_first_subscription = false then 'paid returning sub'
           when has_successful_payment = false and termination_date isnull then 'current_trial'
           when has_successful_payment = false then 'unpaid' end as payment_made,
       coupon_type,
       count(distinct sso_user_id)                               as subs

from p
where termination_date :: date >= '2022-11-01'
group by 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12;


select current_plan, *
from elements.fact_subscription_team_members t
         left join elements.dim_elements_subscription s on s.dim_subscription_key = t.dim_subscription_key
where t.dim_subscription_key = '2916829'
limit 100;



select date_trunc('week', first_pause_period_started_at_aet) as date_week,
       case
           when first_pause_period_started_at_aet < getdate() - 150 then 'clean_data'
           else 'can still be pending' end                   as timeframe,
       count(*)                                              as paused_subs,
       count(case
                 when termination_date isnull or first_pause_period_resumed_at_aet + 31 > termination_date
                     then 1 end)                             as resumed_and_paid_for_at_least_one_more_month

from elements.dim_elements_subscription
where first_pause_period_started_at_aet notnull
group by 1, 2;


select first_pause_period_resumed_at_aet + 31,
       termination_date,
       *

from elements.dim_elements_subscription
where first_pause_period_started_at_aet notnull
  and first_pause_period_resumed_at_aet > last_canceled_at
limit 200;

with a as (
    select sso_user_id
    from elements.dim_elements_subscription
    where termination_date between '2022-12-25' and '2022-12-28'
      and has_successful_payment = true
      and plan_change = false
      and current_plan = 'cybermonday144_annual'
    group by 1),
     b as (
         select fullvisitorid
         from webanalytics.ds_bq_events_elements
         where user_uuid notnull
           and date between 20221125 and 20221210
         group by 1
         having count(distinct user_uuid)
                    > 1
     ),
     c as (
         select fullvisitorid,
                user_uuid
         from webanalytics.ds_bq_events_elements e
                  join b using (fullvisitorid)
         where date between 20221125 and 20221210
           and user_uuid notnull
         group by 1, 2
     ),
     d as (
         select fullvisitorid,
                user_uuid,
                s.sso_user_id,
                case
                    when subscription_start_date >= '2022-11-25' then current_plan
                    when s.sso_user_id notnull then 'continuing_sub'
                    else 'non sub account' end                                 as plan,
                case when a.sso_user_id notnull then 'churning_old_CM_sub' end as old_cm

         from c
                  left join a on a.sso_user_id = c.user_uuid
                  left join elements.dim_elements_subscription s
                            on c.user_uuid = s.sso_user_id and has_successful_payment = true
         group by 1, 2, 3, 4, 5
     )
        ,
     e as (
         select d.fullvisitorid,
                d.user_uuid,
                d.sso_user_id,
                d.plan,
                d.old_cm
         from d
                  join d da on d.fullvisitorid = da.fullvisitorid and da.old_cm notnull
         where d.sso_user_id notnull
         order by 1, 2
     )

select fullvisitorid,
       max(case when old_cm = 'churning_old_CM_sub' then 1 end) as acrit

from e
group by 1
having min(sso_user_id) != max(sso_user_id)


;

with a as (
select
user_uuid,
       category
from webanalytics.rpt_google_search_terms_elements se
join webanalytics.ds_bq_sessions_elements s on se.fullvisitorid = s.fullvisitorid :: varchar and se.visitid = s.visitid :: varchar
where date_trunc('month', se.date_time_aest) = '2022-11-01'
  and category notnull and category not in ('all-items', '') and category !~ '[A-Z0-9]'
and s.date between 20221101 and 20221201
group by 1,2
    order by 1,2),
b as (
    select sso_user_id,
           channel,
           max(case
                   when subscription_start_date > '2022-10-01' then '1month_Old'
                   when subscription_start_date > '2022-5-01' then '6month_Old'
                   else '1year_plus' end) as user_age,
           listagg(category, ',') WITHIN GROUP (ORDER BY category) as categories_searched

    from a
             join elements.dim_elements_subscription s on a.user_uuid = s.sso_user_id
             left join elements.dim_elements_channel c on s.dim_elements_channel_key = c.dim_elements_channel_key
    where has_successful_payment = true
      and termination_date isnull
    group by 1, 2
)
select
categories_searched, user_age, channel, count(*) as users_searching
from b
group by 1,2,3
having count(*) > 1
