--feb 2021 experiment for free trials


--get all enrolled users
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
--find all users enrolled in both experiments (by SSO)
     uq as
         (select user_uuid
          from enrollments b
                   join webanalytics.ds_bq_events_elements c on c.fullvisitorid::varchar = b.fullvisitorid
              and c.visitid = b.visitid
              and c.date between 20210208 and 20210304
          group by 1
          having min(variant) = max(variant)),
--remove all users exposed to more than 1 experiment
     allocations as (select user_uuid,
                            variant,
                            country
                     from webanalytics.ds_bq_events_elements c
                              join uq using (user_uuid)
                              join enrollments b on c.fullvisitorid::varchar = b.fullvisitorid
                         and c.visitid = b.visitid
                         and c.date between 20210208 and 20210304
                     group by 1, 2, 3),
--get all remaining users and determine if they had a free trial, signed up for the first time in the experiment or are returining
     trials as
         (select user_uuid,
                 country,
                 variant,
                 max(case
                         when trial_period_started_at_aet :: date between '2021-02-08' and '2021-03-04'
                             then 'trial' end) as trialU,
                                  max(case
                         when subscription_start_date :: date between '2021-02-08' and '2021-03-04'
                             and is_first_subscription = true
                             then 'new_non_trial' end) as newU

          from allocations a
                   join elements.dim_elements_subscription s on a.user_uuid = s.sso_user_id
          group by 1, 2, 3),
-- get item downloads per sso id (not relevant)
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
--get a list of dates for the cohort calculation
     dates as (
         select date_trunc('week', date_dl) as date_cal
         from content_t
         where date_dl notnull
         group by 1
     ),
--get country, sub start and finish date and other information for all users subscribed and in the experiment
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
                    else 'ROW' end                                                     as country,

                sso_user_id || '|' || dim_subscription_key || '|' || case
                                                                         when current_plan like '%enterprise%'
                                                                             then 'enterprise'
                                                                         when current_plan like '%team%' then 'team'
                                                                         when current_plan like '%student_annual%'
                                                                             then 'student_annual'
                                                                         when current_plan like '%student_monthly%'
                                                                             then 'student_monthly'
                                                                         else plan_type end  as sso_key,
                variant                        as Variant,
                nvl(trialu, newU, 'returning') as returns,


                case when date_trunc('week', min(subscription_start_date :: date)) <  '2021-02-08' then  '2021-02-08'
                    else date_trunc('week', min(subscription_start_date :: date)) end as subdate,
                date_trunc('week', max(termination_date :: date))                      as ter_date


         from elements.dim_elements_subscription s
                  join trials a on a.user_uuid = s.sso_user_id

         group by 1, 2, 3, 4
     ) ,
--(not used) count subscribed users assigned per variant and how many new subs reactiviated after a free trial (or new sub)
     resubs as
         (
             select split_part(sso_key, '|', 1) as uid,
                    max(subdate) as submax
             from subs
             where returns in ('new_non_trial', 'trial')
              group by 1 having count(*) > 1)

--calculate retention cohorts per user start date / sso + dim sub key / plan type
select subdate :: date || '|' || split_part(sso_key, '|', 3) as cohort,
       variant,
       returns as returning_hroup,
       country,

       date_cal :: date                                                                            as day_date,
       datediff(week, subdate:: date, date_cal :: date)                                            as weeks_since_sub,
       count(distinct case
                          when date_cal >= subdate and (ter_date >= date_cal or ter_date isnull)
                              then
--                               sso_key
                              split_part(sso_key, '|', 1)
           end)                                                                                    as remaining_users,

       count(distinct case
                          when date_cal >= subdate and (ter_date >= date_cal or ter_date isnull)
                              then
                              r.uid
           end)                                                                                    as resubbed_new_users

--        ,count(distinct case
--                           when date_cal >= subdate and (ter_date >= date_cal or ter_date isnull) and c.sso_uuid notnull
--                               then
-- --                               sso_key
--                               split_part(sso_key, '|', 1)
--            end)                                                                                    as downloading_users
from subs s


         cross join dates da
         left join resubs r on split_part(sso_key, '|', 1) = uid and submax > subdate
--          left join content_t c on sso_uuid = split_part(sso_key, '|', 1) and date_dl = date_cal
where date_cal >= subdate

group by 1, 2, 3, 4, 5, 6
;

--same query but for experiment in October 2021

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
                              se.date between 20211025 and 20211118
                      left join elements.rpt_elements_session_channel sc on se.sessionid = sc.sessionid


             where experiment_id = 'geInrbFNTa2AZdiRswkP2A'
               and a.date between '2021-10-25' and '2021-11-18'
             group by 1, 2, 3, 4
         ),
     uq as
         (select user_uuid
          from enrollments b
                   join webanalytics.ds_bq_events_elements c on c.fullvisitorid::varchar = b.fullvisitorid
              and c.visitid = b.visitid
              and c.date between 20211025 and 20211118
          group by 1
          having min(variant) = max(variant)),

     allocations as (select user_uuid,
                            variant,
                            country
                     from webanalytics.ds_bq_events_elements c
                              join uq using (user_uuid)
                              join enrollments b on c.fullvisitorid::varchar = b.fullvisitorid
                         and c.visitid = b.visitid
                         and c.date between 20211025 and 20211118
                     group by 1, 2, 3),
     trials as
         (select user_uuid,
                 a.country,
                 variant,
                 max(case
                         when trial_period_started_at_aet :: date between '2021-10-25' and '2021-11-18'
                             then 'trial' end) as trialU,
                                  max(case
                         when s.subscription_start_date :: date between '2021-10-25' and '2021-11-18'
                             and is_first_subscription = true
                             then 'new_non_trial' end) as newU


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
                    WHEN a.country in ('United States', 'United Kingdom', 'Germany',
                                     'Canada', 'Australia', 'France', 'Italy', 'Spain',
                                     'Netherlands', 'Brazil',
                                     'India', 'South Korea', 'Turkey', 'Switzerland',
                                     'Japan', 'Spain')
                        then a.country
                    when a.country in ('Argentina', 'Bolivia', 'Chile',
                                     'Colombia', 'Costa Rica', 'Cuba', 'Ecuador', 'Mexico',
                                     'Paraguay', 'Uruguay',
                                     'Venezuela') then 'LATAM'
                    when a.country in ('Belarus', 'Kazakhstan', 'Russia', 'Ukraine')
                        then 'RU'
                    else 'ROW' end                                                     as country,

                s.sso_user_id || '|' || s.dim_subscription_key || '|' || case
                                                                         when current_plan like '%enterprise%'
                                                                             then 'enterprise'
                                                                         when current_plan like '%team%' then 'team'
                                                                         when current_plan like '%student_annual%'
                                                                             then 'student_annual'
                                                                         when current_plan like '%student_monthly%'
                                                                             then 'student_monthly'
                                                                         else s.plan_type end  as sso_key,
                variant                        as Variant,
                nvl(trialu, newU, 'returning') as returns,


                case when date_trunc('week', min(s.subscription_start_date :: date)) <  '2021-10-25' then  '2021-10-25'
                    else date_trunc('week', min(s.subscription_start_date :: date)) end as subdate,
                date_trunc('week', max(termination_date :: date))                      as ter_date,
                                 max(ltv) as ltv


         from elements.dim_elements_subscription s
                  join trials a on a.user_uuid = s.sso_user_id
            left join analysts.view_elements_paying_subscriber_ltv_user_level l on l.sso_user_id = s.sso_user_id
         group by 1, 2, 3, 4
     ) ,
     resubs as
         (
             select split_part(sso_key, '|', 1) as uid,
                    max(subdate) as submax
             from subs
             where returns in ('new_non_trial', 'trial')
              group by 1 having count(*) > 1)

                select subdate :: date || '|' || split_part(sso_key, '|', 3) as cohort,
                       variant,
                       returns                                               as returning_group,
                       country,

                       date_cal :: date                                      as day_date,
                       datediff(week, subdate:: date, date_cal :: date)      as weeks_since_sub,
                       count(distinct case
                                          when date_cal >= subdate and (ter_date >= date_cal or ter_date isnull)
                                              then
--                               sso_key
                                              split_part(sso_key, '|', 1)
                           end)                                              as remaining_users,

                       count(distinct case
                                          when date_cal >= subdate and (ter_date >= date_cal or ter_date isnull)
                                              then
                                              r.uid
                           end)                                              as resubbed_new_users,
                       max(ltv) as ltv

--        ,count(distinct case
--                           when date_cal >= subdate and (ter_date >= date_cal or ter_date isnull) and c.sso_uuid notnull
--                               then
-- --                               sso_key
--                               split_part(sso_key, '|', 1)
--            end)                                                                                    as downloading_users
                from subs s


                         cross join dates da
                         left join resubs r on split_part(sso_key, '|', 1) = uid and submax > subdate
--          left join content_t c on sso_uuid = split_part(sso_key, '|', 1) and date_dl = date_cal
                where date_cal >= subdate

                group by 1, 2, 3, 4, 5, 6
;
