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
                      and ev.date >= 20220220=
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
                    split_part(min(sc.dss_update_time || '|' || channel), '|', 2) as referer,
                    max(case when s.sso_user_id notnull  then 1 end) as converted
             from pric ev
                      join webanalytics.ds_bq_sessions_elements se
                           on ev.fullvisitorid::varchar = se.fullvisitorid::varchar
                               and ev.visitid::varchar = se.visitid::varchar
                      left join elements.rpt_elements_session_channel sc on sc.sessionid = se.sessionid
             left join elements.dim_elements_subscription s on se.user_uuid = s.sso_user_id
                        and subscription_start_date :: date >='2022-02-21'
                        and has_successful_payment = true
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
                                count(distinct case
                                   when subscribe = 1 and converted = 1
                                       then p.fullvisitorid end) as paid_subscription,
                count(distinct p.fullvisitorid)                  as cookies
         from pric p
                  join fv on p.fullvisitorid = fv.fullvisitorid
         group by 1, 2, 3)
select *
from a UNPIVOT (
                hits FOR stage IN (pricing_page, sub_page, subscription, paid_subscription, signup, cookies)
    )
