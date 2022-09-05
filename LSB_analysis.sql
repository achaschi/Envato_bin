with enrollments as (
    select s.fullvisitorid :: varchar                                                       as fullvisitorid,
           split_part(max(s.date_aest || '|' || s.user_uuid), '|', 2)                       as user_uuid,
           max(dim_subscription_key)                                                        as sub_key,
           split_part(min(s.date_aest || '|' || variant), '|', 2)                           as first_var,
           count(distinct s.user_uuid)                                                      as num_accounts,
           split_part(min(s.date_aest || '|' || device_devicecategory), '|', 2)             as device_type,

           nvl(split_part(min(si.signup_date || '|' || si.trafficsource_medium), '|', 2),
               split_part(min(s.date_aest || '|' || s.trafficsource_medium), '|', 2))       as referer,
           split_part(max(case
                              when visitnumber = 1 and signup_date notnull then '2|first_cookie_and_signup'
                              when visitnumber = 1 then '1|first_cookie'
                              else '0|returning_cookie'
               end), '|', 2)                                                                as new_user,
           count(case
                     when hits_eventinfo_eventcategory = 'larger-search-bar-search-form' and
                          hits_eventinfo_eventaction = 'submit' then 1 end)                 as lsb_searches,
           count(case
                     when hits_eventinfo_eventcategory = 'header-search-form' and hits_eventinfo_eventaction = 'submit'
                         then 1 end)                                                        as other_searches,
           count(case when hits_eventinfo_eventaction = 'license with download' then 1 end) as downloads
    from webanalytics.ds_bq_sessions_elements s
             join webanalytics.ds_bq_abtesting_enrolments_elements a
                  on a.fullvisitorid :: varchar = s.fullvisitorid::varchar
                      and a.visitid ::varchar = s.visitid ::varchar
                      and s.date between 20220818 and 20220901
                      and experiment_id = 'mYHMlDfhRtqm7FjVAT3BEQ'
             left join elements.dim_elements_subscription su on s.user_uuid = su.sso_user_id
        and s.date_aest between subscription_start_date and nvl(termination_date, current_date)
             left join webanalytics.ds_bq_events_elements ev on s.sessionid = ev.sessionid
             left join elements.rpt_elements_user_signup_session si
                       on si.sessionid = s.sessionid and signup_date >= '2022-08-18'
    group by 1),
     dupes as (
         select fullvisitorid
         from webanalytics.ds_bq_abtesting_enrolments_elements a
         where date between '2022-08-18' and '2022-09-01'
           and experiment_id = 'mYHMlDfhRtqm7FjVAT3BEQ'
         group by 1
         having min(variant) = max(variant)
     )

select first_var,
       device_type                                                                           as first_device_type,
       case
           when d.fullvisitorid isnull then 'multiple_variant'
           when num_accounts > 1 then 'multiple_accounts'
           else 'one_variant' end                                                            as clean_allocation,
       new_user,
       case when lsb_searches > 0 then 'used_feature' else 'didnt_use_feature' end           as lsb,
       case
           when referer in (
               'organic'
                   'cpc'
                   'affiliate'
                   'referral'
                   'promos'
                   'email') then referer
           when referer in ('(none)', '(not set)') then 'direct'
           else 'other' end                                                                  as referer,
--        max(case when user_uuid isnull then 'not_logged' end)                                 as registered_user,
       sum(lsb_searches + other_searches)                                                    as total_searches,
       count(case when (lsb_searches + other_searches) > 0 then 1 end)                       as searching_cookies,
       sum(downloads)                                                                        as downloads,
       count(distinct case
                          when has_successful_payment = false and subscription_started_on_trial = true and
                               termination_date notnull
                              then sso_user_id end)                                          as unconverted_free_trials,
       count(distinct case
                          when has_successful_payment = true and is_first_subscription = true and
                               subscription_start_date >= '2022-08-18' then sso_user_id end) as new_subs,
       count(distinct case
                          when has_successful_payment = true and
                               subscription_start_date >= '2022-08-18' then sso_user_id end) as returning_subs,
       count(*)                                                                              as cookies


from enrollments e
         left join dupes d on e.fullvisitorid = d.fullvisitorid :: varchar
         left join elements.dim_elements_subscription su on e.sub_key = su.dim_subscription_key

group by 1, 2, 3, 4, 5, 6
;
