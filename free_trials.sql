-- query for free trials experiment analysis Oct 27 - nov 18 2021
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
                              se.date between 20211027 and 20211118
                      left join elements.rpt_elements_session_channel sc on se.sessionid = sc.sessionid
             where experiment_id = 'geInrbFNTa2AZdiRswkP2A'
               and a.date between '2021-10-27' and '2021-11-18'
             group by 1, 2, 3, 4
         ),
     failed_payments as
         (SELECT s.sso_user_id,
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
            AND subscription_start_date :: date between '2021-10-27' and '2021-11-18'
          group by 1, 2)
        ,
     clean_sso as (
         select user_uuid,
                max(country)                                                     as country,
                variant,
                split_part(min(sc.dss_update_time || '|' || sc.channel), '|', 2) as referer,
                max(plan_type)                                                   as plan_type,
                max(case
                        when d.trial_period_started_at_aet :: date between '2021-10-27' and '2021-11-18'
                            then 1 end)                                          as free_trial_status,
                max(case when hits_page_pagepath like '%/subscribe%' then 1 end) as sub_page,
                max(case when hits_page_pagepath like '%/pricing%' then 1 end)   as pricing_page,
                max(case
                        when hits_eventinfo_eventcategory = 'sign-up' and hits_eventinfo_eventaction = 'submit'
                            then 1 end)                                          as signups,
                max(case when d.sso_user_id isnull then 1 end)                   as free_account_users,

                max(case
                        when d.subscription_start_date :: date between '2021-10-27' and '2021-11-18' and
                             d.trial_period_started_at_aet isnull
                            then 1 end)                                          as non_free_trial_signups,
                max(case
                        when d.subscription_start_date :: date between '2021-10-27' and '2021-11-18'
                            then 1 end)                                          as total_new_signups,
                max(case
                        when d.trial_period_started_at_aet :: date between '2021-10-27' and '2021-11-18' and
                             termination_date isnull and has_successful_payment = true
                            then 1 end)                                          as trial_sub_remaining,
                max(case
                        when d.subscription_start_date :: date between '2021-10-27' and '2021-11-18' and
                             termination_date isnull and
                             has_successful_payment = true
                            and first_canceled_at isnull
                            then 1 end)                                          as total_subs_remaining,
                max(case
                        when d.subscription_start_date :: date >= '2021-10-27' and
                             termination_date :: date >= '2021-10-27'
                            then 1 end)                                          as total_new_subs_terminated,
                max(case
                        when d.subscription_start_date :: date < '2021-10-27' and termination_date >= '2021-10-27'
                            then 1 end)                                          as total_returning_subs_terminated,
                max(case when termination_date isnull then 1 end)                as overall_subs,
                max(case
                        when d.subscription_start_date :: date < '2021-10-27' and termination_date isnull
                            then 1 end)                                          as returning_subs_retained,

                max(case
                        when d.subscription_start_date :: date between '2021-10-27' and '2021-11-18' and
                             rf.sso_user_id is not null then 1 end)              as refunds,
                max(case
                        when d.subscription_start_date :: date between '2021-10-27' and '2021-11-18' and
                             first_canceled_at isnull and
                             f.sso_user_id is not null then 1 end)               as failed_payments,

                max(case
                        when d.subscription_start_date :: date between '2021-10-27' and '2021-11-18' and
                             first_canceled_at isnull and
                             f.payment_method = 'credit_card' then 1 end)        as failed_payments_cc,
                max(case
                        when d.subscription_start_date :: date between '2021-10-27' and '2021-11-18' and
                             first_canceled_at isnull and
                             f.payment_method = 'paypal' then 1 end)             as failed_payments_paypal,

                max(case
                        when d.subscription_start_date :: date between '2021-10-27' and '2021-11-18' and
                             first_canceled_at is not null then 1 end)           as cancellation,
                max(case
                        when d.subscription_start_date :: date between '2021-10-27' and '2021-11-18' and
                             first_canceled_at isnull then 1 end)                as minus_cancellation,
                max(case
                        when d.subscription_start_date :: date between '2021-10-27' and '2021-11-18' and
                             first_canceled_at isnull
                            and has_successful_payment = true
                            and f.sso_user_id isnull then 1 end)                 as minus_failed_payments,
                max(case
                        when d.subscription_start_date :: date between '2021-10-27' and '2021-11-18' and
                             first_canceled_at isnull
                            and has_successful_payment = true
                            and f.sso_user_id isnull
                            and rf.sso_user_id isnull then 1 end)                as minus_refunds,


                sum(case when dls > 0 then dls end)                              as downloads


         from webanalytics.ds_bq_events_elements c
                  join (select user_uuid
                        from enrollments b
                                 join webanalytics.ds_bq_events_elements c on c.fullvisitorid::varchar = b.fullvisitorid
                            and c.visitid = b.visitid
                            and c.date between 20211027 and 20211118
                        group by 1
                        having min(variant) = max(variant)) using (user_uuid)
                  join enrollments b on c.fullvisitorid::varchar = b.fullvisitorid
             and c.visitid = b.visitid
             and c.date between 20211027 and 20211118

                join elements.ds_elements_sso_users sso on sso.id = c.user_uuid and split_part(email, '@', 2) != 'envato.com'

                  left join elements.dim_elements_subscription d on c.user_uuid = d.sso_user_id

                  left join (select sso_uuid, count(*) as dls
                             from ds_elements_item_downloads id
                                      join dim_users du on id.user_id = du.elements_id
                                 and id.download_started_at between '2021-10-27' and '2021-11-18'
                             group by 1) dl on c.user_uuid = dl.sso_uuid

                  left join analysts.view_free_trial_refunds rf
                            on c.user_uuid = rf.sso_user_id

                  left join elements.rpt_elements_user_signup_session ss
                            on ss.sso_user_id = c.user_uuid

                  left join elements.rpt_elements_session_channel sc on sc.sessionid = ss.sessionid

                  left join failed_payments f on d.sso_user_id = f.sso_user_id
         group by 1, 3),

     other_users as
         (select b.fullvisitorid,
                 max(country)                                                     as country,
                 variant,
                 referer                                                          as referer,
                 null                                                             as plan_type,
                 null                                                             as free_trial_status,
                 max(case when hits_page_pagepath like '%/subscribe%' then 1 end) as sub_page,
                 count(case when hits_page_pagepath like '%/pricing%' then 1 end) as pricing_page,
                 null                                                             as signups,
                 null                                                             as free_account_users,
                 null                                                             as non_free_trial_signups,
                 null                                                             as total_new_signups,
                 null                                                             as trial_sub_remaining,
                 null                                                             as total_subs_remaining,
                 null                                                             as total_subs_terminated,
                 null                                                             as total_returning_subs_terminated,
                 null                                                             as total_new_subs_terminated,
                 null                                                             as overall_subs,
                 null                                                             as refunds,
                 null                                                             as failed_payments,
                 null                                                             as failed_payments_cc,
                 null                                                             as failed_payments_paypal,
                 null                                                             as cancelations,
                 null                                                             as minus_cancellation,
                 null                                                             as minus_failed_payments,
                 null                                                             as minus_refunds,
                 null                                                             as downloaders

          from enrollments b
                   join (select fullvisitorid
                         from ds_bq_abtesting_enrolments_elements
                         where experiment_id = 'geInrbFNTa2AZdiRswkP2A'
                           and date between '2021-10-27' and '2021-11-18'
                         group by 1
                         having min(variant) = max(variant)) using (fullvisitorid)
                   join webanalytics.ds_bq_events_elements c on c.fullvisitorid::varchar = b.fullvisitorid
              and c.visitid = b.visitid
              and c.date between 20211027 and 20211118
              and c.user_uuid isnull
          group by 1, 3, 4
         )

select variant,
       country,
       referer,
       plan_type,
       count(free_trial_status)               as free_trials_started,
       count(*)                               as users,
       count(sub_page)                        as sub_page,
       count(free_account_users)              as free_account_users,
       count(pricing_page)                    as pricing_page,
       count(signups)                         as signups,
       count(non_free_trial_signups)          as non_free_trial_signups,
       count(total_new_signups)               as total_new_signups,
       count(trial_sub_remaining)             as trial_sub_remaining,
       count(total_subs_remaining)            as total_subs_remaining,
       count(total_new_subs_terminated)       as total_new_subs_terminated,
       count(total_returning_subs_terminated) as total_returning_subs_terminated,
       count(returning_subs_retained)         as returning_subs_retained,
       count(overall_subs)                    as overall_subscribers,
       count(refunds)                         as refunds,
       count(failed_payments)                 as failed_payments,
       count(failed_payments_cc)              as failed_payments_cc,
       count(failed_payments_paypal)          as failed_payments_paypal,
       count(cancellation)                    as cancelations,
       count(minus_cancellation)              as minus_cancellation,
       count(minus_failed_payments)           as minus_failed_payments,
       count(minus_refunds)                   as minus_refunds,
       count(downloads)                       as downloaders,
       sum(downloads)                         as downloads
from (select *
      from clean_sso
      union all
      select *
      from other_users) c
group by 1, 2, 3, 4
;
