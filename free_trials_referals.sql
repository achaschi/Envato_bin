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
                    split_part(min(sc.dss_update_time || '|' || channel), '|', 2) as referer,
                    variant
             from ds_bq_abtesting_enrolments_elements a
                      join webanalytics.ds_bq_sessions_elements se
                           on a.fullvisitorid = se.fullvisitorid::varchar and a.visitid = se.visitid and
                              se.date between 20211027 and 20211118
                      left join elements.rpt_elements_session_channel sc on se.sessionid = sc.sessionid
             where experiment_id = 'geInrbFNTa2AZdiRswkP2A'
               and a.date between '2021-10-27' and '2021-11-18'
             group by 1, 2, 3, 5
         ),
     merge_t as
         (with other_users as
                   (select b.fullvisitorid,

                           max(country)                                                     as country,
                           variant,
                           referer,
                           max(case when hits_page_pagepath like '%/subscribe%' then 1 end) as sub_page,
                           max(case when hits_page_pagepath like '%/pricing%' then 1 end)   as pricing_page

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
                 count(*)            as users,
                 count(sub_page)     as sub_page,
                 count(pricing_page) as pricing_page

          from other_users
          group by 1, 2, 3
         ),

     content_t
         as (select u.sso_uuid,
--                           i.content_type,
                    case when user_project_id isnull then 'download' else 'project' end as license,
                    count(i.item_id)                                                    as q
             from elements.ds_elements_item_downloads dl
                      join elements.ds_elements_item_licenses l on dl.item_license_id = l.id
                      join elements.dim_elements_items i on i.item_id = l.item_id
                      join dim_users u on dl.user_id = u.elements_id
             where download_started_at :: date between '2021-10-27' and '2021-11-25'
             group by 1, 2),

     failed_payments
         as (SELECT s.sso_user_id --|| '|' || dim_subscription_key
                        as sso_key

--                  t.payment_method

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
               AND subscription_start_date :: date between '2021-10-27' and '2021-11-18'
             group by 1),

     payments as
         (select s.sso_user_id --|| '|' || s.dim_subscription_key
                                        as sso_key,
--                  ta.payment_method,
                 sum(t.total_amount)    as total_amount,
                 sum(t.tax_amount)      as tax_amount,
                 sum(t.discount_amount) as discount_amount
          from elements.fact_elements_subscription_transactions t
                   join elements.dim_elements_subscription s on t.dim_subscription_key = s.dim_subscription_key
              AND subscription_start_date :: date between '2021-10-27' and '2021-11-18' and
                                                                s.current_plan not like '%enterprise%'
                   join elements.dim_elements_transaction_attributes ta
                        on t.dim_elements_transaction_key = ta.dim_elements_transaction_key
          group by 1),

     uq as
         (select user_uuid
          from enrollments b
                   join webanalytics.ds_bq_events_elements c on c.fullvisitorid::varchar = b.fullvisitorid
              and c.visitid = b.visitid
              and c.date between 20211027 and 20211118
          group by 1
          having min(variant) = max(variant)),

-- explain
     clean_sso as (
         select user_uuid --|| '|' || dim_subscription_key
                                                                                 as sso_key,
                max(country)                                                     as country,
                variant,
                referer,

                max(case
                        when d.current_plan like '%enterprise%' then 'enterprise'
                        when d.current_plan like '%team%' then 'team'
                        when d.current_plan like '%student_annual%' then 'student_annual'
                        when d.current_plan like '%student_monthly%' then 'student_monthly'
                        else d.plan_type end)                                    as plan_type,
--                 max(split_part(q || '|' || dl.content_type, '|', 2))             as content_type,
                max(case
                        when d.trial_period_started_at_aet :: date between '2021-10-27' and '2021-11-18'
                            then 1 end)                                          as free_trial_status,
                max(case when hits_page_pagepath like '%/subscribe%' then 1 end) as sub_page,
                max(case when hits_page_pagepath like '%/pricing%' then 1 end)   as pricing_page,
                max(case
                        when d.subscription_start_date :: date between '2021-10-27' and '2021-11-18' and
                             d.trial_period_started_at_aet isnull
                            then 1 end)                                          as non_free_trial_signups,
                max(case
                        when d.subscription_start_date :: date between '2021-10-27' and '2021-11-18' and
                             d.trial_period_started_at_aet is not null
                            then 1 end)                                          as free_trial_signups,
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
                        when d.subscription_start_date :: date between '2021-10-27' and '2021-11-18' and
                             termination_date :: date >= '2021-10-27'
                            then 1 end)                                          as total_new_subs_terminated,
                max(case
                        when d.subscription_start_date :: date < '2021-10-27' and termination_date >= '2021-10-27'
                            then 1 end)                                          as total_returning_subs_terminated,
                max(case
                        when d.subscription_start_date :: date < '2021-10-27' and termination_date isnull
                            then 1 end)                                          as returning_subs_retained,

--                 max(case
--                         when d.subscription_start_date :: date between '2021-10-27' and '2021-11-18' and
--                              rf.sso_user_id is not null then 1 end)              as refunds,
                max(case
                        when d.subscription_start_date :: date between '2021-10-27' and '2021-11-18' and
                             first_canceled_at isnull and
                             has_successful_payment = false
                            and f.sso_key is not null then 1 end)                as failed_payments,

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
                            and f.sso_key isnull then 1 end)                     as minus_failed_payments,
--                 max(case
--                         when d.subscription_start_date :: date between '2021-10-27' and '2021-11-18' and
--                              first_canceled_at isnull
--                             and has_successful_payment = true
--                             and f.sso_key isnull
--                             and rf.sso_user_id isnull then 1 end)                as minus_refunds,


                max(case when license = 'project' and q > 0 then q end)          as project_downloads,
                max(case when license = 'download' and q > 0 then q end)         as downloads,
                max(total_amount)                                                as revenue,
                max(discount_amount)                                             as discounts,
                max(case
                        when d.trial_period_started_at_aet :: date between '2021-10-27' and '2021-11-18'
                            and has_successful_payment = false
                            and first_canceled_at notnull then 1 end)            as t_sub_cancel,
                max(case
                        when d.trial_period_started_at_aet :: date between '2021-10-27' and '2021-11-18'
                            and first_canceled_at notnull
                            and has_successful_payment = false
                            and f.sso_key is not null
                            then 1 end)                                          as t_sub_fail,
                max(case
                        when d.subscription_start_date :: date between '2021-10-27' and '2021-11-18' and
                             rf.SSO_USER_ID isnull then 1 end)                   as refunds


         from webanalytics.ds_bq_events_elements c
                  join uq using (user_uuid)
                  join enrollments b on c.fullvisitorid::varchar = b.fullvisitorid
             and c.visitid = b.visitid
             and c.date between 20211027 and 20211118

             --              --remove envato users
--                   join elements.ds_elements_sso_users sso
--                        on sso.id = c.user_uuid and split_part(email, '@', 2) != 'envato.com'

                  left join elements.dim_elements_subscription d
                            on c.user_uuid = d.sso_user_id -- || '|' || dim_subscription_key


                  left join content_t dl on c.user_uuid = dl.sso_uuid
             --
--
                  left join analysts.view_free_trial_refunds rf
                            on c.user_uuid = rf.sso_user_id

                  left join failed_payments f on c.user_uuid = f.sso_key -- || '|' || dim_subscription_key

                  left join payments p on c.user_uuid = p.sso_key -- || '|' || dim_subscription_key
         group by 1, 3, 4)

select variant || '|oct' :: varchar                                    as variant,
       country,
       referer,
       plan_type,
       case
           when t_sub_cancel = 1 then 'FT|Cancelled'
           when (free_trial_status) = 1 then 'FT|paid'
           else 'regular' end                                          as free_trials,
       count(*)                                                        as users,
--        count(distinct split_part(sso_key, '|', 1))                     as users,
       count(sub_page)                                                 as sub_page,
       count(pricing_page)                                             as pricing_page,
       count(non_free_trial_signups)                                   as non_free_trial_signups,
       count(total_new_signups)                                        as total_new_signups,
       count(trial_sub_remaining)                                      as trial_sub_remaining,
       count(total_subs_remaining)                                     as total_subs_remaining,
       count(total_new_subs_terminated)                                as total_new_subs_terminated,
       count(total_returning_subs_terminated)                          as total_returning_subs_terminated,
       count(returning_subs_retained)                                  as returning_subs_retained,
--        count(refunds)                                                  as refunds,
       count(failed_payments)                                          as failed_payments,
       count(minus_cancellation)                                       as minus_cancellation,
       count(minus_failed_payments)                                    as minus_failed_payments,
--        count(minus_refunds)                                            as minus_refunds,
       sum(downloads)                                                  as downloads,
       sum(project_downloads)                                          as project_downloads,
       sum(revenue)                                                    as revenue_usd,
       sum(discounts)                                                  as discounts,
       sum(case when free_trial_status = 1 then downloads end)         as FT_dl,
       sum(case when free_trial_status = 1 then project_downloads end) as FT_licensed_dl,
       sum(t_sub_cancel)                                               as cancelled_trial,
       sum(t_sub_fail)                                                 as failpayment_trial,
       sum(cancellation)                                               as cancellations,
       sum(refunds)                                                    as refunds
from clean_sso
group by 1, 2, 3, 4, 5

union all

(select variant || '|oct' :: varchar,
        country :: varchar,
        referer,
        null as plan_type,
        null as free_trials,
        users :: bigint,
        sub_page :: bigint,
        pricing_page :: bigint,
        null as non_free_trial_signups,
        null as total_new_signups,
        null as trial_sub_remaining,
        null as total_subs_remaining,
        null as total_new_subs_terminated,
        null as total_returning_subs_terminated,
        null as returning_subs_retained,
        null as failed_payments,
        null as minus_cancellation,
        null as minus_failed_payments,
        null as downloads,
        null as project_downloads,
        null as revenue_usd,
        null as discounts,
        null as FT_dl,
        null as FT_licensed_dl,
        null as cancelled_trial,
        null as failpayment_trial,
        null as cancellations,
        null as refunds
 from merge_t)
;
