-- console 5

with pr as (select sso_user_id,
                   dim_subscription_key,
                   has_successful_payment,
                   case when trial_period_started_at_aet notnull then true end                    as trial,
                   row_number() over (partition by sso_user_id order by dim_subscription_key asc) as rn
            from elements.dim_elements_subscription
            where date(subscription_start_date) > '2020-01-01'
),
     tr as (select sso_user_id,
                   max(case when rn = 1 and trial = true then 1 end)                                     as start_trial,
                   max(case when rn = 2 and has_successful_payment = true then dim_subscription_key end) as trsub

            from pr
            group by 1),

     a as (select nvl(trial_period_ends_at_aet, subscription_start_date) :: date                                       as date_day,
                  is_first_subscription,
                  s.has_successful_payment,
                  plan_type,
                  case
                      when is_first_subscription = true or s.dim_subscription_key = tr.trsub then true
                      else false end                                                    as real_is_first_subscription,
                  case when start_trial = 1 then true else false end              as subscription_started_on_trial,

                  count(distinct s.sso_user_id)                                         as subs

           from elements.dim_elements_subscription s
                    left join tr on s.sso_user_id = tr.sso_user_id --and s.dim_subscription_key = tr.trsub and start_trial = 1
           where date(subscription_start_date) > '2020-01-01'

           group by 1, 2, 3, 4, 5, 6),

     b as
         (select termination_date :: date    as date_day,
                  is_first_subscription,
                  s.has_successful_payment,
                  plan_type,
                  case
                      when is_first_subscription = true or s.dim_subscription_key = tr.trsub then true
                      else false end                                                    as real_is_first_subscription,
                  case when start_trial = 1 then true else false end              as subscription_started_on_trial,
                 count(distinct s.sso_user_id) as terminations

          from elements.dim_elements_subscription s
          left join tr on s.sso_user_id = tr.sso_user_id
          where date(subscription_start_date) > '2020-01-01'

          group by 1, 2, 3, 4, 5, 6)

select *
from a
         join b
              using (date_day, is_first_subscription, has_successful_payment, plan_type, real_is_first_subscription, subscription_started_on_trial);


select sso_user_id,
       dim_subscription_key,
       subscription_start_date,
       subscription_started_on_trial,
       is_first_subscription,
       row_number() over (partition by sso_user_id order by dim_subscription_key asc)

from elements.dim_elements_subscription
where has_successful_payment = true
  and subscription_start_date >= '2022-01-01'
order by 1, 2 asc


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
                               and a.date between '2022-02-21' and '2022-04-26'
                               and se.date between 20220221 and 20220426
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
     trial as (
         select sso_user_id,
                dim_subscription_key
         from elements.dim_elements_subscription
         where is_first_subscription = true
           and subscription_started_on_trial = true
           and has_successful_payment = false
           and subscription_start_date :: date > '2022-02-21'
         group by 1, 2)

select b.subscription_start_date :: date as date_day,
       var,
       plan_type,
       country_group,
       channel,
       is_first_subscription,
       case
           when subscription_started_on_trial = true and trial_period_started_at_aet :: date >= '2022-02-21'
               then 'free_trials_continued'
           when a.sso_user_id is not null then 'free_trials_adjusted'
           when is_first_subscription = true and trial_period_started_at_aet isnull then 'new_non_free_trial'
           when first_successful_payment_date_aet :: date < '2022-02-21' then 'returning_sub'
           else 'other_sub' end          as sub_type,
       count(distinct b.sso_user_id)     as subscribers
from elements.dim_elements_subscription b
         join market.dim_geo_network g on b.dim_geo_network_key = g.dim_geo_network_key
         join elements.dim_elements_channel ch on b.dim_elements_channel_key = ch.dim_elements_channel_key
         left join trial a
                   on b.dim_subscription_key > a.dim_subscription_key and a.sso_user_id = b.sso_user_id
         left join allocations al
                   on al.sso_user_id = b.sso_user_id and b.subscription_start_date :: date >= exp_day_date
where has_successful_payment = true
  and subscription_start_date >= '2021-01-01'
group by 1, 2, 3, 4, 5, 6, 7;

with trial as (
    select sso_user_id,
           dim_subscription_key
    from elements.dim_elements_subscription
    where is_first_subscription = true
      and subscription_started_on_trial = true
      and has_successful_payment = false
    group by 1, 2)
select *
from elements.dim_elements_subscription b
         join trial a on b.dim_subscription_key > a.dim_subscription_key and a.sso_user_id = b.sso_user_id;


select date_trunc('month', date :: date)                                 as date_month,
       case
           when category isnull or category in ('', 'home', '') then 'no_category'
           when category ~ '[A-Z0-9]' then 'item_page'
           else split_part(category, '?', 1) end                         as category,
       case when searchterms isnull then 'no_keyword' else 'keyword' end as keyword,
       count(*)                                                          as searches,
       sum(impressions)                                                  as impressions,
       sum(dls)                                                          as downloads,
       sum(item_clicks)                                                  as item_clicks,
       count(case when filtered > 0 then 1 end)                          as filtered_searches,
       count(case when sorted > 0 then 1 end)                            as sorted_searches

from google_analytics.raw_elements_search_analysis
where date :: date between '2021-01-01' and '2022-05-01'
group by 1, 2, 3
having count(*) > 40
;



select sum(total_amount)                                       as paym,
       sum(total_amount - tax_amount - discount_amount) * 0.48 as rev
from elements.fact_elements_subscription_transactions t
         join elements.dim_elements_subscription s on t.dim_subscription_key = s.dim_subscription_key
    and date_trunc('month', date(dim_date_key)) = '2022-04-01'
--           group by 1
;

select ev.fullvisitorid,
       ev.visitid,
       variant,
       count(case
                 when hits_eventinfo_eventaction = 'license with download'
                     then 1
                 else 0 end)                                                       as downloads,
       count(case
                 when hits_eventinfo_eventaction = 'click;a'
                     and split_part(hits_eventinfo_eventcategory, ';', 1) = 'item-results' then 1
                 else 0 end)                                                       as item_pages,
       count(distinct case
                          when hits_eventinfo_eventaction = 'click;a'
                              and split_part(hits_eventinfo_eventcategory, ';', 1) = 'item-results'
                              then split_part(hits_eventinfo_eventcategory, ';', 2) = 'item-results'
           end)                                                                    as categories_viewed,
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
                  and ev.date between 20210812 and 20211020
                  and a.date between '2021-08-12' and '2021-10-20'
                  and experiment_id = 'DOAc8dPeTXWS2PtAH9QPyg'
group by 1, 2, 3
limit 100;



select variant,
       hits_eventinfo_eventcategory,
       hits_eventinfo_eventaction,
       hits_eventinfo_eventlabel,
       count(*) as ct


from webanalytics.ds_bq_events_elements ev
         join webanalytics.ds_bq_abtesting_enrolments_elements a
              on a.fullvisitorid::varchar = ev.fullvisitorid::varchar
                  and a.visitid::varchar = ev.visitid::varchar
                  and ev.date between 20210812 and 20211020
                  and a.date between '2021-08-12' and '2021-10-20'
                  and experiment_id = 'DOAc8dPeTXWS2PtAH9QPyg'
where hits_eventinfo_eventcategory like '%trending-search%'
   or hits_eventinfo_eventlabel like '%trending-search%'
    and ev.date between 20210812 and 20211020
group by 1, 2, 3, 4;


select *

from analysts.content_elements_usage_summary
limit 100;

select *
from (select id                                                                 as survey_id,
             coalesce(deic.content_type, 'unknown')                             as elements_content_type,
             rating,
             cast(created_at as date)                                           as created_at,
             case
                 when is_valid_json(last_search_query)
                     then json_extract_path_text(last_search_query, 'page') end as page_number,
             case
                 when is_valid_json(last_search_query) then coalesce(
                         nullif(json_extract_path_text(last_search_query, 'type'), ''),
                         'unknown') end                                         as shopfront_item_type,
             case
                 when is_valid_json(last_search_query) then coalesce(
                         json_extract_path_text(last_search_query, 'search_terms'),
                         'unknown') end                                         as search_terms,
             case
                 when is_valid_json(last_search_query) then coalesce(
                         json_extract_path_text(last_search_query, 'language_code'),
                         'unknown') end                                         as language_code
      from content.ds_content_discovery_survey_results cs
               left join elements.dim_elements_items_current deic
                         on deic.humane_id = cs.item_humane_id --left join content.dim_content_type ct on deic.dim_elements_items_categories_key = ct.dim_item_categories_key where id not in ('f57fba3c-b9ae-4940-86e2-c54aade23dba','90f5d79a-b046-4c0e-aeda-da739a8601a6')) a --record with truncated json object left join  (select calendar_date ,elements_shopfront_content_type ,sum(licenses) as licenses from analysts.content_elements_usage_summary where license_type in ('trial','project') and calendar_date >>= '2020-03-01' group by 1,2) b on a.elements_content_type = b.elements_shopfront_content_type and a.created_at = b.calendar_date

;
select *
from content.ds_content_discovery_survey_results
limit 100;


with a as (
    select fullvisitorid,
           min(date_aest) as seq
    from webanalytics.ds_bq_events_elements
    where hits_eventinfo_eventaction = 'Technical: Subscription Complete'
      and date = 20220510
    group by 1
    limit 1)

select eventid,
       sessionid,
       e.fullvisitorid,
       e.visitid,
       date,
       visitstarttime,
       hits_hitnumber,
       hits_hour,
       hits_issecure,
       hits_isinteraction,
       hits_minute,
       hits_time,
       hits_referer,
       hits_social_socialinteractionaction,
       hits_social_socialinteractionnetwork,
       hits_page_pagepath,
       hits_page_hostname,
       hits_page_pagetitle,
       hits_transaction_transactionid,
       hits_transaction_transactionrevenue,
       hits_transaction_transactiontax,
       hits_transaction_affiliation,
       hits_transaction_transactioncoupon,
       hits_eventinfo_eventcategory,
       hits_eventinfo_eventaction,
       hits_eventinfo_eventlabel,
       hits_eventinfo_eventvalue,
       user_uuid,
       sessioneventsequence,
       hosteventsequence,
       hits_ecommerceaction_action_type,
       hits_ecommerceaction_step,
       date_aest,
       upload_timestamp,
       seq
from webanalytics.ds_bq_events_elements e
         join a on a.fullvisitorid :: varchar = e.fullvisitorid :: varchar
where date = 20220510;


select *
from elements.dim_elements_subscription
where sso_user_id = '4753a54a-fe81-42be-b940-13d6942cafea';



select cd,
       envato_user,
       count(*) as ct

from (select fullvisitorid,
             count(distinct hits_eventinfo_eventlabel) as cd,
             max(case when sso.id notnull then 1 end)  as envato_user
      from webanalytics.ds_bq_events_elements
               left join elements.ds_elements_sso_users sso
                         on sso.id = hits_eventinfo_eventlabel and split_part(email, '@', 2) = 'envato.com'
      where hits_eventinfo_eventaction = 'Technical: Subscription Complete'
        and date between 20220501 and 20220510
      group by 1)
group by 1, 2



select *
from elements.dim_elements_subscription s
         join (
    select fullvisitorid,
           hits_eventinfo_eventlabel
    from webanalytics.ds_bq_events_elements
    where hits_eventinfo_eventaction = 'Technical: Subscription Complete'
      and date between 20220501 and 20220510
      and fullvisitorid = '3680817004732905433') on hits_eventinfo_eventlabel = sso_user_id
         join elements.ds_elements_sso_users sso
              on sso.id = s.sso_user_id and split_part(email, '@', 2) != 'envato.com';


select *,
       ratio_to_report(active_subs) over () as perc_total
from (
         select plan_type,

                plan_price_type,

                count(distinct sso_user_id) as active_subs

         from elements.dim_elements_subscription
         where termination_date is not null
         group by 1, 2);


select date,
       hits_page_pagepath,
       device_browser,
       device_devicecategory,
       geonetwork_country,
       count(distinct fullvisitorid)                  as sub_form_visitors,
       count(distinct case
                          when hits_eventinfo_eventlabel like 'Previous:%'
                              then fullvisitorid end) as switch_plan,
       count(distinct case
                          when hits_eventinfo_eventlabel like 'Previous:%monthly'
                              then fullvisitorid end) as switch_plan_to_annual,
       count(distinct case
                          when hits_eventinfo_eventaction = 'Error On: Card Number'
                              then fullvisitorid end) as card_number_error
from (select ee.fullvisitorid,
             ee.date_aest::date      as date,
             ee.visitid,
             ee.hits_page_pagepath,
             se.device_browser,
             se.geonetwork_country,
             se.device_devicecategory,
             hits_eventinfo_eventlabel,
             hits_eventinfo_eventaction,
             count(distinct eventid) as hits
      from webanalytics.ds_bq_events_elements ee
               inner join webanalytics.ds_bq_sessions_elements se
                          on ee.fullvisitorid = se.fullvisitorid and ee.visitid = se.visitid
      where hits_eventinfo_eventcategory = 'SubscriptionForm'
        and ee.date >>= '20210101'
        and se.date >>= '20210101'
      group by 1, 2, 3, 4, 5, 6, 7, 8, 9)
group by 1, 2, 3, 4, 5;

SELECT *
FROM stl_scan ss
JOIN pg_user pu
    ON ss.userid = pu.usesysid
JOIN svl_query_metrics_summary sqms
    ON ss.query = sqms.query
JOIN temp_mone_tables tmt
    ON tmt.table_id = ss.tbl AND tmt.table = ss.perm_table_name



SELECT tbl,
       MAX(endtime) last_scan,
       Nvl(COUNT(DISTINCT query || LPAD(segment,3,'0')),0) num_scans
FROM stl_scan s
WHERE s.userid > 1
-- AND   s.tbl IN (SELECT oid FROM tbl_ids)
GROUP BY tbl;

select * from stll_s3query limit 10

select tablename, tableowner From pg_tables
-- where tableowner = 'achaschin'
order by 2 desc;


select
       date(date_aet) as date_day,
       case when event_action = 'Elements Click' then event_category else null end as cat,
       count(case when event_action = 'Elements Click' then 1 end) as elements_click,
       count(distinct fullvisitorid) as cookies
from  webanalytics.rpt_bq_events_mixkit
where date_aet >=20220501
--and event_action = 'Elements Click'
group by 1,2

with a as (
select fullvisitorid
from  webanalytics.rpt_bq_sessions_mixkit
where date_aet = '20220501')
select count(fullvisitorid )
from  webanalytics.ds_bq_sessions_elements e join a using (fullvisitorid)
where date_aest = '20220501'


select
date_aest :: date as date_day,
       count(case when totals_transactions > 0 then 1 end ) as transactions,
       count(*) as cookies
from  webanalytics.ds_bq_sessions_elements
where trafficsource_source like '%mixkit%'
and date > 20220501
group by 1;


select * from SVV_SCHEMA_QUOTA_STATE limit 199;


select * from elements.fact_elements_subscription_transactions a limit 100;



  select
         is_first_subscription,
                    case
               when s.current_plan like '%enterprise%'
                   then 'enterprise'
               when s.current_plan like '%team%' then 'team'
               when s.current_plan like '%student_annual%'
                   then 'student_annual'
               when s.current_plan like '%student_monthly%'
                   then 'student_monthly'
               else s.plan_type end                                                       as plan_type,
         date(dim_date_key) as date_day,
                 sum(total_amount)                                as paym,
                 sum(total_amount - tax_amount) as rev,
									sum(total_amount - tax_amount) *0.48 as net_rev
          from elements.fact_elements_subscription_transactions t
                   join elements.dim_elements_subscription s on t.dim_subscription_key = s.dim_subscription_key
              and date(dim_date_key) >= '2020-01-01'
          group by 1,2,3;


select
date_aest :: date as date_day,
trafficsource_medium,
       count(distinct sessionid) as sessions
from webanalytics.ds_bq_sessions_elements
where date_aest :: date >= '2021-01-01'
group by 1,2
;


select * from elements.ds_elements_external_service_user_mapping
--where date_aest :: date = '2022-03-01'
limit 100;

with getu as (
    select c.reference as userid,
           sso_uuid
    from mytable c
             join -- elements.ds_elements_get_feedback_user_mappings
                 elements.ds_elements_external_service_user_mapping m on c.reference = m.id
             join dim_users on m.user_id = elements_id ) ,
     content_t as (select u.sso_uuid,
                          i.content_type,
                          count(*) as q
                   from elements.ds_elements_item_downloads dl
                            join elements.ds_elements_item_licenses l on dl.item_license_id = l.id
                       and download_started_at :: date > '2022-01-01'
                            join elements.dim_elements_items i on i.item_id = l.item_id
                            join dim_users u
                                 on dl.user_id = u.elements_id and dl.download_started_at :: date > '2022-02-01'
                   group by 1, 2)

select
       userid,
       g.sso_uuid,
       max(country) as country,
                           min(case
               when current_plan like '%enterprise%'
                   then 'enterprise'
               when current_plan like '%team%' then 'team'
               when current_plan like '%student_annual%'
                   then 'student_annual'
               when current_plan like '%student_monthly%'
                   then 'student_monthly'
               else plan_type end        )                                               as plan_type,
       max(case when termination_date isnull then 1 end) as currently_subscribed,
       max(termination_date) as last_termination_date,
       min(subscription_start_date) as first_subscription_date,
       max(subscription_start_date) as last_subscription_date,
       max(case when trial_period_started_at_aet notnull then 'true' else 'false' end) as had_a_free_trial,
       min(trial_period_started_at_aet :: date)                                        as trial_start_date,
       split_part(max(q || '|' || content_type), '|', 2) as top_download,
       sum(q) as downloads


from getu g
         left join elements.dim_elements_subscription e on g.sso_uuid = e.sso_user_id
left join dim_geo_network dgn on e.dim_geo_network_key = dgn.dim_geo_network_key
left join content_t c on g.sso_uuid = c.sso_uuid
group by 1, 2


select *
from elements.ds_elements_sso_users so
join elements.dim_elements_subscription s on sso_user_id = id limit 100
;
  join  dim_users u on so.id = u.elements_id limit 100 -- where id = 'pqnomp1v6lv6da3x5mo2zpqnomwff0ek';


select * from dim_users where id = '41292aa3-9133-42cc-a6b1-4734355ab1ef'

select * from elements.dim_elements_subscription s
                where sso_user_id = '643dbfa5-ff4c-45b2-bc7f-f75f7b2fd608'


with family_bing_prep as (
    SELECT distinct a.campaignname,a.adgroupname,a.adgroupfamily
    FROM elements.view_elements_paid_search_adgroups a
    WHERE 1=1 and a.campaignname like '%bing%'
),
elements_coupons_prep as (
    SELECT
        a.dim_subscription_key,
        a.dim_elements_coupon_key,
        case
            when b.discount_percent=100 then 'full free'
                when lower(b.name) like '%free%' then 'full free'
                when b.dim_elements_coupon_key=0 then 'no coupon'
                else 'partial free'
        end as coupon_type,
        -- beginning of additional code
        case
            when b.coupon_code like '%1first%' then '$1 coupon' --all the 1 dollar coupons have this string "1first" in the coupon_code
            when b.coupon_code like '%9first%' then '$9 coupon' --all the 1 dollar coupons have this string "9first" in the coupon_code
            else 'other coupon'
        end as coupon_discount_name_group,
    -- end of additional code
        row_number() over (partition by a.dim_subscription_key order by dim_date_key asc) as invoice_number
    FROM
        elements.fact_elements_subscription_transactions a
        join elements.dim_elements_coupon b on (a.dim_elements_coupon_key=b.dim_elements_coupon_key)
    WHERE 1=1 and a.dim_elements_coupon_key>0
)
,prep_step_1 as (
SELECT
    a.*,
    c.country,
    c.country_group,
    b.channel as channel,
    b.sub_channel,
    b.channel_detail,
    case when  b.channel in ('Paid Search','Paid Display','Paid Video') then coalesce(ad.adgroupname,ad2.adgroupname) end as adgroup,
    case when  b.channel in ('Paid Search','Paid Display','Paid Video') then coalesce(ad.adgroupfamily,ad2.adgroupfamily) end as family,
    s.session_date as attributed_session_date,
    e.coupon_type,
    e.discount_in_dollars,
    e.coupon_discount_name_group,
    row_number() over (partition by a.sso_user_id order by a.subscription_start_date asc) sso_sub_number,
    case when row_number() over (partition by case when a.has_successful_payment then a.sso_user_id end order by a.first_successful_payment_date_aet asc)=1 then TRUE else FALSe end as is_first_paid_susbcription,
    case when is_first_paid_susbcription=TRUE then a.first_successful_payment_date_aet end as first_user_successful_payment_date,
    min(case when a.subscription_started_on_trial is true and a.subscription_start_date::date>='2021-02-08' and a.subscription_platform='recurly' then a.trial_period_started_at_aet end) over (partition by a.sso_user_id) as sso_started_on_free_trial,
    case when sso_started_on_free_trial notnull then TRUE else FALSE end as sso_started_as_free_trial,
    case when max(case when a.has_successful_payment then 1 else 0 end) over (partition by a.sso_user_id) then TRUE else FALSE end as sso_had_paying_subscription_at_some_point
FROM
    elements.dim_elements_subscription a
    join elements.dim_elements_channel b on (a.dim_elements_channel_key=b.dim_elements_channel_key)
    join market.dim_geo_network c on (a.dim_geo_network_key=c.dim_geo_network_key)
    left join elements.rpt_elements_subscription_session s on (s.dim_subscription_key=a.dim_subscription_key) --we do left joins so w also bring the data for paying subscriptions in unknown
    left join webanalytics.ds_bq_sessions_elements es on (es.sessionid=s.sessionid)
    left join elements.view_elements_paid_search_adgroups ad on (ad.adgroupid=es.trafficsource_adwordsclickinfo_adgroupid)
    left join family_bing_prep ad2 on (ad2.adgroupname=es.trafficsource_adcontent and ad2.campaignname=es.trafficsource_campaign)
    left join elements_coupons_prep e on (a.dim_subscription_key=e.dim_subscription_key and e.invoice_number=1)
WHERE 1=1
    and a.subscription_start_date::date<getdate_aest()::date
)
,prep_step_2 as (
    SELECT
        a.*,
        coalesce(b.geo,'ROW') as geo_paid_channels,
        max(a.first_successful_payment_date_aet) over (partition by a.sso_user_id) as sso_first_successful_payment_date_aet,
        datediff('day',a.attributed_session_date,a.subscription_start_date) as days_between_attributed_session_date_and_first_subscription_date,
        datediff('day',a.subscription_start_date,sso_first_successful_payment_date_aet) as days_between_first_sub_date_and_first_paying_date,
        datediff('day',a.attributed_session_date,sso_first_successful_payment_date_aet) as days_between_attributed_session_date_and_first_paying_date,
        max(case when a.is_first_paid_susbcription then sso_sub_number end) over (partition by a.sso_user_id) as payment_in_subscription_number
    FROM
        prep_step_1 a
        left join analysts.elements_campaign_report_paid_traffic_geo_mapping b on (a.channel=b.channel and a.country=b.country)
    WHERE 1=1
        --and is_first_paid_susbcription
        --and sso_user_id='007a60aa-a0f9-4cb3-9463-51ae2fe0bf34'
        --and sso_user_id='0a3e28d4-c2a8-4132-b933-b4c593c0c6ab'
        and a.subscription_start_date::date>='2020-01-01'
)
SELECT *
FROM prep_step_2
WHERE 1=1
    and is_first_subscription;



select * from webanalytics.ds_bq_events_elements where ds_bq_events_elements.hits_eventinfo_eventaction like '%cancel%'
and date = 20220601
limit 100

visitid,date
1654007363,20220601

select * from webanalytics.ds_bq_events_elements where visitid = '1654007363';



-- console 4

--ft analysis

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
                      and ev.date > 20220220
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
                      and ev.date > 20220220
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
    );


--ft 2 as

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
                      join elements.dim_elements_subscription s on se.user_uuid = s.sso_user_id
                      left join elements.rpt_elements_session_channel sc on se.sessionid = sc.sessionid

             where experiment_id = 'geInrbFNTa2AZdiRswkP2A'
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
                          count(distinct item_license_id) as q
                   from elements.ds_elements_item_downloads dl
                            join dim_users u
                                 on dl.user_id = u.elements_id and
                                    dl.download_started_at :: date between '2021-10-28' and '2021-11-18'
                   group by 1),
     paid as
         (select s.sso_user_id,
                 sum(total_amount)                                as paym,
                 sum(total_amount - tax_amount - discount_amount) as rev
          from elements.fact_elements_subscription_transactions t
                   join elements.dim_elements_subscription s on t.dim_subscription_key = s.dim_subscription_key
              and date(dim_date_key) >= '2021-10-28'
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
               AND subscription_start_date :: date between '2021-10-28' and '2021-11-18'
             group by 1),

     refunds as
         (select sso_user_id, fact.dim_subscription_key
          from elements.fact_elements_subscription_transactions fact
                   inner join elements.dim_elements_transaction_attributes ta
                              on fact.dim_elements_transaction_key = ta.dim_elements_transaction_key
                   inner join elements.dim_elements_subscription sub
                              on fact.dim_subscription_key = sub.dim_subscription_key
                                  and fact.dim_date_key > 20211028
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
                        when d.subscription_start_date :: date <= '2021-10-28' and
                             (termination_date isnull or termination_date > '2021-10-28')
                            then 1 end)                                                            as already_subscribed,
                max(case
                        when d.subscription_start_date >= '2021-10-28'
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
                        when d.subscription_start_date :: date >= '2021-10-28' and
                             d.trial_period_started_at_aet isnull
                            then 1 end)                                                            as non_free_trial_signups,
                max(case
                        when d.subscription_start_date :: date >= '2021-10-28' and
                             d.trial_period_started_at_aet is not null
                            then 1 end)                                                            as free_trial_signups,
                max(case
                        when d.subscription_start_date :: date >= '2021-10-28'
                            then 1 end)                                                            as total_new_signups,
                max(case
                        when d.trial_period_started_at_aet :: date >= '2021-10-28' and
                             termination_date isnull and has_successful_payment = true
                            then 1 end)                                                            as trial_sub_remaining,
                max(case
                        when d.subscription_start_date :: date >= '2021-10-28' and
                             termination_date isnull and
                             has_successful_payment = true
                            then 1 end)                                                            as total_subs_remaining,
                max(case
                        when d.subscription_start_date :: date >= '2021-10-28' and
                             termination_date isnull and
                             has_successful_payment = true
                            and datediff(day, exp_day_date, current_date) >= 11
                            then 1 end)                                                            as true_converted_subs,
                max(case
                        when d.subscription_start_date :: date >= '2021-10-28' and
                             termination_date :: date >= '2021-10-28'
                            then 1 end)                                                            as total_new_subs_terminated,
                max(case
                        when d.subscription_start_date :: date >= '2021-10-28' and
                             termination_date :: date >= '2021-10-28'
                            then 1 end)                                                            as total_returning_subs_terminated,
                max(case
                        when d.subscription_start_date :: date >= '2021-10-28' and termination_date isnull
                            then 1 end)                                                            as returning_subs_retained,
                max(case
                        when d.subscription_start_date :: date >= '2021-10-28' and
                             has_successful_payment = false
                            and f.sso_user_id is not null
                            then 1 end)                                                            as failed_payments,
                max(case
                        when d.trial_period_started_at_aet :: date >= '2021-10-28' and
                             last_canceled_at >= '2021-10-28'
                            and has_successful_payment = false
                            then 1 end)                                                            as trials_cancellation_unpaid,
                max(case
                        when d.trial_period_started_at_aet :: date >= '2021-10-28' and
                             last_canceled_at >= '2021-10-28'
                            and has_successful_payment = true
                            then 1 end)                                                            as trials_cancellation_paid,
                max(case
                        when last_canceled_at >= '2021-10-28'
                            then 1 end)                                                            as all_cancellation,
                max(q)                                                                             as downloads,
                max(rev)                                                                           as revenue,
                max(case
                        when rf.sso_user_id notnull then 1 end)                                    as refunds_trials,
                max(case when trial_period_ends_at_aet :: date > current_date then 1 end)          as current_trials
         from allocations c


                  --              --remove envato users
                  join elements.ds_elements_sso_users sso
                       on sso.id = c.sso_user_id and split_part(email, '@', 2) != 'envato.com'
                  join elements.dim_elements_subscription d
                       on c.sso_user_id = d.sso_user_id
                           and (termination_date isnull or termination_date >= '2021-10-28')
                  left join content_t dl on c.sso_user_id = dl.sso_uuid
                  left join refunds rf
                            on d.sso_user_id = rf.sso_user_id and d.dim_subscription_key = rf.dim_subscription_key
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
       count(refunds_trials)                                                     as refunds,
       count(current_trials)                                                     as current_trials

from clean_sso
group by 1, 2, 3, 4, 5, 6, 7;


select case when trial_period_ends_at_aet isnull then null else trial_period_ends_at_aet :: date end as s,
       count(*)
from elements.dim_elements_subscription
where subscription_start_date >= '2022-03-06'
  and trial_period_started_at_aet notnull
group by 1;


Event Action
click;
a
Event Category
individual-offering
Event Label
sign-in
--
Event Action
click;
a
Event Category
page-header-top
Event Label
sign-in

select date(date) as date_day,
       hits_eventinfo_eventcategory,
       count(*)   as ct
from webanalytics.ds_bq_events_elements
where hits_eventinfo_eventaction = 'click;a'
  and date > 20220201
  and hits_eventinfo_eventlabel = 'sign-in'
group by 1, 2;


select count(distinct fullvisitorid)                                                                         as cookies,
       count(distinct case
                          when hits_page_pagepath like '%/account/downloads'
                              then fullvisitorid end)                                                        as dl_page,
       count(distinct case
                          when hits_page_pagepath like '%/account/downloads'
                              and hits_eventinfo_eventaction = 'license with download'
                              then fullvisitorid end)                                                        as dl_page_dl,
       count(distinct case when hits_eventinfo_eventaction = 'license with download' then fullvisitorid end) as dlers
from webanalytics.ds_bq_events_elements
where date between 20220201 and 20220301

--group by 1,2

select *, user_uuid
from webanalytics.ds_bq_sessions_elements
where date_aest :: date = '2022-03-01'
limit 100;


select 'items',
       count(dim_elements_items_key) as ct,
       count(distinct humane_id)     as uq
from elements.dim_elements_items

union all

select 'current',
       count(dim_elements_items_key) as ct,
       count(distinct humane_id)     as uq
from elements.dim_elements_items_current;

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
                        else 'ROW' end                                   as country,
                    variant,
                    max(case when sc.sessionid notnull then variant end) as sub_var,
                    min(a.date)                                          as edate
             from ds_bq_abtesting_enrolments_elements a
                      join webanalytics.ds_bq_sessions_elements se
                           on a.fullvisitorid = se.fullvisitorid::varchar and a.visitid = se.visitid and
                              se.date between 20211025 and 20211118
                      left join elements.rpt_elements_subscription_session sc on se.sessionid = sc.sessionid
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

     allocations as (select c.user_uuid,
                            min(split_part(edate || '|' || variant, '|', 2))    as variant,
                            min(split_part(edate || '|' || sub_var, '|', 2))    as sub_var,
                            max(case when uq.user_uuid notnull then 'both' end) as reallocated,
                            max(country)                                        as country
                     from webanalytics.ds_bq_events_elements c
                              left join uq on c.user_uuid = uq.user_uuid
                              join enrollments b on c.fullvisitorid::varchar = b.fullvisitorid
                         and c.visitid = b.visitid
                         and c.date between 20211025 and 20211118
                     group by 1)


--

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
                    min(timestamp 'epoch' + visitstarttime * interval '1 second') as exp_date
             from webanalytics.ds_bq_abtesting_enrolments_elements a
                      join webanalytics.ds_bq_sessions_elements se
                           on a.fullvisitorid = se.fullvisitorid::varchar and a.visitid = se.visitid
                               and a.date between '2021-10-28' and '2021-11-18'
                               and se.date between 20211028 and 20211118
                      join elements.dim_elements_subscription s on se.user_uuid = s.sso_user_id
--                       left join elements.rpt_elements_session_channel sc on se.sessionid = sc.sessionid

             where experiment_id = 'geInrbFNTa2AZdiRswkP2A'
             group by 1, 2
         ),
     multiple_allocations as
         (select sso_user_id
          from enrollments
          group by 1
          having min(variant) != max(variant)),
     allocations as (
         select a.sso_user_id,
                max(country)                                                                           as country,
                split_part(min(exp_date || '|' || variant), '|', 2)                                    as var,
                max(case when m.sso_user_id is not null then 'reallocated' else 'not_reallocated' end) as reallocation,
                min(CAST(DATE(exp_date) AS DATE))                                                      as exp_day_date
         from enrollments a
                  left join multiple_allocations m on a.sso_user_id = m.sso_user_id
         group by 1),

     content_t as (select u.sso_uuid,
                          -- ,i.content_type,
                          date_trunc('day', download_started_at) as date_dl
                   from elements.ds_elements_item_downloads dl
                            --                            join elements.ds_elements_item_licenses l on dl.item_license_id = l.id
--                       and download_started_at :: date > '2022-01-01'
--                            join elements.dim_elements_items i on i.item_id = l.item_id
                            join dim_users u
                                 on dl.user_id = u.elements_id and dl.download_started_at :: date > '2021-10-01'
                   group by 1, 2),

     dates as (
         select date_trunc('day', date_dl) as date_cal
         from content_t
         where date_dl notnull
         group by 1
     ),
     subs as (
         select a.sso_user_id || '|' || dim_subscription_key as sso_key,

                case
                    when current_plan like '%enterprise%'
                        then 'enterprise'
                    when current_plan like '%team%' then 'team'
                    when current_plan like '%student_annual%'
                        then 'student_annual'
                    when current_plan like '%student_monthly%'
                        then 'student_monthly'
                    else plan_type end                       as plan_type,
                country,

                var                                          as Variant,

                case
                    when trial_period_started_at_aet :: date between '2021-10-28' and '2021-11-18'
                        and is_first_subscription = true then 'new_trial'
                    when trial_period_started_at_aet :: date between '2021-10-28' and '2021-11-18'
                        and is_first_subscription = false then 'not_new_trial_wtf'
                    when subscription_start_date :: date between '2021-10-28' and '2021-11-18'
                        and trial_period_started_at_aet isnull
                        and is_first_subscription = true then 'new_non_trial'
                    when subscription_start_date :: date between '2021-10-28' and '2021-11-18'
                        and trial_period_started_at_aet isnull
                        and is_first_subscription = false then 'returning_non_trial'
                    when subscription_start_date :: date < '2021-10-28'
                        then 'continuing_user' end           as u_type,
                case
                    when subscription_start_date :: date < '2021-10-28' then date_trunc('day', '2021-11-27' :: date)
                    else date_trunc('day', subscription_start_date :: date) end
                                                             as subdate,
                date_trunc('day', termination_date :: date)  as ter_date

         from elements.dim_elements_subscription s
                  join allocations a on a.sso_user_id = s.sso_user_id
             and (termination_date :: date >= '2021-10-28' or termination_date isnull)

         group by 1, 2, 3, 4, 5, 6, 7
     )
        ,
     orig as
         (select split_part(sso_key, '|', 1)      as ssoid,
                 min(split_part(sso_key, '|', 2)) as skey
          from subs
          where u_type is not null
          group by 1)


select subdate :: date                                                      as cohort,
       plan_type,
       case
           when split_part(sso_key, '|', 2) > skey then 'post_FT_return'
           when skey = split_part(sso_key, '|', 2) then 'during_ft_sub' end as ret,
       variant,
       u_type,
       country,

       date_cal :: date                                                     as day_date,
       datediff(day, subdate:: date, date_cal :: date)                      as days_since_sub,
       count(distinct case
                          when date_cal >= subdate and (ter_date >= date_cal or ter_date isnull)
                              then
                              sso_key
--                               split_part(sso_key, '|', 1)
           end)                                                             as remaining_users

from subs s


         cross join dates d
         join orig o on ssoid = split_part(sso_key, '|', 1)
--          left join content_t c on sso_uuid = split_part(sso_key, '|', 1) and date_dl = date_cal
where d.date_cal >= subdate
  and u_type is not null

group by 1, 2, 3, 4, 5, 6, 7, 8;



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
                          count(distinct item_license_id) as q
                   from elements.ds_elements_item_downloads dl
                            join dim_users u
                                 on dl.user_id = u.elements_id and dl.download_started_at :: date >= '2022-02-20'
                   group by 1),
     paid as
         (select s.sso_user_id,
                 case
                     when max(subscription_start_date :: date) < (current_date - 10) then '11_days_or_older'
                     when max(subscription_start_date :: date) < (current_date - 6) then '7_10_days'
                     else '7_days_or_younger' end                 as data_age,
                 sum(total_amount)                                as paym,
                 sum(total_amount - tax_amount - discount_amount) as rev
          from elements.fact_elements_subscription_transactions t
                   join elements.dim_elements_subscription s on t.dim_subscription_key = s.dim_subscription_key
              and date(dim_date_key) >= '2022-02-20'
          group by 1),

     failed_payments
         as (SELECT s.sso_user_id,
                    dim_subscription_key,
                    payment_method
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
             group by 1,2),

     refunds as
         (select sso_user_id, fact.dim_subscription_key
          from elements.fact_elements_subscription_transactions fact
                   inner join elements.dim_elements_transaction_attributes ta
                              on fact.dim_elements_transaction_key = ta.dim_elements_transaction_key
                   inner join elements.dim_elements_subscription sub
                              on fact.dim_subscription_key = sub.dim_subscription_key
                                  and fact.dim_date_key > 20220101
          where ta.transaction_type = 'Refund'
          group by 1, 2),

     clean_sso as (
         select c.sso_user_id
                                                                                                   as sso_key,
                max(country)                                                                       as country,
                var                                                                                as variant,
                reallocation,
                referer,
                case
                    when max(subscription_start_date :: date) < (current_date - 10) then '11_days_or_older'
                    when max(subscription_start_date :: date) < (current_date - 6) then '7_10_days'
                    else '7_days_or_younger' end                                                   as data_age,
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
                        when rf.sso_user_id notnull then 1 end)                                    as refunds_trials,
                max(case when trial_period_ends_at_aet :: date > current_date then 1 end)          as current_trials
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
                  left join failed_payments f on c.sso_user_id = f.sso_user_id
                  left join paid p on c.sso_user_id = p.sso_user_id and data_age = p.data_age
         group by 1, 3, 4, 5
     )

select variant                                                                   as variant,
       country,
       plan_type,
       reallocation,
       referer,
       data_age,
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
       count(refunds_trials)                                                     as refunds,
       count(current_trials)                                                     as current_trials

from clean_sso
group by 1, 2, 3, 4, 5, 6, 7, 8

--

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
                      and ev.date between 20220220 and 20220323
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
                      and ev.date between 20220220 and 20220323
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
                    max(case when s.sso_user_id notnull then 1 end)               as converted
             from pric ev
                      join webanalytics.ds_bq_sessions_elements se
                           on ev.fullvisitorid::varchar = se.fullvisitorid::varchar
                               and ev.visitid::varchar = se.visitid::varchar
                      left join elements.rpt_elements_session_channel sc on sc.sessionid = se.sessionid
                      left join elements.dim_elements_subscription s on se.user_uuid = s.sso_user_id
                 and subscription_start_date :: date >= '2022-02-21'
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
    );



with enrollments as
         (
             select sso_user_id,
                    min(dim_subscription_key)                                     as dim_subscription_key,
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
                    split_part(min(sc.dss_update_time || '|' || variant), '|', 2) as variant
             from webanalytics.ds_bq_abtesting_enrolments_elements a
                      join webanalytics.ds_bq_sessions_elements se
                           on a.fullvisitorid = se.fullvisitorid::varchar and a.visitid = se.visitid
                               and a.date between '2022-02-20' and '2022-03-23'
                               and se.date between 20220220 and 20220323
                      join elements.dim_elements_subscription s on se.user_uuid = s.sso_user_id
                 and is_first_subscription = true
                 and subscription_start_date :: date between '2022-02-20' and '2022-03-23'
                      left join elements.rpt_elements_session_channel sc on se.sessionid = sc.sessionid

             where experiment_id = 'FTr-7_qaRO-UzNOru4HW1Q'
             group by 1)
select
subscription_start_date :: date as date_day,
       is_first_subscription,
       has_successful_payment,
       variant,
       count(distinct s.sso_user_id) as subs

from enrollments se
         join elements.dim_elements_subscription s
              on se.sso_user_id = s.sso_user_id and se.dim_subscription_key >= s.dim_subscription_key
                  and subscription_start_date > '2022-01-01'
-- order by s.sso_user_id, s.dim_subscription_key desc
group by 1,2,3,4
-- group by 1,2,3,4

with a as (
    select sso_user_id,
           dim_subscription_key,
           case
               when trial_period_started_at_aet between '2022-02-21' and '2022-03-20' then 'FreeTrial'
               else 'regular' end as FT,
           has_successful_payment

    from elements.dim_elements_subscription
    where subscription_start_date between '2022-02-21' and '2022-03-20'
      and is_first_subscription = true
    group by 1, 2, 3, 4)

select ft,
       count(distinct case when a.has_successful_payment = true then a.sso_user_id end)       as initial_users_paid,
       count(distinct
             case when a.has_successful_payment = true or s.has_successful_payment = true then a.sso_user_id end)
           - count(distinct case when a.has_successful_payment = true then a.sso_user_id end) as returning_users_paid
from a
         left join elements.dim_elements_subscription s
                   on a.sso_user_id = s.sso_user_id and s.dim_subscription_key > a.dim_subscription_key
group by 1


-- console 3

with pric as (
    select ev.fullvisitorid,
           ev.visitid,
           variant,
           max(case
                   when (hits_page_pagePath like '%/pricing'
                       or hits_page_pagePath like '%/pricing/%') then 1 end)                   as pricing,
           max(case when hits_eventinfo_eventlabel = 'subscribe-individuals;month' then 1 end) as month_sub_click,
           max(case when hits_eventinfo_eventlabel = 'subscribe-individuals;year' then 1 end)  as year_sub_click,
           max(case when hits_eventinfo_eventlabel = 'subscribe-individuals' then 1 end)       as sub_click,
           max(case
                   when (hits_page_pagePath like '%/subscribe'
                       or hits_page_pagePath like '%/subscribe/%'
                       or hits_page_pagePath like '%/subscribe?%') then 1 end)                 as sub_page,
           max(case
                   when hits_eventinfo_eventaction = 'Focus On: Create Your Account'
                       or (hits_eventinfo_eventcategory = 'Google Auth' and
                           hits_eventinfo_eventaction = 'Sign Up Success') then 1 end)         as signup,
           max(case
                   when hits_eventinfo_eventaction = 'Technical: Subscription Complete'
                       then 1 end)                                                             as subscribe

    from webanalytics.ds_bq_events_elements ev
             join webanalytics.ds_bq_abtesting_enrolments_elements a
                  on a.fullvisitorid::varchar = ev.fullvisitorid::varchar
                      and a.visitid::varchar = ev.visitid::varchar
                      and ev.date between 20220220 and 20220323
                      and a.date between '2022-02-20' and '2022-03-23'
                      and experiment_id = 'FTr-7_qaRO-UzNOru4HW1Q'
    group by 1, 2, 3

    union all

    select ev.fullvisitorid,
           ev.visitid,
           variant,
           max(case
                   when (hits_page_pagePath like '%/pricing'
                       or hits_page_pagePath like '%/pricing/%') then 1 end)   as pricing,
           null,
           null,
           null,
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
                      and ev.date between 20220220 and 20220323
                      and a.date between '2022-02-20' and '2022-03-23'
                      and experiment_id = 'FTr-7_qaRO-UzNOru4HW1Q'
    group by 1, 2, 3
),
     fv as
         (
             select ev.fullvisitorid,
                    max(case when s.sso_user_id notnull then 1 end) as converted
             from pric ev
                      join webanalytics.ds_bq_sessions_elements se
                           on ev.fullvisitorid::varchar = se.fullvisitorid::varchar
                               and ev.visitid::varchar = se.visitid::varchar
                               and se.date between 20220220 and 20220323
                      left join elements.dim_elements_subscription s on se.user_uuid = s.sso_user_id
                 and subscription_start_date :: date >= '2022-02-21'
                 and has_successful_payment = true
             group by 1
         ),

     a as (
         select variant,
                count(distinct case
                                   when pricing = 1
                                       then p.fullvisitorid end) as pricing_page,
                count(distinct case
                                   when year_sub_click = 1
                                       then p.fullvisitorid end) as year_sub_click,
                count(distinct case
                                   when month_sub_click = 1
                                       then p.fullvisitorid end) as month_sub_click,
                count(distinct case
                                   when sub_click = 1
                                       then p.fullvisitorid end) as default_sub_click,
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
         group by 1 )
select *
from a UNPIVOT (
                hits FOR stage IN (pricing_page, year_sub_click,
month_sub_click,
default_sub_click, sub_page, subscription, paid_subscription, signup, cookies)
    );


select *
from webanalytics.ds_bq_events_elements
where hits_eventinfo_eventcategory like '%offering'
  and date = 20220305
limit 100

select * from webanalytics.ds_bq_abtesting_enrolments_elements limit 100

with butt as (
select ev.fullvisitorid,
       a.visitid,
       max(variant) as variant,
       max(case when hits_eventinfo_eventlabel = 'subscribe-individuals;month' then 1 end) as month_sub_click,
           max(case when hits_eventinfo_eventlabel = 'subscribe-individuals;year' then 1 end)  as year_sub_click,
           max(case when hits_eventinfo_eventlabel = 'subscribe-individuals' then 1 end)       as sub_click,
           max(case
                   when (hits_page_pagePath like '%/subscribe'
                       or hits_page_pagePath like '%/subscribe/%'
                       or hits_page_pagePath like '%/subscribe?%') then 1 end)                 as sub_page,
           max(case
                   when hits_eventinfo_eventaction = 'Focus On: Create Your Account'
                       or (hits_eventinfo_eventcategory = 'Google Auth' and
                           hits_eventinfo_eventaction = 'Sign Up Success') then 1 end)         as signup,
           max(case
                   when hits_eventinfo_eventaction = 'Technical: Subscription Complete'
                       then 1 end)                                                             as subscribe

    from webanalytics.ds_bq_events_elements ev
             join webanalytics.ds_bq_abtesting_enrolments_elements a
                  on a.fullvisitorid::varchar = ev.fullvisitorid::varchar
                      and a.visitid::varchar = ev.visitid::varchar
                      and ev.date between 20220220 and 20220323
                      and a.date between '2022-02-20' and '2022-03-23'
                      and experiment_id = 'FTr-7_qaRO-UzNOru4HW1Q'
group by 1,2)
,
     fv as
         (
             select ev.fullvisitorid,
                    se.visitid
             from butt ev
                      join webanalytics.ds_bq_sessions_elements se
                           on ev.fullvisitorid::varchar = se.fullvisitorid::varchar
                               and ev.visitid::varchar = se.visitid::varchar
                               and se.date between 20220220 and 20220323
                       join elements.dim_elements_subscription s on se.user_uuid = s.sso_user_id
                 and subscription_start_date :: date >= '2022-02-21'
                 and has_successful_payment = true
             group by 1,2
         )

select
variant,
    case when month_sub_click = 1 then 'month'
        when year_sub_click = 1 then 'year'
        when sub_click = 1 then 'other/default' end as type,
        count(*) as clicks,
       count(case when subscribe = 1 then 1 end ) as subscribed,
        count(case when subscribe = 1 and fv.fullvisitorid notnull then 1 end ) as subscribed_paid
from butt b
left join fv on b.fullvisitorid = fv.fullvisitorid and b.visitid = fv.visitid
group by 1,2
;


select split_part('subscribe-individuals', ';',1)


select
searchterms, count(*) as ct,
       case when searchterms like '%s' then 1 end
from google_analytics.raw_elements_search_analysis
where searchterms notnull
-- and searchterms like '%s'
and date = '2022-05-01'
group by 1, 3
    order by 2 desc

-- console 1

with elements_sessions_base as (
    SELECT a.date_aest::date as session_date,
           a.sessionid,
           a.fullvisitorid,
           a.geonetwork_country,
           c.channel         as channel,
           c.sub_channel     as sub_channel,
           c.channel_detail  AS channel_detail
    FROM webanalytics.ds_bq_sessions_elements a
             left join elements.rpt_elements_session_channel c on (a.sessionid = c.sessionid)
    WHERE 1 = 1
      and a.date between 20220301 and 20220324
),
     rd_signups_cy AS (
         select 'Current Year'                      as period,
                null                                as currency,
                cast(a.signup_date as date)         as calendar_date,
                cast(a.session_date as date)        as session_date,
                coalesce(f.factor, f1.factor, 0.00) as factor,
                c.channel                           as channel,
                c.sub_channel                       as sub_channel,
                c.channel_detail                    AS channel_detail,
                b.geonetwork_country                AS geonetwork_country,
--         a.sessionid as sig_ses,
--            b.sessionid as ds_bq_ses,
--            c.sessionid as session_channel_ses,
                null                                as preferred_locale,
                null                                as coupon_type_first_invoice,
                null                                as has_paying_subscription,
                0                                   as sessions,
                0                                   as visitors,
                count(*)                            as signups,
                0                                   as signups_ly,
                0                                   as first_subs,
                0                                   as return_subs,
                0                                   as total_subs,
                0                                   as first_subs_annual,
                0                                   as return_subs_annual,
                0                                   as total_subs_annual,
                0                                   as first_subs_ly,
                0                                   as return_subs_ly,
                0                                   as total_subs_ly,
                0                                   as first_subs_annual_ly,
                0                                   as return_subs_annual_ly,
                0                                   as total_subs_annual_ly,
                0                                   as terminations,
                0                                   as terminations_ly,
                0                                   as sessions_ly,
                0                                   as visitors_ly
         from elements.rpt_elements_user_signup_session a
                  left join elements_sessions_base b on a.sessionid = b.sessionid
                  left join elements.rpt_elements_session_channel c on a.sessionid = c.sessionid
             --if a channel exists in factors table we take channel level factor
                  LEFT JOIN analysts.rpt_elements_campaign_report_factors f on
             --last day available in the report is today-1, so this day needs to have 0 daysdiff factor
                     CASE
                         WHEN DATEDIFF(DAY, a.session_date::date, cast('2022-03-24' - 1 as date)) >= 365
                             THEN 365
                         ELSE DATEDIFF(DAY, a.session_date::date, cast('2022-03-24' - 1 as date))
                         END = f.Daysdiff
                 AND c.channel = f.channel
                 --factor changes retroactively instead of using lates available version
                 and a.session_date::date >= f.start_date and a.session_date::date < f.end_date
             --if a factor does not exist on channel level we take overall level
                  LEFT JOIN analysts.rpt_elements_campaign_report_factors f1 on
                     CASE
                         WHEN DATEDIFF(DAY, a.session_date::date, cast('2022-03-24' - 1 as date)) >= 365 THEN 365
                         ELSE DATEDIFF(DAY, a.session_date::date, cast('2022-03-24' - 1 as date))
                         END = f1.Daysdiff
                 AND f1.channel = 'overall'
                 --factor changes retroactively instead of using lates available version
                 and a.session_date::date >= f1.start_date and a.session_date::date < f1.end_date
         WHERE 1 = 1
           AND a.signup_date::date < '2022-03-24'::date
           AND a.signup_date::date >= '2022-03-01'::date
           and a.session_date:: date between '2022-03-01' and '2022-03-24'
         group by 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13) --,14,15)
select geonetwork_country
                   session_date,
       count(*) as ct
from rd_signups_cy
group by 1, 2;


select case when sessionid isnull then 'unknown' else 'known' end                             as known_session,
--        ss.sso_user_id,
--        ss.username,
       ss.signup_date :: date                                                                 as date_day,
--        country,
--        subscription_start_date,
       case when trial_period_started_at_aet notnull then 'free_trial' else 'regular_sub' end as trial,
       has_successful_payment,
       count(distinct ss.sso_user_id)
from elements.rpt_elements_user_signup_session ss
         left join dim_users d on d.username = ss.username
         left join elements.dim_elements_subscription su on ss.sso_user_id = su.sso_user_id
         left join market.dim_geography g on su.dim_geo_network_key = g.dim_geography_key
where 1 = 1 --sessionid isnull
  and signup_date :: date >= '2022-02-01'
-- and ss.username = 'terrykirk'
group by 1, 2, 3, 4
--group by 1


select *
from dim_users
limit 300


select *
from elements.dim_elements_subscription
where sso_user_id = '1814d6c4-69db-496d-b961-78dfe75f0ab5';

drop table if exists smet_one;
create temporary table smet_one as;


select hits_page_pagepath,
       case
           when split_part(hits_page_pagepath, '/', 2) in ('pt-br', 'de', 'es', 'fr', 'ru')
               then split_part(hits_page_pagepath, '/', 3)
           else split_part(hits_page_pagepath, '/', 2)
               in ('stock-video', 'video-templates', 'audio', 'sound-effects', 'graphic-templates',
                   'graphics', 'presentation-templates', 'photos', 'fonts', 'add-ons', 'web-templates')

from webanalytics.ds_bq_events_elements ev

where ev.date = 20220301 -- and 20220324
  -- and hits_eventinfo_eventaction like '%impression%'
  and


limit 100;

select *
from webanalytics.ds_bq_hits_elements
where eve
limit 100;
with cats as
         (select sessionid,
                 case
                     when split_part(hits_page_pagepath, '/', 2) in ('pt-br', 'de', 'es', 'fr', 'ru')
                         then split_part(hits_page_pagepath, '/', 3)
                     else split_part(hits_page_pagepath, '/', 2) end as category
                 -- ,hits_page_pagepath
          from webanalytics.ds_bq_events_elements ev

          where ev.date between 20220228 and 20220304
            and (
                  (split_part(hits_page_pagepath, '/', 2) in ('pt-br', 'de', 'es', 'fr', 'ru')
                      and split_part(hits_page_pagepath, '/', 3) in
                          ('stock-video', 'video-templates', 'audio', 'sound-effects', 'graphic-templates',
                           'graphics', 'presentation-templates', 'photos', 'fonts', 'add-ons', 'web-templates')
                      ) or
                  (split_part(hits_page_pagepath, '/', 2) in
                   ('stock-video', 'video-templates', 'audio', 'sound-effects', 'graphic-templates',
                    'graphics', 'presentation-templates', 'photos', 'fonts', 'add-ons', 'web-templates'))
              )
          group by 1, 2 --limit 500
         ),
;
with pric as (
    select
--         ev.fullvisitorid,
ev.sessionid,
date(ev.date)                                           as date_day,
split_part(min(case
                   when (sso_user_id notnull and termination_date isnull) or
                        date(ev.date) between subscription_start_date :: date and termination_date :: date
                       then 'a|subscribed'
                   when s.sso_user_id notnull and date(ev.date) > termination_date then 'b|subscribed_inactive'
                   when s.sso_user_id notnull and date(ev.date) < termination_date then 'bb|pre-subscription'
                   when s.sso_user_id notnull then 'bbb|subscribed_wrong'
                   when ev.user_uuid notnull then 'c|free_account'
                   else 'd|not_signed_up' end), '|', 2) as subscription_status,
--            max(case when visitnumber = 1 then 'first_visit' else 'not_first_visit' end) as visit_number,
--         max(case when is_authenticated is true then 1  else 0 end) as logged_in,
max(case
        when (ev.hits_eventinfo_eventcategory = 'autosuggest-with-type'
            ) then 1
        else 0 end)                                     as auto_suggest_search,
max(case
        when (ev.hits_eventinfo_eventaction = 'submit'
            and ev.hits_eventinfo_eventcategory = 'header-search-form'
                 )
            or
             hits_eventinfo_eventaction = 'change: search-field' then 1
        else 0 end)                                     as search,
max(case
        when (split_part(ev.hits_eventinfo_eventlabel, '-', 1) = 'refinement'
            ) then 1
        else 0 end)                                     as filter,
max(case
        when (split_part(ev.hits_eventinfo_eventlabel, '-', 1) = 'sort'
            ) then 1
        else 0 end)                                     as sorted,

max(case
        when hits_eventinfo_eventaction = 'click;a'
            and split_part(hits_eventinfo_eventcategory, ';', 1) = 'item-results' then 1
        else 0 end)                                     as item_page,
max(case
        when hits_page_pagepath like '%/account/downloads'
            and hits_eventinfo_eventaction = 'license with download' then 1
        else 0 end)                                     as my_downloads_page_dl,
max(case
        when hits_page_pagepath like '%/account/downloads%' then 1
        else 0 end)                                     as my_downloads_page,
max(case
        when hits_eventinfo_eventaction = 'license with download'
            then 1
        else 0 end)                                     as downloaded

    from webanalytics.ds_bq_events_elements ev

             left join elements.dim_elements_subscription s on ev.user_uuid = s.sso_user_id
    where ev.date between 20220228 and 20220304

--             left join elements.rpt_elements_user_sessions_all sa on ev.sessionid = sa.sessionid
--             and sa.session_date_aet :: date between '2022-03-01' and '2022-03-15'

    group by 1, 2
) --select * from pric where subscription_status = 'subscribed_wrong' limit 100

select *
from (
         select date_day,
                subscription_status,
                downloaded,
--                 category,

                count(*)                                                                       as visits,
                count(case when search + auto_suggest_search > 0 and filter = 1 then 1 end)    as search_filter_visits,
                count(case when search + auto_suggest_search > 0 and sorted = 1 then 1 end)    as search_sort_visits,
                count(case when auto_suggest_search = 1 then 1 end)                            as auto_search_visits,
                count(case when my_downloads_page = 1 then 1 end)                              as my_downloads_page,
                count(case when my_downloads_page_dl = 1 then 1 end)                           as my_downloads_page_downloads,
                count(case when search + auto_suggest_search > 0 then 1 end)                   as search_visits,
                count(case when search + auto_suggest_search > 0 and item_page = 1 then 1 end) as search_and_item_page_visits,

                count(case when search + auto_suggest_search = 0 then 1 end)                   as no_search,
                count(case
                          when search + auto_suggest_search = 0 and filter = 1
                              then 1 end)                                                      as no_search_filter_dl_visits
         from pric p
--          left join cats c using (sessionid)
         group by 1, 2, 3) UNPIVOT (
                                    hits FOR stage IN (
                                     visits,
                                  search_visits,
                                  auto_search_visits,
                                  search_filter_visits,
                                  search_sort_visits,
                                     my_downloads_page,
                                     my_downloads_page_downloads,
                                  search_and_item_page_visits,
                                  no_search,
                                  no_search_filter_dl_visits)
    );

with pric as (
    select ev.fullvisitorid,
--         ev.sessionid,
           date_trunc('week', date(ev.date))                                            as date_day,
           split_part(max(case
                              when date(ev.date) between subscription_start_date :: date and termination_date :: date
                                  then 'a|subscribed'
                              when s.sso_user_id notnull and date(ev.date) > termination_date :: date
                                  then 'b|subscribed_inactive'
                              when s.sso_user_id notnull then 'bb|other|wtf'
                              when ev.user_uuid notnull then 'c|free_account'
                              else 'd|not_signed_up' end), '|', 2)                      as subscription_status,
           max(case when visitnumber = 1 then 'first_visit' else 'not_first_visit' end) as visit_number,
--         max(case when is_authenticated is true then 1  else 0 end) as logged_in,
           max(case
                   when (ev.hits_eventinfo_eventcategory = 'autosuggest-with-type'
                       ) then 1
                   else 0 end)                                                          as auto_suggest_search,
           max(case
                   when (ev.hits_eventinfo_eventaction = 'submit'
                       and ev.hits_eventinfo_eventcategory = 'header-search-form'
                            )
                       or
                        hits_eventinfo_eventaction = 'change: search-field' then 1
                   else 0 end)                                                          as search,
           max(case
                   when (split_part(ev.hits_eventinfo_eventlabel, '-', 1) = 'refinement'
                       ) then 1
                   else 0 end)                                                          as filter,
           max(case
                   when (split_part(ev.hits_eventinfo_eventlabel, '-', 1) = 'sort'
                       ) then 1
                   else 0 end)                                                          as sorted,

           max(case
                   when hits_eventinfo_eventaction = 'click;a'
                       and split_part(hits_eventinfo_eventcategory, ';', 1) = 'item-results' then 1
                   else 0 end)                                                          as item_page,
           max(case
                   when hits_eventinfo_eventaction = 'license with download'
                       then 1
                   else 0 end)                                                          as download

    from webanalytics.ds_bq_events_elements ev
             join webanalytics.ds_bq_sessions_elements a
                  on ev.sessionid = a.sessionid
                      and ev.date between 20220301 and 20220315
                      and a.date between 20220301 and 20220315
        --                       and date(ev.date) between '2022-03-01' and '2022-03-15'
--                        and date(a.date) between '2022-03-01' and '2022-03-15'

             left join elements.dim_elements_subscription s on ev.user_uuid = s.sso_user_id


--             left join elements.rpt_elements_user_sessions_all sa on ev.sessionid = sa.sessionid
--             and sa.session_date_aet :: date between '2022-03-01' and '2022-03-15'

    group by 1, 2)
select *
from (
         select date_day,
                subscription_status,
                visit_number,
                count(*)                                                                       as visits,
                count(case when search + auto_suggest_search > 0 and filter = 1 then 1 end)    as search_filter_visits,
                count(case when search + auto_suggest_search > 0 and sorted = 1 then 1 end)    as search_sort_visits,
                count(case when auto_suggest_search = 1 then 1 end)                            as auto_search_visits,
                count(case when search + auto_suggest_search > 0 then 1 end)                   as search_visits,
                count(case when search = 1 then 1 end)                                         as search_non_auto_visits,
                count(case when search + auto_suggest_search > 0 and item_page = 1 then 1 end) as search_and_item_page_visits,
                count(case
                          when search + auto_suggest_search > 0 and item_page = 1 and download = 1
                              then 1 end)                                                      as search_and_item_page_and_dl_visits,
                count(case
                          when search + auto_suggest_search > 0 and item_page = 1 and download = 1 and filter = 1
                              then 1 end)                                                      as search_and_item_page_and_dl_visits_filter,
                count(case
                          when search + auto_suggest_search > 0 and item_page = 1 and download = 1 and sorted = 1
                              then 1 end)                                                      as search_and_item_page_and_dl_visits_sort,
                count(case when search + auto_suggest_search > 0 and download = 1 then 1 end)  as search_and_dl_visits,
                count(case when search + auto_suggest_search = 0 and download = 1 then 1 end)  as no_search_dl_visits,
                count(case
                          when search + auto_suggest_search = 0 and download = 1 and filter = 1
                              then 1 end)                                                      as no_search_filter_dl_visits
         from pric
         group by 1, 2, 3) UNPIVOT (
                                    hits FOR stage IN (visits,
                                  search_visits,
                                  search_non_auto_visits,
                                  auto_search_visits,
                                  search_filter_visits,
                                  search_sort_visits,
                                  search_and_item_page_visits,
                                  search_and_item_page_and_dl_visits,
                                  search_and_item_page_and_dl_visits_filter,
                                  search_and_item_page_and_dl_visits_sort,
                                  search_and_dl_visits,
                                  no_search_dl_visits,
                                  no_search_filter_dl_visits)
    );

select *
from elements.rpt_elements_user_sessions_all
limit 100

drop table if exists smet_two;
create temporary table smet_two as
with pric as (
    select
        --ev.fullvisitorid,
        ev.sessionid,
        date(date)                                                                                    as date_day,
        max(case when user_uuid notnull then 1 end)                                                   as logged_in,
        max(case
                when (ev.hits_eventinfo_eventaction = 'submit'
                    and ev.hits_eventinfo_eventcategory = 'header-search-form'
                    ) then 1 end)                                                                     as search,
        max(case
                when (split_part(ev.hits_eventinfo_eventlabel, '-', 1) = 'refinement'
                    ) then 1 end)                                                                     as filter,
        max(case
                when (split_part(ev.hits_eventinfo_eventlabel, '-', 1) = 'sort'
                    ) then 1 end)                                                                     as sorted,

        max(case
                when hits_eventinfo_eventaction = 'click;a'
                    and split_part(hits_eventinfo_eventcategory, ';', 1) = 'item-results' then 1 end) as item_page,
        max(case
                when hits_eventinfo_eventaction = 'license with download'
                    then 1 end)                                                                       as download

    from webanalytics.ds_bq_events_elements ev

    where ev.date between 20211201 and 20220131

    group by 1, 2)
select *
from (
         select date_day,
                logged_in,
                count(*)                                                                                 as visits,
                count(case when search = 1 and filter = 1 then 1 end)                                    as search_filter_visits,
                count(case when search = 1 and sorted = 1 then 1 end)                                    as search_sort_visits,
                count(case when search = 1 then 1 end)                                                   as search_visits,
                count(case when search = 1 and item_page = 1 then 1 end)                                 as search_and_item_page_visits,
                count(case when search = 1 and item_page = 1 and download = 1 then 1 end)                as search_and_item_page_and_dl_visits,
                count(case when search = 1 and item_page = 1 and download = 1 and filter = 1 then 1 end) as search_and_item_page_and_dl_visits_filter,
                count(case when search = 1 and item_page = 1 and download = 1 and sorted = 1 then 1 end) as search_and_item_page_and_dl_visits_sort,
                count(case when search = 1 and download = 1 then 1 end)                                  as search_and_dl_visits,
                count(case when search isnull and download = 1 then 1 end)                               as no_search_dl_visits,
                count(case when search isnull and download = 1 and filter = 1 then 1 end)                as no_search_filter_dl_visits
         from pric
         group by 1, 2) UNPIVOT (
                                 hits FOR stage IN (visits,
                                  search_visits,
                                  search_filter_visits,
                                  search_sort_visits,
                                  search_and_item_page_visits,
                                  search_and_item_page_and_dl_visits,
                                  search_and_item_page_and_dl_visits_filter,
                                  search_and_item_page_and_dl_visits_sort,
                                  search_and_dl_visits,
                                  no_search_dl_visits,
                                  no_search_filter_dl_visits)
    );

drop table if exists smet_three;
create temporary table smet_three as
with pric as (
    select
        --ev.fullvisitorid,
        ev.visitid,
        date(date)                                                                                    as date_day,

        max(case
                when (ev.hits_eventinfo_eventaction = 'submit'
                    and ev.hits_eventinfo_eventcategory = 'header-search-form'
                    ) then 1 end)                                                                     as search,

        max(case
                when hits_eventinfo_eventaction = 'click;a'
                    and split_part(hits_eventinfo_eventcategory, ';', 1) = 'item-results' then 1 end) as item_page,
        max(case
                when hits_eventinfo_eventaction = 'license with download'
                    then 1 end)                                                                       as download

    from webanalytics.ds_bq_events_elements ev

    where ev.date between 20210801 and 20211030

    group by 1, 2)
select *
from (
         select date_day,
                count(*)                                                                  as visits,
                count(case when search = 1 then 1 end)                                    as search_visits,
                count(case when search = 1 and item_page = 1 then 1 end)                  as search_and_item_page_visits,
                count(case when search = 1 and item_page = 1 and download = 1 then 1 end) as search_and_item_page_and_dl_visits,
                count(case when search = 1 and item_page = 1 then 1 end)                  as search_and_dl_visits,
                count(case when search isnull and download = 1 then 1 end)                as no_search_dl_visits
         from pric
         group by 1) UNPIVOT (
                              hits FOR stage IN (visits, search_visits, search_and_item_page_visits, search_and_item_page_and_dl_visits, search_and_dl_visits, no_search_dl_visits)
    );
drop table if exists smet_four;
create temporary table smet_four as
with pric as (
    select
        --ev.fullvisitorid,
        ev.visitid,
        date(date)                                                                                    as date_day,

        max(case
                when (ev.hits_eventinfo_eventaction = 'submit'
                    and ev.hits_eventinfo_eventcategory = 'header-search-form'
                    ) then 1 end)                                                                     as search,

        max(case
                when hits_eventinfo_eventaction = 'click;a'
                    and split_part(hits_eventinfo_eventcategory, ';', 1) = 'item-results' then 1 end) as item_page,
        max(case
                when hits_eventinfo_eventaction = 'license with download'
                    then 1 end)                                                                       as download

    from webanalytics.ds_bq_events_elements ev

    where ev.date between 20211101 and 20211131

    group by 1, 2)
select *
from (
         select date_day,
                count(*)                                                                  as visits,
                count(case when search = 1 then 1 end)                                    as search_visits,
                count(case when search = 1 and item_page = 1 then 1 end)                  as search_and_item_page_visits,
                count(case when search = 1 and item_page = 1 and download = 1 then 1 end) as search_and_item_page_and_dl_visits,
                count(case when search = 1 and item_page = 1 then 1 end)                  as search_and_dl_visits,
                count(case when search isnull and download = 1 then 1 end)                as no_search_dl_visits
         from pric
         group by 1) UNPIVOT (
                              hits FOR stage IN (visits, search_visits, search_and_item_page_visits, search_and_item_page_and_dl_visits, search_and_dl_visits, no_search_dl_visits)
    );

drop table if exists smet_five;
create temporary table smet_five as
with pric as (
    select
        --ev.fullvisitorid,
        ev.visitid,
        date(date)                                                                                    as date_day,

        max(case
                when (ev.hits_eventinfo_eventaction = 'submit'
                    and ev.hits_eventinfo_eventcategory = 'header-search-form'
                    ) then 1 end)                                                                     as search,

        max(case
                when hits_eventinfo_eventaction = 'click;a'
                    and split_part(hits_eventinfo_eventcategory, ';', 1) = 'item-results' then 1 end) as item_page,
        max(case
                when hits_eventinfo_eventaction = 'license with download'
                    then 1 end)                                                                       as download

    from webanalytics.ds_bq_events_elements ev

    where ev.date between 20210501 and 20210731

    group by 1, 2)
select *
from (
         select date_day,
                count(*)                                                                  as visits,
                count(case when search = 1 then 1 end)                                    as search_visits,
                count(case when search = 1 and item_page = 1 then 1 end)                  as search_and_item_page_visits,
                count(case when search = 1 and item_page = 1 and download = 1 then 1 end) as search_and_item_page_and_dl_visits,
                count(case when search = 1 and item_page = 1 then 1 end)                  as search_and_dl_visits,
                count(case when search isnull and download = 1 then 1 end)                as no_search_dl_visits
         from pric
         group by 1) UNPIVOT (
                              hits FOR stage IN (visits, search_visits, search_and_item_page_visits, search_and_item_page_and_dl_visits, search_and_dl_visits, no_search_dl_visits)
    );

drop table if exists smet_six;
create temporary table smet_six as
with pric as (
    select
        --ev.fullvisitorid,
        ev.visitid,
        date(date)                                                                                    as date_day,

        max(case
                when (ev.hits_eventinfo_eventaction = 'submit'
                    and ev.hits_eventinfo_eventcategory = 'header-search-form'
                    ) then 1 end)                                                                     as search,

        max(case
                when hits_eventinfo_eventaction = 'click;a'
                    and split_part(hits_eventinfo_eventcategory, ';', 1) = 'item-results' then 1 end) as item_page,
        max(case
                when hits_eventinfo_eventaction = 'license with download'
                    then 1 end)                                                                       as download

    from webanalytics.ds_bq_events_elements ev

    where ev.date between 20210201 and 20210430

    group by 1, 2)
select *
from (
         select date_day,
                count(*)                                                                  as visits,
                count(case when search = 1 then 1 end)                                    as search_visits,
                count(case when search = 1 and item_page = 1 then 1 end)                  as search_and_item_page_visits,
                count(case when search = 1 and item_page = 1 and download = 1 then 1 end) as search_and_item_page_and_dl_visits,
                count(case when search = 1 and item_page = 1 then 1 end)                  as search_and_dl_visits,
                count(case when search isnull and download = 1 then 1 end)                as no_search_dl_visits
         from pric
         group by 1) UNPIVOT (
                              hits FOR stage IN (visits, search_visits, search_and_item_page_visits, search_and_item_page_and_dl_visits, search_and_dl_visits, no_search_dl_visits)
    );

drop table if exists smet_seven;
create temporary table smet_seven as
with pric as (
    select
        --ev.fullvisitorid,
        ev.visitid,
        date(date)                                                                                    as date_day,

        max(case
                when (ev.hits_eventinfo_eventaction = 'submit'
                    and ev.hits_eventinfo_eventcategory = 'header-search-form'
                    ) then 1 end)                                                                     as search,

        max(case
                when hits_eventinfo_eventaction = 'click;a'
                    and split_part(hits_eventinfo_eventcategory, ';', 1) = 'item-results' then 1 end) as item_page,
        max(case
                when hits_eventinfo_eventaction = 'license with download'
                    then 1 end)                                                                       as download

    from webanalytics.ds_bq_events_elements ev

    where ev.date between 20210101 and 20210201

    group by 1, 2)
select *
from (
         select date_day,
                count(*)                                                                  as visits,
                count(case when search = 1 then 1 end)                                    as search_visits,
                count(case when search = 1 and item_page = 1 then 1 end)                  as search_and_item_page_visits,
                count(case when search = 1 and item_page = 1 and download = 1 then 1 end) as search_and_item_page_and_dl_visits,
                count(case when search = 1 and item_page = 1 then 1 end)                  as search_and_dl_visits,
                count(case when search isnull and download = 1 then 1 end)                as no_search_dl_visits
         from pric
         group by 1) UNPIVOT (
                              hits FOR stage IN (visits, search_visits, search_and_item_page_visits, search_and_item_page_and_dl_visits, search_and_dl_visits, no_search_dl_visits)
    );

select *
from smet_one
union all
select *
from smet_two
union all
select *
from smet_three
union all
select *
from smet_four
union all
select *
from smet_five
union all
select *
from smet_six
union all
select *
from smet_seven

select logged, count(*) as ct
from (
         select sessionid, max(case when user_uuid notnull then 1 end) as logged
         from webanalytics.ds_bq_events_elements
         where date_aest :: date = '2022-03-01'
         group by 1)
group by 1;


select *
from google_analytics.raw_elements_search_analysis
limit 100;

select date,
       category,
       count(distinct fullvisitorid)                                                      as searchers,
       count(distinct concat(session_id, concat(category, searchterms)))                  as searches,
       count(distinct case when sub_from_search > 0 then fullvisitorid end)               as subs_from_search,
       count(distinct case
                          when dls > 0
                              then concat(session_id, concat(category, searchterms)) end) as dls_from_search,
       count(distinct case
                          when item_clicks > 0
                              then concat(session_id, concat(category, searchterms)) end) as clicks_from_search,
       count(distinct case
                          when item_audio_plays > 0
                              then concat(session_id, concat(category, searchterms)) end) as audio_plays_from_search,
       count(distinct case
                          when items_opened_full > 0
                              then concat(session_id, concat(category, searchterms)) end) as video_full_opens_from_search,
       count(distinct case
                          when searchresults_max = 0
                              then concat(session_id, concat(category, searchterms)) end) as searches_with_zero_results,
       count(distinct case
                          when filtered > 0
                              then concat(session_id, concat(category, searchterms)) end) as searches_filtered,
       count(distinct case
                          when sorted > 0
                              then concat(session_id, concat(category, searchterms)) end) as searches_sorted,
       count(distinct case
                          when tot_pages > 1
                              then concat(session_id, concat(category, searchterms)) end) as searches_paginated,
       sum(tot_pages)                                                                     as total_pages,
       sum(impressions)                                                                   as total_impressions
from google_analytics.elements_search_analysis
where date >= '2020-08-05'
  and category <> 'user-portfolio' -- remove author pages
group by date, category
limit 100;


select total_m

select getdate_aest() :: date;


select subscription_start_date :: date                                               as date_day,
       case when trial_period_started_at_aet notnull then 'trial' else 'regular' end as sub_type,
       plan_type,
       count(distinct sso_user_id)                                                   as subs

from elements.dim_elements_subscription
where subscription_start_date >= '2022-01-01'
group by 1, 2, 3;


select date(date)                  as day_date,
       is_authenticated,
       count(distinct e.sessionid) as sessions

from webanalytics.ds_bq_events_elements e
         left join elements.rpt_elements_user_sessions_all s on e.sessionid = s.sessionid
where date(date) >= '2022-03-01'
group by 1, 2;


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
                               and a.date between '2022-02-20' and '2022-03-23'
                               and se.date between 20220220 and 20220323
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
                          count(distinct item_license_id) as q
                   from elements.ds_elements_item_downloads dl
                            join dim_users u
                                 on dl.user_id = u.elements_id and dl.download_started_at :: date >= '2022-02-20'
                   group by 1),
     paid as
         (select s.sso_user_id,
                 case
                     when max(subscription_start_date :: date) < (getdate_aest() ::date - 10) then '11_days_or_older'
                     when max(subscription_start_date :: date) < (getdate_aest() ::date - 6) then '7_10_days'
                     else '7_days_or_younger' end      as data_age,
                 sum(total_amount)                     as paym,
                 sum(total_amount - tax_amount)        as rev,
                 sum(total_amount - tax_amount) * 0.48 as net_rev
          from elements.fact_elements_subscription_transactions t
                   join elements.dim_elements_subscription s on t.dim_subscription_key = s.dim_subscription_key
              and date(dim_date_key) >= '2022-02-20'
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
               AND subscription_start_date :: date >= '2022-02-20'
             group by 1),

     refunds as
         (select sso_user_id, fact.dim_subscription_key
          from elements.fact_elements_subscription_transactions fact
                   inner join elements.dim_elements_transaction_attributes ta
                              on fact.dim_elements_transaction_key = ta.dim_elements_transaction_key
                   inner join elements.dim_elements_subscription sub
                              on fact.dim_subscription_key = sub.dim_subscription_key
                                  and fact.dim_date_key >= 20220220
          where ta.transaction_type = 'Refund'
          group by 1, 2),

     clean_sso as (
         select c.sso_user_id
                                                                                                   as sso_key,
                max(country)                                                                       as country,
                var                                                                                as variant,
                reallocation,
                referer,
                min(case
                        when first_successful_payment_date_aet between '2022-02-20' and '2022-03-23'
                            then first_successful_payment_date_aet end)                            as p_date,
                case
                    when max(subscription_start_date :: date) < (current_date - 10) then '11_days_or_older'
                    when max(subscription_start_date :: date) < (current_date - 6) then '7_10_days'
                    else '7_days_or_younger' end                                                   as data_age,
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
                             d.trial_period_started_at_aet isnullxghas
                            then 1 end) as non_free_trial_signups,
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
                max(net_rev)                                                                       as net_revenue,

                max(case
                        when rf.sso_user_id notnull then 1 end)                                    as refunds_trials,
                max(case when trial_period_ends_at_aet :: date > current_date then 1 end)          as current_trials
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
                  left join failed_payments f on c.sso_user_id = f.sso_user_id
                  left join paid p on c.sso_user_id = p.sso_user_id and data_age = p.data_age
         group by 1, 3, 4, 5
     )

select variant                                                                   as variant,
       country,
       plan_type,
       reallocation,
       referer,
       data_age,
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
       sum(case
               when plan_type in ('Annual', 'student_annual')
                   then (revenue * (datediff('day', p_date, current_date) / 365))
               else revenue end)                                                 as adjusted_rev,
       count(refunds_trials)                                                     as refunds,
       count(current_trials)                                                     as current_trials

from clean_sso
group by 1, 2, 3, 4, 5, 6, 7, 8;
w

with a as (
    select CASE
               WHEN n.country in ('United States', 'United Kingdom', 'Germany',
                                  'Canada', 'Australia', 'France', 'Italy', 'Spain',
                                  'Netherlands', 'Brazil',
                                  'India', 'South Korea', 'Turkey', 'Switzerland',
                                  'Japan', 'Spain')
                   then n.country
               when n.country in ('Argentina', 'Bolivia', 'Chile',
                                  'Colombia', 'Costa Rica', 'Cuba', 'Ecuador', 'Mexico',
                                  'Paraguay', 'Uruguay',
                                  'Venezuela') then 'LATAM'
               when n.country in ('Belarus', 'Kazakhstan', 'Russia', 'Ukraine')
                   then 'RU'
               when n.country = 'Unknown' then n.country
               else 'ROW' end              as country,
           nvl(channel, 'oops')            as channel,
--        nvl(sub_channel, 'oops') as sub_channel,
           subscription_start_date :: date as date_day,
           count(distinct sso_user_id)     as new_paid_subs
    from elements.dim_elements_subscription s
             left join market.dim_geo_network n on s.dim_geo_network_key = n.dim_geo_network_key
             left join elements.dim_elements_channel c on s.dim_elements_channel_key = c.dim_elements_channel_key
    where has_successful_payment = true
    group by 1, 2, 3),
     b as (
         select CASE
                    WHEN n.country in ('United States', 'United Kingdom', 'Germany',
                                       'Canada', 'Australia', 'France', 'Italy', 'Spain',
                                       'Netherlands', 'Brazil',
                                       'India', 'South Korea', 'Turkey', 'Switzerland',
                                       'Japan', 'Spain')
                        then n.country
                    when n.country in ('Argentina', 'Bolivia', 'Chile',
                                       'Colombia', 'Costa Rica', 'Cuba', 'Ecuador', 'Mexico',
                                       'Paraguay', 'Uruguay',
                                       'Venezuela') then 'LATAM'
                    when n.country in ('Belarus', 'Kazakhstan', 'Russia', 'Ukraine')
                        then 'RU'
                    when n.country = 'Unknown' then n.country
                    else 'ROW' end          as country,
                nvl(channel, 'oops')        as channel,
--        nvl(sub_channel, 'oops') as sub_channel,
                termination_date :: date    as date_day,
                count(distinct sso_user_id) as terminated_paid_subs
         from elements.dim_elements_subscription s
                  left join market.dim_geo_network n on s.dim_geo_network_key = n.dim_geo_network_key
                  left join elements.dim_elements_channel c on s.dim_elements_channel_key = c.dim_elements_channel_key
         where has_successful_payment = true
         group by 1, 2, 3)

select *
from a
         left join b
                   using (country, channel, date_day)
;


select has_successful_payment,
       count(distinct sso_user_id)
from elements.dim_elements_subscription s
where termination_date isnull
group by 1


select plan_type
from elements.dim_elements_subscription s
group by 1
--          left join elements.dim_elements_channel c on s.dim_elements_channel_key = c.dim_elements_channel_key

limit 100;

with enrollments as
         (
             select sso_user_id,
                    variant,
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
--                             else 'ROW' end)                                       as country,
                    min(timestamp 'epoch' + visitstarttime * interval '1 second') as exp_date,
--                     split_part(min(sc.dss_update_time || '|' || channel), '|', 2) as referer
             from webanalytics.ds_bq_abtesting_enrolments_elements a
                      join webanalytics.ds_bq_sessions_elements se
                           on a.fullvisitorid = se.fullvisitorid::varchar and a.visitid = se.visitid
                               and a.date between '2022-02-20' and '2022-03-23'
                               and se.date between 20220220 and 20220323
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
--                 max(referer)                                                                           as referer,
--                 max(country)                                                                           as country,
                split_part(min(exp_date || '|' || variant), '|', 2)                                    as var,
                max(case when m.sso_user_id is not null then 'reallocated' else 'not_reallocated' end) as reallocation,
                min(CAST(DATE(exp_date) AS DATE))                                                      as exp_day_date
         from enrollments a
                  left join multiple_allocations m on a.sso_user_id = m.sso_user_id
         group by 1),
     a as (
         select CASE
                    WHEN n.country in ('United States', 'United Kingdom', 'Germany',
                                       'Canada', 'Australia', 'France', 'Italy', 'Spain',
                                       'Netherlands', 'Brazil',
                                       'India', 'South Korea', 'Turkey', 'Switzerland',
                                       'Japan', 'Spain')
                        then n.country
                    when n.country in ('Argentina', 'Bolivia', 'Chile',
                                       'Colombia', 'Costa Rica', 'Cuba', 'Ecuador', 'Mexico',
                                       'Paraguay', 'Uruguay',
                                       'Venezuela') then 'LATAM'
                    when n.country in ('Belarus', 'Kazakhstan', 'Russia', 'Ukraine')
                        then 'RU'
                    when n.country = 'Unknown' then n.country
                    else 'ROW' end              as country,
                nvl(channel, 'oops')            as channel,

                case
                    when s.current_plan like '%enterprise%'
                        then 'enterprise'
                    when s.current_plan like '%team%' then 'team'
                    when s.current_plan like '%student_annual%'
                        then 'student_annual'
                    when s.current_plan like '%student_monthly%'
                        then 'student_monthly'
                    else s.plan_type end        as plan_type,
                subscription_start_date :: date as date_day,
                count(distinct sso_user_id)     as new_paid_subs
         from elements.dim_elements_subscription s
                  left join market.dim_geo_network n on s.dim_geo_network_key = n.dim_geo_network_key
                  left join elements.dim_elements_channel c on s.dim_elements_channel_key = c.dim_elements_channel_key
         where has_successful_payment = true
         group by 1, 2, 3, 4),
     b as (
         select CASE
                    WHEN n.country in ('United States', 'United Kingdom', 'Germany',
                                       'Canada', 'Australia', 'France', 'Italy', 'Spain',
                                       'Netherlands', 'Brazil',
                                       'India', 'South Korea', 'Turkey', 'Switzerland',
                                       'Japan', 'Spain')
                        then n.country
                    when n.country in ('Argentina', 'Bolivia', 'Chile',
                                       'Colombia', 'Costa Rica', 'Cuba', 'Ecuador', 'Mexico',
                                       'Paraguay', 'Uruguay',
                                       'Venezuela') then 'LATAM'
                    when n.country in ('Belarus', 'Kazakhstan', 'Russia', 'Ukraine')
                        then 'RU'
                    when n.country = 'Unknown' then n.country
                    else 'ROW' end          as country,
                nvl(channel, 'oops')        as channel,
                case
                    when s.current_plan like '%enterprise%'
                        then 'enterprise'
                    when s.current_plan like '%team%' then 'team'
                    when s.current_plan like '%student_annual%'
                        then 'student_annual'
                    when s.current_plan like '%student_monthly%'
                        then 'student_monthly'
                    else s.plan_type end    as plan_type,
                termination_date :: date    as date_day,
                count(distinct sso_user_id) as terminated_paid_subs
         from elements.dim_elements_subscription s
                  left join market.dim_geo_network n on s.dim_geo_network_key = n.dim_geo_network_key
                  left join elements.dim_elements_channel c on s.dim_elements_channel_key = c.dim_elements_channel_key
         where has_successful_payment = true
         group by 1, 2, 3, 4)

select *
from a
         left join b
                   using (plan_type, country, channel, date_day);

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
                      and ev.date > 20220412
                      and experiment_id = 'gZewu4I5QzaajQXxKH_iEg'
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
                      and ev.date > 20220412
                      and experiment_id = 'gZewu4I5QzaajQXxKH_iEg'
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
                    max(case when s.sso_user_id notnull then 1 end)               as converted
             from pric ev
                      join webanalytics.ds_bq_sessions_elements se
                           on ev.fullvisitorid::varchar = se.fullvisitorid::varchar
                               and ev.visitid::varchar = se.visitid::varchar
                      left join elements.rpt_elements_session_channel sc on sc.sessionid = se.sessionid
                      left join elements.dim_elements_subscription s on se.user_uuid = s.sso_user_id
                 and subscription_start_date :: date >= '2022-04-12'
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
    );

--- darren stuff

with pag as (select ev.fullvisitorid,
                    hits_page_pagepath as tpage,
                    count(*)           as ct
             from webanalytics.ds_bq_events_elements ev
             where ev.date = 20220227
             group by 1, 2)
        ,
     pric as (
         select ev.fullvisitorid,
                ev.visitid,
                variant,
                max(ct || '|' || tpage) as top_page,
                count(case
                          when hits_eventinfo_eventaction = 'license with download'
                              then 1
                          else 0 end)   as downloads,
                count(case
                          when hits_eventinfo_eventaction = 'click;a'
                              and split_part(hits_eventinfo_eventcategory, ';', 1) = 'item-results' then 1
                          else 0 end)   as item_pages,
                count(distinct case
                                   when hits_eventinfo_eventaction = 'click;a'
                                       and split_part(hits_eventinfo_eventcategory, ';', 1) = 'item-results'
                                       then split_part(hits_eventinfo_eventcategory, ';', 2) = 'item-results'
                    end)                as categories_viewed


         from webanalytics.ds_bq_events_elements ev
                  join pag using (fullvisitorid)
                  join webanalytics.ds_bq_abtesting_enrolments_elements a
                       on a.fullvisitorid::varchar = ev.fullvisitorid::varchar
                           and a.visitid::varchar = ev.visitid::varchar
                           and ev.date = 20220227 -- between 20220220 and 20220323
                           and experiment_id = 'FTr-7_qaRO-UzNOru4HW1Q'
         group by 1, 2, 3
     ),;


with fv as
         (
             select sso_user_id,
--                     variant,
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
--                             else 'ROW' end)                                                           as country,
                    max(case when has_successful_payment = true then 1 else 0 end)                    as paid_converted,
                    max(case when trial_period_started_at_aet notnull then 1 else 0 end)              as trial,
                    max(case when is_first_subscription = true then 1 else 0 end)                     as new_u,
                    datediff(day, min(subscription_start_date),
                             max(case
                                     when termination_date isnull then current_date
                                     else termination_date end))                                      as sub_days,
--            max(ct || '|' || tpage) as top_page,
                    count(case
                              when hits_eventinfo_eventaction = 'license with download'
                                  then 1 end)                                                         as downloads,
                    max(case
                            when hits_eventinfo_eventlabel = 'skip'
                                then 1 end)                                                           as skipped_onboarding,
                    max(case
                            when hits_eventinfo_eventcategory = 'onboarding-survey' and
                                 hits_eventinfo_eventaction = 'submit'
                                then 1 end)                                                           as submitted_onboarding,

                    count(case
                              when hits_page_pagepath like '%account/downloads'
                                  then 1 end)                                                         as my_downloads_page,
                    count(case
                              when hits_page_pagepath = '/collections'
                                  then 1 end)                                                         as my_collections_page,
                    count(case
                              when hits_eventinfo_eventaction = 'click;a'
                                  and split_part(hits_eventinfo_eventcategory, ';', 1) = 'item-results'
                                  then 1 end)                                                         as item_pages,
                    count(distinct case
                                       when hits_eventinfo_eventaction = 'click;a'
                                           and split_part(hits_eventinfo_eventcategory, ';', 1) = 'item-results'
                                           then split_part(hits_eventinfo_eventcategory, ';', 2) end) as categories_viewed

             from webanalytics.ds_bq_events_elements ev
                      join webanalytics.ds_bq_sessions_elements se
                           on ev.fullvisitorid::varchar = se.fullvisitorid::varchar
                               and ev.visitid::varchar = se.visitid::varchar
                               and ev.date between 20220221 and 20220227
                               and se.date between 20220221 and 20220227
                      join elements.dim_elements_subscription s on se.user_uuid = s.sso_user_id
                 and subscription_start_date :: date >= '2022-02-21'
--                  and has_successful_payment = true
             group by 1
         ),

     tp as
         (
             select paid_converted,
                    trial,
                    new_u,
                    hits_page_pagepath,
                    count(*) as ct

             from webanalytics.ds_bq_events_elements ev
                      join webanalytics.ds_bq_sessions_elements se
                           on ev.fullvisitorid::varchar = se.fullvisitorid::varchar
                               and ev.visitid::varchar = se.visitid::varchar
                               and ev.date between 20220221 and 20220227
                               and se.date between 20220221 and 20220227
                      join fv s on se.user_uuid = s.sso_user_id
             group by 1, 2, 3, 4
             having count(*) > 1000
         ),
     lics as (select b.sso_uuid,
                     case when dim_elements_projects_key = 0 then 'no_project' else 'project' end as dlt
              from elements.fact_elements_user_licensing a
                       join elements.dim_elements_license_plans b
                            on a.dim_elements_license_plans_key = b.dim_elements_license_plans_key
              where a.licensed_at::date between '2022-02-21' and '2022-02-27'
--      and dim_elements_projects_key > 0
--      and downloads > 0
              group by 1, 2),

     a as (
         select case
                    when new_u = 0 then 'previous_subscriber'
                    when trial = 1 and paid_converted = 1 then 'converted_trial'
                    when trial = 1 and paid_converted = 0 then 'churned_trial'
                    when trial = 0 and paid_converted = 1 then 'converted_no_trial'
                    end                                                       as bucket,
                count(*)                                                      as users,
                sum(sub_days)                                                 as days_subbed,
                sum(downloads)                                                as downloads,
                sum(item_pages)                                               as item_pages,
                sum(categories_viewed)                                        as categories_viewed,
                count(case when categories_viewed > 1 then 1 end)             as more_than_2_categories,
                sum(skipped_onboarding)                                       as skipped_onboarding,
                sum(submitted_onboarding)                                     as submitted_onboarding,
                count(case when my_downloads_page > 1 then 1 end)             as mydownloads_engaged,
                count(case when my_collections_page > 1 then 1 end)           as my_collections_page_engaged,
                count(case when dlt = 'project' then 1 end)                   as licensers,
                count(distinct case when dlt = 'project' then l.sso_uuid end) as licensers_utest,
                count(case when dlt = 'no_project' then 1 end)                as noproject_licensers


         from fv a
--          left join tp t using (paid_converted, trial, new_u)
                  left join lics l on l.sso_uuid = a.sso_user_id

         group by 1
         having count(*) > 100)
select *
from a UNPIVOT (hits FOR stage IN (


days_subbed,
downloads,
item_pages,
categories_viewed,
more_than_2_categories,
skipped_onboarding,
    submitted_onboarding,
mydownloads_engaged,
my_collections_page_engaged,
licensers,
    licensers_utest,
noproject_licensers));

select
;


select dim_users_project_owner_key
from elements.fact_elements_user_licensing
limit 100


select *
from elements.ds_elements_item_licenses
limit 100

select hits_eventinfo_eventaction,
       hits_eventinfo_eventcategory,
       count(*)
from webanalytics.ds_bq_events_elements ev
where ev.date = 20220227
  and hits_eventinfo_eventlabel = 'skip'
group by 1, 2
--limit 100;

    count
(distinct case
    when hits_eventinfo_eventaction = 'click;a'
    and split_part(hits_eventinfo_eventcategory, ';', 1) = 'item-results'
    then split_part(hits_eventinfo_eventcategory, ';', 2) = 'item-results'
    end)
as categories_viewed;


select

from webanalytics.map Event Action
click;
a
Event Category
downloads
Event Label
skip


Event Action
change: downloads
Event Category
onboarding-survey
Event Label
Video, animation & 3D productions

Event Action
change: subscription
Event Category
onboarding-survey
Event Label
My own business / freelance


Event Action
click;
button
Event Category
reasons
Event Label
next

Event Action
change: reasons
Event Category
onboarding-survey
Event Label
Avoid copyright issues;



select b.sso_uuid,
       case when dim_elements_projects_key = 0 then 'no project' else 'project' end as dlt,
       count(*)                                                                     as lics
from elements.fact_elements_user_licensing a
         join elements.dim_elements_license_plans b
              on a.dim_elements_license_plans_key = b.dim_elements_license_plans_key
where a.licensed_at::date = '2022-04-01'
group by 1, 2
--limit 100


select *
from elements.fact_elements_user_licensing a
limit 100;

select *, item_tag_clicks * 1.00 / item_page_from_gallery

from (
         select date,
                count(case when hits_eventinfo_eventcategory = 'item-tags' then 1 end) as item_tag_clicks,
                count(case
                          when split_part(hits_eventinfo_eventcategory, ';', 1) = 'item-results'
                              then 1 end)                                              as item_page_from_gallery

         from webanalytics.ds_bq_events_elements
         where
-- hits_eventinfo_eventcategory = 'item-tags'
--    hits_eventinfo_eventaction = 'submit'
date between 20220501 and 20220511
         group by 1
         order by 1 desc)


with dats as (
    select subscription_start_date :: date as date_day
    from elements.dim_elements_subscription
    where subscription_start_date :: date between '2020-01-01' and '2022-05-10'
    group by 1),

     el as (
         select date_day,
                count(distinct case
                                   when subscription_start_date <= date_day and
                                        (termination_date > date_day or termination_date isnull)
                                       and has_successful_payment = true then sso_user_id end) as active_subs

         from elements.dim_elements_subscription e
                  cross join dats d
         group by 1),

     eln as (
         select subscription_start_date :: date as subscription_start_date,

                count(distinct sso_user_id)     as daily_paying_subs_added


         from elements.dim_elements_subscription e
         where has_successful_payment = true
           and subscription_start_date :: date between '2020-01-01' and '2022-05-10'
         group by 1),

     elt as (
         select termination_date :: date    as termination_date,

                count(distinct sso_user_id) as daily_paying_subs_terminated

         from elements.dim_elements_subscription e
         where has_successful_payment = true
           and termination_date :: date between '2020-01-01' and '2022-05-10'
         group by 1),

     mar as (select dd.calendar_date
                  , count(distinct transaction_key) as transactions
                  , sum(gross_revenue)              as gross_revenue
             from market.fact_marketplaces_sales a
                      join dim_date dd on a.dim_date_key = dd.dim_date_key
             where calendar_date between '2020-01-01' and '2022-05-10'
             group by 1)

select date_day,
       active_subs   as active_subs_elements,
       daily_paying_subs_added,
       daily_paying_subs_terminated,
       transactions  as transactions_market,
       gross_revenue as gross_revenue_market
from el
         join mar on date_day = calendar_date
         join eln on eln.subscription_start_date = date_day
         join elt on elt.termination_date = date_day;



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
                               and a.date > '2022-04-12'
                               and se.date > 20220412
                      join elements.dim_elements_subscription s on se.user_uuid = s.sso_user_id
                      left join elements.rpt_elements_session_channel sc on se.sessionid = sc.sessionid

             where experiment_id = 'gZewu4I5QzaajQXxKH_iEg'
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
                          count(distinct item_license_id) as q
                   from elements.ds_elements_item_downloads dl
                            join dim_users u
                                 on dl.user_id = u.elements_id and dl.download_started_at :: date >= '2022-04-12'
                   group by 1),
     paid as
         (select s.sso_user_id,
                 case
                     when max(subscription_start_date :: date) < (getdate_aest() ::date - 10) then '11_days_or_older'
                     when max(subscription_start_date :: date) < (getdate_aest() ::date - 6) then '7_10_days'
                     else '7_days_or_younger' end      as data_age,
                 sum(total_amount)                     as paym,
                 sum(total_amount - tax_amount)        as rev,
                 sum(total_amount - tax_amount) * 0.48 as net_rev
          from elements.fact_elements_subscription_transactions t
                   join elements.dim_elements_subscription s on t.dim_subscription_key = s.dim_subscription_key
              and date(dim_date_key) >= '2022-04-12'
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
               AND subscription_start_date :: date >= '2022-04-12'
             group by 1),

     refunds as
         (select sso_user_id, fact.dim_subscription_key
          from elements.fact_elements_subscription_transactions fact
                   inner join elements.dim_elements_transaction_attributes ta
                              on fact.dim_elements_transaction_key = ta.dim_elements_transaction_key
                   inner join elements.dim_elements_subscription sub
                              on fact.dim_subscription_key = sub.dim_subscription_key
                                  and fact.dim_date_key >= 20220412
          where ta.transaction_type = 'Refund'
          group by 1, 2),

     clean_sso as (
         select c.sso_user_id
                                                                                          as sso_key,
                max(country)                                                              as country,
                var                                                                       as variant,
                reallocation,
                referer,
                case
                    when max(subscription_start_date :: date) >= '2022-04-26' then 'sale'
                    else 'pre_sale' end                                                   as sale_period,
                case
                    when max(subscription_start_date :: date) < (current_date - 10) then '11_days_or_older'
                    when max(subscription_start_date :: date) < (current_date - 6) then '7_10_days'
                    else '7_days_or_younger' end                                          as data_age,
                max(case
                        when d.subscription_start_date :: date <= '2022-04-12' and
                             (termination_date isnull or termination_date > '2022-04-12')
                            then 1 end)                                                   as already_subscribed,
                max(case
                        when d.subscription_start_date > '2022-04-12'
                            and is_first_subscription is true
                            then 1 end)                                                   as new_sub,
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
                                                                    else d.plan_type end), '|',
                           2)                                                             as plan_type,
                max(case
                        when d.subscription_start_date :: date > '2022-04-12' and
                             d.trial_period_started_at_aet isnull
                            then 1 end)                                                   as non_free_trial_signups,
                max(case
                        when d.subscription_start_date :: date > '2022-04-12' and
                             d.trial_period_started_at_aet is not null
                            then 1 end)                                                   as free_trial_signups,
                max(case
                        when d.subscription_start_date :: date > '2022-04-12'
                            then 1 end)                                                   as total_new_signups,
                max(case
                        when d.trial_period_started_at_aet :: date > '2022-04-12' and
                             termination_date isnull and has_successful_payment = true
                            then 1 end)                                                   as trial_sub_remaining,
                max(case
                        when d.subscription_start_date :: date > '2022-04-12' and
                             termination_date isnull and
                             has_successful_payment = true
                            then 1 end)                                                   as total_subs_remaining,
                max(case
                        when d.subscription_start_date :: date > '2022-04-12' and
                             has_successful_payment = true
                            then 1 end)                                                   as true_converted_subs,
                max(case
                        when d.subscription_start_date :: date > '2022-04-12' and
                             termination_date :: date >= '2022-04-12'
                            then 1 end)                                                   as total_new_subs_terminated,
                max(case
                        when d.subscription_start_date :: date <= '2022-04-12' and
                             termination_date :: date > '2022-04-12'
                            then 1 end)                                                   as total_returning_subs_terminated,
                max(case
                        when d.subscription_start_date :: date <= '2022-04-12' and termination_date isnull
                            then 1 end)                                                   as returning_subs_retained,
                max(case
                        when d.subscription_start_date :: date > '2022-04-12' and
                             has_successful_payment = false
                            and f.sso_user_id is not null
                            then 1 end)                                                   as failed_payments,
                max(case
                        when d.trial_period_started_at_aet :: date > '2022-04-12' and
                             last_canceled_at > '2022-04-12'
                            and has_successful_payment = false
                            then 1 end)                                                   as trials_cancellation_unpaid,
                max(case
                        when d.trial_period_started_at_aet :: date > '2022-04-12' and
                             last_canceled_at > '2022-04-12'
                            and has_successful_payment = true
                            then 1 end)                                                   as trials_cancellation_paid,
                max(case
                        when last_canceled_at > '2022-04-12'
                            then 1 end)                                                   as all_cancellation,
                max(q)                                                                    as downloads,
                max(rev)                                                                  as revenue,
                max(net_rev)                                                              as net_revenue,
                max(case
                        when rf.sso_user_id notnull
                            then 1 end)                                                   as refunds_trials,
                max(case when trial_period_ends_at_aet :: date > current_date then 1 end) as current_trials
         from allocations c


                  --              --remove envato users
                  join elements.ds_elements_sso_users sso
                       on sso.id = c.sso_user_id and split_part(email, '@', 2) != 'envato.com'
                  join elements.dim_elements_subscription d
                       on c.sso_user_id = d.sso_user_id
                           and (termination_date isnull or termination_date > '2022-04-12')

                  left join content_t dl on c.sso_user_id = dl.sso_uuid
                  left join refunds rf
                            on d.sso_user_id = rf.sso_user_id and d.dim_subscription_key = rf.dim_subscription_key
                  left join failed_payments f on c.sso_user_id = f.sso_user_id
                  left join paid p on c.sso_user_id = p.sso_user_id and data_age = p.data_age
         group by 1, 3, 4, 5
     )

select variant                                                                   as variant,
       country,
       plan_type,
       reallocation,
       referer,
       data_age,
       sale_period,
       case when already_subscribed = 1 then 'continuing_sub' else 'new_sub' end as returning,
       case when new_sub = 1 then 'first_timer' else 'resubscriber' end          as experiment_new_sub,
       count(*)                                                                  as subscribers,
       count(free_trial_signups)                                                 as free_trial_signups,
       count(non_free_trial_signups)                                             as non_free_trial_signups,
       count(total_new_signups)                                                  as total_new_signups,
       count(trial_sub_remaining)                                                as trial_sub_remaining,
       count(total_subs_remaining)                                               as total_subs_remaining,
       count(true_converted_subs)                                                as initial_converted,
       count(total_new_subs_terminated)                                          as total_new_subs_terminated,
       count(total_returning_subs_terminated)                                    as total_returning_subs_terminated,
       count(returning_subs_retained)                                            as returning_subs_retained,
       count(failed_payments)                                                    as failed_payments,
       count(all_cancellation)                                                   as all_cancellations,
       count(trials_cancellation_unpaid)                                         as cancelled_trial_users_unpaid,
       count(trials_cancellation_paid)                                           as cancelled_trial_users_paid,
       sum(downloads)                                                            as downloads,
       sum(revenue)                                                              as revenue_usd,
       count(refunds_trials)                                                     as refunds,
       count(current_trials)                                                     as current_trials

from clean_sso
group by 1, 2, 3, 4, 5, 6, 7, 8, 9;

with c as (
    select date_aet :: date as date_day,
--        campaign_name,
           sum(impressions) as impressions,
           sum(cost_micros) as cost
    from adwords.ds_ads_campaign_performance
    where date_aet :: date > '2021-01-01'
      and campaign_name like '%elements%'
    group by 1),
     sub as (select subscription_start_date :: date                                             as date_day,
                    channel,
                    count(distinct sso_user_id)                                                 as subs,
                    count(distinct case when is_first_subscription = true then sso_user_id end) as first_subs,
                    count(distinct case when plan_type = 'Annual' then sso_user_id end)         as annuals,
                    count(distinct case when plan_type = 'Monthly' then sso_user_id end)        as monthlys
             from elements.dim_elements_subscription s
                      left join elements.dim_elements_channel c
                                on s.dim_elements_channel_key = c.dim_elements_channel_key
             where has_successful_payment = true
               and subscription_start_date :: date > '2021-01-01'
             group by 1, 2
     )

select *
from sub
         left join c on c.date_day = sub.date_day


select plan_type

from elements.dim_elements_subscription
group by 1
limit 10


select *
from elements.ds_elements_get_feedback_user_mappings
where id = '7f612512-60f2-4906-8888-f22c67cbcd8d'
limit 100


select *
from webanalytics.ds_bq_sessions_elements
where user_uuid = 'c0ac0261-59d2-493e-b716-d71d4d58d396';


select *
from dim_users
where elements_id = 'c0ac0261-59d2-493e-b716-d71d4d58d396'
limit 100;


select *
from elements.ds_elements_sso_users so
join elements.dim_elements_subscription s on sso_user_id = id limit 100
with getu as (
    select c.reference as email,
           id as sso_uuid
    from adria c
             join elements.ds_elements_sso_users so on so.email = c.reference
),
     content_t as (select u.sso_uuid,
                          i.content_type,
                          count(*) as q
                   from elements.ds_elements_item_downloads dl
                            join elements.ds_elements_item_licenses l on dl.item_license_id = l.id
                       and download_started_at :: date > '2022-01-01'
                            join elements.dim_elements_items i on i.item_id = l.item_id
                            join dim_users u
                                 on dl.user_id = u.elements_id and dl.download_started_at :: date > '2022-02-01'
                   group by 1, 2)

select email,
       g.sso_uuid,
       max(country) as country,
                           min(case
               when current_plan like '%enterprise%'
                   then 'enterprise'
               when current_plan like '%team%' then 'team'
               when current_plan like '%student_annual%'
                   then 'student_annual'
               when current_plan like '%student_monthly%'
                   then 'student_monthly'
               else plan_type end        )                                               as plan_type,
       max(case when termination_date isnull then 1 end) as currently_subscribed,
       max(termination_date) as last_termination_date,
       min(subscription_start_date) as first_subscription_date,
       max(subscription_start_date) as last_subscription_date,
       max(case when trial_period_started_at_aet notnull then 'true' else 'false' end) as had_a_free_trial,
       min(trial_period_started_at_aet :: date)                                        as trial_start_date,
       split_part(max(q || '|' || content_type), '|', 2) as top_download,
       sum(q) as downloads


from getu g
         left join elements.dim_elements_subscription e on g.sso_uuid = e.sso_user_id
left join dim_geo_network dgn on e.dim_geo_network_key = dgn.dim_geo_network_key
left join content_t c on g.sso_uuid = c.sso_uuid
group by 1, 2

select *


from elements.dim_elements_subscription
where sso_user_id = '270c7574-279e-423c-b3e6-3d127d5a35dd' Event Action
click;
a
Event Category
page-header-category-navigation;
photos
Event Label
view-all


Event Action
click;
a
Event Category
page-header-category-navigation;
photos
Event Label
category-nav;
photos
Event Value
1;


select date(date),
       count(case
                 when hits_eventinfo_eventcategory = 'page-header-category-navigation;photos'
                     and hits_eventinfo_eventlabel = 'view-all' then 1 end)                   as photos_collections,
       count(case
                 when hits_eventinfo_eventcategory = 'page-header-category-navigation;photos'
                     and hits_eventinfo_eventlabel = 'nav'
                     then 1 end)                                                              as photos_category_top_click,

       count(case
                 when hits_eventinfo_eventcategory = 'page-header-category-navigation;photos'
                     and hits_eventinfo_eventlabel = 'category-nav;photos' then 1 end)        as photos_elements_photos,

       count(case
                 when hits_eventinfo_eventcategory = 'page-header-category-navigation;video-templates'
                     and hits_eventinfo_eventlabel = 'view-all' then 1 end)                   as videos_all,
       count(case
                 when hits_eventinfo_eventcategory = 'page-header-category-navigation;video-templates'
                     and hits_eventinfo_eventlabel = 'nav'
                     then 1 end)                                                              as videos_category_top_click,

       count(case
                 when hits_eventinfo_eventcategory = 'page-header-category-navigation;video-templates'
                     and hits_eventinfo_eventlabel = 'category-nav;After Effects' then 1 end) as videos_after_effects


from webanalytics.ds_bq_events_elements ev
where ev.date between 20220501 and 20220520

group by 1
order by 1;

with a as (
    select sso_user_id,
           dim_subscription_key,
           min(subscription_start_date) as sdate

    from elements.dim_elements_subscription
    where subscription_started_on_trial = true
      and subscription_start_date > '2022-01-01'
      and has_successful_payment = false
    group by 1, 2)

select date_trunc('month', sdate)                    as trial_month,
       datediff(day, sdate, subscription_start_date) as resub_days,
       count(distinct s.dim_subscription_key)        as resubs

from elements.dim_elements_subscription s
         join a on s.sso_user_id = a.sso_user_id and a.dim_subscription_key < s.dim_subscription_key
group by 1, 2;



select date(ev.date) as date_day,
       geonetwork_country    as country,
       CASE
           WHEN geonetwork_country in ('France',
                                       'Slovenia',
                                       'United Kingdom',
                                       'Monaco',
                                       'Moldova',
                                       'Lithuania',
                                       'Serbia',
                                       'Faroe Islands',
                                       'Luxembourg',
                                       'Romania',
                                       'Croatia',
                                       'Portugal',
                                       'Poland',
                                       'Gibraltar',
                                       'Bulgaria',
                                       'Hungary',
                                       'San Marino',
                                       'Norway',
                                       'Czechia',
                                       'Bosnia & Herzegovina',
                                       'Denmark',
                                       'Czech Republic',
                                       'Latvia',
                                       'Netherlands',
                                       'Montenegro',
                                       'Estonia',
                                       'Italy',
                                       'Finland',
                                       'Spain',
                                       'Austria',
                                       'Andorra',
                                       'Vatican City',
                                       'Malta',
                                       'Slovakia',
                                       'Belgium',
                                       'Switzerland',
                                       'Liechtenstein',
                                       'Greece',
                                       'Guernsey',
                                       'Albania',
                                       'North Macedonia',
                                       'Jersey',
                                       'Sweden',
                                       'Åland Islands',
                                       'Isle of Man',
                                       'Ireland',
                                       'Germany',
                                       'Macedonia (FYROM)',
                                       'Iceland') then 'is_eu'
           else 'not_eu' end as is_eu,
       count(distinct case when date(ev.date) = date(subscription_start_date) then  sso_user_id end ) as subs,
       count(distinct case when date(ev.date) = date(subscription_start_date) and has_successful_payment = true then  sso_user_id end ) as paid_subs,
       count(distinct ev.fullvisitorid) as cookies,
                  count(distinct case
                   when (hits_page_pagePath like '%/pricing'
                       or hits_page_pagePath like '%/pricing/%') then ev.fullvisitorid end)           as pricing_pages,
           count(distinct case
                   when (hits_page_pagePath like '%/subscribe'
                       or hits_page_pagePath like '%/subscribe/%'
                       or hits_page_pagePath like '%/subscribe?%') then ev.fullvisitorid end)         as sub_pages,
           count(case
                   when hits_eventinfo_eventaction = 'Focus On: Create Your Account'
                       or (hits_eventinfo_eventcategory = 'Google Auth' and
                           hits_eventinfo_eventaction = 'Sign Up Success') then 1 end) as signups,
           count(case
                   when hits_eventinfo_eventaction = 'Technical: Subscription Complete'
                       then 1 end)                                                     as subscribe_page_complete



from webanalytics.ds_bq_events_elements ev
         join webanalytics.ds_bq_sessions_elements se
              on ev.fullvisitorid::varchar  = se.fullvisitorid::varchar and ev.visitid::varchar  = se.visitid::varchar
                  and ev.date between 20220401 and 20220530
                  and se.date between 20220401 and 20220530

    left join elements.dim_elements_subscription s on se.user_uuid = s.sso_user_id and subscription_start_date >= '2022-04-01'
group by 1,2,3
;



select

       count(case when split_part(hits_eventinfo_eventlabel, ';', 1) = 'profile' and split_part(hits_eventinfo_eventcategory, ';', 1) = 'item-results' then 1 end) as author_clicks,
count(case when split_part(hits_eventinfo_eventlabel, ';', 1) = 'item-card' and split_part(hits_eventinfo_eventcategory, ';', 1) = 'item-results' then 1 end) as item_clicks,
count(case when split_part(hits_eventinfo_eventcategory, ';', 1) = 'item-block-more-from-contributor' then 1 end) as item_page_author

from webanalytics.ds_bq_events_elements ev

where date between 20220520 and 20220530;


select from analyst.lt


-- console 6


-- quick view modal research

with content_t as (select u.sso_uuid,
                          i.content_type,
                          count(*) as q
                   from elements.ds_elements_item_downloads dl
                            join elements.ds_elements_item_licenses l on dl.item_license_id = l.id
                       and download_started_at :: date > '2022-01-01'
                            join elements.dim_elements_items i on i.item_id = l.item_id
                            join dim_users u
                                 on dl.user_id = u.elements_id and dl.download_started_at :: date > '2022-02-01'
                   group by 1, 2),

     enrollments as
         (
             select a.fullvisitorid :: varchar                                      as fullvisitorid,
                    variant,
--                    count(distinct e.sessionid) as sessions,
                    max(sso_user_id)                                                as sso_user_id,
                    count(case
                              when hits_eventinfo_eventcategory = 'item interaction' and
                                   hits_eventinfo_eventlabel = 'product' and
                                   hits_eventinfo_eventaction = 'view'
                                  then 1 end)                                       as adpage_views,
                    count(case
                              when split_part(hits_eventinfo_eventcategory, ';', 1) = 'item-results' and
                                   split_part(hits_eventinfo_eventlabel, ';', 1) = 'item-card' and
                                   hits_eventinfo_eventaction = 'click;a'
                                  then 1 end)                                       as modal_clicks,
                    count(case
                              when hits_eventinfo_eventcategory = 'modal-control' and
                                   split_part(hits_eventinfo_eventlabel, ';', 1) = 'next-item' and
                                   hits_eventinfo_eventaction = 'click;button'
                                  then 1 end)                                       as next_modal,
                    count(case
                              when
                                  --                                   hits_eventinfo_eventcategory = 'header-search-form' and
--                                    split_part(hits_eventinfo_eventlabel, ';', 1) = 'item-type' and
                                  hits_eventinfo_eventaction = 'license with download'
                                  then 1 end)                                       as download_success,
                    count(case
                              when hits_eventinfo_eventcategory = 'header-search-form' and
                                   hits_eventinfo_eventlabel = 'open-item-types' and
                                   hits_eventinfo_eventaction = 'click;button'
                                  then 1 end)                                       as dropdown_clicks,
                    count(case
                              when hits_eventinfo_eventcategory = 'header-search-form' and
                                   split_part(hits_eventinfo_eventlabel, ';', 1) = 'item-type' and
                                   hits_eventinfo_eventaction = 'click;button'
                                  then 1 end)                                       as category_picks,
                    max(case when hits_page_pagepath like '%pricing%' then 1 end)   as pricing_page,
                    max(case when hits_page_pagepath like '%subscribe%' then 1 end) as sub_page,
                    max(case
                            when subscription_start_date between '2022-02-01' and '2022-02-20'
                                then 1 end)                                         as new_subs,
                    max(case
                            when termination_date between '2022-02-01' and '2022-02-20'
                                then 1 end)                                         as terminations


             from webanalytics.ds_bq_abtesting_enrolments_elements a
                      join webanalytics.ds_bq_sessions_elements se
                           on a.fullvisitorid :: varchar = se.fullvisitorid::varchar and a.visitid = se.visitid
                               and a.date between '2022-02-01' and '2022-02-20'
                               and se.date between 20220201 and 20220220

                      join webanalytics.ds_bq_events_elements e
                           on se.fullvisitorid ::varchar = e.fullvisitorid ::varchar and se.visitid = e.visitid
                               and e.date between 20220201 and 20220220
--
                      left join elements.dim_elements_subscription s on se.user_uuid = s.sso_user_id
                 and (termination_date >= '2022-02-01' or termination_date isnull)
                      left join
                  where experiment_id = 'H-AEPd9aTZqkZA9Q0WWTwg'

             group by 1, 2
         ),
     multiple_allocations as
         (select sso_user_id
          from enrollments
          group by 1
          having min(variant) != max(variant))


select variant,
       case when m.sso_user_id notnull then 'reallocated' else 'not_reallocated' end as reallocation,
       case when e.sso_user_id notnull then 'subscribed' else 'unsubbed' end         as sub,
       count(distinct fullvisitorid)                                                 as cookies,
       count(distinct e.sso_user_id)                                                 as subscribers,
       sum(new_subs)                                                                 as new_subs,
       sum(terminations)                                                             as terminations,
       sum(pricing_page)                                                             as pricing_page,
       sum(sub_page)                                                                 as sub_page,
       sum(dropdown_clicks)                                                          as modal_sub_clicks,
       sum(modal_clicks)                                                             as modal_clicks,
       sum(category_picks)                                                           as category_picks,
       sum(download_success)                                                         as download_clicks,
       sum(next_modal)                                                               as next_modal_clicks,
       sum(adpage_views)                                                             as ad_pages_viewed

from enrollments e
         left join multiple_allocations m on e.sso_user_id = m.sso_user_id
group by 1, 2, 3
;

-- retention based on experiment

with content_t as (select u.sso_uuid,
                          -- ,i.content_type,
                          date_trunc('week', download_started_at) as date_dl,
                          count(distinct  item_license_id) as dls
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
         group by 1),

     subs as (
         select sso_user_id || '|' || dim_subscription_key || '|' || case
                                                                         when current_plan like '%enterprise%'
                                                                             then 'enterprise'
                                                                         when current_plan like '%team%' then 'team'
                                                                         when current_plan like '%student_annual%'
                                                                             then 'student_annual'
                                                                         when current_plan like '%student_monthly%'
                                                                             then 'student_monthly'
                                                                         else plan_type end as sso_key,


                date_trunc('week', min(subscription_start_date :: date))                    as subdate,
                date_trunc('week', max(termination_date :: date))                           as ter_date


         from elements.dim_elements_subscription s
         where subscription_start_date :: date between '2020-01-01' and '2021-06-25'
         group by 1
     ),

     keepers as (select sso_key,
                        case
                            when ter_date isnull then 'unterminated'
                            when datediff(week, subdate, ter_date) > 36 or ter_date isnull then '36week_sub'
                            when datediff(week, subdate, ter_date) > 24 then '24week_sub'
                            when datediff(week, subdate, ter_date) > 12 then '12week_sub'
                            when datediff(week, subdate, ter_date) > 1 then '1week_sub'
                            else 'churned_before_1week' end as keeper
                 from subs)

select keeper,
       case when dls isnull then 'none'
           when dls > 50 then '50plus'
           when dls > 20 then '20to50'
               else 'lessthan20'
               end as download_bucket,
       subdate :: date || '|' || split_part(sso_key, '|', 3)   as cohort,
       date_cal :: date                                        as day_date,
       datediff(week, subdate:: date, date_cal :: date)        as weeks_since_sub,
       count(distinct case
                          when date_cal >= subdate and (ter_date >= date_cal or ter_date isnull)
                              then
                              split_part(sso_key, '|', 1) end) as remaining_users,
       count(distinct case
                          when date_cal >= subdate and (ter_date >= date_cal or ter_date isnull) and c.sso_uuid notnull
                              then
                              split_part(sso_key, '|', 1) end) as downloading_users
from subs s
         join keepers k using (sso_key)
         cross join dates d
         left join content_t c on sso_uuid = split_part(sso_key, '|', 1) and date_dl = date_cal
where d.date_cal >= subdate

group by 1, 2, 3, 4, 5;

with a as (
select
fullvisitorid, visitid, date
from webanalytics.ds_bq_events_elements
where date = 20220301

 and  (hits_eventinfo_eventcategory = 'Google Auth' or hits_eventinfo_eventcategory = 'google-auth-panel')
 and hits_eventinfo_eventaction in ('Sign Up Success') --,'Sign In Success')
 limit 1)
select * from webanalytics.ds_bq_events_elements e
join a using (fullvisitorid, visitid, date);

with a as (
select
fullvisitorid, visitid
from webanalytics.ds_bq_events_elements
where date = '2021-03-01'

 and  (hits_eventinfo_eventcategory = 'Google Auth' or hits_eventinfo_eventcategory = 'google-auth-panel')
 and hits_eventinfo_eventaction in ('Sign Up Success') --,'Sign In Success')
 limit 1);


with content_t as (select u.sso_uuid,

                          date_trunc('week', download_started_at) as date_dl,
                          count(distinct i.content_type) as cts,
                          count(distinct  item_license_id) as dls
                   from elements.ds_elements_item_downloads dl
                                                       join elements.ds_elements_item_licenses l on dl.item_license_id = l.id
                      and download_started_at :: date > '2022-01-01'
                           join elements.dim_elements_items i on i.item_id = l.item_id
                            join dim_users u
                                 on dl.user_id = u.elements_id and dl.download_started_at :: date > '2020-01-01'
                   group by 1, 2),

     dates as (
         select date_trunc('week', date_dl) as date_cal
         from content_t
         where date_dl notnull
         group by 1),

     subs as (
         select sso_user_id || '|' || dim_subscription_key || '|' || case
                                                                         when current_plan like '%enterprise%'
                                                                             then 'enterprise'
                                                                         when current_plan like '%team%' then 'team'
                                                                         when current_plan like '%student_annual%'
                                                                             then 'student_annual'
                                                                         when current_plan like '%student_monthly%'
                                                                             then 'student_monthly'
                                                                         else plan_type end as sso_key,


                date_trunc('week', min(subscription_start_date :: date))                    as subdate,
                date_trunc('week', max(termination_date :: date))                           as ter_date


         from elements.dim_elements_subscription s
         where subscription_start_date :: date between '2020-01-01' and '2021-06-25'
         group by 1
     ),

     keepers as (select sso_key,
                        case
                            when ter_date isnull then 'unterminated'
                            when datediff(week, subdate, ter_date) > 36 or ter_date isnull then '36week_sub'
                            when datediff(week, subdate, ter_date) > 24 then '24week_sub'
                            when datediff(week, subdate, ter_date) > 12 then '12week_sub'
                            when datediff(week, subdate, ter_date) > 1 then '1week_sub'
                            else 'churned_before_1week' end as keeper
                 from subs)

select keeper,
       case when dls isnull then 'none'
           when dls > 50 then '50plus'
           when dls > 20 then '20to50'
               else 'lessthan20'
               end as download_bucket,
       subdate :: date || '|' || split_part(sso_key, '|', 3)   as cohort,
       date_cal :: date                                        as day_date,
       datediff(week, subdate:: date, date_cal :: date)        as weeks_since_sub,
       count(distinct case
                          when date_cal >= subdate and (ter_date >= date_cal or ter_date isnull)
                              then
                              split_part(sso_key, '|', 1) end) as remaining_users,
       count(distinct case
                          when date_cal >= subdate and (ter_date >= date_cal or ter_date isnull) and c.sso_uuid notnull
                              then
                              split_part(sso_key, '|', 1) end) as downloading_users
from subs s
         join keepers k using (sso_key)
         cross join dates d
         left join content_t c on sso_uuid = split_part(sso_key, '|', 1) and date_dl = date_cal
where d.date_cal >= subdate

group by 1, 2, 3, 4, 5;


SELECT psi.*
FROM
    page_speed_insights.ds_page_speed_insights AS psi
WHERE
    psi.domain = 'elements'
