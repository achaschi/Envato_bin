with subs as
(
select
    a.date,
    a.clientid,
    a.fullvisitorid,
    b.campaign_rpt_trafficsource_source site,
    campaign_rpt_trafficsource_campaign banner_name,
    d.dim_subscription_key subscription,
       e.dim_elements_channel_key,  -- first click attributed channel for the user
    case when  e.has_successful_payment is true then d.dim_subscription_key end paid_subscription,
    case when e.has_successful_payment is true and e.is_first_subscription is true then d.dim_subscription_key end first_paid_subscription,
    e.plan_type
from webanalytics.ds_bq_sessions_elements a
    join elements.rpt_elements_session_channel b on a.sessionid=b.sessionid   --- relax this to include all channel
    join elements.rpt_elements_subscription_session d on a.sessionid=d.last_sessionid and (d.subscription_start_date::date>='2021-04-25')
    join elements.dim_elements_subscription e on d.dim_subscription_key=e.dim_subscription_key and e.plan_change is false  --- check if it's first/last click
where a.date>=20210425
    and d.subscription_start_date::date>='2021-04-25'
    and a.date>=20210425
    and b.channel ='Internal Promotion'
    and b.campaign_rpt_trafficsource_medium='promos'
    and (b.campaign_rpt_trafficsource_campaign like 'elements_mkt-footer%'
    or b.campaign_rpt_trafficsource_campaign like 'elements_mkt-header_%')
) ,
signup as
(
select
    sess.date,
    a.sso_user_id signup_user,
    b.campaign_rpt_trafficsource_source site,
    campaign_rpt_trafficsource_campaign banner_name
from webanalytics.ds_bq_sessions_elements sess
    join elements.rpt_elements_user_signup_session a on a.sessionid=sess.sessionid
    join elements.rpt_elements_session_channel b on a.sessionid=b.sessionid
where a.signup_date::date>='2021-04-25'
    and b.channel ='Internal Promotion'
    and b.campaign_rpt_trafficsource_medium='promos'
    and (b.campaign_rpt_trafficsource_campaign like 'elements_mkt-footer%'
    or b.campaign_rpt_trafficsource_campaign like 'elements_mkt-header_%')
)
, staging as
(
select
    date,
    site,
    banner_name,
    count(distinct subscription) total_subs,
    count(distinct paid_subscription) paid_subs,
    count(distinct first_paid_subscription) first_paid_subs,
    null signups
from subs
group by 1,2,3

union all

select
    date,
    site,
    banner_name,
    null total_subs,
    null paid_subs,
    null first_paid_subs,
    count(distinct signup_user) signups
from signup
group by 1,2,3
)
select distinct site
from staging
;
select
    date,
    site,
    banner_name,  --campaign

    sum(total_subs) total_subs,
    sum(paid_subs)paid_subs,
    sum(first_paid_subs)first_paid_subs,
    sum(signups)signups
-- need to add termination query
from staging
group by 1,2,3
limit 10
;

select
*
from dim_elements_channel