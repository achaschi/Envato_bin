-- add in another section for new visitors??
/*
select * from elements.rpt_elements_user_sessions where user_uuid = 'eb9a9aae-dec0-45bb-a8c7-847dbd86b96a'
select * from webanalytics.ds_bq_hits_elements where fullvisitorid = '9194865709128950995' order by date_aest asc  188171846
select * from webanalytics.ds_bq_sessions_elements where sessionid = 188171846

*/

with daily_guests_sessions as (
select
		date_trunc('day',date_aest) calendar_date,
		fullvisitorid,
		count(sessionid) sessions,
		sum(nvl(totals_timeonsite,0)) sessions_duration,
		sum(nvl(totals_bounces,0)) bounces,
		sum(nvl(totals_pageviews,0)) pageviews
from
		webanalytics.ds_bq_sessions_elements
where
		nullif(user_uuid,'') is null
        and date_aest between DATE_TRUNC('month', dateadd('months', -13, getdate_aest()))::date and   getdate_aest() -1


group by 1,2
)
,
daily_guests_searches as (
select
		date_trunc('day',date_aest) calendar_date,
		fullvisitorid,
		count(distinct hits_page_searchkeyword) searches,
		count(distinct sessionid) sessions_with_searches
from
		webanalytics.ds_bq_hits_elements
where
		nullif(user_uuid,'') is null
		and nullif(hits_page_searchkeyword,'') is not null
 and date_aest between DATE_TRUNC('month', dateadd('months', -13, getdate_aest()))::date and   getdate_aest() -1

group by 1,2
),

daily_guests_item_views as (
select
		date_trunc('day',date_aest) calendar_date,
		fullvisitorid,
		count(item_id) item_views,
		count(distinct sessionid) sessions_with_item_views
from
		webanalytics.ds_bq_hits_elements
where
		nullif(user_uuid,'') is null
		and nullif(item_id,'') is not null
 and date_aest between DATE_TRUNC('month', dateadd('months', -13, getdate_aest()))::date and   getdate_aest() -1

group by 1,2
)
,
/*
     daily_guests_summary as (
select
		date_trunc('day',daily_guests_sessions.calendar_date) calendar_date,
		'Daily' date_base,
		'Guests' user_type,
        'Not Applicable' plan_type,
       'Not Applicable' plan_detail,
		count(distinct daily_guests_sessions.fullvisitorid) visitors,
		sum(daily_guests_sessions.sessions) sessions,
		sum(daily_guests_sessions.sessions_duration) sessions_duration,
		sum(daily_guests_sessions.bounces) bounces,
		sum(daily_guests_sessions.pageviews) pageviews,
		sum(0) downloading_visitors,
		sum(0) downloads,
		count(distinct daily_guests_searches.fullvisitorid) searching_visitors,
		sum(daily_guests_searches.searches) searches,
		sum(daily_guests_searches.sessions_with_searches) sessions_with_searches,
        count(distinct daily_guests_item_views.fullvisitorid) item_view_visitors,
		sum(daily_guests_item_views.item_views) item_views,
		sum(daily_guests_item_views.sessions_with_item_views) sessions_with_item_views
from
		daily_guests_sessions
		left join daily_guests_searches
		on daily_guests_sessions.calendar_date=daily_guests_searches.calendar_date
		and daily_guests_sessions.fullvisitorid=daily_guests_searches.fullvisitorid
		left join daily_guests_item_views
		on daily_guests_sessions.calendar_date=daily_guests_item_views.calendar_date
		and daily_guests_sessions.fullvisitorid=daily_guests_item_views.fullvisitorid
group by 1,2,3,4,5
)
,
weekly_guests_summary as (
select
		date_trunc('week',daily_guests_sessions.calendar_date) calendar_date,
		'Weekly' date_base,
		'Guests' user_type,
        'Not Applicable' plan_type,
              'Not Applicable' plan_detail,
		count(distinct daily_guests_sessions.fullvisitorid) visitors,
		sum(daily_guests_sessions.sessions) sessions,
		sum(daily_guests_sessions.sessions_duration) sessions_duration,
		sum(daily_guests_sessions.bounces) bounces,
		sum(daily_guests_sessions.pageviews) pageviews,
		sum(0) downloading_visitors,
		sum(0) downloads,
		count(distinct daily_guests_searches.fullvisitorid) searching_visitors,
		sum(daily_guests_searches.searches) searches,
		sum(daily_guests_searches.sessions_with_searches) sessions_with_searches,
        count(distinct daily_guests_item_views.fullvisitorid) item_view_visitors,
		sum(daily_guests_item_views.item_views) item_views,
		sum(daily_guests_item_views.sessions_with_item_views) sessions_with_item_views
from
		daily_guests_sessions
		left join daily_guests_searches
		on daily_guests_sessions.calendar_date=daily_guests_searches.calendar_date
		and daily_guests_sessions.fullvisitorid=daily_guests_searches.fullvisitorid
		left join daily_guests_item_views
		on daily_guests_sessions.calendar_date=daily_guests_item_views.calendar_date
		and daily_guests_sessions.fullvisitorid=daily_guests_item_views.fullvisitorid
group by 1,2,3,4,5
)
,

 */
monthly_guests_summary as (
select
		date_trunc('month',daily_guests_sessions.calendar_date) calendar_date,
		'Monthly' date_base,
		'Guests' user_type,
     'Not Applicable' plan_type,
              'Not Applicable' plan_detail,
		count(distinct daily_guests_sessions.fullvisitorid) visitors,
		sum(daily_guests_sessions.sessions) sessions,
		sum(daily_guests_sessions.sessions_duration) sessions_duration,
		sum(daily_guests_sessions.bounces) bounces,
		sum(daily_guests_sessions.pageviews) pageviews,
		sum(0) downloading_visitors,
		sum(0) downloads,
		count(distinct daily_guests_searches.fullvisitorid) searching_visitors,
		sum(daily_guests_searches.searches) searches,
		sum(daily_guests_searches.sessions_with_searches) sessions_with_searches,
        count(distinct daily_guests_item_views.fullvisitorid) item_view_visitors,
		sum(daily_guests_item_views.item_views) item_views,
		sum(daily_guests_item_views.sessions_with_item_views) sessions_with_item_views
from
		daily_guests_sessions
		left join daily_guests_searches
		on daily_guests_sessions.calendar_date=daily_guests_searches.calendar_date
		and daily_guests_sessions.fullvisitorid=daily_guests_searches.fullvisitorid
		left join daily_guests_item_views
		on daily_guests_sessions.calendar_date=daily_guests_item_views.calendar_date
		and daily_guests_sessions.fullvisitorid=daily_guests_item_views.fullvisitorid
group by 1,2,3,4,5
)
,
/*FREE ACCOUNTS*/
accounts as (
select
		sso_user_id,
		cast(signup_date as date) signup_date,
		cast(coalesce(first_subscription_start_date,envato.getdate_aest()) as date) first_subscription_start_date
from
		analysts.elements_users_all
)
,
daily_free_accounts_sessions as (
select
		date_trunc('day',a.date_aest) calendar_date,
		b.sso_user_id,
		count(case when cast(a.date_aest as date)>=b.signup_date and cast(a.date_aest as date)<b.first_subscription_start_date then a.sessionid end) sessions,
		sum(case when cast(a.date_aest as date)>=b.signup_date and cast(a.date_aest as date)<b.first_subscription_start_date then a.totals_timeonsite else 0 end) sessions_duration,
		sum(case when cast(a.date_aest as date)>=b.signup_date and cast(a.date_aest as date)<b.first_subscription_start_date then a.totals_bounces else 0 end) bounces,
		sum(case when cast(a.date_aest as date)>=b.signup_date and cast(a.date_aest as date)<b.first_subscription_start_date then a.totals_pageviews else 0 end) pageviews
from
		webanalytics.ds_bq_sessions_elements a
		join accounts b
		on a.user_uuid=b.sso_user_id
where a.date_aest between DATE_TRUNC('month', dateadd('months', -13, getdate_aest()))::date and   getdate_aest() -1

group by 1,2
having sessions>0
)
,
daily_free_accounts_downloads as (
select
		date_trunc('day',a.download_started_at) calendar_date,
		b.sso_uuid sso_user_id,
		count(case when cast(a.download_started_at as date)>=c.signup_date and cast(a.download_started_at as date)<c.first_subscription_start_date then a.id end) downloads
from
		elements.ds_Elements_item_downloads a
		join envato.dim_users b
		on a.user_id=b.elements_id
		join accounts c
		on b.sso_uuid=c.sso_user_id
where a.download_started_at between DATE_TRUNC('month', dateadd('months', -13, getdate_aest()))::date and   getdate_aest() -1

group by 1,2
having downloads>0
)
,
daily_free_accounts_searches as (
select
		date_trunc('day',a.date_aest) calendar_date,
		a.user_uuid sso_user_id,
		count(distinct case when cast(a.date_aest as date)>=b.signup_date and cast(a.date_aest as date)<b.first_subscription_start_date then a.hits_page_searchkeyword end) searches,
		count(distinct case when cast(a.date_aest as date)>=b.signup_date and cast(a.date_aest as date)<b.first_subscription_start_date then a.sessionid end) sessions_with_searches
from
		webanalytics.ds_bq_hits_elements a
		join accounts b
		on a.user_uuid=b.sso_user_id
where
		nullif(hits_page_searchkeyword,'') is not null
and a.date_aest between DATE_TRUNC('month', dateadd('months', -13, getdate_aest()))::date and   getdate_aest() -1

group by 1,2
having searches>0
),

daily_free_accounts_item_views as (
select
		date_trunc('day',a.date_aest) calendar_date,
		a.user_uuid sso_user_id,
		count(case when cast(a.date_aest as date)>=b.signup_date and cast(a.date_aest as date)<b.first_subscription_start_date then a.item_id end) item_views,
		count(distinct case when cast(a.date_aest as date)>=b.signup_date and cast(a.date_aest as date)<b.first_subscription_start_date then a.sessionid end) sessions_with_item_views
from
		webanalytics.ds_bq_hits_elements a
		join accounts b
		on a.user_uuid=b.sso_user_id
where
		nullif(item_id,'') is not null
and a.date_aest between DATE_TRUNC('month', dateadd('months', -13, getdate_aest()))::date and   getdate_aest() -1


group by 1,2
having item_views>0
)
,
     /*
daily_free_accounts_summary as (
select
		date_trunc('day',daily_free_accounts_sessions.calendar_date) calendar_date,
		'Daily' date_base,
		'Free Users' user_type,
        'Not Applicable' plan_type,
              'Not Applicable' plan_detail,

		count(distinct daily_free_accounts_sessions.sso_user_id) visitors,
		sum(daily_free_accounts_sessions.sessions) sessions,
		sum(daily_free_accounts_sessions.sessions_duration) sessions_duration,
		sum(daily_free_accounts_sessions.bounces) bounces,
		sum(daily_free_accounts_sessions.pageviews) pageviews,
		count(distinct daily_free_accounts_downloads.sso_user_id) downloading_visitors,
		sum(daily_free_accounts_downloads.downloads) downloads,
		count(distinct daily_free_accounts_searches.sso_user_id) searching_visitors,
		sum(daily_free_accounts_searches.searches) searches,
		sum(daily_free_accounts_searches.sessions_with_searches) sessions_with_searches,
         count(distinct daily_free_accounts_item_views.sso_user_id) item_view_visitors,
  		sum(daily_free_accounts_item_views.item_views) item_views,
		sum(daily_free_accounts_item_views.sessions_with_item_views) sessions_with_item_views
from
		daily_free_accounts_sessions
		left join daily_free_accounts_downloads
		on daily_free_accounts_sessions.calendar_date=daily_free_accounts_downloads.calendar_date
		and daily_free_accounts_sessions.sso_user_id=daily_free_accounts_downloads.sso_user_id
		left join daily_free_accounts_searches
		on daily_free_accounts_sessions.calendar_date=daily_free_accounts_searches.calendar_date
		and daily_free_accounts_sessions.sso_user_id=daily_free_accounts_searches.sso_user_id
		left join daily_free_accounts_item_views
		on daily_free_accounts_sessions.calendar_date=daily_free_accounts_item_views.calendar_date
		and daily_free_accounts_sessions.sso_user_id=daily_free_accounts_item_views.sso_user_id
group by 1,2,3,4,5
)
,
weekly_free_accounts_summary as (
select
		date_trunc('week',daily_free_accounts_sessions.calendar_date) calendar_date,
		'Weekly' date_base,
		'Free Users' user_type,
         'Not Applicable' plan_type,
              'Not Applicable' plan_detail,

		count(distinct daily_free_accounts_sessions.sso_user_id) visitors,
		sum(daily_free_accounts_sessions.sessions) sessions,
		sum(daily_free_accounts_sessions.sessions_duration) sessions_duration,
		sum(daily_free_accounts_sessions.bounces) bounces,
		sum(daily_free_accounts_sessions.pageviews) pageviews,
		count(distinct daily_free_accounts_downloads.sso_user_id) downloading_visitors,
		sum(daily_free_accounts_downloads.downloads) downloads,
		count(distinct daily_free_accounts_searches.sso_user_id) searching_visitors,
		sum(daily_free_accounts_searches.searches) searches,
		sum(daily_free_accounts_searches.sessions_with_searches) sessions_with_searches,
        count(distinct daily_free_accounts_item_views.sso_user_id) item_view_visitors,
    	sum(daily_free_accounts_item_views.item_views) item_views,
		sum(daily_free_accounts_item_views.sessions_with_item_views) sessions_with_item_views
from
		daily_free_accounts_sessions
		left join daily_free_accounts_downloads
		on daily_free_accounts_sessions.calendar_date=daily_free_accounts_downloads.calendar_date
		and daily_free_accounts_sessions.sso_user_id=daily_free_accounts_downloads.sso_user_id
		left join daily_free_accounts_searches
		on daily_free_accounts_sessions.calendar_date=daily_free_accounts_searches.calendar_date
		and daily_free_accounts_sessions.sso_user_id=daily_free_accounts_searches.sso_user_id
		left join daily_free_accounts_item_views
		on daily_free_accounts_sessions.calendar_date=daily_free_accounts_item_views.calendar_date
		and daily_free_accounts_sessions.sso_user_id=daily_free_accounts_item_views.sso_user_id
group by 1,2,3,4,5
)

      */

monthly_free_accounts_summary as (
select
		date_trunc('month',daily_free_accounts_sessions.calendar_date) calendar_date,
		'Monthly' date_base,
		'Free Users' user_type,
      'Not Applicable' plan_type,
              'Not Applicable' plan_detail,

		count(distinct daily_free_accounts_sessions.sso_user_id) visitors,
		sum(daily_free_accounts_sessions.sessions) sessions,
		sum(daily_free_accounts_sessions.sessions_duration) sessions_duration,
		sum(daily_free_accounts_sessions.bounces) bounces,
		sum(daily_free_accounts_sessions.pageviews) pageviews,
		count(distinct daily_free_accounts_downloads.sso_user_id) downloading_visitors,
		sum(daily_free_accounts_downloads.downloads) downloads,
		count(distinct daily_free_accounts_searches.sso_user_id) searching_visitors,
		sum(daily_free_accounts_searches.searches) searches,
		sum(daily_free_accounts_searches.sessions_with_searches) sessions_with_searches,
        count(distinct daily_free_accounts_item_views.sso_user_id) item_view_visitors,
      	sum(daily_free_accounts_item_views.item_views) item_views,
		sum(daily_free_accounts_item_views.sessions_with_item_views) sessions_with_item_views
from
		daily_free_accounts_sessions
		left join daily_free_accounts_downloads
		on daily_free_accounts_sessions.calendar_date=daily_free_accounts_downloads.calendar_date
		and daily_free_accounts_sessions.sso_user_id=daily_free_accounts_downloads.sso_user_id
		left join daily_free_accounts_searches
		on daily_free_accounts_sessions.calendar_date=daily_free_accounts_searches.calendar_date
		and daily_free_accounts_sessions.sso_user_id=daily_free_accounts_searches.sso_user_id
		left join daily_free_accounts_item_views
		on daily_free_accounts_sessions.calendar_date=daily_free_accounts_item_views.calendar_date
		and daily_free_accounts_sessions.sso_user_id=daily_free_accounts_item_views.sso_user_id
group by 1,2,3,4,5

)
,
/*ACTIVE SUBS*/
daily_active_subs_actives as (
select
		date_trunc('day',calendar_date) calendar_date,
		b.sso_uuid sso_user_id,
        max(s.plan_type) as plan_type,
        max(s.current_plan) as plan_detail,
		sum(a.active_subscription_count) active_subscription_count
from
		elements.fact_elements_active_subscriptions a
		join envato.dim_users b
		on a.dim_users_key=b.dim_users_key
		join envato.dim_date c
		on a.dim_date_key=c.dim_date_key
        join elements.dim_elements_subscription s
        on a.dim_subscription_key = s.dim_subscription_key
where c.calendar_date between DATE_TRUNC('month', dateadd('months', -13, getdate_aest()))::date and   getdate_aest() -1

group by 1,2
)

,
daily_active_subs_sessions as (
select
		date_trunc('day',date_aest) calendar_date,
		user_uuid sso_user_id,
		count(sessionid) sessions,
		sum(nvl(totals_timeonsite,0)) sessions_duration,
		sum(nvl(totals_bounces,0)) bounces,
		sum(nvl(totals_pageviews,0)) pageviews
from
		webanalytics.ds_bq_sessions_elements
where
		nullif(user_uuid,'') is not null
and date_aest between DATE_TRUNC('month', dateadd('months', -13, getdate_aest()))::date and   getdate_aest() -1


group by 1,2
)
,
daily_active_subs_downloads as (
select
		date_trunc('day',a.download_started_at) calendar_date,
		b.sso_uuid sso_user_id,
		count(id) downloads
from
		elements.ds_Elements_item_downloads a
		join envato.dim_users b
		on a.user_id=b.elements_id
where download_started_at between DATE_TRUNC('month', dateadd('months', -13, getdate_aest()))::date and   getdate_aest() -1

group by 1,2
)
,
daily_active_subs_searches as (
select
		date_trunc('day',date_aest) calendar_date,
		user_uuid sso_user_id,
		count(distinct hits_page_searchkeyword) searches,
		count(distinct sessionid) sessions_with_searches
from
		webanalytics.ds_bq_hits_elements
where
		nullif(user_uuid,'') is not null
		and nullif(hits_page_searchkeyword,'') is not null
and date_aest between DATE_TRUNC('month', dateadd('months', -13, getdate_aest()))::date and   getdate_aest() -1

group by 1,2
)
,

  daily_active_subs_item_views as (
select
		date_trunc('day',date_aest) calendar_date,
		user_uuid sso_user_id,
		count(item_id) item_views,
		count(distinct sessionid) sessions_with_item_views
from
		webanalytics.ds_bq_hits_elements
where
		nullif(user_uuid,'') is not null
		and nullif(item_id,'') is not null
and date_aest between DATE_TRUNC('month', dateadd('months', -13, getdate_aest()))::date and   getdate_aest() -1

group by 1,2
),
/*
daily_active_subs_summary as (
select
		date_trunc('day',daily_active_subs_actives.calendar_date) calendar_date,
		'Daily' date_base,
		'Active Subscribers' user_type,
        plan_type,
        plan_detail,
		count(distinct daily_active_subs_sessions.sso_user_id) visitors,
		sum(daily_active_subs_sessions.sessions) sessions,
		sum(daily_active_subs_sessions.sessions_duration) sessions_duration,
		sum(daily_active_subs_sessions.bounces) bounces,
		sum(daily_active_subs_sessions.pageviews) pageviews,
		count(distinct daily_active_subs_downloads.sso_user_id) downloading_visitors,
		sum(daily_active_subs_downloads.downloads) downloads,
		count(distinct daily_active_subs_searches.sso_user_id) searching_visitors,
		sum(daily_active_subs_searches.searches) searches,
		sum(daily_active_subs_searches.sessions_with_searches) sessions_with_searches,
  		count(distinct daily_active_subs_item_views.sso_user_id) item_view_visitors,
		sum(daily_active_subs_item_views.item_views) item_views,
		sum(daily_active_subs_item_views.sessions_with_item_views) sessions_with_item_views
from
		daily_active_subs_actives
		left join daily_active_subs_sessions
		on daily_active_subs_actives.calendar_date=daily_active_subs_sessions.calendar_date
		and daily_active_subs_actives.sso_user_id=daily_active_subs_sessions.sso_user_id
		left join daily_active_subs_downloads
		on daily_active_subs_actives.calendar_date=daily_active_subs_downloads.calendar_date
		and daily_active_subs_actives.sso_user_id=daily_active_subs_downloads.sso_user_id
		left join daily_active_subs_searches
		on daily_active_subs_actives.calendar_date=daily_active_subs_searches.calendar_date
		and daily_active_subs_actives.sso_user_id=daily_active_subs_searches.sso_user_id
		left join daily_active_subs_item_views
		on daily_active_subs_actives.calendar_date=daily_active_subs_item_views.calendar_date
		and daily_active_subs_actives.sso_user_id=daily_active_subs_item_views.sso_user_id
group by 1,2,3,4,5
)
,
weekly_active_subs_summary as (
select
		date_trunc('week',daily_active_subs_actives.calendar_date) calendar_date,
		'Weekly' date_base,
		'Active Subscribers' user_type,
       plan_type,
       plan_detail,
		count(distinct daily_active_subs_sessions.sso_user_id) visitors,
		sum(daily_active_subs_sessions.sessions) sessions,
		sum(daily_active_subs_sessions.sessions_duration) sessions_duration,
		sum(daily_active_subs_sessions.bounces) bounces,
		sum(daily_active_subs_sessions.pageviews) pageviews,
		count(distinct daily_active_subs_downloads.sso_user_id) downloading_visitors,
		sum(daily_active_subs_downloads.downloads) downloads,
		count(distinct daily_active_subs_searches.sso_user_id) searching_visitors,
		sum(daily_active_subs_searches.searches) searches,
		sum(daily_active_subs_searches.sessions_with_searches) sessions_with_searches,
    	count(distinct daily_active_subs_item_views.sso_user_id) item_view_visitors,
  		sum(daily_active_subs_item_views.item_views) item_views,
		sum(daily_active_subs_item_views.sessions_with_item_views) sessions_with_item_views
from
		daily_active_subs_actives
		left join daily_active_subs_sessions
		on daily_active_subs_actives.calendar_date=daily_active_subs_sessions.calendar_date
		and daily_active_subs_actives.sso_user_id=daily_active_subs_sessions.sso_user_id
		left join daily_active_subs_downloads
		on daily_active_subs_actives.calendar_date=daily_active_subs_downloads.calendar_date
		and daily_active_subs_actives.sso_user_id=daily_active_subs_downloads.sso_user_id
		left join daily_active_subs_searches
		on daily_active_subs_actives.calendar_date=daily_active_subs_searches.calendar_date
		and daily_active_subs_actives.sso_user_id=daily_active_subs_searches.sso_user_id
		left join daily_active_subs_item_views
		on daily_active_subs_actives.calendar_date=daily_active_subs_item_views.calendar_date
		and daily_active_subs_actives.sso_user_id=daily_active_subs_item_views.sso_user_id
group by 1,2,3,4,5
)
,

 */
monthly_active_subs_summary as (
select
		date_trunc('month',daily_active_subs_actives.calendar_date) calendar_date,
		'Monthly' date_base,
		'Active Subscribers' user_type,
        plan_type,
       plan_detail,
		count(distinct daily_active_subs_sessions.sso_user_id) visitors,
		sum(daily_active_subs_sessions.sessions) sessions,
		sum(daily_active_subs_sessions.sessions_duration) sessions_duration,
		sum(daily_active_subs_sessions.bounces) bounces,
		sum(daily_active_subs_sessions.pageviews) pageviews,
		count(distinct daily_active_subs_downloads.sso_user_id) downloading_visitors,
		sum(daily_active_subs_downloads.downloads) downloads,
		count(distinct daily_active_subs_searches.sso_user_id) searching_visitors,
		sum(daily_active_subs_searches.searches) searches,
		sum(daily_active_subs_searches.sessions_with_searches) sessions_with_searches,
    	count(distinct daily_active_subs_item_views.sso_user_id) item_view_visitors,
    	sum(daily_active_subs_item_views.item_views) item_views,
		sum(daily_active_subs_item_views.sessions_with_item_views) sessions_with_item_views
from
		daily_active_subs_actives
		left join daily_active_subs_sessions
		on daily_active_subs_actives.calendar_date=daily_active_subs_sessions.calendar_date
		and daily_active_subs_actives.sso_user_id=daily_active_subs_sessions.sso_user_id
		left join daily_active_subs_downloads
		on daily_active_subs_actives.calendar_date=daily_active_subs_downloads.calendar_date
		and daily_active_subs_actives.sso_user_id=daily_active_subs_downloads.sso_user_id
		left join daily_active_subs_searches
		on daily_active_subs_actives.calendar_date=daily_active_subs_searches.calendar_date
		and daily_active_subs_actives.sso_user_id=daily_active_subs_searches.sso_user_id
		left join daily_active_subs_item_views
		on daily_active_subs_actives.calendar_date=daily_active_subs_item_views.calendar_date
		and daily_active_subs_actives.sso_user_id=daily_active_subs_item_views.sso_user_id
group by 1,2,3,4,5
)
,
/*CHURNED SUBS*/
daily_churned_subs_churned as (
select
		date_trunc('day',a.calendar_date) calendar_date,
		b.sso_user_id,
		sum(case when cast(b.subscription_start_date as date)<=a.calendar_date
		and nvl(cast(b.termination_date as date), cast(envato.getdate_aest() as date))>=a.calendar_date
		then 1 else 0 end) active_subscription_count
from
		envato.dim_date a
		left join elements.dim_elements_subscription b
		on cast(b.subscription_start_date as date) <= a.calendar_date
where a.calendar_date  between DATE_TRUNC('month', dateadd('months', -13, getdate_aest()))::date and   getdate_aest() -1
group by 1,2
having active_subscription_count=0)
,
daily_churned_subs_sessions as (
select
		date_trunc('day',date_aest) calendar_date,
		user_uuid sso_user_id,
		count(sessionid) sessions,
		sum(nvl(totals_timeonsite,0)) sessions_duration,
		sum(nvl(totals_bounces,0)) bounces,
		sum(nvl(totals_pageviews,0)) pageviews
from
		webanalytics.ds_bq_sessions_elements
where
		nullif(user_uuid,'') is not null
and date_aest  between DATE_TRUNC('month', dateadd('months', -13, getdate_aest()))::date and   getdate_aest() -1
group by 1,2
)
,
daily_churned_subs_downloads as (
select
		date_trunc('day',a.download_started_at) calendar_date,
		b.sso_uuid sso_user_id,
		count(id) downloads
from
		elements.ds_Elements_item_downloads a
		join envato.dim_users b
		on a.user_id=b.elements_id
 where a.download_started_at between DATE_TRUNC('month', dateadd('months', -13, getdate_aest()))::date and   getdate_aest() -1
group by 1,2
)
,
daily_churned_subs_searches as (
select
		date_trunc('day',date_aest) calendar_date,
		user_uuid sso_user_id,
		count(distinct hits_page_searchkeyword) searches,
		count(distinct sessionid) sessions_with_searches
from
		webanalytics.ds_bq_hits_elements
where
		nullif(user_uuid,'') is not null
		and nullif(hits_page_searchkeyword,'') is not null
and date_aest  between DATE_TRUNC('month', dateadd('months', -13, getdate_aest()))::date and   getdate_aest() -1
group by 1,2
)
,
daily_churned_subs_item_views as (
select
		date_trunc('day',date_aest) calendar_date,
		user_uuid sso_user_id,
		count(item_id) item_views,
		count(distinct sessionid) sessions_with_item_views
from
		webanalytics.ds_bq_hits_elements
where
		nullif(user_uuid,'') is not null
		and nullif(item_id,'') is not null
and date_aest  between DATE_TRUNC('month', dateadd('months', -13, getdate_aest()))::date and   getdate_aest() -1
group by 1,2
)
,
     /*
daily_churned_subs_summary as (
select
		date_trunc('day',daily_churned_subs_churned.calendar_date) calendar_date,
		'Daily' date_base,
		'Churned Subscribers' user_type,
         'Not Applicable' plan_type,
         'Not Applicable' plan_detail,
		count(distinct daily_churned_subs_sessions.sso_user_id) visitors,
		sum(daily_churned_subs_sessions.sessions) sessions,
		sum(daily_churned_subs_sessions.sessions_duration) sessions_duration,
		sum(daily_churned_subs_sessions.bounces) bounces,
		sum(daily_churned_subs_sessions.pageviews) pageviews,
		count(distinct daily_churned_subs_downloads.sso_user_id) downloading_visitors,
		sum(daily_churned_subs_downloads.downloads) downloads,
		count(distinct daily_churned_subs_searches.sso_user_id) searching_visitors,
		sum(daily_churned_subs_searches.searches) searches,
		sum(daily_churned_subs_searches.sessions_with_searches) sessions_with_searches,
		count(distinct daily_churned_subs_item_views.sso_user_id) item_view_visitors,
		sum(daily_churned_subs_item_views.item_views) item_views,
		sum(daily_churned_subs_item_views.sessions_with_item_views) sessions_with_item_views
from
		daily_churned_subs_churned
		left join daily_churned_subs_sessions
		on daily_churned_subs_churned.calendar_date=daily_churned_subs_sessions.calendar_date
		and daily_churned_subs_churned.sso_user_id=daily_churned_subs_sessions.sso_user_id
		left join daily_churned_subs_downloads
		on daily_churned_subs_churned.calendar_date=daily_churned_subs_downloads.calendar_date
		and daily_churned_subs_churned.sso_user_id=daily_churned_subs_downloads.sso_user_id
		left join daily_churned_subs_searches
		on daily_churned_subs_churned.calendar_date=daily_churned_subs_searches.calendar_date
		and daily_churned_subs_churned.sso_user_id=daily_churned_subs_searches.sso_user_id
		left join daily_churned_subs_item_views
		on daily_churned_subs_churned.calendar_date=daily_churned_subs_item_views.calendar_date
		and daily_churned_subs_churned.sso_user_id=daily_churned_subs_item_views.sso_user_id
group by 1,2,3,4,5
)
,
weekly_churned_subs_summary as (
select
		date_trunc('week',daily_churned_subs_churned.calendar_date) calendar_date,
		'Weekly' date_base,
		'Churned Subscribers' user_type,
          'Not Applicable' plan_type,
       'Not Applicable' plan_detail,
		count(distinct daily_churned_subs_sessions.sso_user_id) visitors,
		sum(daily_churned_subs_sessions.sessions) sessions,
		sum(daily_churned_subs_sessions.sessions_duration) sessions_duration,
		sum(daily_churned_subs_sessions.bounces) bounces,
		sum(daily_churned_subs_sessions.pageviews) pageviews,
		count(distinct daily_churned_subs_downloads.sso_user_id) downloading_visitors,
		sum(daily_churned_subs_downloads.downloads) downloads,
		count(distinct daily_churned_subs_searches.sso_user_id) searching_visitors,
		sum(daily_churned_subs_searches.searches) searches,
		sum(daily_churned_subs_searches.sessions_with_searches) sessions_with_searches,
  		count(distinct daily_churned_subs_item_views.sso_user_id) item_view_visitors,
		sum(daily_churned_subs_item_views.item_views) item_views,
		sum(daily_churned_subs_item_views.sessions_with_item_views) sessions_with_item_views
from
		daily_churned_subs_churned
		left join daily_churned_subs_sessions
		on daily_churned_subs_churned.calendar_date=daily_churned_subs_sessions.calendar_date
		and daily_churned_subs_churned.sso_user_id=daily_churned_subs_sessions.sso_user_id
		left join daily_churned_subs_downloads
		on daily_churned_subs_churned.calendar_date=daily_churned_subs_downloads.calendar_date
		and daily_churned_subs_churned.sso_user_id=daily_churned_subs_downloads.sso_user_id
		left join daily_churned_subs_searches
		on daily_churned_subs_churned.calendar_date=daily_churned_subs_searches.calendar_date
		and daily_churned_subs_churned.sso_user_id=daily_churned_subs_searches.sso_user_id
		left join daily_churned_subs_item_views
		on daily_churned_subs_churned.calendar_date=daily_churned_subs_item_views.calendar_date
		and daily_churned_subs_churned.sso_user_id=daily_churned_subs_item_views.sso_user_id
group by 1,2,3,4,5
)
,

      */
monthly_churned_subs_summary as (
select
		date_trunc('month',daily_churned_subs_churned.calendar_date) calendar_date,
		'Monthly' date_base,
		'Churned Subscribers' user_type,
         'Not Applicable' plan_type,
       'Not Applicable' plan_detail,
		count(distinct daily_churned_subs_sessions.sso_user_id) visitors,
		sum(daily_churned_subs_sessions.sessions) sessions,
		sum(daily_churned_subs_sessions.sessions_duration) sessions_duration,
		sum(daily_churned_subs_sessions.bounces) bounces,
		sum(daily_churned_subs_sessions.pageviews) pageviews,
		count(distinct daily_churned_subs_downloads.sso_user_id) downloading_visitors,
		sum(daily_churned_subs_downloads.downloads) downloads,
		count(distinct daily_churned_subs_searches.sso_user_id) searching_visitors,
		sum(daily_churned_subs_searches.searches) searches,
		sum(daily_churned_subs_searches.sessions_with_searches) sessions_with_searches,
  		count(distinct daily_churned_subs_item_views.sso_user_id) item_view_visitors,
		sum(daily_churned_subs_item_views.item_views) item_views,
		sum(daily_churned_subs_item_views.sessions_with_item_views) sessions_with_item_views
from
		daily_churned_subs_churned
		left join daily_churned_subs_sessions
		on daily_churned_subs_churned.calendar_date=daily_churned_subs_sessions.calendar_date
		and daily_churned_subs_churned.sso_user_id=daily_churned_subs_sessions.sso_user_id
		left join daily_churned_subs_downloads
		on daily_churned_subs_churned.calendar_date=daily_churned_subs_downloads.calendar_date
		and daily_churned_subs_churned.sso_user_id=daily_churned_subs_downloads.sso_user_id
		left join daily_churned_subs_searches
		on daily_churned_subs_churned.calendar_date=daily_churned_subs_searches.calendar_date
		and daily_churned_subs_churned.sso_user_id=daily_churned_subs_searches.sso_user_id
		left join daily_churned_subs_item_views
		on daily_churned_subs_churned.calendar_date=daily_churned_subs_item_views.calendar_date
		and daily_churned_subs_churned.sso_user_id=daily_churned_subs_item_views.sso_user_id
group by 1,2,3,4,5
)

,
/*TOTAL*/

     /*
daily_total_sessions as (
select
		date_trunc('day',date_aest) calendar_date,
		count(distinct case when nullif(user_uuid,'') is not null then user_uuid else cast(fullvisitorid as nvarchar(max)) end) visitors,
		count(sessionid) sessions,
		sum(nvl(totals_timeonsite,0)) sessions_duration,
		sum(nvl(totals_bounces,0)) bounces,
		sum(nvl(totals_pageviews,0)) pageviews
from
		webanalytics.ds_bq_sessions_elements
where date_aest  between DATE_TRUNC('month', dateadd('months', -13, getdate_aest()))::date and   getdate_aest() -1
group by 1
)
,
daily_total_downloads as (
select
		date_trunc('day',a.download_started_at) calendar_date,
		count(distinct b.sso_uuid) downloading_visitors,
		count(id) downloads
from
		elements.ds_Elements_item_downloads a
		join envato.dim_users b
		on a.user_id=b.elements_id
where download_started_at  between DATE_TRUNC('month', dateadd('months', -13, getdate_aest()))::date and   getdate_aest() -1
group by 1
)
,
daily_total_searches as (
select
		date_trunc('day',date_aest) calendar_date,
		count(distinct case when nullif(user_uuid,'') is not null then user_uuid else cast(fullvisitorid as nvarchar(max)) end) searching_visitors,
		count(distinct hits_page_searchkeyword) searches,
		count(distinct sessionid) sessions_with_searches
from
		webanalytics.ds_bq_hits_elements
where
		nullif(hits_page_searchkeyword,'') is not null
and date_aest  between DATE_TRUNC('month', dateadd('months', -13, getdate_aest()))::date and   getdate_aest() -1
group by 1
)
,
daily_total_item_views as (
select
		date_trunc('day',date_aest) calendar_date,
		count(distinct case when nullif(user_uuid,'') is not null then user_uuid else cast(fullvisitorid as nvarchar(max)) end) item_view_visitors,
		count(item_id) item_views,
		count(distinct sessionid) sessions_with_item_views
from
		webanalytics.ds_bq_hits_elements
where
		nullif(item_id,'') is not null
and date_aest  between DATE_TRUNC('month', dateadd('months', -13, getdate_aest()))::date and   getdate_aest() -1
group by 1
)
,

daily_total_summary as (
select
		date_trunc('day',daily_total_sessions.calendar_date) calendar_date,
		'Daily' date_base,
		'All' user_type,
      'Not Applicable' plan_type,
       'Not Applicable' plan_detail,
		daily_total_sessions.visitors,
		daily_total_sessions.sessions,
		daily_total_sessions.sessions_duration,
		daily_total_sessions.bounces,
		daily_total_sessions.pageviews,
		daily_total_downloads.downloading_visitors,
		daily_total_downloads.downloads,
		daily_total_searches.searching_visitors,
		daily_total_searches.searches,
		daily_total_searches.sessions_with_searches,
        daily_total_item_views.item_view_visitors,
  		daily_total_item_views.item_views,
		daily_total_item_views.sessions_with_item_views
from
		daily_total_sessions
		left join daily_total_downloads
		on daily_total_sessions.calendar_date=daily_total_downloads.calendar_date
		left join daily_total_searches
		on daily_total_sessions.calendar_date=daily_total_searches.calendar_date
		left join daily_total_item_views
		on daily_total_sessions.calendar_date=daily_total_item_views.calendar_date
)
,
weekly_total_sessions as (
select
		date_trunc('week',date_aest) calendar_date,
		count(distinct case when nullif(user_uuid,'') is not null then user_uuid else cast(fullvisitorid as nvarchar(max)) end) visitors,
		count(sessionid) sessions,
		sum(nvl(totals_timeonsite,0)) sessions_duration,
		sum(nvl(totals_bounces,0)) bounces,
		sum(nvl(totals_pageviews,0)) pageviews
from
		webanalytics.ds_bq_sessions_elements
where date_aest  between DATE_TRUNC('month', dateadd('months', -13, getdate_aest()))::date and   getdate_aest() -1
group by 1
)
,
weekly_total_downloads as (
select
		date_trunc('week',a.download_started_at) calendar_date,
		count(distinct b.sso_uuid) downloading_visitors,
		count(id) downloads
from
		elements.ds_Elements_item_downloads a
		join envato.dim_users b
		on a.user_id=b.elements_id
where download_started_at between DATE_TRUNC('month', dateadd('months', -13, getdate_aest()))::date and   getdate_aest() -1
group by 1
)
,
weekly_total_searches as (
select
		date_trunc('week',date_aest) calendar_date,
		count(distinct case when nullif(user_uuid,'') is not null then user_uuid else cast(fullvisitorid as nvarchar(max)) end) searching_visitors,
		count(distinct hits_page_searchkeyword) searches,
		count(distinct sessionid) sessions_with_searches
from
		webanalytics.ds_bq_hits_elements
where
		nullif(hits_page_searchkeyword,'') is not null
and date_aest  between DATE_TRUNC('month', dateadd('months', -13, getdate_aest()))::date and   getdate_aest() -1

group by 1
),
weekly_total_item_views as (
select
		date_trunc('week',date_aest) calendar_date,
		count(distinct case when nullif(user_uuid,'') is not null then user_uuid else cast(fullvisitorid as nvarchar(max)) end) item_view_visitors,
		count(item_id) item_views,
		count(distinct sessionid) sessions_with_item_views
from
		webanalytics.ds_bq_hits_elements
where
		nullif(item_id,'') is not null
and date_aest  between DATE_TRUNC('month', dateadd('months', -13, getdate_aest()))::date and   getdate_aest() -1

group by 1
)
,
weekly_total_summary as (
select
		date_trunc('week',weekly_total_sessions.calendar_date) calendar_date,
		'Weekly' date_base,
		'All' user_type,
         'Not Applicable' plan_type,
       'Not Applicable' plan_detail,
		weekly_total_sessions.visitors,
		weekly_total_sessions.sessions,
		weekly_total_sessions.sessions_duration,
		weekly_total_sessions.bounces,
		weekly_total_sessions.pageviews,
		weekly_total_downloads.downloading_visitors,
		weekly_total_downloads.downloads,
		weekly_total_searches.searching_visitors,
		weekly_total_searches.searches,
		weekly_total_searches.sessions_with_searches,
        weekly_total_item_views.item_view_visitors,
  		weekly_total_item_views.item_views,
		weekly_total_item_views.sessions_with_item_views
from
		weekly_total_sessions
		left join weekly_total_downloads
		on weekly_total_sessions.calendar_date=weekly_total_downloads.calendar_date
		left join weekly_total_searches
		on weekly_total_sessions.calendar_date=weekly_total_searches.calendar_date
		left join weekly_total_item_views
		on weekly_total_sessions.calendar_date=weekly_total_item_views.calendar_date
)
,

      */
monthly_total_sessions as (
select
		date_trunc('month',date_aest) calendar_date,
		count(distinct case when nullif(user_uuid,'') is not null then user_uuid else cast(fullvisitorid as nvarchar(max)) end) visitors,
		count(sessionid) sessions,
		sum(nvl(totals_timeonsite,0)) sessions_duration,
		sum(nvl(totals_bounces,0)) bounces,
		sum(nvl(totals_pageviews,0)) pageviews
from
		webanalytics.ds_bq_sessions_elements
where date_aest  between DATE_TRUNC('month', dateadd('months', -13, getdate_aest()))::date and   getdate_aest() -1

group by 1
)
,
monthly_total_downloads as (
select
		date_trunc('month',a.download_started_at) calendar_date,
		count(distinct b.sso_uuid) downloading_visitors,
		count(id) downloads
from
		elements.ds_Elements_item_downloads a
		join envato.dim_users b
		on a.user_id=b.elements_id
where download_started_at  between DATE_TRUNC('month', dateadd('months', -13, getdate_aest()))::date and   getdate_aest() -1

group by 1
)
,
monthly_total_searches as (
select
		date_trunc('month',date_aest) calendar_date,
		count(distinct case when nullif(user_uuid,'') is not null then user_uuid else cast(fullvisitorid as nvarchar(max)) end) searching_visitors,
		count(distinct hits_page_searchkeyword) searches,
		count(distinct sessionid) sessions_with_searches
from
		webanalytics.ds_bq_hits_elements
where
		nullif(hits_page_searchkeyword,'') is not null
and date_aest  between DATE_TRUNC('month', dateadd('months', -13, getdate_aest()))::date and   getdate_aest() -1
group by 1
),
monthly_total_item_views as (
select
		date_trunc('month',date_aest) calendar_date,
		count(distinct case when nullif(user_uuid,'') is not null then user_uuid else cast(fullvisitorid as nvarchar(max)) end) item_view_visitors,
		count(item_id) item_views,
		count(distinct sessionid) sessions_with_item_views
from
		webanalytics.ds_bq_hits_elements
where
		nullif(item_id,'') is not null
and date_aest  between DATE_TRUNC('month', dateadd('months', -13, getdate_aest()))::date and   getdate_aest() -1

group by 1
)
,
monthly_total_summary as (
select
		date_trunc('month',monthly_total_sessions.calendar_date) calendar_date,
		'Monthly' date_base,
		'All' user_type,
        'Not Applicable' plan_type,
       'Not Applicable' plan_detail,
		monthly_total_sessions.visitors,
		monthly_total_sessions.sessions,
		monthly_total_sessions.sessions_duration,
		monthly_total_sessions.bounces,
		monthly_total_sessions.pageviews,
		monthly_total_downloads.downloading_visitors,
		monthly_total_downloads.downloads,
		monthly_total_searches.searching_visitors,
		monthly_total_searches.searches,
		monthly_total_searches.sessions_with_searches,
  		monthly_total_item_views.item_view_visitors,
		monthly_total_item_views.item_views,
		monthly_total_item_views.sessions_with_item_views
from
		monthly_total_sessions
		left join monthly_total_downloads
		on monthly_total_sessions.calendar_date=monthly_total_downloads.calendar_date
		left join monthly_total_searches
		on monthly_total_sessions.calendar_date=monthly_total_searches.calendar_date
		left join monthly_total_item_views
		on monthly_total_sessions.calendar_date=monthly_total_item_views.calendar_date
)


,
final_summary as (
-- select * from daily_guests_summary
-- union
-- select * from weekly_guests_summary
-- union
select * from monthly_guests_summary
union
-- select * from daily_free_accounts_summary
-- union
-- select * from weekly_free_accounts_summary
-- union
select * from monthly_free_accounts_summary
union
-- select * from daily_active_subs_summary
-- union
-- select * from weekly_active_subs_summary
-- union
select * from monthly_active_subs_summary
union
-- select * from daily_churned_subs_summary
-- union
-- select * from weekly_churned_subs_summary
-- union
select * from monthly_churned_subs_summary
union
-- select * from daily_total_summary
-- union
-- select * from weekly_total_summary
-- union
select * from monthly_total_summary
)
select * from final_summary
