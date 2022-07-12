with elements_coupons_prep as (
    SELECT
        a.dim_subscription_key,
        a.dim_elements_coupon_key,
        case
            when b.discount_percent=100 then 'full free'
            when lower(b.name) like '%free%' then 'full free'
            when b.dim_elements_coupon_key=0 then 'no coupon'
            else 'partial free'
        end as coupon_type,
        row_number() over (partition by a.dim_subscription_key order by dim_date_key asc) as invoice_number
    FROM
        elements.fact_elements_subscription_transactions a
        join elements.dim_elements_coupon b on (a.dim_elements_coupon_key=b.dim_elements_coupon_key)
    WHERE 1=1 and a.dim_elements_coupon_key>0
),
elements_sessions_base as (
    SELECT
        a.date_aest::date as session_date,
        a.sessionid,
        a.fullvisitorid,
        a.geonetwork_country,
        c.channel as channel,
        c.sub_channel as sub_channel,
        c.channel_detail AS channel_detail
    FROM
        webanalytics.ds_bq_sessions_elements a
        left join elements.rpt_elements_session_channel c on (a.sessionid=c.sessionid)
    WHERE 1=1
        and a.date<to_char(getdate_aest(),'YYYYMMDD')::INT
),
rd_sessions_cy AS (
    select
        'Current Year' as period,
        null as currency,
        a.session_date as calendar_date,
        a.session_date as session_date,
        1.00 as factor,
        a.channel as channel,
        a.sub_channel as sub_channel,
        a.channel_detail AS channel_detail,
        a.geonetwork_country AS geonetwork_country,
        null as initial_plan,
        null as preferred_locale,
        null as coupon_type_first_invoice,
        null as has_paying_subscription,
        count(a.sessionid) as sessions,
        count (distinct a.fullvisitorid) as visitors,
        0 as signups,
        0 as signups_ly,
        0 as first_subs,
        0 as return_subs,
        0 as total_subs,
        0 as first_subs_annual,
        0 as return_subs_annual,
        0 as total_subs_annual,
        0 as first_subs_ly,
        0 as return_subs_ly,
        0 as total_subs_ly,
        0 as first_subs_annual_ly,
        0 as return_subs_annual_ly,
        0 as total_subs_annual_ly,
        0 as terminations,
        0 as terminations_ly,
        0 as sessions_ly,
        0 as visitors_ly
    from
        elements_sessions_base a
    where 1=1
	and a.session_date>=dateadd('year', -3, date_trunc('year',getdate_aest()))::date
    group by 1,2,3,4,5,6,7,8,9,10,11,12,13
),
 --2. Sessions Query Last Year
rd_sessions_ly AS (
    select
        'Last Year' as period,
        null as currency,
        dateadd('year', +1, a.session_date) as calendar_date,
        dateadd('year', +1, a.session_date) as session_date,
        1.00 as factor,
        a.channel,
        a.sub_channel,
        a.channel_detail,
        a.geonetwork_country,
        null as initial_plan,
        null as preferred_locale,
        null as coupon_type_first_invoice,
        null as has_paying_subscription,
        0 as sessions,
        0 as visitors,
        0 as signups,
        0 as signups_ly,
        0 as first_subs,
        0 as return_subs,
        0 as total_subs,
        0 as first_subs_annual,
        0 as return_subs_annual,
        0 as total_subs_annual,
        0 as first_subs_ly,
        0 as return_subs_ly,
        0 as total_subs_ly,
        0 as first_subs_annual_ly,
        0 as return_subs_annual_ly,
        0 as total_subs_annual_ly,
        0 as terminations,
        0 as terminations_ly,
        count(a.sessionid) as sessions_ly,
        count (distinct a.fullvisitorid) as visitors_ly
    from
        elements_sessions_base a
    WHERE 1=1
        and a.session_date<=dateadd(day, -180, getdate_aest())::date
	and a.session_date>=dateadd('year', -3, date_trunc('year',getdate_aest()))::date
    group by 1,2,3,4,5,6,7,8,9,10,11,12,13
),
--3. Signups Query Current Year
rd_signups_cy AS (
    select
        'Current Year' as period,
        null as currency,
        cast(a.signup_date as date) as calendar_date,
        cast(a.session_date as date) as session_date,
        coalesce(f.factor,f1.factor,0.00) as factor,
        c.channel as channel,
        c.sub_channel as sub_channel,
        c.channel_detail AS channel_detail,
        b.geonetwork_country AS geonetwork_country,
        null as initial_plan,
        null as preferred_locale,
        null as coupon_type_first_invoice,
        null as has_paying_subscription,
        0 as sessions,
        0 as visitors,
        count(*) as signups,
        0 as signups_ly,
        0 as first_subs,
        0 as return_subs,
        0 as total_subs,
        0 as first_subs_annual,
        0 as return_subs_annual,
        0 as total_subs_annual,
        0 as first_subs_ly,
        0 as return_subs_ly,
        0 as total_subs_ly,
        0 as first_subs_annual_ly,
        0 as return_subs_annual_ly,
        0 as total_subs_annual_ly,
        0 as terminations,
        0 as terminations_ly,
        0 as sessions_ly,
        0 as visitors_ly
    from
        elements.rpt_elements_user_signup_session a
        left join elements_sessions_base b on a.sessionid=b.sessionid
        left join elements.rpt_elements_session_channel c on a.sessionid=c.sessionid
        --if a channel exists in factors table we take channel level factor
        LEFT JOIN analysts.rpt_elements_campaign_report_factors f on
        --last day available in the report is today-1, so this day needs to have 0 daysdiff factor
            CASE
                WHEN DATEDIFF(DAY,a.session_date::date,cast(envato.getdate_aest()-1 as date))>=365
                THEN 365
                ELSE DATEDIFF(DAY,a.session_date::date,cast(envato.getdate_aest()-1 as date))
            END = f.Daysdiff
            AND c.channel=f.channel
            --factor changes retroactively instead of using lates available version
            and a.session_date::date>=f.start_date and a.session_date::date<f.end_date
            --if a factor does not exist on channel level we take overall level
        LEFT JOIN analysts.rpt_elements_campaign_report_factors f1 on
            CASE WHEN DATEDIFF(DAY,a.session_date::date,cast(envato.getdate_aest()-1 as date))>=365 THEN 365
                ELSE DATEDIFF(DAY,a.session_date::date,cast(envato.getdate_aest()-1 as date))
            END = f1.Daysdiff
            AND f1.channel='overall'
            --factor changes retroactively instead of using lates available version
            and a.session_date::date>=f1.start_date and a.session_date::date<f1.end_date
    WHERE 1=1
        AND a.signup_date::date<getdate_aest()::date
        AND a.signup_date::date>=dateadd('year',-3,date_trunc('year',getdate_aest()))::date
    group by 1,2,3,4,5,6,7,8,9,10,11,12,13
),
--4. Signups Query Last Year
rd_signups_ly AS (
    select
        'Last Year' as period,
        null as currency,
        dateadd('year', +1, cast(a.signup_date as date)) as calendar_date,
        dateadd('year', +1, cast(a.session_date as date)) as session_date,
        coalesce(f.factor,f1.factor,0.00) as factor,
        c.channel as channel,
        c.sub_channel as sub_channel,
        c.channel_detail AS channel_detail,
        b.geonetwork_country AS geonetwork_country,
        null as initial_plan,
        null as preferred_locale,
        null as coupon_type_first_invoice,
        null as has_paying_subscription,
        0 as sessions,
        0 as visitors,
        0 as signups,
        count(*) as signups_ly,
        0 as first_subs,
        0 as return_subs,
        0 as total_subs,
        0 as first_subs_annual,
        0 as return_subs_annual,
        0 as total_subs_annual,
        0 as first_subs_ly,
        0 as return_subs_ly,
        0 as total_subs_ly,
        0 as first_subs_annual_ly,
        0 as return_subs_annual_ly,
        0 as total_subs_annual_ly,
        0 as terminations,
        0 as terminations_ly,
        0 as sessions_ly,
        0 as visitors_ly
    from
        elements.rpt_elements_user_signup_session a
        left join elements_sessions_base b on a.sessionid=b.sessionid
        left join elements.rpt_elements_session_channel c on a.sessionid=c.sessionid
        --if a channel exists in factors table we take channel level factor
        LEFT JOIN analysts.rpt_elements_campaign_report_factors f on
        --last day available in the report is today-1, so this day needs to have 0 daysdiff factor
            CASE
            WHEN DATEDIFF(DAY,a.session_date::date,cast(envato.getdate_aest()-1 as date))>=365 THEN 365
                ELSE DATEDIFF(DAY,a.session_date::date,cast(envato.getdate_aest()-1 as date))
            END = f.Daysdiff
            AND c.channel=f.channel
            --factor changes retroactively instead of using lates available version
            and a.session_date::date>=f.start_date and a.session_date::date<f.end_date
        --if a factor does not exist on channel level we take overall level
        LEFT JOIN analysts.rpt_elements_campaign_report_factors f1 on
            CASE
                WHEN DATEDIFF(DAY,a.session_date::date,cast(envato.getdate_aest()-1 as date))>=365 THEN 365
                ELSE DATEDIFF(DAY,a.session_date::date,cast(envato.getdate_aest()-1 as date))
            END = f1.Daysdiff
        AND f1.channel='overall'
        --factor changes retroactively instead of using lates available version
        and a.session_date::date>=f1.start_date and a.session_date::date<f1.end_date
    WHERE 1=1
        AND a.signup_date::date<=dateadd('day',-180,getdate_aest())::date
        AND a.signup_date::date>=dateadd('year',-3,date_trunc('year',getdate_aest()))::date
        group by 1,2,3,4,5,6,7,8,9,10,11,12,13
),
--5. Subscriptions Query Current Year
rd_subs_cy as (
    select
        'Current Year' as period,
        res1.currency,
        cast(res1.subscription_start_date as date) as calendar_date,
        cast(e.session_date as date) as session_date,
        coalesce(h.factor,h1.factor,0.00) as factor,
        b.channel as channel,
        b.sub_channel as sub_channel,
        b.channel_detail AS channel_detail,
        f.geonetwork_country,
        res1.initial_plan,
        res1.preferred_locale,
        case when res1.subscription_started_on_trial is true and res1.subscription_start_date::date>='2021-02-08' and res1.subscription_platform='recurly' then 'free trial' else coalesce(ec.coupon_type,'no coupon') end as coupon_type_first_invoice,
        case when res1.has_successful_payment then 'Paying Subscription' else 'Not Paying Subscription' end as has_paying_subscription,
        0 as sessions,
        0 as visitors,
        0 as signups,
        0 as signups_ly,
        sum(case when is_first_subscription then 1 end) as first_subs,
        sum(case when not is_first_subscription then 1 end) return_subs,
        sum(1) as total_subs,
        sum(case when is_first_subscription and initial_plan like '%_annual' then 1 end) as first_subs_annual,
        sum(case when not is_first_subscription and initial_plan like '%_annual' then 1 end) as return_subs_annual,
        sum(case when initial_plan like '%_annual' then 1 end) as total_subs_annual,
        0 as first_subs_ly,
        0 as return_subs_ly,
        0 as total_subs_ly,
        0 as first_subs_annual_ly,
        0 as return_subs_annual_ly,
        0 as total_subs_annual_ly,
        0 as terminations,
        0 as terminations_ly,
        0 as sessions_ly,
        0 as visitors_ly
    from
    elements.dim_elements_subscription as res1
    left join elements.dim_elements_channel b on (res1.dim_elements_channel_key=b.dim_elements_channel_key)
    left join elements.rpt_elements_subscription_session e on (res1.dim_subscription_key=e.dim_subscription_key)
    left join elements_sessions_base f on (e.sessionid=f.sessionid)
    --if a channel exists in factors table we take channel level factor
    LEFT JOIN analysts.rpt_elements_campaign_report_factors h on
    --last day available in the report is today-1, so this day needs to have 0 daysdiff factor
        CASE
            WHEN DATEDIFF(DAY,e.session_date::date,cast(envato.getdate_aest()-1 as date))>=365 THEN 365
            ELSE DATEDIFF(DAY,e.session_date::date,cast(envato.getdate_aest()-1 as date))
        END = h.Daysdiff
        AND b.channel=h.channel
        --factor changes retroactively instead of using lates available version
        and e.session_date::date>=h.start_date and e.session_date::date<h.end_date
        --if a factor does not exist on channel level we take overall level
    LEFT JOIN analysts.rpt_elements_campaign_report_factors h1 on
        CASE
            WHEN DATEDIFF(DAY,e.session_date::date,cast(envato.getdate_aest()-1 as date))>=365 THEN 365
            ELSE DATEDIFF(DAY,e.session_date::date,cast(envato.getdate_aest()-1 as date))
        END = h1.Daysdiff
        AND h1.channel='overall'
        --factor changes retroactively instead of using lates available version
        and e.session_date::date>=h1.start_date and e.session_date::date<h1.end_date
    left join elements_coupons_prep ec on (res1.dim_subscription_key=ec.dim_subscription_key and ec.invoice_number=1)
    WHERE 1=1
        AND res1.subscription_start_date::date<getdate_aest()::date
        AND res1.subscription_start_date::date>=dateadd('year',-3,date_trunc('year',getdate_aest()))::date
    and not plan_change
    group by 1,2,3,4,5,6,7,8,9,10,11,12,13
),
--6. Subscriptions Query Last Year
rd_subs_ly as (
    select
    'Last Year' as period,
    res1.currency,
    dateadd('year', +1, cast(res1.subscription_start_date as date)) as calendar_date,
    dateadd('year', +1, cast(e.session_date as date)) as session_date,
    coalesce(h.factor,h1.factor,0.00) as factor,
    b.channel as channel,
    b.sub_channel as sub_channel,
    b.channel_detail AS channel_detail,
    f.geonetwork_country,
    res1.initial_plan,
    res1.preferred_locale,
    case when res1.subscription_started_on_trial is true and res1.subscription_start_date::date>='2021-02-08' and res1.subscription_platform='recurly' then 'free trial' else coalesce(ec.coupon_type,'no coupon') end as coupon_type_first_invoice,
    case when res1.has_successful_payment then 'Paying Subscription' else 'Not Paying Subscription' end as has_paying_subscription,
    0 as sessions,
    0 as visitors,
    0 as signups,
    0 as signups_ly,
    0 as first_subs,
    0 as return_subs,
    0 as total_subs,
    0 as first_subs_annual,
    0 as return_subs_annual,
    0 as total_subs_annual,
    sum(case when is_first_subscription then 1 end) as first_subs_ly,
    sum(case when not is_first_subscription then 1 end) as return_subs_ly,
    sum(1) as total_subs_ly,
    sum(case when is_first_subscription and initial_plan like '%_annual' then 1 end) as first_subs_annual_ly,
    sum(case when not is_first_subscription and initial_plan like '%_annual' then 1 end) as return_subs_annual_ly,
    sum(case when initial_plan like '%_annual' then 1 end) as total_subs_annual_ly,
    0 as terminations,
    0 as terminations_ly,
    0 as sessions_ly,
    0 as visitors_ly
    from
    elements.dim_elements_subscription as res1
    left join elements.dim_elements_channel b on (res1.dim_elements_channel_key=b.dim_elements_channel_key)
    left join elements.rpt_elements_subscription_session e on (res1.dim_subscription_key=e.dim_subscription_key)
    left join elements_sessions_base f on (e.sessionid=f.sessionid)
    --if a channel exists in factors table we take channel level factor
    LEFT JOIN analysts.rpt_elements_campaign_report_factors h on
    --last day available in the report is today-1, so this day needs to have 0 daysdiff factor
        CASE
            WHEN DATEDIFF(DAY,e.session_date::date,cast(envato.getdate_aest()-1 as date))>=365 THEN 365
            ELSE DATEDIFF(DAY,e.session_date::date,cast(envato.getdate_aest()-1 as date))
        END = h.Daysdiff
        AND b.channel=h.channel
        --factor changes retroactively instead of using lates available version
        and e.session_date::date>=h.start_date and e.session_date::date<h.end_date
        --if a factor does not exist on channel level we take overall level
    LEFT JOIN analysts.rpt_elements_campaign_report_factors h1 on
        CASE
            WHEN DATEDIFF(DAY,e.session_date::date,cast(envato.getdate_aest()-1 as date))>=365 THEN 365
            ELSE DATEDIFF(DAY,e.session_date::date,cast(envato.getdate_aest()-1 as date))
        END = h1.Daysdiff
        AND h1.channel='overall'
        --factor changes retroactively instead of using lates available version
        and e.session_date::date>=h1.start_date and e.session_date::date<h1.end_date
    left join elements_coupons_prep ec on (res1.dim_subscription_key=ec.dim_subscription_key and ec.invoice_number=1)
    WHERE 1=1
        AND res1.subscription_start_date::date <= dateadd('day',-180,getdate_aest())::date
        AND res1.subscription_start_date::date>=dateadd('year',-3,date_trunc('year',getdate_aest()))::date
        and not plan_change
    group by 1,2,3,4,5,6,7,8,9,10,11,12,13
),
--7. Terminations Query Current Year
rd_terminations_cy as (
		select
        'Current Year' as period,
        null as currency,
        cast(termination_date as date) as calendar_date,
        cast(termination_date as date) as session_date,
        1.00 as factor,
        b.channel as channel,
        b.sub_channel as sub_channel,
        b.channel_detail AS channel_detail,
        f.geonetwork_country,
		a.initial_plan,
		a.preferred_locale,
        case when a.subscription_started_on_trial is true and a.subscription_start_date::date>='2021-02-08' and a.subscription_platform='recurly' then 'free trial' else coalesce(ec.coupon_type,'no coupon') end as coupon_type_first_invoice,
        case when a.has_successful_payment then 'Paying Subscription' else 'Not Paying Subscription' end as has_paying_subscription,
        0 as sessions,
		0 as visitors,
		0 as signups,
        0 as signups_ly,
        0 as first_subs,
        0 as return_subs,
        0 as total_subs,
        0 as first_subs_annual,
        0 as return_subs_annual,
        0 as total_subs_annual,
        0 as first_subs_ly,
        0 as return_subs_ly,
        0 as total_subs_ly,
        0 as first_subs_annual_ly,
        0 as return_subs_annual_ly,
        0 as total_subs_annual_ly,
        sum(1) as terminations,
        0 as terminations_ly,
        0 as sessions_ly,
        0 as visitors_ly
				--If Recurly - need to examine the lastest record by start date, and check if this one has a termination date or not
		from
				elements.dim_elements_subscription a
				left join elements.dim_elements_channel b on (a.dim_elements_channel_key=b.dim_elements_channel_key)
                left join elements_coupons_prep ec on (a.dim_subscription_key=ec.dim_subscription_key and ec.invoice_number=1)
                left join elements.rpt_elements_subscription_session e on (a.dim_subscription_key=e.dim_subscription_key)
                left join elements_sessions_base f on (e.sessionid=f.sessionid)
		where
				subscription_platform='braintree'
				and termination_date is not null
                AND termination_date::date <getdate_aest()::date
                AND termination_date::date>=dateadd('year',-3,date_trunc('year',getdate_aest()))::date
		group by 1,2,3,4,5,6,7,8,9,10,11,12,13
		union all
		--Recurly terminations
		--If Recurly - need to examine the lastest record by start date, and check if this one has a termination date or not
		select
        'Current Year' as period,
        null as currency,
        cast(termination_date as date) as calendar_date,
        cast(termination_date as date) as session_date,
        1.00 as factor,
        b.channel as channel,
        b.sub_channel as sub_channel,
        b.channel_detail AS channel_detail,
        f.geonetwork_country,
        a.initial_plan,
        a.preferred_locale,
        case when a.subscription_started_on_trial is true and a.subscription_start_date::date>='2021-02-08' and a.subscription_platform='recurly' then 'free trial' else coalesce(ec.coupon_type,'no coupon') end as coupon_type_first_invoice,
        case when a.has_successful_payment then 'Paying Subscription' else 'Not Paying Subscription' end as has_paying_subscription,
        0 as sessions,
        0 as visitors,
        0 as signups,
        0 as signups_ly,
        0 as first_subs,
        0 as return_subs,
        0 as total_subs,
        0 as first_subs_annual,
        0 as return_subs_annual,
        0 as total_subs_annual,
        0 as first_subs_ly,
        0 as return_subs_ly,
        0 as total_subs_ly,
        0 as first_subs_annual_ly,
        0 as return_subs_annual_ly,
        0 as total_subs_annual_ly,
        sum(1) as terminations,
        0 as terminations_ly,
        0 as sessions_ly,
        0 as visitors_ly

		from
				(select
						*
				from
						(select
								*,
								row_number() over (partition by recurly_subscription_id order by subscription_start_date desc) recurly_subscription_id_index
						from
								elements.dim_elements_subscription
						where
								recurly_subscription_id is not null
						)
				where
						recurly_subscription_id_index=1) a
				left join elements.dim_elements_channel b on (a.dim_elements_channel_key=b.dim_elements_channel_key)
                left join elements_coupons_prep ec on (a.dim_subscription_key=ec.dim_subscription_key and ec.invoice_number=1)
                left join elements.rpt_elements_subscription_session e on (a.dim_subscription_key=e.dim_subscription_key)
                left join elements_sessions_base f on (e.sessionid=f.sessionid)
		where
				--Though not needed, as recurly sub id is not null - but just for the clarification:
				subscription_platform<>'braintree'
				and termination_date is not null
                AND termination_date::date <getdate_aest()::date
                AND termination_date::date>=dateadd('year',-3,date_trunc('year',getdate_aest()))::date
		group by 1,2,3,4,5,6,7,8,9,10,11,12,13
),
--8. Terminations Query Last Year
rd_termination_ly as (
		select
        'Last Year' as period,
        null as currency,
        dateadd('year', +1, cast(termination_date as date)) as calendar_date,
        dateadd('year', +1, cast(termination_date as date)) as session_date,
        1.00 as factor,
        b.channel as channel,
        b.sub_channel as sub_channel,
        b.channel_detail AS channel_detail,
        f.geonetwork_country,
		a.initial_plan,
		a.preferred_locale,
        case when a.subscription_started_on_trial is true and a.subscription_start_date::date>='2021-02-08' and a.subscription_platform='recurly' then 'free trial' else coalesce(ec.coupon_type,'no coupon') end as coupon_type_first_invoice,
        case when a.has_successful_payment then 'Paying Subscription' else 'Not Paying Subscription' end as has_paying_subscription,
        0 as sessions,
		0 as visitors,
		0 as signups,
        0 as signups_ly,
        0 as first_subs,
        0 as return_subs,
        0 as total_subs,
        0 as first_subs_annual,
        0 as return_subs_annual,
        0 as total_subs_annual,
        0 as first_subs_ly,
        0 as return_subs_ly,
        0 as total_subs_ly,
        0 as first_subs_annual_ly,
        0 as return_subs_annual_ly,
        0 as total_subs_annual_ly,
        0 as terminations,
		sum(1) as terminations_ly,
        0 as sessions_ly,
        0 as visitors_ly

				--If Recurly - need to examine the lastest record by start date, and check if this one has a termination date or not
		from
				elements.dim_elements_subscription a
				left join elements.dim_elements_channel b on (a.dim_elements_channel_key=b.dim_elements_channel_key)
                left join elements_coupons_prep ec on (a.dim_subscription_key=ec.dim_subscription_key and ec.invoice_number=1)
                left join elements.rpt_elements_subscription_session e on (a.dim_subscription_key=e.dim_subscription_key)
                left join elements_sessions_base f on (e.sessionid=f.sessionid)
		where
				subscription_platform='braintree'
				and termination_date is not null
                AND termination_date::date<dateadd('day',-180,getdate_aest()::date)
                AND termination_date::date>=dateadd('year',-3,date_trunc('year',getdate_aest()))::date
		group by 1,2,3,4,5,6,7,8,9,10,11,12,13
		union all
		--Recurly terminations
		--If Recurly - need to examine the lastest record by start date, and check if this one has a termination date or not
		select
        'Last Year' as period,
        null as currency,
        dateadd('year', +1, cast(termination_date as date)) as calendar_date,
        dateadd('year', +1, cast(termination_date as date)) as session_date,
        1.00 as factor,
        b.channel as channel,
        b.sub_channel as sub_channel,
        b.channel_detail AS channel_detail,
        f.geonetwork_country,
        a.initial_plan,
        a.preferred_locale,
        case when a.subscription_started_on_trial is true and a.subscription_start_date::date>='2021-02-08' and a.subscription_platform='recurly' then 'free trial' else coalesce(ec.coupon_type,'no coupon') end as coupon_type_first_invoice,
        case when a.has_successful_payment then 'Paying Subscription' else 'Not Paying Subscription' end as has_paying_subscription,
        0 as sessions,
        0 as visitors,
        0 as signups,
        0 as signups_ly,
        0 as first_subs,
        0 as return_subs,
        0 as total_subs,
        0 as first_subs_annual,
        0 as return_subs_annual,
        0 as total_subs_annual,
        0 as first_subs_ly,
        0 as return_subs_ly,
        0 as total_subs_ly,
        0 as first_subs_annual_ly,
        0 as return_subs_annual_ly,
        0 as total_subs_annual_ly,
        0 as terminations,
		sum(1) as terminations_ly,
        0 as sessions_ly,
        0 as visitors_ly

		from
				(select
						*
				from
						(select
								*,
								row_number() over (partition by recurly_subscription_id order by subscription_start_date desc) recurly_subscription_id_index
						from
								elements.dim_elements_subscription
						where
								recurly_subscription_id is not null
						)
				where
						recurly_subscription_id_index=1) a
				left join elements.dim_elements_channel b on (a.dim_elements_channel_key=b.dim_elements_channel_key)
                left join elements_coupons_prep ec on (a.dim_subscription_key=ec.dim_subscription_key and ec.invoice_number=1)
                left join elements.rpt_elements_subscription_session e on (a.dim_subscription_key=e.dim_subscription_key)
                left join elements_sessions_base f on (e.sessionid=f.sessionid)
		where
				--Though not needed, as recurly sub id is not null - but just for the clarification:
				subscription_platform<>'braintree'
				and termination_date is not null
                AND termination_date::date<dateadd('day',-180,getdate_aest()::date)
                AND termination_date::date>=dateadd('year',-3,date_trunc('year',getdate_aest()))::date
		group by 1,2,3,4,5,6,7,8,9,10,11,12,13
)
--9. Final Query
, final
select
    dateadd('day',-1,getdate_aest()::date) as last_date, --This will be used as the Pivot point between
    RD.period,
    RD.session_date,
    RD.factor,
    RD.currency,
    RD.calendar_date,
    RD.channel,
    RD.sub_channel as sub_channel,
    RD.channel_detail AS channel_detail,
    RD.geonetwork_country AS geonetwork_country,
	RD.initial_plan,
    Rd.preferred_locale,
    rd.coupon_type_first_invoice,
    rd.has_paying_subscription,
    sum(sessions) as sessions,
    sum(visitors) as visitors,
    sum(signups) as signups,
    sum(signups_ly) as signups_ly,
    sum(first_subs) as first_subs,
    sum(return_subs) as return_subs,
    sum(total_subs) as total_subs,
    sum(first_subs_annual) as first_subs_annual,
    sum(return_subs_annual) as return_subs_annual,
    sum(total_subs_annual) as total_subs_annual,
    sum(first_subs_ly) as first_subs_ly,
    sum(return_subs_ly) as return_subs_ly,
    sum(total_subs_ly) as total_subs_ly,
    sum(first_subs_annual_ly) as first_subs_annual_ly,
    sum(return_subs_annual_ly) as return_subs_annual_ly,
    sum(total_subs_annual_ly) as total_subs_annual_ly,
    sum(terminations) as terminations,
    sum(terminations_ly) as terminations_ly,
    sum(sessions_ly) as sessions_ly,
    sum(visitors_ly) as visitors_ly
from
  (
    select * from rd_sessions_cy
    union
    select * from rd_sessions_ly
    union
    select * from rd_signups_cy
    union
    select * from rd_signups_ly
    union
    select * from rd_subs_cy
    union
    select * from rd_subs_ly
    union
    select * from rd_terminations_cy
    union
    select * from rd_termination_ly
  ) AS RD
group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14


limit 10


;

