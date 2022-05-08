-- get cookies/ sessions/ subscribers
SELECT
_TABLE_SUFFIx as date_day,
exp.experimentvariant,
count(distinct a.fullvisitorid) as cookies,
count(distinct a.fullvisitorid||'_'||visitid) sessions,
count(distinct so.ssoId) as ssos


  FROM
    `fabled-autonomy-89301.113666438.ga_sessions_*` a
    ,UNNEST (hits) hits
    ,unnest(hits.experiment) exp 
    -- left join unnest(hits.product) product
    -- left join UNNEST(hits.customDimensions) cd
    left join fact_utility.elements_ssoIds so on a.fullvisitorid = so.fullVisitorId
    and so.date between '20211028' and '20211118'
    where _TABLE_SUFFIx between '20211028' and '20211118'
 and exp.experimentid ='geInrbFNTa2AZdiRswkP2A'
 group by 1,2

 -- get subscribers enrolled per day based on their first variant assignment

 with enrollments as (
SELECT
so.ssoId,
exp.experimentvariant as variant,
min(timestamp_seconds(visitStartTime)) as exp_date
  FROM
    `fabled-autonomy-89301.113666438.ga_sessions_*` a
    ,UNNEST (hits) hits
    ,unnest(hits.experiment) exp
 join fact_utility.elements_ssoIds so on a.fullvisitorid = so.fullVisitorId
    -- and so.date between '20211028' and '20211118'
    where _TABLE_SUFFIx between '2021028' and '20211118'
 and exp.experimentid ='geInrbFNTa2AZdiRswkP2A'
 and exp.experimentvariant in ('0', '1')
 group by 1,2),

multiple_allocations as
 (select
 ssoId
 from enrollments
 group by 1
 having min(variant) != max(variant)),

 allocations as
 (select
 a.ssoId,
SPLIT(min(exp_date || '|' || variant), '|')[OFFSET(1)] as var,
 max(case when m.ssoId is not null then 'reallocated' else 'not_reallocated' end) as reallocation,
 min(CAST(DATE(exp_date) AS DATE)) as day_date
 from enrollments a
 left join multiple_allocations m on a.ssoId = m.ssoId
 group by 1)

 select
var,
reallocation,
day_date,
count(distinct ssoId) as sso_subs

from allocations
 group by 1,2,3
