-- market referral traffic

select
date,
site,
banner_name,
count(distinct impression_visitor) distinct_visitors_with_impressions,
count(distinct impression_session_id)distinct_session_with_impressions,
count(distinct impressions) total_impressions,
count(distinct click_visitor) click_visitors

from

(SELECT
date,
fullvisitorid impression_visitor,
fullvisitorId||'_'||VisitId impression_session_id,
fullvisitorId||'_'||VisitId||'_'||hits.hitnumber AS impressions,
"" click_visitor,
product.v2ProductName banner_name,
hits.page.hostname site,
max(case when cd.index = 2 then cd.value else null end ) user_id
FROM
`fabled-autonomy-89301.98771639.ga_sessions_*` , UNNEST(hits) hits, UNNEST(hits.product) product left join unnest (hits.customDimensions) cd
WHERE _TABLE_SUFFIX between '20210425' AND FORMAT_DATE('%Y%m%d',CURRENT_DATE())
AND hits.page.hostname in ('themeforest.net','codecanyon.net','audiojungle.net','videohive.net','graphicriver.net','photodune.net','3docean.net')
AND hits.eventinfo.eventCategory = 'Banner Impression'
AND hits.eventinfo.eventAction = 'view'
and (product.v2ProductName like 'elements_mkt-header_%' or product.v2ProductName like 'elements_mkt-footer_%' or product.v2ProductName like 'market_mkt-header%' or product.v2ProductName like 'market_mkt-footer%')
group by 1,2,3,4,5,6,7

union all

SELECT
date,
fullvisitorid impression_visitor,
fullvisitorId||'_'||VisitId impression_session_id,
fullvisitorId||'_'||VisitId||'_'||hits.hitnumber AS impressions,
"" click_visitor,
promo.promoId banner_name,
hits.page.hostname site,
max(case when cd.index = 2 then cd.value else null end ) user_id
FROM
`fabled-autonomy-89301.98771639.ga_sessions_*` , UNNEST(hits) hits, UNNEST(hits.promotion) promo left join unnest (hits.customDimensions) cd
WHERE _TABLE_SUFFIX between '20210425' AND FORMAT_DATE('%Y%m%d',CURRENT_DATE())
and hits.page.hostname in ('themeforest.net','codecanyon.net','audiojungle.net','videohive.net','graphicriver.net','photodune.net','3docean.net')
and hits.eventInfo.eventCategory = 'Banner Impression'
and hits.eventInfo.eventAction = 'view'
and (promo.promoId like 'elements_mkt-header_%' or promo.promoId like 'elements_mkt-footer_%'or promo.promoid like 'market_mkt-header%' or promo.promoid like 'market_mkt-footer%')
group by 1,2,3,4,5,6,7

union all

select
date,
"" impression_visitor,
"" impression_session_id,
"" impressions,
fullvisitorid click_visitor,
promo.promoId banner_name,
hits.page.hostname site,
max(case when cd.index = 2 then cd.value else null end ) user_id
from
`fabled-autonomy-89301.98771639.ga_sessions_*` , UNNEST(hits) hits, unnest(hits.promotion) promo  left join unnest (hits.customDimensions) cd
WHERE _TABLE_SUFFIX between '20210425' AND FORMAT_DATE('%Y%m%d',CURRENT_DATE())
and hits.type = 'EVENT'
and hits.eventInfo.eventCategory = 'Internal Promotions'
and hits.eventInfo.eventAction = 'click'
and hits.page.hostname in ('themeforest.net','codecanyon.net','audiojungle.net','videohive.net','graphicriver.net','photodune.net','3docean.net')
group by 1,2,3,4,5,6,7

)
group by 1,2,3