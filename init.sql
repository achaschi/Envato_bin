-- hello world
-- test merge
select
heaps_of_data
from  a_source
where source is not null
group by 1
having heaps_of_data > 0
;
