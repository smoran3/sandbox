with tblA as(
select *, st_length(st_transform(geometry, 2272))/5280 as miles
from district_thresholds dt 
)
select sum(miles)
from tblA
where "VPHPD" > 0
and "VPHPD" <= 850


-- need to handle divided roads; mileage likely double counted now
