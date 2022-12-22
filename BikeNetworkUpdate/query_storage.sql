--create subsets of network links to work with in different ways
create table usable_link_types as(
    select *
    from lts_network ln2 
    where "LinkType" not in (0, 1, 2, 3, 4, 5, 6, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19)
    and "LinkType" < 80)


create table oneway as(
	select *
	from usable_link_types ult  
	where ("OneWay" = 1
	or "R_OneWay" = 1)
	and "BikeFac" <> 9
	
)
create table bike_fac as(
	select *
	from usable_link_types ult 
	where "BikeFac"  = 9
	or "No" in (
		select "No"
		from usable_link_types ult2 
		where "BikeFac" = 9)
		)

create table rest as(
	select *
	from usable_link_types ult  
	where "OneWay" = 0
	and "R_OneWay" = 0
	and "BikeFac" <> 9
	and "No" not in (
		select "No" from bike_fac bf)
)

-------------
CREATE table undirected_links (like lts_network);

-- ONE WAY
-- single record
with single_nums as(
	select "No", count("No")
	from oneway o 
	group by "No"
	having count("No") <= 1 
)
insert into undirected_links 
select *
from oneway o2 
where "No" in(
	select "No"
	from single_nums)

--duplicate record
with double_nums as(
	select "No", count("No")
	from oneway o 
	group by "No"
	having count("No") > 1 
)
insert into undirected_links 
select distinct on("No") *
from oneway o2 
where "No" in(
	select "No"
	from double_nums)

-- BIKE FAC
-- bikefac = 9 in 2 directions; keep record with higher speed, or either one if speed is same
with tblA as(
select *
from bike_fac bf 
where "BikeFac" = 9
),
tblC as(
select "No", count("No")
from tblA  
group by "No"
having count("No") > 1
),
tblD as(
select *
from bike_fac
where "No" in (
	select "No"
	from tblC)
	)
insert into undirected_links
select distinct on ("No") *
from tblD 
order by "No", "Speed" desc

-- bikefac = 9 in 1 direction; drop it and keep the opposite direction
with tblA as(
	select *
	from bike_fac bf 
	where "BikeFac" = 9
),
tblC as(
	select "No", count("No")
	from tblA  
	group by "No"
	having count("No") <= 1
)
insert into undirected_links
select *
from bike_fac
where "BikeFac" <> 9
and "No" in (
	select "No"
	from tblC)

-- REST
-- if there are 2, keep one
with double_nums as(
	select "No", count("No")
	from rest r
	group by "No"
	having count("No") > 1 
)
insert into undirected_links 
select distinct on("No") *
from rest r2
where "No" in(
	select "No"
	from double_nums)

-- if there is one, keep it
with single_nums as(
	select "No", count("No")
	from rest r
	group by "No"
	having count("No") = 1 
)
insert into undirected_links 
select distinct on("No") *
from rest r2
where "No" in(
	select "No"
	from single_nums)

--clean up by dropping interim tables
DROP TABLE rest;
DROP TABLE bike_fac;
DROP TABLE oneway;