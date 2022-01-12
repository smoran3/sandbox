--select and buffer existing bike lanes
WITH tblA AS(
	SELECT bikefacili, st_buffer(geom, 10) AS geom
	FROM lts_clip
	WHERE bikefacili = 'Bike Lane'),
-- combine adjacent multipolygon buffers into single polygon
tblB AS (
	SELECT (ST_Dump(geom)).geom 
	FROM (SELECT ST_Union(geom) AS geom FROM tblA) s),
-- create points along facility result segments
tblC AS (
    SELECT facility_results.*,
    st_lineinterpolatepoint(facility_results.geometry, LEAST(n*(250/ST_Length(geometry)), 1.0)) as pointgeom
    FROM facility_results
    CROSS JOIN
        Generate_Series(0, CEIL(ST_Length(geometry)/250)::INT) AS n),
-- create start and end points along facility results
tblD AS(
    SELECT facility_results.*,
    st_startpoint(facility_results.geometry) as startgeom,
    st_endpoint(facility_results.geometry) as endgeom
    FROM facility_results
),
-- select subset of points created in table C that do not include start and end points (tblD)
tblE AS(
    SELECT tblC.*
    FROM tblC
    WHERE 
        tblC.pointgeom not IN(
        select tblC.pointgeom
        from tblC, tblD
        where st_intersects(tblC.pointgeom, tblD.startgeom)
        OR st_intersects(tblC.pointgeom, tblD.endgeom)
        ))
-- select facility results that do not have vertices within the buffer of a segment with an existing bike lane
SELECT tblE.*
FROM tblE
WHERE 
	tblE.geometry not in(
	select tblE.geometry
	from tblE, tblB
	where ST_within(tblE.pointgeom, tblB.geom)
	)
