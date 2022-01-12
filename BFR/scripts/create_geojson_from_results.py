import env_vars as ev
from env_vars import ENGINE
import geopandas as gpd

gdf = gpd.read_postgis(
    """
    SELECT *
    FROM facility_results;
""",
    con=ENGINE,
    geom_col="geometry",
)

gdf.to_file(
    fr"D:/dvrpc_shared/Sandbox/BFR/shapes/facility_results.geojson",
    driver="GeoJSON",
)
print("To GeoJSON: Complete!")
