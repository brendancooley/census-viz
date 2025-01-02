import folium
import json
import branca.colormap as cm
from pathlib import Path
import polars as pl


class CensusMapper:
    """Creates interactive census visualizations"""

    def __init__(self, demo_table_path: str, geo_table_path: str, state: str):
        """
        Initialize the mapper

        Args:
            demo_table_path: Path to demographics delta table
            geo_table_path: Path to geometries delta table
        """
        # Load data
        demo_df = (
            pl.scan_delta(demo_table_path)
            .filter(pl.col("total_population") > 0)
            .with_columns(
                (pl.col("under_18") / pl.col("total_population")).alias("under_18_pct"),
                (pl.col("school_age") / pl.col("total_population")).alias(
                    "school_age_pct"
                ),
            )
        )
        geo_df = pl.scan_delta(geo_table_path)

        self.df = (
            demo_df.join(geo_df, on="geo_id", how="left")
            .filter(pl.col("state") == state)
            .collect()
        )

    def create_map(
        self,
        value_column: str,
        filters: list[pl.Expr] | None = None,
        center: tuple[float, float] = (37.8, -122.4),  # SF Bay Area
        zoom_start: int = 9,
    ) -> folium.Map:
        """
        Create an interactive choropleth map

        Args:
            value_column: Column to use for choropleth coloring
            center: (lat, lon) tuple for map center
            zoom_start: Initial zoom level

        Returns:
            Folium Map object
        """
        # Create base map
        m = folium.Map(location=center, zoom_start=zoom_start, tiles="cartodbpositron")

        if filters:
            df = self.df.filter(*filters)
        else:
            df = self.df

        # Create color scale
        values = df.get_column(value_column)
        colormap = cm.LinearColormap(
            colors=[
                "#f7fcfd",
                "#e5f5f9",
                "#ccece6",
                "#99d8c9",
                "#66c2a4",
                "#41ae76",
                "#238b45",
                "#005824",
            ],
            vmin=values.min(),
            vmax=values.max(),
        )

        # Add GeoJSON layer
        folium.GeoJson(
            data={
                "type": "FeatureCollection",
                "features": [
                    {
                        "type": "Feature",
                        "geometry": json.loads(row["geometry"]),
                        "properties": {
                            "value": row[value_column],
                            "name": row["name"],
                            "total_population": row["total_population"],
                            "median_income": row["median_income"],
                        },
                    }
                    for row in df.iter_rows(named=True)
                ],
            },
            style_function=lambda x: {
                "fillColor": colormap(x["properties"]["value"]),
                "color": "black",
                "weight": 1,
                "fillOpacity": 0.7,
            },
            tooltip=folium.GeoJsonTooltip(
                fields=["name", "value", "total_population", "median_income"],
                aliases=[
                    "Location:",
                    f"{value_column}:",
                    "Total Population:",
                    "Median Income:",
                ],
                localize=True,
            ),
        ).add_to(m)

        # Add color scale
        colormap.add_to(m)

        return m

    def save_map(self, map_obj: folium.Map, output_path: str | Path):
        """Save map to HTML file"""
        map_obj.save(str(output_path))
