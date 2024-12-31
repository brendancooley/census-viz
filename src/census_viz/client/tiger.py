from typing import Optional
import httpx
from pathlib import Path
import json


class TigerClient:
    """Client for fetching TIGER/Line geographic data"""

    def __init__(
        self,
        base_url: str = "https://tigerweb.geo.census.gov/arcgis/rest/services/TIGERweb/tigerWMS_Current/MapServer",
        cache_dir: Optional[Path] = None,
    ):
        self.base_url = base_url
        self.cache_dir = cache_dir or Path("./data/tiger")
        self._client = httpx.AsyncClient()

    async def get_block_groups(self, state: str, county: str, year: int = 2020) -> dict:
        """
        Fetch block group geometries for a county using Census Web Mapping Service

        Args:
            state: State FIPS code
            county: County FIPS code
            year: Year of TIGER/Line data (defaults to 2020)

        Returns:
            GeoJSON FeatureCollection
        """
        # Create cache directory if it doesn't exist
        cache_path = self.cache_dir / f"{year}" / f"{state}" / f"{county}"
        cache_path.mkdir(parents=True, exist_ok=True)

        geojson_path = cache_path / "block_groups.geojson"

        # Check cache first
        if geojson_path.exists():
            with open(geojson_path) as f:
                return json.load(f)

        # Layer 10 is Block Groups in the current TIGERweb service
        url = f"{self.base_url}/10/query"

        # Query parameters for the REST API
        params = {
            "where": f"STATE='{state}' AND COUNTY='{county}'",
            "outFields": "*",
            "f": "geojson",
            "geometryPrecision": 5,
            "returnGeometry": "true",
            "spatialRel": "esriSpatialRelIntersects",
            "outSR": "4326",
            "geometry": "",
            "geometryType": "esriGeometryEnvelope",
            "inSR": "4326",
        }

        try:
            response = await self._client.get(url, params=params)
            response.raise_for_status()

            geojson = response.json()

            # Cache the result
            with open(geojson_path, "w") as f:
                json.dump(geojson, f)

            return geojson

        except httpx.HTTPStatusError as e:
            if e.response.status_code == 404:
                raise ValueError(
                    f"No TIGER/Line data found for state {state}, county {county}, year {year}"
                ) from e
            raise

    async def close(self):
        """Close the HTTP client"""
        await self._client.aclose()
