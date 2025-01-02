from typing import Optional
import polars as pl
from deltalake import DeltaTable
from pathlib import Path
import asyncio
from census_viz.client import CensusClient, TigerClient
from datetime import datetime
import json


class DataCollector:
    """Collects Census data and stores it in Delta Lake tables"""

    def __init__(
        self,
        table_path: str | Path,
        client: Optional[CensusClient] = None,
        tiger_client: Optional[TigerClient] = None,
    ):
        """
        Initialize the collector

        Args:
            table_path: Path to the Delta tables directory
            client: Optional CensusClient instance
            tiger_client: Optional TigerClient instance
        """
        self.table_path = Path(table_path)
        self.client = client or CensusClient()
        self.tiger_client = tiger_client or TigerClient()

    async def collect_state_data(
        self,
        state: str,
        county: str,
        year: int = 2021,
    ) -> tuple[pl.DataFrame, pl.DataFrame]:
        """
        Collect all block group data for a state

        Args:
            state: State FIPS code
            county: County FIPS code
            year: Year of ACS data

        Returns:
            Tuple of (demographics DataFrame, geometries DataFrame)
        """
        # Collect demographic data
        results = await self.client.get_population_data(
            year=year, state=state, county=county
        )

        # Convert demographic results to records
        demo_records = [
            {
                **r.geographic_area.model_dump(),
                **r.population_data.model_dump(),
                "state": state,
                "year": year,
                "collection_timestamp": datetime.utcnow(),
            }
            for r in results
        ]

        # Collect geometric data
        geojson = await self.tiger_client.get_block_groups(state=state, county=county)

        # Convert GeoJSON features to records
        geo_records = [
            {
                "geo_id": feature["properties"]["GEOID"],  # Block Group GEOID
                "geometry": json.dumps(feature["geometry"]),  # Store as GeoJSON string
                "state": state,
                "county": county,
                "tract": feature["properties"]["TRACT"],
                "block_group": feature["properties"]["BLKGRP"],
                "collection_timestamp": datetime.now(),
            }
            for feature in geojson["features"]
        ]

        return pl.DataFrame(demo_records), pl.DataFrame(geo_records)

    async def update_state(self, state: str, year: int = 2021) -> None:
        """
        Update data for a state, only inserting new records.
        Fetches data for all counties in the state.

        Args:
            state: State FIPS code
            year: Year of ACS data
        """
        # Get all counties in the state
        counties = await self.client.get_counties(state)

        # Collect data for each county
        demo_data = []
        geo_data = []
        for county in counties:
            demo_df, geo_df = await self.collect_state_data(
                state=state, county=county, year=year
            )
            demo_data.append(demo_df)
            geo_data.append(geo_df)

        # Combine all county data
        demo_df = pl.concat(demo_data)
        geo_df = pl.concat(geo_data)

        # Demographics table
        demo_table_path = self.table_path / "demographics"
        if not DeltaTable.is_deltatable(str(demo_table_path)):
            demo_df.write_delta(target=demo_table_path)
        else:
            demo_df.write_delta(
                target=demo_table_path,
                mode="merge",
                delta_merge_options={
                    "predicate": "s.geo_id = t.geo_id and s.year = t.year",
                    "source_alias": "s",
                    "target_alias": "t",
                },
            ).when_matched_update_all().when_not_matched_insert_all().execute()

        # Geometries table
        geo_table_path = self.table_path / "geometries"
        if not DeltaTable.is_deltatable(str(geo_table_path)):
            geo_df.write_delta(target=geo_table_path)
        else:
            geo_df.write_delta(
                target=geo_table_path,
                mode="merge",
                delta_merge_options={
                    "predicate": "s.geo_id = t.geo_id",
                    "source_alias": "s",
                    "target_alias": "t",
                },
            ).when_matched_update_all().when_not_matched_insert_all().execute()

    async def close(self):
        """Close the client connections"""
        await self.client.close()
        await self.tiger_client.close()


if __name__ == "__main__":
    asyncio.run(DataCollector(table_path="./data/pop_bg").update_state("06", 2021))
