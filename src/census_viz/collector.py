from typing import Optional
import polars as pl
from deltalake import DeltaTable
from pathlib import Path
import asyncio
from census_viz.client import CensusClient
from datetime import datetime


class CensusCollector:
    """Collects Census data and stores it in a Delta Lake table"""

    def __init__(self, table_path: str | Path, client: Optional[CensusClient] = None):
        """
        Initialize the collector

        Args:
            table_path: Path to the Delta table
            client: Optional CensusClient instance
        """
        self.table_path = Path(table_path)
        self.client = client or CensusClient()

    async def collect_state_data(
        self,
        state: str,
        county: str,
        year: int = 2021,
    ) -> pl.DataFrame:
        """
        Collect all block group data for a state

        Args:
            state: State FIPS code
            year: Year of ACS data

        Returns:
            Polars DataFrame with the collected data
        """
        results = await self.client.get_population_data(
            year=year, state=state, county=county
        )

        # Convert results to records
        records = [
            {
                **r.geographic_area.model_dump(),
                **r.population_data.model_dump(),
                "state": state,
                "year": year,
                "collection_timestamp": datetime.utcnow(),
            }
            for r in results
        ]

        return pl.DataFrame(records)

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
        all_data = []
        for county in counties:
            county_data = await self.collect_state_data(
                state=state, county=county, year=year
            )
            all_data.append(county_data)

        # Combine all county data
        df = pl.concat(all_data)
        if not DeltaTable.is_deltatable(str(self.table_path)):
            df.write_delta(
                target=self.table_path,
            )
        else:
            df.write_delta(
                target=self.table_path,
                mode="merge",
                delta_merge_options={
                    "predicate": "s.geo_id = t.geo_id and s.year = t.year",
                    "source_alias": "s",
                    "target_alias": "t",
                },
            ).when_matched_update_all().when_not_matched_insert_all().execute()

    async def close(self):
        """Close the client connection"""
        await self.client.close()


if __name__ == "__main__":
    asyncio.run(CensusCollector(table_path="./data/pop_bg").update_state("06", 2021))
