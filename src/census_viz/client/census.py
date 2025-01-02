from typing import List, Optional
import httpx
from census_viz.models import CensusResponse, GeographicArea, PopulationData
from census_viz.config import settings


class CensusAPIError(Exception):
    """Custom exception for Census API errors"""

    pass


class CensusClient:
    """Client for interacting with the Census Bureau API"""

    def __init__(
        self, api_key: str | None = None, base_url: str = "https://api.census.gov/data"
    ):
        self.api_key = api_key or settings.census_api_key
        self.base_url = base_url
        self._client = httpx.AsyncClient()

    async def get_population_data(
        self,
        year: int = 2021,
        state: Optional[str] = None,
        county: Optional[str] = None,
    ) -> List[CensusResponse]:
        """
        Fetch population and income data from the ACS 5-year estimates.

        Variables:
        - B01001: Sex by Age
        - B19013: Median Household Income
        """
        variables = [
            "B01001_001E",  # Total population
            "B01001_003E",
            "B01001_027E",  # Under 5 (M/F)
            "B01001_004E",
            "B01001_028E",  # 5-9 years (M/F)
            "B01001_005E",
            "B01001_029E",  # 10-14 years (M/F)
            "B01001_006E",
            "B01001_030E",  # 15-17 years (M/F)
            "B19013_001E",  # Median household income
        ]

        if county and not state:
            raise ValueError("County filter requires state to be specified")

        # Construct geographic hierarchy
        if state and county:
            geo_hierarchy = f"state:{state} county:{county}"
        elif state:
            geo_hierarchy = f"state:{state}"
        else:
            geo_hierarchy = "us:*"

        url = f"{self.base_url}/{year}/acs/acs5"
        params = {
            "get": ",".join(variables),
            "for": "block group:*",
            "in": geo_hierarchy,
            "key": self.api_key,
        }

        try:
            response = await self._client.get(url, params=params)
            response.raise_for_status()

            if response.status_code == 204:
                raise CensusAPIError("No data found for the specified geography")

            data = response.json()
            if not data or len(data) < 2:
                raise CensusAPIError("Census API returned empty or invalid data")

        except httpx.HTTPStatusError as e:
            if e.response.status_code == 404:
                raise CensusAPIError(
                    f"Census API endpoint not found. The {year} ACS 5-year estimates might not be available."
                ) from e
            elif e.response.status_code == 400:
                raise CensusAPIError(
                    "Invalid request. Check your geographic identifiers and variables."
                ) from e
            elif e.response.status_code == 403:
                raise CensusAPIError("Invalid API key") from e
            raise

        headers = data[0]
        results = []

        # Process results
        for row in data[1:]:
            row_dict = dict(zip(headers, row))

            # Construct GEOID from components
            state_fips = row_dict["state"]
            county_fips = row_dict["county"]
            tract_id = row_dict["tract"]
            block_group = row_dict["block group"]

            # GEOID format: STATE(2) + COUNTY(3) + TRACT(6) + BLOCK GROUP(1)
            geo_id = f"{state_fips}{county_fips}{tract_id}{block_group}"

            geo_area = GeographicArea(
                geo_id=geo_id,
                name=f"Block Group {block_group}, "
                f"Tract {tract_id}, "
                f"State {state_fips}, "
                f"County {county_fips}",
                type="block group",
            )

            try:
                pop_data = PopulationData(
                    total_population=int(row_dict["B01001_001E"]),
                    under_5=int(row_dict["B01001_003E"]) + int(row_dict["B01001_027E"]),
                    age_5_to_9=int(row_dict["B01001_004E"])
                    + int(row_dict["B01001_028E"]),
                    age_10_to_14=int(row_dict["B01001_005E"])
                    + int(row_dict["B01001_029E"]),
                    age_15_to_17=int(row_dict["B01001_006E"])
                    + int(row_dict["B01001_030E"]),
                    median_income=int(row_dict["B19013_001E"])
                    if row_dict["B19013_001E"] not in ["", "None"]
                    else None,
                )
            except (KeyError, ValueError) as e:
                raise CensusAPIError(
                    f"Invalid data format in Census API response: {e}"
                ) from e

            results.append(
                CensusResponse(geographic_area=geo_area, population_data=pop_data)
            )

        return results

    async def get_counties(self, state: str) -> list[str]:
        """
        Get all county FIPS codes for a given state

        Args:
            state: State FIPS code (e.g., "06" for California)

        Returns:
            List of county FIPS codes

        Example:
            >>> await client.get_counties("06")
            ["001", "003", "005", ...]  # Alameda, Alpine, Amador, etc.
        """
        url = f"{self.base_url}/2020/dec/pl"  # Using PL94-171 for geographic info
        params = {
            "get": "NAME",  # We just need names to get the geography
            "for": "county:*",
            "in": f"state:{state}",
            "key": self.api_key,
        }

        try:
            response = await self._client.get(url, params=params)
            response.raise_for_status()

            if response.status_code == 204:
                raise CensusAPIError("No counties found for the specified state")

            data = response.json()
            if not data or len(data) < 2:
                raise CensusAPIError("Census API returned empty or invalid data")

            # Extract county codes from response
            # Response format is [["NAME", "state", "county"], ["County Name", "06", "001"], ...]
            return [row[2] for row in data[1:]]

        except httpx.HTTPStatusError as e:
            if e.response.status_code == 404:
                raise CensusAPIError("Census API endpoint not found") from e
            elif e.response.status_code == 400:
                raise CensusAPIError(
                    "Invalid request. Check your state FIPS code."
                ) from e
            elif e.response.status_code == 403:
                raise CensusAPIError("Invalid API key") from e
            raise

    async def get_states(self) -> dict[str, str]:
        """
        Get all state FIPS codes and names

        Returns:
            Dictionary mapping state names to FIPS codes

        Example:
            >>> await client.get_states()
            {"Alabama": "01", "Alaska": "02", ...}
        """
        url = f"{self.base_url}/2020/dec/pl"
        params = {"get": "NAME", "for": "state:*", "key": self.api_key}

        try:
            response = await self._client.get(url, params=params)
            response.raise_for_status()

            if response.status_code == 204:
                raise CensusAPIError("No state data found")

            data = response.json()
            if not data or len(data) < 2:
                raise CensusAPIError("Census API returned empty or invalid data")

            # Response format is [["NAME", "state"], ["Alabama", "01"], ...]
            return {row[0]: row[1] for row in data[1:]}

        except httpx.HTTPStatusError as e:
            if e.response.status_code == 404:
                raise CensusAPIError("Census API endpoint not found") from e
            elif e.response.status_code == 400:
                raise CensusAPIError("Invalid request") from e
            elif e.response.status_code == 403:
                raise CensusAPIError("Invalid API key") from e
            raise

    async def close(self):
        """Close the HTTP client"""
        await self._client.aclose()
