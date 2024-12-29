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
        year: int = 2021,  # Most recent 5-year ACS is 2017-2021
        state: Optional[str] = None,
        county: Optional[str] = None,
    ) -> List[CensusResponse]:
        """
        Fetch population data from the American Community Survey (ACS) 5-year estimates.

        The ACS provides detailed demographic data including:
        - Total population counts
        - Detailed age breakdowns
        - Sex, race, and ethnicity
        - Income and education
        - Housing characteristics

        This method specifically fetches from table B01001 (SEX BY AGE):
        - B01001_001E: Total Population
        - B01001_003E: Male, Under 5 years
        - B01001_004E: Male, 5 to 9 years
        - B01001_005E: Male, 10 to 14 years
        - B01001_027E: Female, Under 5 years
        - B01001_028E: Female, 5 to 9 years
        - B01001_029E: Female, 10 to 14 years

        Note: ACS data are estimates based on samples, not complete counts.
        Margins of error are available but not currently returned.

        Args:
            year: The year to fetch data for (defaults to 2021 for 2017-2021 estimates)
            state: Optional state FIPS code to filter by (e.g., "06" for California)
            county: Optional county FIPS code to filter by (e.g., "001" for Alameda County)

        Returns:
            List of CensusResponse objects containing population data by block group

        Raises:
            ValueError: If county is provided without state
            CensusAPIError: If the Census API returns an error
            httpx.HTTPError: For other HTTP-related errors
        """
        variables = [
            "B01001_001E",  # Total population
            "B01001_003E",  # Male: Under 5 years
            "B01001_004E",  # Male: 5 to 9 years
            "B01001_005E",  # Male: 10 to 14 years
            "B01001_027E",  # Female: Under 5 years
            "B01001_028E",  # Female: 5 to 9 years
            "B01001_029E",  # Female: 10 to 14 years
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

            geo_area = GeographicArea(
                geo_id=f"{row_dict.get('state', '')}{row_dict.get('county', '')}{row_dict['block group']}",
                name=f"Block Group {row_dict['block group']}, "
                f"{'State '+row_dict['state'] if 'state' in row_dict else ''}"
                f"{', County '+row_dict['county'] if 'county' in row_dict else ''}",
                type="block group",
            )

            try:
                pop_data = PopulationData(
                    total_population=int(row_dict["B01001_001E"]),
                    under_18=sum(
                        int(row_dict[v])
                        for v in [
                            "B01001_003E",
                            "B01001_004E",
                            "B01001_005E",  # Male under 15
                            "B01001_027E",
                            "B01001_028E",
                            "B01001_029E",  # Female under 15
                        ]
                    ),
                    under_5=int(row_dict["B01001_003E"]) + int(row_dict["B01001_027E"]),
                    age_5_to_9=int(row_dict["B01001_004E"])
                    + int(row_dict["B01001_028E"]),
                    age_10_to_14=int(row_dict["B01001_005E"])
                    + int(row_dict["B01001_029E"]),
                )
            except (KeyError, ValueError) as e:
                raise CensusAPIError(
                    f"Invalid data format in Census API response: {e}"
                ) from e

            results.append(
                CensusResponse(geographic_area=geo_area, population_data=pop_data)
            )

        return results

    async def close(self):
        """Close the HTTP client"""
        await self._client.aclose()
