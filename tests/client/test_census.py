import pytest
from census_viz.client.census import CensusClient
from census_viz.config import settings
import os


@pytest.fixture
async def census_client():
    # Try to get API key from settings first (which checks .env), then environment
    api_key = settings.census_api_key or os.getenv("CENSUS_API_KEY")
    if not api_key:
        pytest.skip("CENSUS_API_KEY not found in .env or environment variables")

    client = CensusClient(api_key=api_key)
    yield client
    await client.close()


@pytest.mark.asyncio
async def test_client_initialization(census_client):
    assert census_client.api_key is not None
    assert census_client.base_url == "https://api.census.gov/data"


@pytest.mark.asyncio
async def test_get_population_data_real_api(census_client):
    """Test fetching data from actual Census API for Alameda County, CA"""
    results = await census_client.get_population_data(state="06", county="001")

    # Basic validation of response
    assert len(results) > 0
    first_result = results[0]

    # Validate structure
    assert first_result.geographic_area.geo_id.startswith("06001")  # Alameda County
    assert first_result.geographic_area.type == "block group"
    assert first_result.population_data.total_population > 0
    assert first_result.population_data.under_18 >= 0
    assert first_result.population_data.under_5 >= 0

    # Validate data consistency
    assert first_result.population_data.under_18 >= (
        first_result.population_data.under_5
        + first_result.population_data.age_5_to_9
        + first_result.population_data.age_10_to_14
    )


@pytest.mark.asyncio
async def test_invalid_county_without_state(census_client):
    with pytest.raises(
        ValueError, match="County filter requires state to be specified"
    ):
        await census_client.get_population_data(county="001")


@pytest.mark.asyncio
async def test_get_counties(census_client):
    """Test fetching county codes for California"""
    counties = await census_client.get_counties("06")

    # Basic validation
    assert len(counties) > 0
    assert "001" in counties  # Alameda County
    assert all(len(county) == 3 for county in counties)  # County codes are 3 digits
    assert all(county.isdigit() for county in counties)  # All digits


@pytest.mark.asyncio
async def test_get_states(census_client):
    """Test fetching state FIPS codes"""
    states = await census_client.get_states()

    # Basic validation
    assert len(states) > 0
    assert "Maryland" in states
    assert states["Maryland"] == "24"  # Maryland's FIPS code
    assert "California" in states
    assert states["California"] == "06"  # California's FIPS code

    # Check format
    for state, code in states.items():
        assert isinstance(state, str)
        assert isinstance(code, str)
        assert len(code) == 2  # State FIPS codes are always 2 digits
        assert code.isdigit()
