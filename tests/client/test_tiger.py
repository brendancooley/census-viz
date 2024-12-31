import pytest
from census_viz.client.tiger import TigerClient


@pytest.fixture
async def tiger_client(tmp_path):
    client = TigerClient(cache_dir=tmp_path)
    yield client
    await client.close()


@pytest.mark.asyncio
async def test_get_block_groups(tiger_client):
    """Test fetching block group geometries"""
    # Fetch Alameda County, CA
    geojson = await tiger_client.get_block_groups(state="06", county="001")

    # Basic validation
    assert geojson["type"] == "FeatureCollection"
    assert "features" in geojson
    assert len(geojson["features"]) > 0  # Should have some features

    # Check cache
    cache_path = tiger_client.cache_dir / "2020" / "06" / "001" / "block_groups.geojson"
    assert cache_path.exists()
