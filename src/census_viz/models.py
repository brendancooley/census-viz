from pydantic import BaseModel, Field


class GeographicArea(BaseModel):
    """Represents a geographic area from the Census API"""

    geo_id: str = Field(..., description="Geographic identifier")
    name: str = Field(..., description="Name of the geographic area")
    type: str = Field(..., description="Type of geographic area (e.g., 'block group')")


class PopulationData(BaseModel):
    """Represents population data for a geographic area"""

    total_population: int = Field(..., description="Total population in the area")
    under_5: int = Field(..., description="Population under 5 years")
    age_5_to_9: int = Field(..., description="Population 5 to 9 years")
    age_10_to_14: int = Field(..., description="Population 10 to 14 years")
    under_18: int = Field(..., description="Total population under 18 years")


class CensusResponse(BaseModel):
    """Represents a response from the Census API"""

    geographic_area: GeographicArea
    population_data: PopulationData
