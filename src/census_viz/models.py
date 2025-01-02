from pydantic import BaseModel, Field, computed_field
from typing import Optional


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
    age_15_to_17: int = Field(..., description="Population 15 to 17 years")
    median_income: Optional[int] = Field(None, description="Median household income")

    @computed_field
    def under_18(self) -> int:
        return self.under_5 + self.age_5_to_9 + self.age_10_to_14 + self.age_15_to_17

    @computed_field
    def school_age(self) -> int:
        return self.age_5_to_9 + self.age_10_to_14 + self.age_15_to_17


class CensusResponse(BaseModel):
    """Represents a response from the Census API"""

    geographic_area: GeographicArea
    population_data: PopulationData
