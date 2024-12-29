from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Application settings loaded from environment variables"""

    census_api_key: str

    model_config = SettingsConfigDict(
        env_file=".env", env_file_encoding="utf-8", env_prefix="CENSUS_"
    )


settings = Settings()
