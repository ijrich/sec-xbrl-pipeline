from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """SEC pipeline settings. Only used as fallback when explicit args aren't provided."""

    # SEC API Configuration
    SEC_USER_AGENT_NAME: str = "SEC XBRL Pipeline"
    SEC_USER_AGENT_EMAIL: str = "contact@example.com"

    model_config = SettingsConfigDict(
        env_file=".env",
        case_sensitive=True,
        extra="allow"
    )


settings = Settings()
