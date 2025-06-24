from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    DB_HOST: str
    DB_NAME: str
    DB_USER: str
    DB_PASSWORD: str
    DB_PORT: str

    CSS_PASSWORD_CORE: str
    GCLOUD_BUCKET: str | None
    VERBOSE: str

    class Config:
        env_file = ".env"


settings = Settings()
