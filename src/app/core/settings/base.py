from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    DB_HOST: str
    DB_NAME: str
    DB_USER: str
    DB_PASSWORD: str
    DB_PORT: str
    CSS_PASSWORD_CORE: str
    VERBOSE: str
    FTP_USER: str | None
    FTP_PASSW: str | None
    BASE_FTP: str | None

    class Config:
        env_file = ".env"


settings = Settings()
