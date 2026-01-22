from pydantic_settings import BaseSettings
from pydantic import Field
import os


class Settings(BaseSettings):
    """Application settings loaded from environment variables"""
    
    # NOTE: Database connection is handled by db_manager.cnxn.get_engine('PUReporting')
    # No database credentials needed here
    
    # Security settings
    secret_key: str = Field(default="dev-secret-key-change-in-production", alias="SECRET_KEY")
    algorithm: str = Field(default="HS256", alias="ALGORITHM")
    access_token_expire_minutes: int = Field(default=480, alias="ACCESS_TOKEN_EXPIRE_MINUTES")
    secret_password: str = Field(default="", alias="SECRET_PASSWORD")

    # File upload settings
    upload_dir: str = Field(default="./uploads", alias="UPLOAD_DIR")
    max_upload_size_mb: int = Field(default=50, alias="MAX_UPLOAD_SIZE_MB")
    
    # Application settings
    environment: str = Field(default="development", alias="ENVIRONMENT")
    
    class Config:
        env_file = ".env"
        case_sensitive = False
        extra = "allow"


# Create global settings instance
settings = Settings()

# Ensure upload directory exists
os.makedirs(settings.upload_dir, exist_ok=True)