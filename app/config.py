"""
Конфигурация приложения
"""
import os
from typing import Optional
from pydantic_settings import BaseSettings
from pydantic import Field, validator
from datetime import timedelta


class Settings(BaseSettings):
    """Настройки приложения"""
    
    # Telegram
    telegram_token: str = Field(..., env="TELEGRAM_TOKEN")
    main_admin_id: int = Field(526710245, env="MAIN_ADMIN_ID")
    
    # Google Sheets
    sheet_id: str = Field(..., env="SHEET_ID")
    google_credentials_file: Optional[str] = Field(None, env="GOOGLE_CREDENTIALS_FILE")
    
    # Названия колонок
    col_first_name: str = "Имя"
    col_last_name: str = "Фамилия"
    col_birth_date: str = "Дата рождения"
    
    # Дата колонки
    date_columns: list = ["Дата рождения", "Дата", "Дата регистрации"]
    
    # Настройки сервера
    environment: str = Field("production", env="ENVIRONMENT")
    debug: bool = Field(False, env="DEBUG")
    log_level: str = Field("INFO", env="LOG_LEVEL")
    
    # Настройки сессий
    session_timeout_minutes: int = 30
    session_storage: str = Field("memory", env="SESSION_STORAGE")  # memory или redis
    redis_url: Optional[str] = Field(None, env="REDIS_URL")
    
    # Cloud Run
    project_id: Optional[str] = Field(None, env="GOOGLE_CLOUD_PROJECT")
    service_name: str = Field("church-telegram-bot", env="K_SERVICE")
    service_url: Optional[str] = Field(None, env="SERVICE_URL")
    
    @property
    def session_timeout(self) -> timedelta:
        """Таймаут сессии"""
        return timedelta(minutes=self.session_timeout_minutes)
    
    @property
    def is_production(self) -> bool:
        """Проверка production окружения"""
        return self.environment.lower() == "production"
    
    @property
    def is_development(self) -> bool:
        """Проверка development окружения"""
        return not self.is_production
    
    @validator('telegram_token')
    def validate_telegram_token(cls, v):
        if not v or len(v) < 10:
            raise ValueError('Invalid Telegram token')
        return v
    
    @validator('sheet_id')
    def validate_sheet_id(cls, v):
        if not v or len(v) < 10:
            raise ValueError('Invalid Google Sheet ID')
        return v
    
    class Config:
        env_file = ".env"
        case_sensitive = False


# Глобальный экземпляр настроек
settings = Settings()