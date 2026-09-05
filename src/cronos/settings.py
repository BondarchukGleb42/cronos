from functools import lru_cache

from pydantic import Field, SecretStr
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=None, extra="ignore")

    database_url: SecretStr
    admin_database_url: SecretStr | None = None
    rabbitmq_url: SecretStr = SecretStr("amqp://guest:guest@localhost/")
    redis_url: SecretStr = SecretStr("redis://localhost:6379/0")
    telegram_bot_token: SecretStr = SecretStr("")
    telegram_proxy: SecretStr | None = None
    alltokens_api_key: SecretStr = SecretStr("")
    alltokens_base_url: str = "https://api.alltokens.ru/api/v1"
    provider_tls12: bool = False
    model_free: str = "deepseek/deepseek-v4-flash"
    model_tools: str = "deepseek/deepseek-v4-flash"
    model_vision: str = "qwen/qwen3.7-flash"
    model_reasoning: str = "deepseek/deepseek-v4-flash"
    model_search: str = "perplexity/sonar"
    model_image: str = "google/gemini-3.1-flash-image-preview"
    model_fallbacks: str = "meta-llama/llama-3.1-8b-instruct,inclusionai/ling-3.0-flash"
    alpha_soft_limits: bool = True
    max_model_steps: int = Field(default=8, ge=1, le=24)
    max_output_tokens: int = Field(default=4096, ge=64, le=32768)
    max_run_cost_rub: float = Field(default=25, gt=0)
    proactive_max_cost_rub: float = Field(default=1, gt=0)
    provider_timeout_seconds: float = 120
    worker_concurrency: int = Field(default=2, ge=1, le=8)
    artifacts_dir: str = "/data/artifacts"
    default_timezone: str = "UTC"
    port: int = 8000
    log_level: str = "INFO"


@lru_cache
def get_settings() -> Settings:
    return Settings()
