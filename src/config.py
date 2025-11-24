"""Configuration settings for the Doctoralia scraper."""
import os
from dataclasses import dataclass, field
from typing import List, Optional
from dotenv import load_dotenv

load_dotenv()


@dataclass
class ScraperConfig:
    """Main configuration for the scraper."""
    
    BASE_URL: str = "https://www.doctoralia.es"
    
    # Threading settings
    MAX_WORKERS: int = int(os.getenv("MAX_WORKERS", "5"))
    
    # Rate limiting
    REQUESTS_PER_MINUTE: int = int(os.getenv("REQUESTS_PER_MINUTE", "30"))
    REQUEST_DELAY_MIN: float = 1.0
    REQUEST_DELAY_MAX: float = 3.0
    
    # Retry settings
    RETRY_ATTEMPTS: int = int(os.getenv("RETRY_ATTEMPTS", "3"))
    RETRY_DELAY: float = 5.0
    
    # Output settings
    OUTPUT_DIR: str = "data"
    OUTPUT_FORMAT: str = os.getenv("OUTPUT_FORMAT", "csv")
    DB_PATH: str = os.getenv("DB_PATH", "data/doctors.db")
    
    # Proxy settings
    USE_PROXIES: bool = True
    PROXY_ROTATION_INTERVAL: int = 10
    
    # Request headers
    DEFAULT_HEADERS: dict = field(default_factory=lambda: {
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
        "Accept-Language": "es-ES,es;q=0.9,en;q=0.8",
        "Accept-Encoding": "gzip, deflate, br",
        "Connection": "keep-alive",
        "Upgrade-Insecure-Requests": "1",
        "Cache-Control": "max-age=0",
    })


# Spanish cities to scrape (major cities)
SPANISH_CITIES: List[str] = [
    "madrid", "barcelona", "valencia", "sevilla", "zaragoza",
    "malaga", "murcia", "palma-de-mallorca", "las-palmas-de-gran-canaria",
    "bilbao", "alicante", "cordoba", "valladolid", "vigo",
    "gijon", "hospitalet-de-llobregat", "vitoria-gasteiz", "a-coruna",
    "granada", "elche", "oviedo", "badalona", "cartagena",
    "terrassa", "jerez-de-la-frontera", "sabadell", "mostoles",
    "santa-cruz-de-tenerife", "pamplona", "almeria", "alcala-de-henares",
    "fuenlabrada", "leganes", "san-sebastian", "getafe", "burgos",
    "albacete", "santander", "castellon-de-la-plana", "alcorcon",
    "logrono", "badajoz", "salamanca", "huelva", "marbella",
    "lleida", "tarragona", "leon", "cadiz", "jaen"
]

# Medical specialties to scrape
MEDICAL_SPECIALTIES: List[str] = [
    "medico-general", "dentista", "ginecologo", "dermatologo",
    "psicologo", "psiquiatra", "traumatologo", "oftalmologo",
    "pediatra", "cardiologo", "urologo", "otorrino",
    "endocrino", "neurologo", "cirujano-general", "fisioterapeuta",
    "nutricionista", "alergologo", "reumatologo", "nefrologo",
    "gastroenterologo", "neumologo", "hematologo", "oncologo",
    "angiologo-y-cirujano-vascular", "cirujano-plastico", "medico-estetico",
    "podologo", "logopeda", "internista", "radiologo",
    "anestesiologo", "geriatra", "medico-deportivo", "infectologo"
]


def get_config() -> ScraperConfig:
    """Get scraper configuration instance."""
    return ScraperConfig()
