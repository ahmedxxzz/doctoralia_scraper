"""Data models for doctor information."""
from dataclasses import dataclass, field, asdict
from typing import List, Optional
from datetime import datetime


@dataclass
class DoctorAddress:
    """Doctor's practice address."""
    street: Optional[str] = None
    city: Optional[str] = None
    postal_code: Optional[str] = None
    province: Optional[str] = None
    latitude: Optional[float] = None
    longitude: Optional[float] = None
    clinic_name: Optional[str] = None


@dataclass
class DoctorContact:
    """Doctor's contact information."""
    phone: Optional[str] = None
    email: Optional[str] = None
    website: Optional[str] = None
    doctoralia_url: Optional[str] = None


@dataclass
class Doctor:
    """Complete doctor information model."""
    
    # Basic info
    name: str
    specialty: str
    city: str
    
    # Profile details
    title: Optional[str] = None
    sub_specialties: List[str] = field(default_factory=list)
    diseases_treated: List[str] = field(default_factory=list)
    services: List[str] = field(default_factory=list)
    
    # Contact info
    contact: DoctorContact = field(default_factory=DoctorContact)
    
    # Address(es)
    addresses: List[DoctorAddress] = field(default_factory=list)
    
    # Professional info
    experience_years: Optional[int] = None
    education: List[str] = field(default_factory=list)
    languages: List[str] = field(default_factory=list)
    insurance_accepted: List[str] = field(default_factory=list)
    
    # Ratings
    rating: Optional[float] = None
    review_count: Optional[int] = None
    
    # Pricing
    consultation_price: Optional[str] = None
    online_consultation_price: Optional[str] = None
    offers_online_consultation: bool = False
    
    # Metadata
    scraped_at: str = field(default_factory=lambda: datetime.now().isoformat())
    profile_url: Optional[str] = None
    
    def to_dict(self) -> dict:
        """Convert to dictionary for storage."""
        data = asdict(self)
        data['sub_specialties'] = "|".join(self.sub_specialties)
        data['diseases_treated'] = "|".join(self.diseases_treated)
        data['services'] = "|".join(self.services)
        data['education'] = "|".join(self.education)
        data['languages'] = "|".join(self.languages)
        data['insurance_accepted'] = "|".join(self.insurance_accepted)
        
        # Flatten contact
        if self.contact:
            data['phone'] = self.contact.phone
            data['email'] = self.contact.email
            data['website'] = self.contact.website
            data['doctoralia_url'] = self.contact.doctoralia_url
        del data['contact']
        
        # Flatten first address
        if self.addresses:
            addr = self.addresses[0]
            data['address_street'] = addr.street
            data['address_city'] = addr.city
            data['address_postal_code'] = addr.postal_code
            data['address_province'] = addr.province
            data['address_latitude'] = addr.latitude
            data['address_longitude'] = addr.longitude
            data['clinic_name'] = addr.clinic_name
            data['all_addresses'] = "|".join([
                f"{a.clinic_name or ''}: {a.street or ''}, {a.city or ''}"
                for a in self.addresses
            ])
        del data['addresses']
        
        return data
    
    def to_flat_dict(self) -> dict:
        """Convert to flat dictionary for CSV export."""
        return self.to_dict()


@dataclass
class ScrapeTask:
    """Represents a scraping task for the queue."""
    specialty: str
    city: str
    page: int = 1
    url: Optional[str] = None
    retries: int = 0
    
    def get_url(self, base_url: str) -> str:
        """Generate the URL for this task."""
        if self.url:
            return self.url
        if self.page == 1:
            return f"{base_url}/{self.specialty}/{self.city}"
        return f"{base_url}/{self.specialty}/{self.city}/{self.page}"


@dataclass
class ScrapeResult:
    """Result from a scraping operation."""
    success: bool
    task: ScrapeTask
    doctors: List[Doctor] = field(default_factory=list)
    next_page_url: Optional[str] = None
    has_more_pages: bool = False
    error: Optional[str] = None
    response_time: float = 0.0
