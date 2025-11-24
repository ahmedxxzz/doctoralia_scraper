"""HTML parser for Doctoralia pages."""
import re
import json
import logging
from typing import List, Optional, Tuple
from bs4 import BeautifulSoup

from .models import Doctor, DoctorContact, DoctorAddress

logger = logging.getLogger(__name__)


class DoctorListParser:
    """Parser for doctor listing pages."""
    
    def __init__(self, html: str, base_url: str = "https://www.doctoralia.es"):
        self.soup = BeautifulSoup(html, 'html.parser')
        self.base_url = base_url
    
    def get_doctor_profile_urls(self) -> List[str]:
        """Extract all doctor profile URLs from listing page."""
        urls = []
        
        # Find doctor cards - look for h3 links which contain doctor names
        # The structure is: <h3><a href="/doctor-name/specialty/city">Dr. Name</a></h3>
        h3_links = self.soup.select('h3 a[href]')
        
        for link in h3_links:
            href = link.get('href', '')
            if href and self._is_doctor_profile_url(href):
                full_url = href if href.startswith('http') else f"{self.base_url}{href}"
                if full_url not in urls:
                    urls.append(full_url)
        
        # Also look for links with doctor-specific patterns
        all_links = self.soup.find_all('a', href=True)
        for link in all_links:
            href = link.get('href', '')
            if href and self._is_doctor_profile_url(href):
                full_url = href if href.startswith('http') else f"{self.base_url}{href}"
                if full_url not in urls:
                    urls.append(full_url)
        
        return urls
    
    def _is_doctor_profile_url(self, url: str) -> bool:
        """Check if URL is a doctor profile page."""
        # Doctor profiles follow pattern: /name-surname/specialty/city
        # Example: /teofilo-sainz-martin/alergologo/madrid
        
        # Exclude patterns - these are NOT doctor profiles
        exclude_patterns = [
            r'/clinicas',
            r'/\d+$',  # Pagination
            r'/aseguradoras',
            r'/especialidades',
            r'/tratamientos',
            r'/enfermedades',
            r'/preguntas',
            r'/medicamentos',
            r'/faq',
            r'/blog',
            r'/app',
            r'/contacto',
            r'/privacidad',
            r'#',
            r'\?',
            # Insurance company filters (these are listing pages, not profiles)
            r'/adeslas$',
            r'/asisa$',
            r'/sanitas$',
            r'/dkv-seguros$',
            r'/mapfre',
            r'/aegon',
            r'/axa',
            r'/caser',
            r'/cigna',
            r'/fiatc',
            r'/generali',
            r'/mutua',
            r'/nectar',
            r'/online$',  # Online consultation filter
            # District/neighborhood filters
            r'/municipality-',
            r'/distrito-',
            r'/chamberi$',
            r'/centro$',
            r'/chamartin$',
            r'/retiro$',
        ]
        
        for pattern in exclude_patterns:
            if re.search(pattern, url, re.IGNORECASE):
                return False
        
        # Must have at least 3 path segments (name/specialty/city)
        path = url.replace(self.base_url, '').strip('/')
        # Remove any hash or query params
        if '#' in path:
            path = path.split('#')[0]
        if '?' in path:
            path = path.split('?')[0]
            
        segments = [s for s in path.split('/') if s]
        
        # Doctor profile must have exactly 3 segments: name/specialty/city
        if len(segments) != 3:
            return False
        
        name_segment = segments[0]
        specialty_segment = segments[1]
        city_segment = segments[2]
        
        # Doctor names typically have 2+ hyphens: teofilo-sainz-martin, carmen-lopez-calderon
        # Single hyphen names exist too: paloma-bonelli
        # But filter pages have patterns like: medico-general (specialty), madrid (city)
        
        # Name must have at least one hyphen
        if '-' not in name_segment:
            return False
        
        # Name should look like a person's name, not a specialty
        # Specialties often start with: medico-, cirujano-, especialista-
        specialty_prefixes = ['medico-', 'cirujano-', 'especialista-', 'clinica-', 'centro-']
        if any(name_segment.startswith(prefix) for prefix in specialty_prefixes):
            return False
        
        # The middle segment should be a specialty (contains hyphen or known specialty word)
        # Common specialty patterns
        if specialty_segment in ['clinicas', 'enfermedades', 'tratamientos']:
            return False
            
        return True
    
    def _extract_urls_from_scripts(self) -> List[str]:
        """Extract doctor URLs from JSON-LD scripts."""
        urls = []
        scripts = self.soup.find_all('script', type='application/ld+json')
        
        for script in scripts:
            try:
                data = json.loads(script.string)
                if isinstance(data, list):
                    for item in data:
                        url = item.get('url', '')
                        if url and self._is_doctor_profile_url(url):
                            urls.append(url)
                elif isinstance(data, dict):
                    url = data.get('url', '')
                    if url and self._is_doctor_profile_url(url):
                        urls.append(url)
            except (json.JSONDecodeError, TypeError):
                continue
        
        return urls
    
    def get_next_page_url(self) -> Optional[str]:
        """Get URL for next page of results."""
        # Look for pagination links
        pagination_selectors = [
            'a[rel="next"]',
            'a.pagination-next',
            'a[aria-label*="siguiente"]',
            'a[aria-label*="Siguiente"]',
            '.pagination a:contains("Siguiente")',
            'nav[aria-label="pagination"] a:last-child',
        ]
        
        for selector in pagination_selectors:
            try:
                link = self.soup.select_one(selector)
                if link and link.get('href'):
                    href = link['href']
                    return href if href.startswith('http') else f"{self.base_url}{href}"
            except Exception:
                continue
        
        # Try to find page numbers and get next
        page_links = self.soup.select('a[href*="/"][class*="page"]')
        current_page = 1
        
        for link in page_links:
            href = link.get('href', '')
            match = re.search(r'/(\d+)$', href)
            if match:
                page_num = int(match.group(1))
                if page_num > current_page:
                    return href if href.startswith('http') else f"{self.base_url}{href}"
        
        return None
    
    def has_results(self) -> bool:
        """Check if page has doctor results."""
        # Check for "no results" messages
        no_results_indicators = [
            'No hemos encontrado',
            'sin resultados',
            '0 resultados',
            'No hay especialistas',
        ]
        
        page_text = self.soup.get_text().lower()
        for indicator in no_results_indicators:
            if indicator.lower() in page_text:
                return False
        
        # Check if we found any doctor links
        return len(self.get_doctor_profile_urls()) > 0


class DoctorProfileParser:
    """Parser for individual doctor profile pages."""
    
    def __init__(self, html: str, url: str, specialty: str, city: str):
        self.soup = BeautifulSoup(html, 'html.parser')
        self.url = url
        self.specialty = specialty
        self.city = city
    
    def parse(self) -> Optional[Doctor]:
        """Parse doctor profile and return Doctor object."""
        try:
            name = self._extract_name()
            if not name:
                logger.warning(f"Could not extract name from {self.url}")
                return None
            
            doctor = Doctor(
                name=name,
                specialty=self.specialty,
                city=self.city,
                title=self._extract_title(),
                sub_specialties=self._extract_sub_specialties(),
                diseases_treated=self._extract_diseases(),
                services=self._extract_services(),
                contact=self._extract_contact(),
                addresses=self._extract_addresses(),
                experience_years=self._extract_experience(),
                education=self._extract_education(),
                languages=self._extract_languages(),
                insurance_accepted=self._extract_insurance(),
                rating=self._extract_rating(),
                review_count=self._extract_review_count(),
                consultation_price=self._extract_price(),
                online_consultation_price=self._extract_online_price(),
                offers_online_consultation=self._check_online_consultation(),
                profile_url=self.url
            )
            
            return doctor
            
        except Exception as e:
            logger.error(f"Error parsing doctor profile {self.url}: {e}")
            return None
    
    def _extract_name(self) -> Optional[str]:
        """Extract doctor's name."""
        selectors = [
            'h1[itemprop="name"]',
            'h1.doctor-name',
            'h1',
            '[data-test="doctor-name"]',
            '.profile-header h1',
        ]
        
        for selector in selectors:
            elem = self.soup.select_one(selector)
            if elem:
                name = elem.get_text(strip=True)
                # Clean up name (remove Dr./Dra. prefix if needed)
                name = re.sub(r'^(Dr\.|Dra\.|Doctor|Doctora)\s*', '', name, flags=re.IGNORECASE)
                if name:
                    return name.strip()
        
        # Try JSON-LD
        return self._extract_from_json_ld('name')
    
    def _extract_title(self) -> Optional[str]:
        """Extract professional title (Dr./Dra.)."""
        h1 = self.soup.select_one('h1')
        if h1:
            text = h1.get_text(strip=True)
            if text.startswith('Dr.'):
                return 'Dr.'
            elif text.startswith('Dra.'):
                return 'Dra.'
        return None
    
    def _extract_sub_specialties(self) -> List[str]:
        """Extract sub-specialties."""
        specialties = []
        
        selectors = [
            '[data-test="specializations"] span',
            '.specializations span',
            '.profile-specialty',
            'span[itemprop="medicalSpecialty"]',
        ]
        
        for selector in selectors:
            elems = self.soup.select(selector)
            for elem in elems:
                text = elem.get_text(strip=True)
                if text and text not in specialties:
                    specialties.append(text)
        
        return specialties
    
    def _extract_diseases(self) -> List[str]:
        """Extract diseases treated."""
        diseases = []
        
        # Look for diseases section
        section = self.soup.find(string=re.compile(r'enfermedades tratadas', re.I))
        if section:
            parent = section.find_parent(['div', 'section'])
            if parent:
                items = parent.select('li, span.tag, a')
                for item in items:
                    text = item.get_text(strip=True)
                    if text and len(text) > 2:
                        diseases.append(text)
        
        return diseases[:20]  # Limit to 20
    
    def _extract_services(self) -> List[str]:
        """Extract services offered."""
        services = []
        
        selectors = [
            '[data-test="services"] li',
            '.services-list li',
            '#services li',
        ]
        
        for selector in selectors:
            elems = self.soup.select(selector)
            for elem in elems:
                text = elem.get_text(strip=True)
                if text:
                    services.append(text)
        
        return services[:30]  # Limit
    
    def _extract_contact(self) -> DoctorContact:
        """Extract contact information."""
        contact = DoctorContact(doctoralia_url=self.url)
        
        # Phone
        phone_selectors = [
            'a[href^="tel:"]',
            '[data-test="phone"]',
            '.phone-number',
            '[itemprop="telephone"]',
        ]
        
        for selector in phone_selectors:
            elem = self.soup.select_one(selector)
            if elem:
                phone = elem.get('href', '') or elem.get_text(strip=True)
                phone = phone.replace('tel:', '').strip()
                if phone:
                    contact.phone = phone
                    break
        
        # Website
        website_selectors = [
            'a[data-test="website"]',
            'a[rel="nofollow"][href*="http"]:not([href*="doctoralia"])',
        ]
        
        for selector in website_selectors:
            elem = self.soup.select_one(selector)
            if elem:
                href = elem.get('href', '')
                if href and 'doctoralia' not in href:
                    contact.website = href
                    break
        
        return contact
    
    def _extract_addresses(self) -> List[DoctorAddress]:
        """Extract practice addresses."""
        addresses = []
        
        # Find address containers
        address_containers = self.soup.select('[itemprop="address"], .address, [data-test="address"]')
        
        for container in address_containers:
            addr = DoctorAddress()
            
            # Street
            street_elem = container.select_one('[itemprop="streetAddress"], .street')
            if street_elem:
                addr.street = street_elem.get_text(strip=True)
            
            # City
            city_elem = container.select_one('[itemprop="addressLocality"], .city')
            if city_elem:
                addr.city = city_elem.get_text(strip=True)
            
            # Postal code
            postal_elem = container.select_one('[itemprop="postalCode"], .postal-code')
            if postal_elem:
                addr.postal_code = postal_elem.get_text(strip=True)
            
            # Province
            province_elem = container.select_one('[itemprop="addressRegion"], .province')
            if province_elem:
                addr.province = province_elem.get_text(strip=True)
            
            # Clinic name
            clinic_elem = container.select_one('.clinic-name, .facility-name, h4, h5')
            if clinic_elem:
                addr.clinic_name = clinic_elem.get_text(strip=True)
            
            # Coordinates from map link
            map_link = container.select_one('a[href*="maps"]')
            if map_link:
                href = map_link.get('href', '')
                coords = re.search(r'query=([-\d.]+),([-\d.]+)', href)
                if coords:
                    addr.latitude = float(coords.group(1))
                    addr.longitude = float(coords.group(2))
            
            if addr.street or addr.city:
                addresses.append(addr)
        
        # Fallback: try to get from text
        if not addresses:
            addr_text = self.soup.select_one('.address-text, [data-test="full-address"]')
            if addr_text:
                addresses.append(DoctorAddress(
                    street=addr_text.get_text(strip=True),
                    city=self.city
                ))
        
        return addresses
    
    def _extract_experience(self) -> Optional[int]:
        """Extract years of experience."""
        text = self.soup.get_text()
        match = re.search(r'(\d+)\s*años?\s*de\s*experiencia', text, re.I)
        if match:
            return int(match.group(1))
        return None
    
    def _extract_education(self) -> List[str]:
        """Extract education/training."""
        education = []
        
        section = self.soup.find(string=re.compile(r'formación|educación|estudios', re.I))
        if section:
            parent = section.find_parent(['div', 'section'])
            if parent:
                items = parent.select('li, p')
                for item in items:
                    text = item.get_text(strip=True)
                    if text and len(text) > 5:
                        education.append(text)
        
        return education[:10]
    
    def _extract_languages(self) -> List[str]:
        """Extract languages spoken."""
        languages = []
        
        section = self.soup.find(string=re.compile(r'idiomas', re.I))
        if section:
            parent = section.find_parent(['div', 'section', 'li'])
            if parent:
                text = parent.get_text()
                # Common languages
                lang_patterns = ['español', 'inglés', 'francés', 'alemán', 'italiano', 
                               'portugués', 'catalán', 'euskera', 'gallego', 'valenciano']
                for lang in lang_patterns:
                    if lang.lower() in text.lower():
                        languages.append(lang.capitalize())
        
        return languages
    
    def _extract_insurance(self) -> List[str]:
        """Extract accepted insurance companies."""
        insurance = []
        
        selectors = [
            '[data-test="insurances"] li',
            '.insurance-list li',
            '.accepted-insurances span',
        ]
        
        for selector in selectors:
            elems = self.soup.select(selector)
            for elem in elems:
                text = elem.get_text(strip=True)
                if text:
                    insurance.append(text)
        
        return insurance[:20]
    
    def _extract_rating(self) -> Optional[float]:
        """Extract average rating."""
        selectors = [
            '[itemprop="ratingValue"]',
            '[data-test="rating"]',
            '.rating-value',
            '.stars-rating',
        ]
        
        for selector in selectors:
            elem = self.soup.select_one(selector)
            if elem:
                text = elem.get('content') or elem.get_text(strip=True)
                try:
                    # Handle both "4.5" and "4,5" formats
                    rating = float(text.replace(',', '.'))
                    if 0 <= rating <= 5:
                        return rating
                except ValueError:
                    continue
        
        return None
    
    def _extract_review_count(self) -> Optional[int]:
        """Extract number of reviews."""
        selectors = [
            '[itemprop="reviewCount"]',
            '[data-test="reviews-count"]',
            '.reviews-count',
        ]
        
        for selector in selectors:
            elem = self.soup.select_one(selector)
            if elem:
                text = elem.get('content') or elem.get_text(strip=True)
                match = re.search(r'(\d+)', text.replace('.', '').replace(',', ''))
                if match:
                    return int(match.group(1))
        
        # Try to find in text
        text = self.soup.get_text()
        match = re.search(r'(\d+)\s*opiniones?', text, re.I)
        if match:
            return int(match.group(1))
        
        return None
    
    def _extract_price(self) -> Optional[str]:
        """Extract consultation price."""
        selectors = [
            '[data-test="price"]',
            '.consultation-price',
            '.price',
        ]
        
        for selector in selectors:
            elem = self.soup.select_one(selector)
            if elem:
                text = elem.get_text(strip=True)
                if '€' in text or 'euro' in text.lower():
                    return text
        
        # Try to find price in text
        text = self.soup.get_text()
        match = re.search(r'(\d+)\s*€', text)
        if match:
            return f"{match.group(1)}€"
        
        return None
    
    def _extract_online_price(self) -> Optional[str]:
        """Extract online consultation price."""
        section = self.soup.find(string=re.compile(r'consulta\s*online', re.I))
        if section:
            parent = section.find_parent(['div', 'li', 'tr'])
            if parent:
                text = parent.get_text()
                match = re.search(r'(\d+)\s*€', text)
                if match:
                    return f"{match.group(1)}€"
        return None
    
    def _check_online_consultation(self) -> bool:
        """Check if doctor offers online consultations."""
        indicators = ['consulta online', 'videoconsulta', 'telemedicina']
        text = self.soup.get_text().lower()
        return any(ind in text for ind in indicators)
    
    def _extract_from_json_ld(self, field: str) -> Optional[str]:
        """Extract field from JSON-LD data."""
        scripts = self.soup.find_all('script', type='application/ld+json')
        
        for script in scripts:
            try:
                data = json.loads(script.string)
                if isinstance(data, dict):
                    if field in data:
                        return data[field]
                    if '@graph' in data:
                        for item in data['@graph']:
                            if field in item:
                                return item[field]
            except (json.JSONDecodeError, TypeError):
                continue
        
        return None
