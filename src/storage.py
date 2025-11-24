"""Data storage handlers for scraped doctor data."""
import os
import csv
import json
import sqlite3
import logging
from typing import List, Optional, Set
from threading import Lock
from datetime import datetime

from .models import Doctor

logger = logging.getLogger(__name__)


class CSVStorage:
    """CSV file storage handler."""
    
    def __init__(self, output_dir: str = "data"):
        self.output_dir = output_dir
        self._lock = Lock()
        self._file_initialized = False
        self._filepath: Optional[str] = None
        self._fieldnames: Optional[List[str]] = None
        os.makedirs(output_dir, exist_ok=True)
    
    def _get_filepath(self) -> str:
        """Get output filepath with timestamp."""
        if not self._filepath:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            self._filepath = os.path.join(self.output_dir, f"doctors_{timestamp}.csv")
        return self._filepath
    
    def save_doctor(self, doctor: Doctor) -> bool:
        """Save single doctor to CSV."""
        with self._lock:
            try:
                data = doctor.to_flat_dict()
                filepath = self._get_filepath()
                
                if not self._file_initialized:
                    self._fieldnames = list(data.keys())
                    with open(filepath, 'w', newline='', encoding='utf-8') as f:
                        writer = csv.DictWriter(f, fieldnames=self._fieldnames)
                        writer.writeheader()
                    self._file_initialized = True
                
                with open(filepath, 'a', newline='', encoding='utf-8') as f:
                    writer = csv.DictWriter(f, fieldnames=self._fieldnames)
                    writer.writerow(data)
                
                return True
            except Exception as e:
                logger.error(f"Error saving doctor to CSV: {e}")
                return False
    
    def save_doctors(self, doctors: List[Doctor]) -> int:
        """Save multiple doctors to CSV."""
        saved = 0
        for doctor in doctors:
            if self.save_doctor(doctor):
                saved += 1
        return saved
    
    def get_filepath(self) -> str:
        """Get current output filepath."""
        return self._get_filepath()


class JSONStorage:
    """JSON file storage handler."""
    
    def __init__(self, output_dir: str = "data"):
        self.output_dir = output_dir
        self._lock = Lock()
        self._doctors: List[dict] = []
        self._filepath: Optional[str] = None
        os.makedirs(output_dir, exist_ok=True)
    
    def _get_filepath(self) -> str:
        """Get output filepath with timestamp."""
        if not self._filepath:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            self._filepath = os.path.join(self.output_dir, f"doctors_{timestamp}.json")
        return self._filepath
    
    def save_doctor(self, doctor: Doctor) -> bool:
        """Save single doctor to JSON."""
        with self._lock:
            try:
                self._doctors.append(doctor.to_dict())
                self._write_to_file()
                return True
            except Exception as e:
                logger.error(f"Error saving doctor to JSON: {e}")
                return False
    
    def save_doctors(self, doctors: List[Doctor]) -> int:
        """Save multiple doctors to JSON."""
        with self._lock:
            saved = 0
            for doctor in doctors:
                try:
                    self._doctors.append(doctor.to_dict())
                    saved += 1
                except Exception as e:
                    logger.error(f"Error adding doctor to JSON: {e}")
            self._write_to_file()
            return saved
    
    def _write_to_file(self) -> None:
        """Write all doctors to file."""
        filepath = self._get_filepath()
        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(self._doctors, f, ensure_ascii=False, indent=2)
    
    def get_filepath(self) -> str:
        """Get current output filepath."""
        return self._get_filepath()


class SQLiteStorage:
    """SQLite database storage handler."""
    
    def __init__(self, db_path: str = "data/doctors.db"):
        self.db_path = db_path
        self._lock = Lock()
        os.makedirs(os.path.dirname(db_path), exist_ok=True)
        self._init_database()
    
    def _init_database(self) -> None:
        """Initialize database schema."""
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS doctors (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    name TEXT NOT NULL,
                    title TEXT,
                    specialty TEXT NOT NULL,
                    city TEXT NOT NULL,
                    sub_specialties TEXT,
                    diseases_treated TEXT,
                    services TEXT,
                    phone TEXT,
                    email TEXT,
                    website TEXT,
                    doctoralia_url TEXT UNIQUE,
                    address_street TEXT,
                    address_city TEXT,
                    address_postal_code TEXT,
                    address_province TEXT,
                    address_latitude REAL,
                    address_longitude REAL,
                    clinic_name TEXT,
                    all_addresses TEXT,
                    experience_years INTEGER,
                    education TEXT,
                    languages TEXT,
                    insurance_accepted TEXT,
                    rating REAL,
                    review_count INTEGER,
                    consultation_price TEXT,
                    online_consultation_price TEXT,
                    offers_online_consultation INTEGER,
                    profile_url TEXT,
                    scraped_at TEXT,
                    created_at TEXT DEFAULT CURRENT_TIMESTAMP
                )
            """)
            
            conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_doctors_specialty 
                ON doctors(specialty)
            """)
            
            conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_doctors_city 
                ON doctors(city)
            """)
            
            conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_doctors_url 
                ON doctors(doctoralia_url)
            """)
            
            conn.commit()
    
    def save_doctor(self, doctor: Doctor) -> bool:
        """Save single doctor to database."""
        with self._lock:
            try:
                data = doctor.to_flat_dict()
                
                with sqlite3.connect(self.db_path) as conn:
                    # Use INSERT OR REPLACE to handle duplicates
                    columns = list(data.keys())
                    placeholders = ', '.join(['?' for _ in columns])
                    column_names = ', '.join(columns)
                    
                    conn.execute(f"""
                        INSERT OR REPLACE INTO doctors ({column_names})
                        VALUES ({placeholders})
                    """, list(data.values()))
                    conn.commit()
                
                return True
            except Exception as e:
                logger.error(f"Error saving doctor to database: {e}")
                return False
    
    def save_doctors(self, doctors: List[Doctor]) -> int:
        """Save multiple doctors to database."""
        saved = 0
        for doctor in doctors:
            if self.save_doctor(doctor):
                saved += 1
        return saved
    
    def get_existing_urls(self) -> Set[str]:
        """Get set of already scraped URLs."""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.execute("SELECT doctoralia_url FROM doctors WHERE doctoralia_url IS NOT NULL")
            return {row[0] for row in cursor.fetchall()}
    
    def get_doctor_count(self) -> int:
        """Get total number of doctors in database."""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.execute("SELECT COUNT(*) FROM doctors")
            return cursor.fetchone()[0]
    
    def export_to_csv(self, filepath: str) -> bool:
        """Export database to CSV file."""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.execute("SELECT * FROM doctors")
                columns = [desc[0] for desc in cursor.description]
                rows = cursor.fetchall()
            
            with open(filepath, 'w', newline='', encoding='utf-8') as f:
                writer = csv.writer(f)
                writer.writerow(columns)
                writer.writerows(rows)
            
            return True
        except Exception as e:
            logger.error(f"Error exporting to CSV: {e}")
            return False


class StorageManager:
    """Unified storage manager supporting multiple backends."""
    
    def __init__(self, output_dir: str = "data", output_format: str = "csv", db_path: str = "data/doctors.db"):
        self.output_dir = output_dir
        self.output_format = output_format.lower()
        
        # Always use SQLite for deduplication
        self.db = SQLiteStorage(db_path)
        
        # Primary output format
        if self.output_format == "json":
            self.primary = JSONStorage(output_dir)
        else:
            self.primary = CSVStorage(output_dir)
        
        self._saved_count = 0
        self._lock = Lock()
    
    def save_doctor(self, doctor: Doctor) -> bool:
        """Save doctor to all storage backends."""
        with self._lock:
            # Save to database (for deduplication)
            db_saved = self.db.save_doctor(doctor)
            
            # Save to primary format
            primary_saved = self.primary.save_doctor(doctor)
            
            if db_saved or primary_saved:
                self._saved_count += 1
                return True
            return False
    
    def save_doctors(self, doctors: List[Doctor]) -> int:
        """Save multiple doctors."""
        saved = 0
        for doctor in doctors:
            if self.save_doctor(doctor):
                saved += 1
        return saved
    
    def is_url_scraped(self, url: str) -> bool:
        """Check if URL has already been scraped."""
        existing = self.db.get_existing_urls()
        return url in existing
    
    def get_saved_count(self) -> int:
        """Get total saved count."""
        return self._saved_count
    
    def get_output_filepath(self) -> str:
        """Get primary output filepath."""
        return self.primary.get_filepath()
    
    def export_final_csv(self) -> str:
        """Export final CSV from database."""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filepath = os.path.join(self.output_dir, f"doctors_final_{timestamp}.csv")
        self.db.export_to_csv(filepath)
        return filepath
