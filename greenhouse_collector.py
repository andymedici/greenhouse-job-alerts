"""
Enhanced ATS Collector - Supporting Greenhouse and Lever
--------------------------------------------------------
Extended version of the Greenhouse collector with Lever support
and Fortune 500 company URLs for better coverage.
"""

import requests
import sqlite3
import time
import random
import smtplib
import json
import logging
import os
import urllib.robotparser
from contextlib import contextmanager
from datetime import datetime
from configparser import ConfigParser
from typing import Tuple, List, Optional, Dict, Any
from urllib.parse import urlparse, urljoin
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

from bs4 import BeautifulSoup

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    logging.warning("python-dotenv not installed. Using system environment variables only.")


class Config:
    """Enhanced configuration management class."""
    
    def __init__(self, config_file: str = "config.ini"):
        self.config = ConfigParser()
        self.config_file = config_file
        self._load_config()
    
    def _load_config(self):
        """Load configuration from file with sensible defaults."""
        if not os.path.exists(self.config_file):
            self._create_default_config()
        
        self.config.read(self.config_file)
    
    def _create_default_config(self):
        """Create a default configuration file with Fortune 500 URLs."""
        self.config['database'] = {
            'path': 'ats_tokens.db',
            'backup_enabled': 'true',
            'max_job_titles': '100'
        }
        
        self.config['scraping'] = {
            'min_delay': '10',
            'max_delay': '30',
            'max_retries': '3',
            'timeout': '15',
            'user_agent': 'ATSCollectorBot/2.0 (Research Purpose; contact: researcher@example.com)',
            'respect_robots_txt': 'true'
        }
        
        self.config['email'] = {
            'smtp_server': 'smtp.gmail.com',
            'smtp_port': '587',
            'enabled': 'true'
        }
        
        # Fortune 500 and major company URLs
        self.config['fortune_500_urls'] = {
            'urls': '''https://careers.walmart.com/
https://www.amazon.jobs/
https://careers.apple.com/
https://careers.google.com/
https://careers.microsoft.com/
https://jobs.netflix.com/
https://careers.meta.com/
https://careers.salesforce.com/
https://jobs.tesla.com/
https://careers.berkshirehathaway.com/
https://www.exxonmobil.com/en/careers
https://careers.jpmorgan.com/
https://www.jnj.com/careers/
https://careers.pg.com/
https://careers.homedepot.com/
https://jobs.chevron.com/
https://careers.verizon.com/
https://careers.att.com/
https://careers.ge.com/
https://www.mcdonalds.com/us/en-us/about-us/careers.html
https://corporate.target.com/careers/
https://careers.ups.com/
https://careers.ibm.com/
https://careers.intel.com/
https://jobs.lowes.com/
https://careers.disney.com/
https://careers.nike.com/
https://careers.pfizer.com/
https://careers.oracle.com/
https://careers.cisco.com/
https://careers.adidas-group.com/
https://careers.merck.com/
https://careers.abbott.com/
https://careers.ge.com/healthcare
https://careers.3m.com/
https://careers.caterpillar.com/
https://careers.boeing.com/
https://careers.lockheedmartin.com/
https://careers.ford.com/
https://careers.gm.com/
https://careers.fedex.com/
https://www.coca-colacompany.com/careers
https://careers.pepsi.com/
https://careers.starbucks.com/
https://careers.costco.com/
https://www.linkedin.com/company/*/jobs/
https://jobs.github.com/
https://stripe.com/jobs
https://notion.so/careers
https://figma.com/careers/
https://discord.com/jobs
https://dropbox.com/jobs
https://zoom.us/careers
https://doordash.com/careers/
https://instacart.careers/
https://robinhood.com/careers/
https://coinbase.com/careers/
https://plaid.com/careers/
https://segment.com/careers/
https://openai.com/careers/
https://anthropic.com/careers/
https://huggingface.co/jobs/
https://scale.com/careers/
https://cohere.ai/careers/
https://runwayml.com/careers/
https://replicate.com/careers/
https://perplexity.ai/careers/'''
        }
        
        # Discovery URLs for finding more ATS systems
        self.config['seed_urls'] = {
            'urls': '''https://github.com/poteto/hiring-without-whiteboards
https://github.com/donnemartin/awesome-interview-questions
https://github.com/jwasham/coding-interview-university
https://github.com/remoteintech/remote-jobs
https://github.com/lukasz-madon/awesome-remote-job
https://github.com/yanirs/established-remote
https://themuse.com/companies
https://builtin.com/companies
https://www.keyvalues.com/companies
https://angel.co/jobs
https://worklist.fyi/
https://www.ycombinator.com/companies
https://www.crunchbase.com/discover/organization.companies
https://techcrunch.com/startups/
https://www.producthunt.com/topics/hiring-and-recruiting
https://jobs.ashbyhq.com/
https://www.wellfound.com/jobs
https://remote.co/remote-jobs/
https://remoteok.io/
https://weworkremotely.com/
https://flexjobs.com/
https://justremote.co/
https://remotehub.io/
https://whoishiring.io/
https://startup.jobs/
https://unicornhunt.io/
https://stackoverflow.com/jobs/companies
https://news.ycombinator.com/jobs
https://jobs.lever.co/
https://boards.greenhouse.io/'''
        }
        
        # Known tokens for both systems
        self.config['seed_tokens'] = {
            'greenhouse_tokens': '''stripe,notion,figma,discord,dropbox,zoom,doordash,instacart,robinhood,coinbase,plaid,segment,openai,anthropic,huggingface,scale,cohere,runway,replicate,perplexity,
airbnb,reddit,gitlab,hashicorp,mongodb,elastic,cockroachlabs,mixpanel,sendgrid,twilio,okta,auth0,datadog,newrelic,pagerduty,
salesforce,snowflake,databricks,palantir,atlassian,asana,slack,zoom,calendly,notion,airtable,zapier,retool,webflow''',
            'lever_tokens': '''lever,uber,netflix,postmates,box,github,thumbtack,lyft,twitch,palantir,atlassian,asana,slack,zoom,calendly,linear,
mixpanel,sendgrid,twilio,okta,auth0,datadog,newrelic,pagerduty,mongodb,elastic,cockroachlabs,
brex,mercury,ramp,checkr,chime,affirm,klarna,stripe,square,adyen,marqeta'''
        }
        
        with open(self.config_file, 'w') as f:
            self.config.write(f)
        
        logging.info(f"Created default config file: {self.config_file}")
    
    def get(self, section: str, key: str, fallback: Any = None) -> str:
        """Get configuration value with fallback."""
        return self.config.get(section, key, fallback=fallback)
    
    def getint(self, section: str, key: str, fallback: int = 0) -> int:
        """Get integer configuration value."""
        return self.config.getint(section, key, fallback=fallback)
    
    def getboolean(self, section: str, key: str, fallback: bool = False) -> bool:
        """Get boolean configuration value."""
        return self.config.getboolean(section, key, fallback=fallback)


class DatabaseManager:
    """Enhanced database manager supporting multiple ATS systems."""
    
    def __init__(self, db_path: str):
        self.db_path = db_path
        self._init_db()
    
    @contextmanager
    def get_connection(self):
        """Context manager for database connections."""
        conn = None
        try:
            conn = sqlite3.connect(self.db_path, timeout=30)
            conn.row_factory = sqlite3.Row
            yield conn
        finally:
            if conn:
                conn.close()
    
    def _init_db(self):
        """Initialize database with tables for multiple ATS systems."""
        with self.get_connection() as conn:
            # Modified table to support multiple ATS types
            conn.execute("""
                CREATE TABLE IF NOT EXISTS ats_companies (
                    id TEXT PRIMARY KEY,
                    ats_type TEXT NOT NULL,
                    token TEXT NOT NULL,
                    source_url TEXT,
                    company_name TEXT,
                    job_count INTEGER,
                    locations TEXT,
                    departments TEXT,
                    job_titles TEXT,
                    remote_jobs_count INTEGER DEFAULT 0,
                    hybrid_jobs_count INTEGER DEFAULT 0,
                    onsite_jobs_count INTEGER DEFAULT 0,
                    work_type_breakdown TEXT,
                    first_seen TIMESTAMP,
                    last_seen TIMESTAMP,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(ats_type, token)
                )
            """)
            
            conn.execute("""
                CREATE TABLE IF NOT EXISTS job_details (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    company_id TEXT,
                    ats_type TEXT,
                    token TEXT,
                    job_title TEXT,
                    job_url TEXT,
                    location TEXT,
                    department TEXT,
                    work_type TEXT,
                    posted_date TEXT,
                    job_description_snippet TEXT,
                    first_seen TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    last_seen TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (company_id) REFERENCES ats_companies (id)
                )
            """)
            
            conn.execute("""
                CREATE TABLE IF NOT EXISTS crawl_history (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    url TEXT,
                    ats_type TEXT,
                    tokens_found INTEGER,
                    success BOOLEAN,
                    error_message TEXT,
                    crawl_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            
            # Create indexes
            conn.execute("CREATE INDEX IF NOT EXISTS idx_ats_type ON ats_companies(ats_type)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_last_seen ON ats_companies(last_seen)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_work_type ON job_details(work_type)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_job_company ON job_details(company_id)")
            
            conn.commit()
            logging.info("Database initialized successfully")
    
    def upsert_company(self, ats_type: str, token: str, source: str, company_name: str, 
                      job_count: int, locations: List[str], departments: List[str], 
                      job_titles: List[str], work_type_counts: Dict[str, int] = None,
                      job_details: List[Dict] = None) -> bool:
        """Insert or update a company record."""
        try:
            now = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
            company_id = f"{ats_type}_{token}"
            titles_json = json.dumps(job_titles[:100])
            locs_str = ", ".join(locations[:50])
            depts_str = ", ".join(departments[:20])
            
            if work_type_counts is None:
                work_type_counts = {'remote': 0, 'hybrid': 0, 'onsite': 0}
            
            work_type_breakdown = json.dumps(work_type_counts)
            
            with self.get_connection() as conn:
                cursor = conn.execute("SELECT id FROM ats_companies WHERE id=?", (company_id,))
                exists = cursor.fetchone() is not None
                
                if exists:
                    conn.execute("""
                        UPDATE ats_companies
                        SET source_url=?, company_name=?, job_count=?, locations=?, 
                            departments=?, job_titles=?, remote_jobs_count=?, hybrid_jobs_count=?,
                            onsite_jobs_count=?, work_type_breakdown=?, last_seen=?, updated_at=?
                        WHERE id=?
                    """, (source, company_name, job_count, locs_str, depts_str, 
                         titles_json, work_type_counts.get('remote', 0), 
                         work_type_counts.get('hybrid', 0), work_type_counts.get('onsite', 0),
                         work_type_breakdown, now, now, company_id))
                    logging.info(f"Updated {ats_type} company: {token}")
                else:
                    conn.execute("""
                        INSERT INTO ats_companies
                        (id, ats_type, token, source_url, company_name, job_count, locations, 
                         departments, job_titles, remote_jobs_count, hybrid_jobs_count,
                         onsite_jobs_count, work_type_breakdown, first_seen, last_seen)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """, (company_id, ats_type, token, source, company_name, job_count, 
                         locs_str, depts_str, titles_json, work_type_counts.get('remote', 0),
                         work_type_counts.get('hybrid', 0), work_type_counts.get('onsite', 0),
                         work_type_breakdown, now, now))
                    logging.info(f"Inserted new {ats_type} company: {token}")
                
                # Store detailed job information
                if job_details:
                    conn.execute("DELETE FROM job_details WHERE company_id=?", (company_id,))
                    
                    for job in job_details[:100]:
                        conn.execute("""
                            INSERT INTO job_details 
                            (company_id, ats_type, token, job_title, job_url, location, 
                             department, work_type, posted_date, job_description_snippet)
                            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """, (company_id, ats_type, token, job.get('title', ''), 
                             job.get('url', ''), job.get('location', ''), 
                             job.get('department', ''), job.get('work_type', ''), 
                             job.get('posted_date', ''), job.get('description_snippet', '')))
                
                conn.commit()
                return True
        except Exception as e:
            logging.error(f"Database error for {ats_type} token {token}: {e}")
            return False
    
    def log_crawl(self, url: str, ats_type: str, tokens_found: int, success: bool, error_message: str = None):
        """Log crawl attempt."""
        try:
            with self.get_connection() as conn:
                conn.execute("""
                    INSERT INTO crawl_history (url, ats_type, tokens_found, success, error_message)
                    VALUES (?, ?, ?, ?, ?)
                """, (url, ats_type, tokens_found, success, error_message))
                conn.commit()
        except Exception as e:
            logging.error(f"Failed to log crawl history: {e}")
    
    def get_all_companies(self) -> List[sqlite3.Row]:
        """Retrieve all companies from database."""
        try:
            with self.get_connection() as conn:
                cursor = conn.execute("""
                    SELECT ats_type, token, company_name, job_count, locations, departments, 
                           job_titles, remote_jobs_count, hybrid_jobs_count, onsite_jobs_count,
                           work_type_breakdown, first_seen, last_seen
                    FROM ats_companies 
                    ORDER BY ats_type, company_name
                """)
                return cursor.fetchall()
        except Exception as e:
            logging.error(f"Failed to retrieve companies: {e}")
            return []


class TokenExtractor:
    """Extract and validate tokens from multiple ATS systems."""
    
    @staticmethod
    def extract_greenhouse_token(url: str) -> Optional[str]:
        """Extract Greenhouse token from URL."""
        try:
            parsed = urlparse(url)
            if parsed.netloc == "boards.greenhouse.io":
                parts = parsed.path.strip("/").split("/")
                if parts and parts[0]:
                    token = parts[0]
                    if TokenExtractor.validate_token(token):
                        return token
        except Exception as e:
            logging.debug(f"Could not extract greenhouse token from {url}: {e}")
        return None
    
    @staticmethod
    def extract_lever_token(url: str) -> Optional[str]:
        """Extract Lever token from URL."""
        try:
            parsed = urlparse(url)
            if parsed.netloc == "jobs.lever.co":
                parts = parsed.path.strip("/").split("/")
                if parts and parts[0]:
                    token = parts[0]
                    if TokenExtractor.validate_token(token):
                        return token
            # Also check for lever URLs in other formats
            elif "lever.co" in parsed.netloc:
                # Some companies use custom subdomains like company.lever.co
                subdomain = parsed.netloc.split('.')[0]
                if subdomain != "jobs" and TokenExtractor.validate_token(subdomain):
                    return subdomain
        except Exception as e:
            logging.debug(f"Could not extract lever token from {url}: {e}")
        return None
    
    @staticmethod
    def validate_token(token: str) -> bool:
        """Validate token format."""
        return (token and 
                len(token) >= 2 and 
                len(token) <= 100 and 
                token.replace('-', '').replace('_', '').replace('.', '').isalnum())


class LeverBoardParser:
    """Parse Lever job board pages."""
    
    def __init__(self, user_agent: str, timeout: int = 15):
        self.user_agent = user_agent
        self.timeout = timeout
        self.session = requests.Session()
        self.session.headers.update({'User-Agent': user_agent})
    
    def parse_board(self, token: str, max_retries: int = 3) -> Tuple[Optional[str], int, List[str], List[str], List[str], Dict[str, int], List[Dict]]:
        """Fetch and parse metadata from Lever board."""
        # Try different URL formats for Lever
        urls_to_try = [
            f"https://jobs.lever.co/{token}",
            f"https://{token}.lever.co/jobs",
            f"https://{token}.jobs.lever.co/"
        ]
        
        for url in urls_to_try:
            for attempt in range(max_retries):
                try:
                    response = self.session.get(url, timeout=self.timeout)
                    
                    if response.status_code == 429:
                        wait_time = int(response.headers.get('Retry-After', 60))
                        logging.warning(f"Rate limited for {token}, waiting {wait_time} seconds...")
                        time.sleep(wait_time)
                        continue
                    elif response.status_code == 404:
                        break  # Try next URL format
                    elif response.status_code != 200:
                        logging.warning(f"HTTP {response.status_code} for {url}")
                        break
                    
                    result = self._parse_html(response.text, token)
                    if result[0]:  # If we got a company name, parsing succeeded
                        return result
                    
                except requests.RequestException as e:
                    logging.warning(f"Attempt {attempt + 1} failed for {url}: {e}")
                    if attempt == max_retries - 1:
                        break
                    time.sleep(random.uniform(5, 15))
        
        return None, 0, [], [], [], {}, []
    
    def _parse_html(self, html: str, token: str) -> Tuple[str, int, List[str], List[str], List[str], Dict[str, int], List[Dict]]:
        """Parse HTML content to extract job information."""
        soup = BeautifulSoup(html, "html.parser")
        
        # Extract company name
        company_name = self._extract_company_name(soup, token)
        
        # Extract jobs data
        jobs_data = self._extract_jobs_data(soup)
        
        job_titles = []
        locations = set()
        departments = set()
        work_type_counts = {'remote': 0, 'hybrid': 0, 'onsite': 0}
        detailed_jobs = []
        
        for job_data in jobs_data:
            if job_data.get('title'):
                job_titles.append(self._sanitize_text(job_data['title']))
                
                work_type = self._classify_work_type(job_data.get('location', ''), job_data.get('title', ''))
                work_type_counts[work_type] += 1
                
                detailed_job = {
                    'title': self._sanitize_text(job_data['title']),
                    'url': job_data.get('url', ''),
                    'location': self._sanitize_text(job_data.get('location', '')),
                    'department': self._sanitize_text(job_data.get('department', '')),
                    'work_type': work_type,
                    'posted_date': job_data.get('posted_date', ''),
                    'description_snippet': self._sanitize_text(job_data.get('description', ''))[:500]
                }
                detailed_jobs.append(detailed_job)
            
            if job_data.get('location'):
                locations.add(self._sanitize_text(job_data['location']))
            if job_data.get('department'):
                departments.add(self._sanitize_text(job_data['department']))
        
        return (company_name, len(job_titles), 
                sorted(list(locations)), sorted(list(departments)), job_titles,
                work_type_counts, detailed_jobs)
    
    def _extract_company_name(self, soup: BeautifulSoup, token: str) -> str:
        """Extract company name from Lever page."""
        selectors = [
            '.company-name',
            '[data-qa="company-name"]',
            'h1',
            'title',
            '.header-company-name'
        ]
        
        for selector in selectors:
            element = soup.select_one(selector)
            if element:
                text = element.get_text(strip=True)
                if text and len(text) > 2:
                    text = text.replace(' Jobs', '').replace(' Careers', '').strip()
                    return self._sanitize_text(text)
        
        return token.replace('-', ' ').replace('_', ' ').title()
    
    def _extract_jobs_data(self, soup: BeautifulSoup) -> List[Dict[str, str]]:
        """Extract job data from Lever page."""
        jobs_data = []
        
        # Lever-specific selectors
        job_selectors = [
            '.posting',
            '[data-qa="posting"]',
            '.posting-card',
            '.job-posting',
            'a[href*="/jobs/"]',
            '.lever-job',
            '.posting-name'
        ]
        
        for selector in job_selectors:
            elements = soup.select(selector)
            if elements:
                for element in elements:
                    job_data = self._extract_job_from_element(element)
                    if job_data:
                        jobs_data.append(job_data)
                break
        
        return jobs_data
    
    def _extract_job_from_element(self, element) -> Optional[Dict[str, str]]:
        """Extract job information from a single element."""
        job_data = {}
        
        # Extract title and URL
        title_element = element if element.name in ['a', 'h1', 'h2', 'h3'] else element.find(['a', 'h1', 'h2', 'h3'])
        if title_element:
            job_data['title'] = title_element.get_text(strip=True)
            if title_element.name == 'a' and title_element.get('href'):
                job_data['url'] = title_element.get('href')
                if job_data['url'].startswith('/'):
                    job_data['url'] = f"https://jobs.lever.co{job_data['url']}"
        
        # Extract location
        location_selectors = ['.posting-location', '.location', '[data-qa="posting-location"]']
        for sel in location_selectors:
            loc_elem = element.find(class_=sel.replace('.', '')) or element.find_next(class_=sel.replace('.', ''))
            if loc_elem:
                job_data['location'] = loc_elem.get_text(strip=True)
                break
        
        # Extract department
        dept_selectors = ['.posting-category', '.department', '[data-qa="posting-category"]']
        for sel in dept_selectors:
            dept_elem = element.find(class_=sel.replace('.', '')) or element.find_next(class_=sel.replace('.', ''))
            if dept_elem:
                job_data['department'] = dept_elem.get_text(strip=True)
                break
        
        return job_data if job_data.get('title') else None
    
    def _classify_work_type(self, location: str, title: str) -> str:
        """Classify job as remote, hybrid, or onsite."""
        location_lower = location.lower()
        title_lower = title.lower()
        
        remote_keywords = [
            'remote', 'anywhere', 'distributed', 'work from home', 'wfh',
            'telecommute', 'virtual', 'global', 'worldwide'
        ]
        
        hybrid_keywords = [
            'hybrid', 'flexible', 'partial remote', 'some remote',
            'remote friendly', 'remote optional'
        ]
        
        for keyword in remote_keywords:
            if keyword in location_lower or keyword in title_lower:
                return 'remote'
        
        for keyword in hybrid_keywords:
            if keyword in location_lower or keyword in title_lower:
                return 'hybrid'
        
        return 'onsite'
    
    @staticmethod
    def _sanitize_text(text: str) -> str:
        """Clean and validate text data."""
        if not text:
            return ""
        return text.strip()[:200]


class EnhancedATSCollector:
    """Enhanced collector supporting multiple ATS systems."""
    
    def __init__(self, config_file: str = "config.ini", dry_run: bool = False):
        self.dry_run = dry_run
        self.config = Config(config_file)
        self._setup_logging()
        self._validate_environment()
        
        # Initialize components
        self.db_manager = DatabaseManager(self.config.get('database', 'path', 'ats_tokens.db'))
        
        # Import original classes from the existing code
        from TokenCollector1 import RobotsChecker, GreenhouseBoardParser, EmailReporter
        
        self.robots_checker = RobotsChecker(self.config.get('scraping', 'user_agent'))
        self.greenhouse_parser = GreenhouseBoardParser(
            self.config.get('scraping', 'user_agent'),
            self.config.getint('scraping', 'timeout', 15)
        )
        self.lever_parser = LeverBoardParser(
            self.config.get('scraping', 'user_agent'),
            self.config.getint('scraping', 'timeout', 15)
        )
        
        # Setup email if enabled
        if self.config.getboolean('email', 'enabled', True):
            self.email_reporter = EmailReporter(
                self.config.get('email', 'smtp_server', 'smtp.gmail.com'),
                self.config.getint('email', 'smtp_port', 587),
                os.getenv('SMTP_USER'),
                os.getenv('SMTP_PASS')
            )
        else:
            self.email_reporter = None
    
    def _setup_logging(self):
        """Configure logging."""
        log_level = logging.DEBUG if self.dry_run else logging.INFO
        
        logging.basicConfig(
            level=log_level,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler('ats_collector.log'),
                logging.StreamHandler()
            ]
        )
        
        logging.getLogger('urllib3').setLevel(logging.WARNING)
        logging.getLogger('requests').setLevel(logging.WARNING)
    
    def _validate_environment(self):
        """Validate required environment variables."""
        required_env_vars = []
        
        if self.config.getboolean('email', 'enabled', True):
            required_env_vars.extend(['SMTP_USER', 'SMTP_PASS', 'EMAIL_RECIPIENT'])
        
        missing_vars = [var for var in required_env_vars if not os.getenv(var)]
        if missing_vars:
            logging.error(f"Missing required environment variables: {missing_vars}")
            raise ValueError(f"Missing required environment variables: {missing_vars}")
    
    def crawl_page(self, url: str) -> Tuple[int, int]:
        """Crawl a single page for ATS tokens. Returns (greenhouse_tokens, lever_tokens)."""
        if self.config.getboolean('scraping', 'respect_robots_txt', True):
            if not self.robots_checker.can_fetch(url):
                logging.warning(f"Skipping {url} due to robots.txt restrictions")
                return 0, 0
        
        greenhouse_tokens = 0
        lever_tokens = 0
        error_message = None
        
        try:
            logging.info(f"Crawling: {url}")
            
            headers = {"User-Agent": self.config.get('scraping', 'user_agent')}
            response = requests.get(url, headers=headers, 
                                 timeout=self.config.getint('scraping', 'timeout', 15))
            
            if response.status_code != 200:
                error_message = f"HTTP {response.status_code}"
                logging.warning(f"Failed to fetch {url}: {error_message}")
                return 0, 0
            
            soup = BeautifulSoup(response.text, "html.parser")
            
            # Find all links that might contain ATS tokens
            for link in soup.find_all("a", href=True):
                href = link["href"]
                
                # Check for Greenhouse tokens
                greenhouse_token = TokenExtractor.extract_greenhouse_token(href)
                if greenhouse_token:
                    if self.dry_run:
                        logging.info(f"[DRY RUN] Would process Greenhouse token: {greenhouse_token}")
                        greenhouse_tokens += 1
                        continue
                    
                    company_name, job_count, locations, departments, job_titles, work_type_counts, job_details = \
                        self.greenhouse_parser.parse_board(greenhouse_token, self.config.getint('scraping', 'max_retries', 3))
                    
                    if company_name:
                        success = self.db_manager.upsert_company(
                            'greenhouse', greenhouse_token, url, company_name, job_count, 
                            locations, departments, job_titles, work_type_counts, job_details
                        )
                        if success:
                            greenhouse_tokens += 1
                            logging.info(f"Processed Greenhouse {greenhouse_token}: {company_name} ({job_count} jobs)")
                
                # Check for Lever tokens
                lever_token = TokenExtractor.extract_lever_token(href)
                if lever_token:
                    if self.dry_run:
                        logging.info(f"[DRY RUN] Would process Lever token: {lever_token}")
                        lever_tokens += 1
                        continue
                    
                    company_name, job_count, locations, departments, job_titles, work_type_counts, job_details = \
                        self.lever_parser.parse_board(lever_token, self.config.getint('scraping', 'max_retries', 3))
                    
                    if company_name:
                        success = self.db_manager.upsert_company(
                            'lever', lever_token, url, company_name, job_count, 
                            locations, departments, job_titles, work_type_counts, job_details
                        )
                        if success:
                            lever_tokens += 1
                            logging.info(f"Processed Lever {lever_token}: {company_name} ({job_count} jobs)")
                
                # Rate limiting between token processing
                if greenhouse_token or lever_token:
                    delay = random.uniform(
                        self.config.getint('scraping', 'min_delay', 10),
                        self.config.getint('scraping', 'max_delay', 30)
                    )
                    time.sleep(delay)
        
        except Exception as e:
            error_message = str(e)
            logging.error(f"Error crawling {url}: {e}")
        
        finally:
            # Log the crawl attempts
            if not self.dry_run:
                if greenhouse_tokens > 0:
                    self.db_manager.log_crawl(url, 'greenhouse', greenhouse_tokens, error_message is None, error_message)
                if lever_tokens > 0:
                    self.db_manager.log_crawl(url, 'lever', lever_tokens, error_message is None, error_message)
        
        return greenhouse_tokens, lever_tokens
    
    def process_seed_tokens(self) -> Tuple[int, int]:
        """Process known ATS tokens directly."""
        greenhouse_tokens = [token.strip() for token in 
                           self.config.get('seed_tokens', 'greenhouse_tokens', '').split(',') if token.strip()]
        lever_tokens = [token.strip() for token in 
                       self.config.get('seed_tokens', 'lever_tokens', '').split(',') if token.strip()]
        
        total_greenhouse = 0
        total_lever = 0
        
        # Process Greenhouse tokens
        if greenhouse_tokens:
            logging.info(f"Processing {len(greenhouse_tokens)} seed Greenhouse tokens")
            for token in greenhouse_tokens:
                if not TokenExtractor.validate_token(token):
                    logging.warning(f"Invalid Greenhouse token format: {token}")
                    continue
                    
                if self.dry_run:
                    logging.info(f"[DRY RUN] Would process Greenhouse seed token: {token}")
                    total_greenhouse += 1
                    continue
                
                try:
                    company_name, job_count, locations, departments, job_titles, work_type_counts, job_details = \
                        self.greenhouse_parser.parse_board(token, self.config.getint('scraping', 'max_retries', 3))
                    
                    if company_name:
                        success = self.db_manager.upsert_company(
                            'greenhouse', token, "seed_token", company_name, job_count, 
                            locations, departments, job_titles, work_type_counts, job_details
                        )
                        if success:
                            total_greenhouse += 1
                            logging.info(f"Processed Greenhouse seed {token}: {company_name} ({job_count} jobs)")
                    else:
                        logging.warning(f"Failed to parse Greenhouse seed token: {token}")
                    
                    delay = random.uniform(
                        self.config.getint('scraping', 'min_delay', 10),
                        self.config.getint('scraping', 'max_delay', 30)
                    )
                    time.sleep(delay)
                    
                except Exception as e:
                    logging.error(f"Error processing Greenhouse seed token {token}: {e}")
        
        # Process Lever tokens
        if lever_tokens:
            logging.info(f"Processing {len(lever_tokens)} seed Lever tokens")
            for token in lever_tokens:
                if not TokenExtractor.validate_token(token):
                    logging.warning(f"Invalid Lever token format: {token}")
                    continue
                    
                if self.dry_run:
                    logging.info(f"[DRY RUN] Would process Lever seed token: {token}")
                    total_lever += 1
                    continue
                
                try:
                    company_name, job_count, locations, departments, job_titles, work_type_counts, job_details = \
                        self.lever_parser.parse_board(token, self.config.getint('scraping', 'max_retries', 3))
                    
                    if company_name:
                        success = self.db_manager.upsert_company(
                            'lever', token, "seed_token", company_name, job_count, 
                            locations, departments, job_titles, work_type_counts, job_details
                        )
                        if success:
                            total_lever += 1
                            logging.info(f"Processed Lever seed {token}: {company_name} ({job_count} jobs)")
                    else:
                        logging.warning(f"Failed to parse Lever seed token: {token}")
                    
                    delay = random.uniform(
                        self.config.getint('scraping', 'min_delay', 10),
                        self.config.getint('scraping', 'max_delay', 30)
                    )
                    time.sleep(delay)
                    
                except Exception as e:
                    logging.error(f"Error processing Lever seed token {token}: {e}")
        
        return total_greenhouse, total_lever
    
    def run(self) -> bool:
        """Run the complete collection process."""
        start_time = datetime.utcnow()
        logging.info(f"Starting ATS token collection {'(DRY RUN)' if self.dry_run else ''}")
        
        total_greenhouse = 0
        total_lever = 0
        
        # Process seed tokens first
        seed_gh, seed_lv = self.process_seed_tokens()
        total_greenhouse += seed_gh
        total_lever += seed_lv
        logging.info(f"Processed {seed_gh} Greenhouse and {seed_lv} Lever seed tokens")
        
        # Crawl Fortune 500 URLs
        fortune_urls = [url.strip() for url in 
                       self.config.get('fortune_500_urls', 'urls', '').split('\n') if url.strip()]
        
        if fortune_urls:
            logging.info(f"Crawling {len(fortune_urls)} Fortune 500 URLs")
            for url in fortune_urls:
                gh_found, lv_found = self.crawl_page(url)
                total_greenhouse += gh_found
                total_lever += lv_found
                
                delay = random.uniform(
                    self.config.getint('scraping', 'min_delay', 10),
                    self.config.getint('scraping', 'max_delay', 30)
                )
                time.sleep(delay)
        
        # Crawl discovery URLs
        seed_urls = [url.strip() for url in 
                    self.config.get('seed_urls', 'urls', '').split('\n') if url.strip()]
        
        if seed_urls:
            logging.info(f"Crawling {len(seed_urls)} discovery URLs")
            for url in seed_urls:
                gh_found, lv_found = self.crawl_page(url)
                total_greenhouse += gh_found
                total_lever += lv_found
                
                delay = random.uniform(
                    self.config.getint('scraping', 'min_delay', 10),
                    self.config.getint('scraping', 'max_delay', 30)
                )
                time.sleep(delay)
        
        # Send email summary if enabled
        if (self.email_reporter and 
            not self.dry_run and 
            self.config.getboolean('email', 'enabled', True)):
            
            recipient = os.getenv('EMAIL_RECIPIENT')
            if recipient:
                companies_data = self.db_manager.get_all_companies()
                self._send_enhanced_summary(companies_data, recipient)
        
        end_time = datetime.utcnow()
        duration = (end_time - start_time).total_seconds()
        
        logging.info(f"Collection completed in {duration:.2f} seconds. "
                    f"Greenhouse: {total_greenhouse}, Lever: {total_lever}")
        
        return True
    
    def _send_enhanced_summary(self, companies_data: List[sqlite3.Row], recipient: str):
        """Send enhanced email summary with both ATS systems."""
        try:
            if not companies_data:
                body = "No companies collected yet."
                html_body = "<p>No companies collected yet.</p>"
            else:
                body = self._format_enhanced_summary(companies_data)
                html_body = self._format_enhanced_html_summary(companies_data)
            
            msg = MIMEMultipart('alternative')
            msg["Subject"] = f"Multi-ATS Collection Report - {datetime.utcnow().strftime('%Y-%m-%d')}"
            msg["From"] = os.getenv('SMTP_USER')
            msg["To"] = recipient
            
            text_part = MIMEText(body, 'plain')
            html_part = MIMEText(html_body, 'html')
            
            msg.attach(text_part)
            msg.attach(html_part)
            
            smtp_server = self.config.get('email', 'smtp_server', 'smtp.gmail.com')
            smtp_port = self.config.getint('email', 'smtp_port', 587)
            
            with smtplib.SMTP(smtp_server, smtp_port) as server:
                server.starttls()
                server.login(os.getenv('SMTP_USER'), os.getenv('SMTP_PASS'))
                server.sendmail(os.getenv('SMTP_USER'), recipient, msg.as_string())
            
            logging.info("Enhanced email summary sent successfully")
            
        except Exception as e:
            logging.error(f"Error sending enhanced email: {e}")
    
    def _format_enhanced_summary(self, companies_data: List[sqlite3.Row]) -> str:
        """Format enhanced text summary."""
        if not companies_data:
            return "No companies collected yet."
        
        # Separate by ATS type
        greenhouse_companies = [c for c in companies_data if c.get('ats_type') == 'greenhouse']
        lever_companies = [c for c in companies_data if c.get('ats_type') == 'lever']
        
        # Calculate totals
        total_jobs = sum(row.get('job_count', 0) or 0 for row in companies_data)
        total_remote = sum(row.get('remote_jobs_count', 0) or 0 for row in companies_data)
        total_hybrid = sum(row.get('hybrid_jobs_count', 0) or 0 for row in companies_data)
        total_onsite = sum(row.get('onsite_jobs_count', 0) or 0 for row in companies_data)
        
        lines = [
            f"Multi-ATS Collection Summary",
            f"=" * 50,
            f"Total Companies: {len(companies_data)}",
            f"  Greenhouse: {len(greenhouse_companies)}",
            f"  Lever: {len(lever_companies)}",
            f"Total Jobs: {total_jobs}",
            f"  Remote: {total_remote} ({total_remote/total_jobs*100:.1f}%)" if total_jobs > 0 else "  Remote: 0",
            f"  Hybrid: {total_hybrid} ({total_hybrid/total_jobs*100:.1f}%)" if total_jobs > 0 else "  Hybrid: 0",
            f"  On-site: {total_onsite} ({total_onsite/total_jobs*100:.1f}%)" if total_jobs > 0 else "  On-site: 0",
            "",
            "Company Details:",
            "=" * 50
        ]
        
        for row in companies_data:
            ats_type = row.get('ats_type', 'unknown').upper()
            remote_count = row.get('remote_jobs_count', 0) or 0
            hybrid_count = row.get('hybrid_jobs_count', 0) or 0
            onsite_count = row.get('onsite_jobs_count', 0) or 0
            
            lines.append(
                f"[{ats_type}] {row.get('company_name', 'Unknown')} ({row.get('token', 'unknown')})\n"
                f"  Total Jobs: {row.get('job_count', 0)}\n"
                f"  Work Types: Remote {remote_count} | Hybrid {hybrid_count} | Onsite {onsite_count}\n"
                f"  Locations: {row.get('locations', 'Not specified')}\n"
                f"  Last seen: {row.get('last_seen', 'Unknown')}\n"
                + "-" * 50
            )
        
        return "\n".join(lines)
    
    def _format_enhanced_html_summary(self, companies_data: List[sqlite3.Row]) -> str:
        """Format enhanced HTML summary."""
        if not companies_data:
            return "<p>No companies collected yet.</p>"
        
        # Calculate totals by ATS type
        greenhouse_companies = [c for c in companies_data if c.get('ats_type') == 'greenhouse']
        lever_companies = [c for c in companies_data if c.get('ats_type') == 'lever']
        
        total_jobs = sum(row.get('job_count', 0) or 0 for row in companies_data)
        total_remote = sum(row.get('remote_jobs_count', 0) or 0 for row in companies_data)
        total_hybrid = sum(row.get('hybrid_jobs_count', 0) or 0 for row in companies_data)
        total_onsite = sum(row.get('onsite_jobs_count', 0) or 0 for row in companies_data)
        
        html = f"""
        <html>
        <body style="font-family: Arial, sans-serif;">
        <h2>Multi-ATS Collection Report - {datetime.utcnow().strftime('%Y-%m-%d')}</h2>
        
        <div style="background-color: #f5f5f5; padding: 15px; border-radius: 5px; margin-bottom: 20px;">
            <h3>Summary Statistics</h3>
            <p><strong>Total Companies:</strong> {len(companies_data)}</p>
            <div style="margin-left: 20px;">
                <p><strong>Greenhouse:</strong> {len(greenhouse_companies)}</p>
                <p><strong>Lever:</strong> {len(lever_companies)}</p>
            </div>
            <p><strong>Total Jobs:</strong> {total_jobs}</p>
            <div style="margin-left: 20px;">
                <p><strong>Remote:</strong> {total_remote} ({total_remote/total_jobs*100:.1f}%)</p>
                <p><strong>Hybrid:</strong> {total_hybrid} ({total_hybrid/total_jobs*100:.1f}%)</p>
                <p><strong>On-site:</strong> {total_onsite} ({total_onsite/total_jobs*100:.1f}%)</p>
            </div>
        </div>
        
        <table border="1" style="border-collapse: collapse; width: 100%;">
        <tr style="background-color: #e0e0e0;">
            <th>ATS Type</th>
            <th>Company</th>
            <th>Token</th>
            <th>Total Jobs</th>
            <th>Remote</th>
            <th>Hybrid</th>
            <th>On-site</th>
            <th>Locations</th>
            <th>Last Seen</th>
        </tr>
        """ if total_jobs > 0 else f"""
        <html>
        <body style="font-family: Arial, sans-serif;">
        <h2>Multi-ATS Collection Report - {datetime.utcnow().strftime('%Y-%m-%d')}</h2>
        
        <div style="background-color: #f5f5f5; padding: 15px; border-radius: 5px; margin-bottom: 20px;">
            <h3>Summary Statistics</h3>
            <p><strong>Total Companies:</strong> {len(companies_data)}</p>
            <p><strong>Total Jobs:</strong> {total_jobs}</p>
        </div>
        
        <table border="1" style="border-collapse: collapse; width: 100%;">
        <tr style="background-color: #e0e0e0;">
            <th>ATS Type</th>
            <th>Company</th>
            <th>Token</th>
            <th>Total Jobs</th>
            <th>Remote</th>
            <th>Hybrid</th>
            <th>On-site</th>
            <th>Locations</th>
            <th>Last Seen</th>
        </tr>
        """
        
        for row in companies_data:
            ats_type = row.get('ats_type', 'unknown')
            remote_count = row.get('remote_jobs_count', 0) or 0
            hybrid_count = row.get('hybrid_jobs_count', 0) or 0
            onsite_count = row.get('onsite_jobs_count', 0) or 0
            
            # Create appropriate URL based on ATS type
            if ats_type == 'greenhouse':
                job_board_url = f"https://boards.greenhouse.io/{row.get('token', '')}"
            elif ats_type == 'lever':
                job_board_url = f"https://jobs.lever.co/{row.get('token', '')}"
            else:
                job_board_url = "#"
            
            html += f"""
            <tr>
                <td><strong>{ats_type.upper()}</strong></td>
                <td><strong>{row.get('company_name', 'Unknown')}</strong></td>
                <td><a href="{job_board_url}" target="_blank">{row.get('token', '')}</a></td>
                <td>{row.get('job_count', 0)}</td>
                <td>{remote_count}</td>
                <td>{hybrid_count}</td>
                <td>{onsite_count}</td>
                <td>{row.get('locations', 'Not specified')}</td>
                <td>{row.get('last_seen', 'Unknown')}</td>
            </tr>
            """
        
        html += """
        </table>
        
        <div style="margin-top: 20px; font-size: 12px; color: #666;">
            <p><em>Click on any token to view the company's live job board.</em></p>
        </div>
        
        </body>
        </html>
        """
        return html


def main():
    """Main entry point."""
    import argparse
    
    parser = argparse.ArgumentParser(description='Enhanced Multi-ATS Collector')
    parser.add_argument('--config', default='config.ini', help='Configuration file path')
    parser.add_argument('--dry-run', action='store_true', help='Run without making changes')
    parser.add_argument('--verbose', '-v', action='store_true', help='Verbose logging')
    
    args = parser.parse_args()
    
    try:
        collector = EnhancedATSCollector(args.config, args.dry_run)
        success = collector.run()
        return 0 if success else 1
    except Exception as e:
        logging.error(f"Fatal error: {e}")
        return 1


if __name__ == "__main__":
    exit(main())
