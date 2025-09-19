def _format_html_summary(self, tokens_data: List[sqlite3.Row]) -> str:
        """Format HTML summary."""
        if not tokens_data:
            return "<p>No tokens collected yet.</p>"
        
        # Calculate totals
        total_jobs = sum(row.get('job_count', 0) or 0 for row in tokens_data)
        total_remote = sum(row.get('remote_jobs_count', 0) or 0 for row in tokens_data)
        total_hybrid = sum(row.get('hybrid_jobs_count', 0) or 0 for row in tokens_data)
        total_onsite = sum(row.get('onsite_jobs_count', 0) or 0 for row in tokens_data)
        
        html = f"""
        <html>
        <body style="font-family: Arial, sans-serif;">
        <h2>🔍 Greenhouse Tokens Report - {datetime.utcnow().strftime('%Y-%m-%d')}</h2>
        
        <div style="background-color: #f5f5f5; padding: 15px; border-radius: 5px; margin-bottom: 20px;">
            <h3>📊 Summary Statistics</h3>
            <p><strong>Total Companies:</strong> {len(tokens_data)}</p>
            <p><strong>Total Jobs:</strong> {total_jobs}</p>
            <div style="margin-left: 20px;">
                <p>🏠 <strong>Remote:</strong> {total_remote} ({total_remote/total_jobs*100:.1f}%)</p>
                <p>🏢 <strong>Hybrid:</strong> {total_hybrid} ({total_hybrid/total_jobs*100:.1f}%)</p>
                <p>🏢 <strong>On-site:</strong> {total_onsite} ({total_onsite/total_jobs*100:.1f}%)</p>
            </div>
        </div>
        
        <table border="1" style="border-collapse: collapse; width: 100%;">
        <tr style="background-color: #e0e0e0;">
            <th>Company</th>
            <th>Token</th>
            <th>Total Jobs</th>
            <th>🏠 Remote</th>
            <th>🏢 Hybrid</th>
            <th>🏢 On-site</th>
            <th>Locations</th>
            <th>Last Seen</th>
        </tr>
        """ if total_jobs > 0 else f"""
        <html>
        <body style="font-family: Arial, sans-serif;">
        <h2>🔍 Greenhouse Tokens Report - {datetime.utcnow().strftime('%Y-%m-%d')}</h2>
        
        <div style="background-color: #f5f5f5; padding: 15px; border-radius: 5px; margin-bottom: 20px;">
            <h3>📊 Summary Statistics</h3>
            <p><strong>Total Companies:</strong> {len(tokens_data)}</p>
            <p><strong>Total Jobs:</strong> {total_jobs}</p>
        </div>
        
        <table border="1" style="border-collapse: collapse; width: 100%;">
        <tr style="background-color: #e0e0e0;">
            <th>Company</th>
            <th>Token</th>
            <th>Total Jobs</th>
            <th>🏠 Remote</th>
            <th>🏢 Hybrid</th>
            <th>🏢 On-site</th>
            <th>Locations</th>
            <th>Last Seen</th>
        </tr>
        """
        
        for row in tokens_data:
            remote_count = row.get('remote_jobs_count', 0) or 0
            hybrid_count = row.get('hybrid_jobs_count', 0) or 0
            onsite_count = row.get('onsite_jobs_count', 0) or 0
            
            html += f"""
            <tr>
                <td><strong>{row.get('company_name', 'Unknown')}</strong></td>
                <td><a href="https://boards.greenhouse.io/{row.get('token', '')}" target="_blank">{row.get('token', '')}</a></td>
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
            <p><em>Click on any token to view the company's live job board on Greenhouse.</em></p>
        </div>
        
        </body>
        </html>
        """
        return html            conn.execute("""
                CREATE TABLE IF NOT EXISTS crawl_history (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    url TEXT,
                    tokens_found INTEGER,
                    success BOOLEAN,
                    error_message TEXT,
                    crawl_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)"""
Enhanced Greenhouse Metadata Collector
--------------------------------------
A robust, secure, and compliant system for collecting Greenhouse job board tokens
with comprehensive error handling, security features, and data validation.

Features:
- Secure credential management
- Robust error handling and retry logic
- Rate limiting compliance
- Database connection management
- Comprehensive logging
- Data validation and sanitization
- robots.txt compliance checking
- Configurable settings
- Dry-run mode for testing

Requirements:
    pip install requests beautifulsoup4 python-dotenv
    
Environment Variables Required:
    SMTP_USER=your_email@example.com
    SMTP_PASS=your_app_password
    EMAIL_RECIPIENT=recipient@example.com
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
    """Configuration management class."""
    
    def __init__(self, config_file: str = "config.ini"):
        self.config = ConfigParser()
        self.config_file = config_file
        self._load_config()
    
    def _load_config(self):
        """Load configuration from file with sensible defaults."""
        # Create default config if it doesn't exist
        if not os.path.exists(self.config_file):
            self._create_default_config()
        
        self.config.read(self.config_file)
    
    def _create_default_config(self):
        """Create a default configuration file."""
        self.config['database'] = {
            'path': 'greenhouse_tokens.db',
            'backup_enabled': 'true',
            'max_job_titles': '100'
        }
        
        self.config['scraping'] = {
            'min_delay': '10',
            'max_delay': '30',
            'max_retries': '3',
            'timeout': '15',
            'user_agent': 'TokenCollectorBot/2.0 (Research Purpose; contact: researcher@example.com)',
            'respect_robots_txt': 'true'
        }
        
        self.config['email'] = {
            'smtp_server': 'smtp.gmail.com',
            'smtp_port': '587',
            'enabled': 'true'
        }
        
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
https://www.glassdoor.com/Job/startup-jobs-SRCH_KO0,7.htm
https://www.indeed.com/q-startup-jobs.html
https://stackoverflow.com/jobs/companies
https://news.ycombinator.com/jobs
https://www.linkedin.com/jobs/search/?keywords=startup
https://jobs.lever.co/
https://careers.google.com/
https://jobs.netflix.com/
https://www.microsoft.com/en-us/careers/
https://jobs.apple.com/
https://www.amazon.jobs/
https://careers.meta.com/
https://careers.salesforce.com/
https://jobs.uber.com/
https://www.airbnb.com/careers/
https://stripe.com/jobs
https://about.gitlab.com/jobs/
https://boards.greenhouse.io/
https://jobs.rubrik.com/
https://grnh.se/
https://apply.workable.com/'''
        }
        
        # Known Greenhouse tokens for initial seeding - comprehensive list
        self.config['seed_tokens'] = {
            'tokens': '''stripe,notion,figma,discord,dropbox,zoom,doordash,instacart,robinhood,coinbase,plaid,segment,openai,anthropic,huggingface,scale,cohere,runway,replicate,perplexity,
airbnb,reddit,gitlab,hashicorp,mongodb,elastic,cockroachlabs,mixpanel,sendgrid,twilio,okta,auth0,datadog,newrelic,pagerduty,
salesforce,snowflake,databricks,palantir,atlassian,asana,slack,zoom,calendly,notion,airtable,zapier,retool,webflow,
brex,mercury,ramp,checkr,chime,affirm,klarna,plaid,stripe,square,adyen,marqeta,
23andme,modernhealth,ro,capsulehealth,headspace,calm,teladoc,veracyte,10xgenomics,illumina,
canva,flexport,benchling,linear,vercel,netlify,github,sourcegraph,replit,cursor,anthropic,
uber,lyft,bird,lime,getaround,turo,waymo,cruise,aurora,
pinterest,snapchat,tiktok,discord,telegram,signal,whatsapp,zoom,teams,
netflix,spotify,hulu,disney,paramount,warner,peacock,crunchyroll,
peloton,mirror,strava,whoop,oura,fitbit,garmin,nike,adidas,
shopify,square,bigcommerce,wix,squarespace,etsy,amazon,ebay,
tesla,rivian,lucid,fisker,canoo,arrival,lordstown,nikola,
spacex,blueorigin,virgin,relativity,rocketlab,astra,firefly,
unity,epic,roblox,valve,ea,activision,ubisoft,take2,
cloudflare,fastly,akamai,amazon,microsoft,google,digitalocean,linode,vultr,
docker,kubernetes,redhat,suse,canonical,vmware,citrix,
intel,amd,nvidia,qualcomm,broadcom,marvell,micron,analog,
apple,samsung,tsmc,asml,lam,amat,klac,novellus,
meta,google,microsoft,amazon,apple,netflix,tesla,nvidia,
ycombinator,techstars,500startups,andreessen,sequoia,kleiner,benchmark,greylock,
accel,gv,nea,lightspeed,foundry,spark,mayfield,matrix,
initialized,firstround,unionSquare,bessemer,insight,general,
coinbase,kraken,binance,gemini,bitgo,chainalysis,circle,ripple,
opensea,foundation,superrare,async,nifty,makersplace,knownorigin,
shopify,magento,prestashop,opencart,woocommerce,bigcommerce,
workday,successfactors,adp,paycom,paylocity,bamboohr,namely,
zendesk,freshworks,intercom,drift,crisp,livechat,olark,
hubspot,marketo,pardot,eloqua,mailchimp,constant,aweber,
tableau,looker,sisense,qlik,microstrategy,powerbi,thoughtspot,
jenkins,travis,circleci,github,gitlab,bitbucket,azure,aws'''
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
    """Database connection and operations manager."""
    
    def __init__(self, db_path: str):
        self.db_path = db_path
        self._init_db()
    
    @contextmanager
    def get_connection(self):
        """Context manager for database connections."""
        conn = None
        try:
            conn = sqlite3.connect(self.db_path, timeout=30)
            conn.row_factory = sqlite3.Row  # Enable column access by name
            yield conn
        finally:
            if conn:
                conn.close()
    
    def _init_db(self):
        """Initialize database with required tables."""
        with self.get_connection() as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS greenhouse_tokens (
                    token TEXT PRIMARY KEY,
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
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            
            conn.execute("""
                CREATE TABLE IF NOT EXISTS job_details (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
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
                    FOREIGN KEY (token) REFERENCES greenhouse_tokens (token)
                )
            """)
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    url TEXT,
                    tokens_found INTEGER,
                    success BOOLEAN,
                    error_message TEXT,
                    crawl_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            
            # Create indexes for better performance
            conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_last_seen ON greenhouse_tokens(last_seen)
            """)
            
            conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_work_type ON job_details(work_type)
            """)
            
            conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_job_token ON job_details(token)
            """)
            
            # Add new columns to existing tables if they don't exist
            try:
                conn.execute("ALTER TABLE greenhouse_tokens ADD COLUMN remote_jobs_count INTEGER DEFAULT 0")
                conn.execute("ALTER TABLE greenhouse_tokens ADD COLUMN hybrid_jobs_count INTEGER DEFAULT 0") 
                conn.execute("ALTER TABLE greenhouse_tokens ADD COLUMN onsite_jobs_count INTEGER DEFAULT 0")
                conn.execute("ALTER TABLE greenhouse_tokens ADD COLUMN work_type_breakdown TEXT")
            except sqlite3.OperationalError:
                # Columns already exist
                pass
            
            conn.commit()
            logging.info("Database initialized successfully")
    
    def upsert_token(self, token: str, source: str, company_name: str, 
                    job_count: int, locations: List[str], departments: List[str], 
                    job_titles: List[str], work_type_counts: Dict[str, int] = None,
                    job_details: List[Dict] = None) -> bool:
        """Insert or update a token record with work type information."""
        try:
            now = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
            titles_json = json.dumps(job_titles[:100])  # Limit to 100 titles
            locs_str = ", ".join(locations[:50])  # Limit locations
            depts_str = ", ".join(departments[:20])  # Limit departments
            
            # Handle work type counts
            if work_type_counts is None:
                work_type_counts = {'remote': 0, 'hybrid': 0, 'onsite': 0}
            
            work_type_breakdown = json.dumps(work_type_counts)
            
            with self.get_connection() as conn:
                # Check if token exists
                cursor = conn.execute("SELECT token FROM greenhouse_tokens WHERE token=?", (token,))
                exists = cursor.fetchone() is not None
                
                if exists:
                    conn.execute("""
                        UPDATE greenhouse_tokens
                        SET source_url=?, company_name=?, job_count=?, locations=?, 
                            departments=?, job_titles=?, remote_jobs_count=?, hybrid_jobs_count=?,
                            onsite_jobs_count=?, work_type_breakdown=?, last_seen=?, updated_at=?
                        WHERE token=?
                    """, (source, company_name, job_count, locs_str, depts_str, 
                         titles_json, work_type_counts.get('remote', 0), 
                         work_type_counts.get('hybrid', 0), work_type_counts.get('onsite', 0),
                         work_type_breakdown, now, now, token))
                    logging.info(f"Updated token: {token}")
                else:
                    conn.execute("""
                        INSERT INTO greenhouse_tokens
                        (token, source_url, company_name, job_count, locations, 
                         departments, job_titles, remote_jobs_count, hybrid_jobs_count,
                         onsite_jobs_count, work_type_breakdown, first_seen, last_seen)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """, (token, source, company_name, job_count, locs_str, 
                         depts_str, titles_json, work_type_counts.get('remote', 0),
                         work_type_counts.get('hybrid', 0), work_type_counts.get('onsite', 0),
                         work_type_breakdown, now, now))
                    logging.info(f"Inserted new token: {token}")
                
                # Store detailed job information
                if job_details:
                    # Clear existing job details for this token
                    conn.execute("DELETE FROM job_details WHERE token=?", (token,))
                    
                    # Insert new job details
                    for job in job_details[:100]:  # Limit to 100 detailed jobs
                        conn.execute("""
                            INSERT INTO job_details 
                            (token, job_title, job_url, location, department, work_type, 
                             posted_date, job_description_snippet)
                            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                        """, (token, job.get('title', ''), job.get('url', ''), 
                             job.get('location', ''), job.get('department', ''),
                             job.get('work_type', ''), job.get('posted_date', ''),
                             job.get('description_snippet', '')))
                
                conn.commit()
                return True
        except Exception as e:
            logging.error(f"Database error for token {token}: {e}")
            return False
    
    def log_crawl(self, url: str, tokens_found: int, success: bool, error_message: str = None):
        """Log crawl attempt."""
        try:
            with self.get_connection() as conn:
                conn.execute("""
                    INSERT INTO crawl_history (url, tokens_found, success, error_message)
                    VALUES (?, ?, ?, ?)
                """, (url, tokens_found, success, error_message))
                conn.commit()
        except Exception as e:
            logging.error(f"Failed to log crawl history: {e}")
    
    def get_all_tokens(self) -> List[sqlite3.Row]:
        """Retrieve all tokens from database."""
        try:
            with self.get_connection() as conn:
                cursor = conn.execute("""
                    SELECT token, company_name, job_count, locations, departments, 
                           job_titles, remote_jobs_count, hybrid_jobs_count, onsite_jobs_count,
                           work_type_breakdown, first_seen, last_seen
                    FROM greenhouse_tokens 
                    ORDER BY company_name
                """)
                return cursor.fetchall()
        except Exception as e:
            logging.error(f"Failed to retrieve tokens: {e}")
            return []
    
    def get_jobs_by_work_type(self, work_type: str) -> List[sqlite3.Row]:
        """Get detailed job listings by work type."""
        try:
            with self.get_connection() as conn:
                cursor = conn.execute("""
                    SELECT jd.*, gt.company_name 
                    FROM job_details jd
                    JOIN greenhouse_tokens gt ON jd.token = gt.token
                    WHERE jd.work_type = ?
                    ORDER BY jd.last_seen DESC
                """, (work_type,))
                return cursor.fetchall()
        except Exception as e:
            logging.error(f"Failed to retrieve jobs by work type: {e}")
            return []


class RobotsChecker:
    """Check robots.txt compliance."""
    
    def __init__(self, user_agent: str):
        self.user_agent = user_agent
        self._cache = {}
    
    def can_fetch(self, url: str) -> bool:
        """Check if URL can be fetched according to robots.txt."""
        try:
            parsed = urlparse(url)
            base_url = f"{parsed.scheme}://{parsed.netloc}"
            
            if base_url in self._cache:
                rp = self._cache[base_url]
            else:
                rp = urllib.robotparser.RobotFileParser()
                robots_url = urljoin(base_url, '/robots.txt')
                rp.set_url(robots_url)
                rp.read()
                self._cache[base_url] = rp
            
            can_fetch = rp.can_fetch(self.user_agent, url)
            if not can_fetch:
                logging.warning(f"robots.txt disallows fetching: {url}")
            return can_fetch
        except Exception as e:
            logging.warning(f"Could not check robots.txt for {url}: {e}")
            return True  # If we can't check, assume it's okay


class TokenExtractor:
    """Extract and validate Greenhouse tokens from URLs."""
    
    @staticmethod
    def extract_token(url: str) -> Optional[str]:
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
            logging.debug(f"Could not extract token from {url}: {e}")
        return None
    
    @staticmethod
    def validate_token(token: str) -> bool:
        """Validate token format."""
        return (token and 
                len(token) >= 2 and 
                len(token) <= 100 and 
                token.replace('-', '').replace('_', '').isalnum())


class GreenhouseBoardParser:
    """Parse Greenhouse job board pages."""
    
    def __init__(self, user_agent: str, timeout: int = 15):
        self.user_agent = user_agent
        self.timeout = timeout
        self.session = requests.Session()
        self.session.headers.update({'User-Agent': user_agent})
    
    def parse_board(self, token: str, max_retries: int = 3) -> Tuple[Optional[str], int, List[str], List[str], List[str], Dict[str, int], List[Dict]]:
        """Fetch and parse metadata from Greenhouse board with retry logic."""
        url = f"https://boards.greenhouse.io/{token}"
        
        for attempt in range(max_retries):
            try:
                response = self.session.get(url, timeout=self.timeout)
                
                if response.status_code == 429:
                    wait_time = int(response.headers.get('Retry-After', 60))
                    logging.warning(f"Rate limited for {token}, waiting {wait_time} seconds...")
                    time.sleep(wait_time)
                    continue
                elif response.status_code == 404:
                    logging.info(f"Token {token} not found (404)")
                    return None, 0, [], [], [], {}, []
                elif response.status_code != 200:
                    logging.warning(f"HTTP {response.status_code} for {token}")
                    return None, 0, [], [], [], {}, []
                
                return self._parse_html(response.text, token)
                
            except requests.RequestException as e:
                logging.warning(f"Attempt {attempt + 1} failed for {token}: {e}")
                if attempt == max_retries - 1:
                    return None, 0, [], [], [], {}, []
                time.sleep(random.uniform(5, 15))
        
        return None, 0, [], [], [], {}, []
    
    def _parse_html(self, html: str, token: str) -> Tuple[str, int, List[str], List[str], List[str], Dict[str, int], List[Dict]]:
        """Parse HTML content to extract job information including work type."""
        soup = BeautifulSoup(html, "html.parser")
        
        # Extract company name
        company_name = self._extract_company_name(soup, token)
        
        # Try multiple strategies to find jobs
        jobs_data = self._extract_jobs_data(soup)
        
        job_titles = []
        locations = set()
        departments = set()
        work_type_counts = {'remote': 0, 'hybrid': 0, 'onsite': 0}
        detailed_jobs = []
        
        for job_data in jobs_data:
            if job_data.get('title'):
                job_titles.append(self._sanitize_text(job_data['title']))
                
                # Classify work type
                work_type = self._classify_work_type(job_data.get('location', ''), job_data.get('title', ''))
                work_type_counts[work_type] += 1
                
                # Create detailed job record
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
        """Extract company name from various sources."""
        # Try multiple selectors for company name
        selectors = [
            'h1',
            '.company-name',
            '[data-company-name]',
            'title'
        ]
        
        for selector in selectors:
            element = soup.select_one(selector)
            if element:
                text = element.get_text(strip=True)
                if text and len(text) > 2:
                    # Clean up common suffixes
                    text = text.replace(' Jobs', '').replace(' Careers', '').strip()
                    return self._sanitize_text(text)
        
        # Fallback to token
        return token.replace('-', ' ').replace('_', ' ').title()
    
    def _extract_jobs_data(self, soup: BeautifulSoup) -> List[Dict[str, str]]:
        """Extract job data using multiple strategies."""
        jobs_data = []
        
        # Strategy 1: Try to find structured JSON data
        json_scripts = soup.find_all('script', {'type': 'application/ld+json'})
        for script in json_scripts:
            try:
                data = json.loads(script.string)
                if isinstance(data, dict) and 'jobPosting' in str(data):
                    # Handle structured job posting data
                    pass  # Could be implemented for better data extraction
            except (json.JSONDecodeError, AttributeError):
                continue
        
        # Strategy 2: Multiple CSS selectors for job listings
        job_selectors = [
            '.opening',
            '[data-board-job]',
            '.job-listing',
            '.position',
            'a[href*="/jobs/"]',
            '.job-post',
            '.career-posting'
        ]
        
        for selector in job_selectors:
            elements = soup.select(selector)
            if elements:
                for element in elements:
                    job_data = self._extract_job_from_element(element)
                    if job_data:
                        jobs_data.append(job_data)
                break  # Use the first selector that finds results
        
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
                    job_data['url'] = f"https://boards.greenhouse.io{job_data['url']}"
        
        # Extract location
        location_selectors = ['.location', '[data-location]', '.job-location', '.office']
        for sel in location_selectors:
            loc_elem = element.find(class_=sel.replace('.', '')) or element.find_next(class_=sel.replace('.', ''))
            if loc_elem:
                job_data['location'] = loc_elem.get_text(strip=True)
                break
        
        # Extract department
        dept_elem = element.find_previous(['h2', 'h3']) or element.find(['h2', 'h3'])
        if dept_elem:
            dept_text = dept_elem.get_text(strip=True)
            if dept_text and len(dept_text) < 100:  # Reasonable department name length
                job_data['department'] = dept_text
        
        # Try to extract posting date
        date_selectors = ['.posted-date', '.date', '[data-posted]']
        for sel in date_selectors:
            date_elem = element.find(class_=sel.replace('.', '')) or element.find_next(class_=sel.replace('.', ''))
            if date_elem:
                job_data['posted_date'] = date_elem.get_text(strip=True)
                break
        
        # Try to extract job description snippet
        desc_elem = element.find_next(['p', '.description', '.summary'])
        if desc_elem:
            job_data['description'] = desc_elem.get_text(strip=True)
        
        return job_data if job_data.get('title') else None
    
    def _classify_work_type(self, location: str, title: str) -> str:
        """Classify job as remote, hybrid, or onsite based on location and title."""
        location_lower = location.lower()
        title_lower = title.lower()
        
        # Remote indicators
        remote_keywords = [
            'remote', 'anywhere', 'distributed', 'work from home', 'wfh',
            'telecommute', 'virtual', 'global', 'worldwide'
        ]
        
        # Hybrid indicators  
        hybrid_keywords = [
            'hybrid', 'flexible', 'partial remote', 'some remote',
            'remote friendly', 'remote optional'
        ]
        
        # Check location first
        for keyword in remote_keywords:
            if keyword in location_lower:
                return 'remote'
        
        for keyword in hybrid_keywords:
            if keyword in location_lower:
                return 'hybrid'
        
        # Check job title as fallback
        for keyword in remote_keywords:
            if keyword in title_lower:
                return 'remote'
                
        for keyword in hybrid_keywords:
            if keyword in title_lower:
                return 'hybrid'
        
        # If no remote/hybrid indicators and has specific location, assume onsite
        if location and location_lower not in ['', 'n/a', 'not specified']:
            return 'onsite'
        
        # Default to onsite if unclear
        return 'onsite'
    
    @staticmethod
    def _sanitize_text(text: str) -> str:
        """Clean and validate text data."""
        if not text:
            return ""
        return text.strip()[:200]  # Limit length


class EmailReporter:
    """Handle email reporting functionality."""
    
    def __init__(self, smtp_server: str, smtp_port: int, smtp_user: str, smtp_pass: str):
        self.smtp_server = smtp_server
        self.smtp_port = smtp_port
        self.smtp_user = smtp_user
        self.smtp_pass = smtp_pass
    
    def send_summary(self, tokens_data: List[sqlite3.Row], recipient: str) -> bool:
        """Send email summary of collected tokens."""
        try:
            if not tokens_data:
                body = "No tokens collected yet."
            else:
                body = self._format_summary(tokens_data)
            
            msg = MIMEMultipart('alternative')
            msg["Subject"] = f"Greenhouse Tokens Report - {datetime.utcnow().strftime('%Y-%m-%d')}"
            msg["From"] = self.smtp_user
            msg["To"] = recipient
            
            # Create both text and HTML versions
            text_part = MIMEText(body, 'plain')
            html_part = MIMEText(self._format_html_summary(tokens_data), 'html')
            
            msg.attach(text_part)
            msg.attach(html_part)
            
            with smtplib.SMTP(self.smtp_server, self.smtp_port) as server:
                server.starttls()
                server.login(self.smtp_user, self.smtp_pass)
                server.sendmail(self.smtp_user, recipient, msg.as_string())
            
            logging.info("Email summary sent successfully")
            return True
        except Exception as e:
            logging.error(f"Error sending email: {e}")
            return False
    
    def _format_summary(self, tokens_data: List[sqlite3.Row]) -> str:
        """Format text summary."""
        if not tokens_data:
            return "No tokens collected yet."
        
        # Calculate totals
        total_jobs = sum(row.get('job_count', 0) or 0 for row in tokens_data)
        total_remote = sum(row.get('remote_jobs_count', 0) or 0 for row in tokens_data)
        total_hybrid = sum(row.get('hybrid_jobs_count', 0) or 0 for row in tokens_data)
        total_onsite = sum(row.get('onsite_jobs_count', 0) or 0 for row in tokens_data)
        
        lines = [
            f"Greenhouse Token Collection Summary",
            f"=" * 50,
            f"Total Companies: {len(tokens_data)}",
            f"Total Jobs: {total_jobs}",
            f"  🏠 Remote: {total_remote} ({total_remote/total_jobs*100:.1f}%)" if total_jobs > 0 else "  🏠 Remote: 0",
            f"  🏢 Hybrid: {total_hybrid} ({total_hybrid/total_jobs*100:.1f}%)" if total_jobs > 0 else "  🏢 Hybrid: 0", 
            f"  🏢 On-site: {total_onsite} ({total_onsite/total_jobs*100:.1f}%)" if total_jobs > 0 else "  🏢 On-site: 0",
            "",
            "Company Details:",
            "=" * 50
        ]
        
        for row in tokens_data:
            try:
                job_titles_list = json.loads(row.get('job_titles', '[]')) if row.get('job_titles') else []
            except (json.JSONDecodeError, TypeError):
                job_titles_list = []
            
            titles_preview = ", ".join(job_titles_list[:3]) + ("..." if len(job_titles_list) > 3 else "")
            
            remote_count = row.get('remote_jobs_count', 0) or 0
            hybrid_count = row.get('hybrid_jobs_count', 0) or 0
            onsite_count = row.get('onsite_jobs_count', 0) or 0
            
            lines.append(
                f"{row.get('company_name', 'Unknown')} ({row.get('token', 'unknown')})\n"
                f"  Total Jobs: {row.get('job_count', 0)}\n"
                f"  Work Types: 🏠{remote_count} | 🏢{hybrid_count} | 🏢{onsite_count}\n"
                f"  Locations: {row.get('locations', 'Not specified')}\n"
                f"  Departments: {row.get('departments', 'Not specified')}\n"
                f"  Sample Titles: {titles_preview}\n"
                f"  Last seen: {row.get('last_seen', 'Unknown')}\n"
                + "-" * 50
            )
        
        return "\n".join(lines)
    
    def _format_html_summary(self, tokens_data: List[sqlite3.Row]) -> str:
        """Format HTML summary."""
        if not tokens_data:
            return "<p>No tokens collected yet.</p>"
        
        html = f"""
        <html>
        <body>
        <h2>Greenhouse Tokens Report - {datetime.utcnow().strftime('%Y-%m-%d')}</h2>
        <p><strong>Total Companies:</strong> {len(tokens_data)}</p>
        <table border="1" style="border-collapse: collapse; width: 100%;">
        <tr>
            <th>Company</th>
            <th>Token</th>
            <th>Jobs</th>
            <th>Locations</th>
            <th>Last Seen</th>
        </tr>
        """
        
        for row in tokens_data:
            html += f"""
            <tr>
                <td>{row['company_name']}</td>
                <td><a href="https://boards.greenhouse.io/{row['token']}">{row['token']}</a></td>
                <td>{row['job_count']}</td>
                <td>{row['locations'] or 'Not specified'}</td>
                <td>{row['last_seen']}</td>
            </tr>
            """
        
        html += "</table></body></html>"
        return html


class GreenhouseCollector:
    """Main collector class orchestrating the entire process."""
    
    def __init__(self, config_file: str = "config.ini", dry_run: bool = False):
        self.dry_run = dry_run
        self.config = Config(config_file)
        self._setup_logging()
        self._validate_environment()
        
        # Initialize components
        self.db_manager = DatabaseManager(self.config.get('database', 'path', 'greenhouse_tokens.db'))
        self.robots_checker = RobotsChecker(self.config.get('scraping', 'user_agent'))
        self.board_parser = GreenhouseBoardParser(
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
                logging.FileHandler('greenhouse_collector.log'),
                logging.StreamHandler()
            ]
        )
        
        # Reduce noise from requests
        logging.getLogger('urllib3').setLevel(logging.WARNING)
        logging.getLogger('requests').setLevel(logging.WARNING)
    
    def _validate_environment(self):
        """Validate required environment variables and configuration."""
        required_env_vars = []
        
        if self.config.getboolean('email', 'enabled', True):
            required_env_vars.extend(['SMTP_USER', 'SMTP_PASS', 'EMAIL_RECIPIENT'])
        
        missing_vars = [var for var in required_env_vars if not os.getenv(var)]
        if missing_vars:
            logging.error(f"Missing required environment variables: {missing_vars}")
            raise ValueError(f"Missing required environment variables: {missing_vars}")
    
    def crawl_page(self, url: str) -> int:
        """Crawl a single page for Greenhouse tokens."""
        if self.config.getboolean('scraping', 'respect_robots_txt', True):
            if not self.robots_checker.can_fetch(url):
                logging.warning(f"Skipping {url} due to robots.txt restrictions")
                return 0
        
        tokens_found = 0
        error_message = None
        
        try:
            logging.info(f"Crawling: {url}")
            
            headers = {"User-Agent": self.config.get('scraping', 'user_agent')}
            response = requests.get(url, headers=headers, 
                                 timeout=self.config.getint('scraping', 'timeout', 15))
            
            if response.status_code != 200:
                error_message = f"HTTP {response.status_code}"
                logging.warning(f"Failed to fetch {url}: {error_message}")
                return 0
            
            soup = BeautifulSoup(response.text, "html.parser")
            
            # Find all links that might contain Greenhouse tokens
            for link in soup.find_all("a", href=True):
                token = TokenExtractor.extract_token(link["href"])
                if token:
                    if self.dry_run:
                        logging.info(f"[DRY RUN] Would process token: {token}")
                        tokens_found += 1
                        continue
                    
                    # Parse the board
                    company_name, job_count, locations, departments, job_titles, work_type_counts, job_details = \
                        self.board_parser.parse_board(token, self.config.getint('scraping', 'max_retries', 3))
                    
                    if company_name:
                        success = self.db_manager.upsert_token(
                            token, url, company_name, job_count, 
                            locations, departments, job_titles, work_type_counts, job_details
                        )
                        if success:
                            tokens_found += 1
                            logging.info(f"Processed {token}: {company_name} ({job_count} jobs - "
                                       f"🏠{work_type_counts.get('remote', 0)} 🏢{work_type_counts.get('hybrid', 0)} "
                                       f"🏢{work_type_counts.get('onsite', 0)})")
                    
                    # Respect rate limits
                    delay = random.uniform(
                        self.config.getint('scraping', 'min_delay', 10),
                        self.config.getint('scraping', 'max_delay', 30)
                    )
                    time.sleep(delay)
        
        except Exception as e:
            error_message = str(e)
            logging.error(f"Error crawling {url}: {e}")
        
        finally:
            # Log the crawl attempt
            if not self.dry_run:
                self.db_manager.log_crawl(url, tokens_found, error_message is None, error_message)
        
        return tokens_found
    
    def process_seed_tokens(self) -> int:
        """Process known Greenhouse tokens directly."""
        seed_tokens = [token.strip() for token in 
                      self.config.get('seed_tokens', 'tokens', '').split(',') if token.strip()]
        
        if not seed_tokens:
            logging.info("No seed tokens configured")
            return 0
        
        logging.info(f"Processing {len(seed_tokens)} seed tokens")
        total_processed = 0
        
        for token in seed_tokens:
            if not TokenExtractor.validate_token(token):
                logging.warning(f"Invalid token format: {token}")
                continue
                
            if self.dry_run:
                logging.info(f"[DRY RUN] Would process seed token: {token}")
                total_processed += 1
                continue
            
            try:
                company_name, job_count, locations, departments, job_titles, work_type_counts, job_details = \
                    self.board_parser.parse_board(token, self.config.getint('scraping', 'max_retries', 3))
                
                if company_name:
                    success = self.db_manager.upsert_token(
                        token, "seed_token", company_name, job_count, 
                        locations, departments, job_titles, work_type_counts, job_details
                    )
                    if success:
                        total_processed += 1
                        logging.info(f"Processed seed token {token}: {company_name} ({job_count} jobs - "
                                   f"🏠{work_type_counts.get('remote', 0)} 🏢{work_type_counts.get('hybrid', 0)} "
                                   f"🏢{work_type_counts.get('onsite', 0)})")
                else:
                    logging.warning(f"Failed to parse seed token: {token}")
                
                # Respect rate limits
                delay = random.uniform(
                    self.config.getint('scraping', 'min_delay', 10),
                    self.config.getint('scraping', 'max_delay', 30)
                )
                time.sleep(delay)
                
            except Exception as e:
                logging.error(f"Error processing seed token {token}: {e}")
        
        return total_processed

    def run(self) -> bool:
        """Run the complete collection process."""
        start_time = datetime.utcnow()
        logging.info(f"Starting Greenhouse token collection {'(DRY RUN)' if self.dry_run else ''}")
        
        total_tokens = 0
        
        # First, process known seed tokens
        seed_tokens_processed = self.process_seed_tokens()
        total_tokens += seed_tokens_processed
        logging.info(f"Processed {seed_tokens_processed} seed tokens")
        
        # Then crawl seed URLs for discovery
        seed_urls = [url.strip() for url in 
                    self.config.get('seed_urls', 'urls', '').split('\n') if url.strip()]
        
        if seed_urls:
            logging.info(f"Crawling {len(seed_urls)} seed URLs for token discovery")
            for url in seed_urls:
                tokens_found = self.crawl_page(url)
                total_tokens += tokens_found
                
                # Add delay between pages
                delay = random.uniform(
                    self.config.getint('scraping', 'min_delay', 10),
                    self.config.getint('scraping', 'max_delay', 30)
                )
                time.sleep(delay)
        else:
            logging.info("No seed URLs configured, skipping URL crawling")
        
        # Send email summary if enabled and not in dry run mode
        if (self.email_reporter and 
            not self.dry_run and 
            self.config.getboolean('email', 'enabled', True)):
            
            recipient = os.getenv('EMAIL_RECIPIENT')
            if recipient:
                tokens_data = self.db_manager.get_all_tokens()
                self.email_reporter.send_summary(tokens_data, recipient)
        
        end_time = datetime.utcnow()
        duration = (end_time - start_time).total_seconds()
        
        logging.info(f"Collection completed in {duration:.2f} seconds. "
                    f"Total tokens processed: {total_tokens}")
        
        return True


def main():
    """Main entry point."""
    import argparse
    
    parser = argparse.ArgumentParser(description='Greenhouse Token Collector')
    parser.add_argument('--config', default='config.ini', help='Configuration file path')
    parser.add_argument('--dry-run', action='store_true', help='Run without making changes')
    parser.add_argument('--verbose', '-v', action='store_true', help='Verbose logging')
    
    args = parser.parse_args()
    
    try:
        collector = GreenhouseCollector(args.config, args.dry_run)
        success = collector.run()
        return 0 if success else 1
    except Exception as e:
        logging.error(f"Fatal error: {e}")
        return 1


if __name__ == "__main__":
    exit(main())
