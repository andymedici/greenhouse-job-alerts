"""
Enhanced Greenhouse Metadata Collector
--------------------------------------
A robust system for collecting Greenhouse job board tokens with work type classification.
"""

import requests
import sqlite3
import time
import random
import smtplib
import json
import logging
import os
from contextlib import contextmanager
from datetime import datetime
from configparser import ConfigParser
from typing import Tuple, List, Optional, Dict, Any
from urllib.parse import urlparse
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from bs4 import BeautifulSoup

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass


class Config:
    def __init__(self, config_file: str = "config.ini"):
        self.config = ConfigParser()
        self.config_file = config_file
        self._load_config()
    
    def _load_config(self):
        if not os.path.exists(self.config_file):
            self._create_default_config()
        self.config.read(self.config_file)
    
    def _create_default_config(self):
        self.config['database'] = {
            'path': os.getenv('DB_PATH', 'greenhouse_tokens.db'),
        }
        
        self.config['scraping'] = {
            'min_delay': '5',
            'max_delay': '15',
            'max_retries': '3',
            'timeout': '15',
            'user_agent': 'TokenCollectorBot/2.0 (Research; contact: researcher@example.com)',
        }
        
        self.config['email'] = {
            'smtp_server': 'smtp.gmail.com',
            'smtp_port': '587',
            'enabled': 'true'
        }
        
        # Seed tokens - major companies known to use Greenhouse
        self.config['seed_tokens'] = {
            'tokens': 'stripe,notion,figma,discord,dropbox,zoom,doordash,instacart,robinhood,coinbase,plaid,openai,anthropic,airbnb,reddit,gitlab,hashicorp,mongodb,elastic,salesforce,snowflake,databricks,atlassian,asana,slack,okta,twilio,brex,mercury,ramp,checkr,chime,affirm,canva,flexport,benchling,retool,vercel,linear,23andme,shopify,tesla,netflix,spotify,unity,cloudflare,docker,intel,nvidia,apple,meta,google,microsoft'
        }
        
        with open(self.config_file, 'w') as f:
            self.config.write(f)
    
    def get(self, section: str, key: str, fallback: Any = None) -> str:
        return self.config.get(section, key, fallback=fallback)
    
    def getint(self, section: str, key: str, fallback: int = 0) -> int:
        return self.config.getint(section, key, fallback=fallback)
    
    def getboolean(self, section: str, key: str, fallback: bool = False) -> bool:
        return self.config.getboolean(section, key, fallback=fallback)


class DatabaseManager:
    def __init__(self, db_path: str):
        self.db_path = db_path
        self._init_db()
    
    @contextmanager
    def get_connection(self):
        conn = None
        try:
            conn = sqlite3.connect(self.db_path, timeout=30)
            conn.row_factory = sqlite3.Row
            yield conn
        finally:
            if conn:
                conn.close()
    
    def _init_db(self):
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
                    first_seen TIMESTAMP,
                    last_seen TIMESTAMP
                )
            """)
            
            try:
                conn.execute("ALTER TABLE greenhouse_tokens ADD COLUMN remote_jobs_count INTEGER DEFAULT 0")
                conn.execute("ALTER TABLE greenhouse_tokens ADD COLUMN hybrid_jobs_count INTEGER DEFAULT 0") 
                conn.execute("ALTER TABLE greenhouse_tokens ADD COLUMN onsite_jobs_count INTEGER DEFAULT 0")
            except sqlite3.OperationalError:
                pass
            
            conn.commit()
    
    def upsert_token(self, token: str, source: str, company_name: str, 
                    job_count: int, locations: List[str], departments: List[str], 
                    job_titles: List[str], work_type_counts: Dict[str, int] = None) -> bool:
        try:
            now = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
            titles_json = json.dumps(job_titles[:50])
            locs_str = ", ".join(locations[:20])
            depts_str = ", ".join(departments[:10])
            
            if work_type_counts is None:
                work_type_counts = {'remote': 0, 'hybrid': 0, 'onsite': 0}
            
            with self.get_connection() as conn:
                cursor = conn.execute("SELECT token FROM greenhouse_tokens WHERE token=?", (token,))
                exists = cursor.fetchone() is not None
                
                if exists:
                    conn.execute("""
                        UPDATE greenhouse_tokens
                        SET source_url=?, company_name=?, job_count=?, locations=?, 
                            departments=?, job_titles=?, remote_jobs_count=?, hybrid_jobs_count=?,
                            onsite_jobs_count=?, last_seen=?
                        WHERE token=?
                    """, (source, company_name, job_count, locs_str, depts_str, 
                         titles_json, work_type_counts.get('remote', 0), 
                         work_type_counts.get('hybrid', 0), work_type_counts.get('onsite', 0),
                         now, token))
                else:
                    conn.execute("""
                        INSERT INTO greenhouse_tokens
                        (token, source_url, company_name, job_count, locations, 
                         departments, job_titles, remote_jobs_count, hybrid_jobs_count,
                         onsite_jobs_count, first_seen, last_seen)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """, (token, source, company_name, job_count, locs_str, 
                         depts_str, titles_json, work_type_counts.get('remote', 0),
                         work_type_counts.get('hybrid', 0), work_type_counts.get('onsite', 0),
                         now, now))
                
                conn.commit()
                return True
        except Exception as e:
            logging.error(f"Database error for token {token}: {e}")
            return False
    
    def get_all_tokens(self) -> List[sqlite3.Row]:
        try:
            with self.get_connection() as conn:
                cursor = conn.execute("""
                    SELECT token, company_name, job_count, locations, departments, 
                           job_titles, remote_jobs_count, hybrid_jobs_count, onsite_jobs_count,
                           first_seen, last_seen
                    FROM greenhouse_tokens 
                    ORDER BY company_name
                """)
                return cursor.fetchall()
        except Exception as e:
            logging.error(f"Failed to retrieve tokens: {e}")
            return []


class TokenExtractor:
    @staticmethod
    def extract_token(url: str) -> Optional[str]:
        try:
            parsed = urlparse(url)
            if parsed.netloc == "boards.greenhouse.io":
                parts = parsed.path.strip("/").split("/")
                if parts and parts[0]:
                    token = parts[0]
                    if TokenExtractor.validate_token(token):
                        return token
        except Exception:
            pass
        return None
    
    @staticmethod
    def validate_token(token: str) -> bool:
        return (token and 
                len(token) >= 2 and 
                len(token) <= 100 and 
                token.replace('-', '').replace('_', '').isalnum())


class GreenhouseBoardParser:
    def __init__(self, user_agent: str, timeout: int = 15):
        self.user_agent = user_agent
        self.timeout = timeout
        self.session = requests.Session()
        self.session.headers.update({'User-Agent': user_agent})
    
    def parse_board(self, token: str, max_retries: int = 3) -> Tuple[Optional[str], int, List[str], List[str], List[str], Dict[str, int]]:
        url = f"https://boards.greenhouse.io/{token}"
        
        for attempt in range(max_retries):
            try:
                response = self.session.get(url, timeout=self.timeout)
                
                if response.status_code == 429:
                    wait_time = int(response.headers.get('Retry-After', 60))
                    time.sleep(wait_time)
                    continue
                elif response.status_code == 404:
                    return None, 0, [], [], [], {}
                elif response.status_code != 200:
                    return None, 0, [], [], [], {}
                
                return self._parse_html(response.text, token)
                
            except requests.RequestException as e:
                if attempt == max_retries - 1:
                    return None, 0, [], [], [], {}
                time.sleep(random.uniform(5, 15))
        
        return None, 0, [], [], [], {}
    
    def _parse_html(self, html: str, token: str) -> Tuple[str, int, List[str], List[str], List[str], Dict[str, int]]:
        soup = BeautifulSoup(html, "html.parser")
        
        # Extract company name
        company_name = self._extract_company_name(soup, token)
        
        # Find job listings
        jobs_data = self._extract_jobs_data(soup)
        
        job_titles = []
        locations = set()
        departments = set()
        work_type_counts = {'remote': 0, 'hybrid': 0, 'onsite': 0}
        
        for job_data in jobs_data:
            if job_data.get('title'):
                job_titles.append(job_data['title'][:100])
                
                # Classify work type
                work_type = self._classify_work_type(job_data.get('location', ''), job_data.get('title', ''))
                work_type_counts[work_type] += 1
            
            if job_data.get('location'):
                locations.add(job_data['location'][:50])
            if job_data.get('department'):
                departments.add(job_data['department'][:50])
        
        return (company_name, len(job_titles), 
                sorted(list(locations)), sorted(list(departments)), job_titles,
                work_type_counts)
    
    def _extract_company_name(self, soup: BeautifulSoup, token: str) -> str:
        selectors = ['h1', '.company-name', 'title']
        
        for selector in selectors:
            element = soup.select_one(selector)
            if element:
                text = element.get_text(strip=True)
                if text and len(text) > 2:
                    text = text.replace(' Jobs', '').replace(' Careers', '').strip()
                    return text[:100]
        
        return token.replace('-', ' ').replace('_', ' ').title()
    
    def _extract_jobs_data(self, soup: BeautifulSoup) -> List[Dict[str, str]]:
        jobs_data = []
        
        job_selectors = ['.opening', '[data-board-job]', '.job-listing', 'a[href*="/jobs/"]']
        
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
        job_data = {}
        
        title_element = element if element.name in ['a', 'h1', 'h2', 'h3'] else element.find(['a', 'h1', 'h2', 'h3'])
        if title_element:
            job_data['title'] = title_element.get_text(strip=True)
        
        location_selectors = ['.location', '[data-location]', '.job-location']
        for sel in location_selectors:
            loc_elem = element.find(class_=sel.replace('.', '')) or element.find_next(class_=sel.replace('.', ''))
            if loc_elem:
                job_data['location'] = loc_elem.get_text(strip=True)
                break
        
        dept_elem = element.find_previous(['h2', 'h3']) or element.find(['h2', 'h3'])
        if dept_elem:
            dept_text = dept_elem.get_text(strip=True)
            if dept_text and len(dept_text) < 100:
                job_data['department'] = dept_text
        
        return job_data if job_data.get('title') else None
    
    def _classify_work_type(self, location: str, title: str) -> str:
        location_lower = location.lower()
        title_lower = title.lower()
        
        remote_keywords = ['remote', 'anywhere', 'distributed', 'work from home', 'wfh', 'telecommute']
        hybrid_keywords = ['hybrid', 'flexible', 'partial remote', 'remote friendly']
        
        for keyword in remote_keywords:
            if keyword in location_lower or keyword in title_lower:
                return 'remote'
        
        for keyword in hybrid_keywords:
            if keyword in location_lower or keyword in title_lower:
                return 'hybrid'
        
        return 'onsite'


class EmailReporter:
    def __init__(self, smtp_server: str, smtp_port: int, smtp_user: str, smtp_pass: str):
        self.smtp_server = smtp_server
        self.smtp_port = smtp_port
        self.smtp_user = smtp_user
        self.smtp_pass = smtp_pass
    
    def send_summary(self, tokens_data: List[sqlite3.Row], recipient: str) -> bool:
        try:
            if not tokens_data:
                body = "No tokens collected yet."
            else:
                body = self._format_summary(tokens_data)
            
            msg = MIMEText(body)
            msg["Subject"] = f"Greenhouse Tokens Report - {datetime.utcnow().strftime('%Y-%m-%d')}"
            msg["From"] = self.smtp_user
            msg["To"] = recipient
            
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
        if not tokens_data:
            return "No tokens collected yet."
        
        total_jobs = sum(row.get('job_count', 0) or 0 for row in tokens_data)
        total_remote = sum(row.get('remote_jobs_count', 0) or 0 for row in tokens_data)
        total_hybrid = sum(row.get('hybrid_jobs_count', 0) or 0 for row in tokens_data)
        total_onsite = sum(row.get('onsite_jobs_count', 0) or 0 for row in tokens_data)
        
        lines = [
            f"Greenhouse Token Collection Summary",
            f"=" * 50,
            f"Total Companies: {len(tokens_data)}",
            f"Total Jobs: {total_jobs}",
            f"  Remote: {total_remote} ({total_remote/total_jobs*100:.1f}%)" if total_jobs > 0 else "  Remote: 0",
            f"  Hybrid: {total_hybrid} ({total_hybrid/total_jobs*100:.1f}%)" if total_jobs > 0 else "  Hybrid: 0", 
            f"  On-site: {total_onsite} ({total_onsite/total_jobs*100:.1f}%)" if total_jobs > 0 else "  On-site: 0",
            "",
            "Company Details:",
            "=" * 50
        ]
        
        for row in tokens_data:
            try:
                job_titles_list = json.loads(row.get('job_titles', '[]')) if row.get('job_titles') else []
            except:
                job_titles_list = []
            
            titles_preview = ", ".join(job_titles_list[:3]) + ("..." if len(job_titles_list) > 3 else "")
            
            remote_count = row.get('remote_jobs_count', 0) or 0
            hybrid_count = row.get('hybrid_jobs_count', 0) or 0
            onsite_count = row.get('onsite_jobs_count', 0) or 0
            
            lines.append(
                f"{row.get('company_name', 'Unknown')} ({row.get('token', 'unknown')})\n"
                f"  Total Jobs: {row.get('job_count', 0)}\n"
                f"  Work Types: Remote:{remote_count} | Hybrid:{hybrid_count} | On-site:{onsite_count}\n"
                f"  Locations: {row.get('locations', 'Not specified')}\n"
                f"  Sample Titles: {titles_preview}\n"
                f"  Last seen: {row.get('last_seen', 'Unknown')}\n"
                + "-" * 50
            )
        
        return "\n".join(lines)


class GreenhouseCollector:
    def __init__(self, config_file: str = "config.ini", dry_run: bool = False):
        self.dry_run = dry_run
        self.config = Config(config_file)
        self._setup_logging()
        
        # Initialize components
        db_path = os.getenv('DB_PATH', 'greenhouse_tokens.db')
        self.db_manager = DatabaseManager(db_path)
        self.board_parser = GreenhouseBoardParser(
            self.config.get('scraping', 'user_agent'),
            self.config.getint('scraping', 'timeout', 15)
        )
        
        # Setup email if enabled
        if self.config.getboolean('email', 'enabled', True) and os.getenv('SMTP_USER'):
            self.email_reporter = EmailReporter(
                self.config.get('email', 'smtp_server', 'smtp.gmail.com'),
                self.config.getint('email', 'smtp_port', 587),
                os.getenv('SMTP_USER'),
                os.getenv('SMTP_PASS')
            )
        else:
            self.email_reporter = None
    
    def _setup_logging(self):
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[logging.StreamHandler()]
        )
    
    def process_seed_tokens(self) -> int:
        seed_tokens = [token.strip() for token in 
                      self.config.get('seed_tokens', 'tokens', '').split(',') if token.strip()]
        
        if not seed_tokens:
            logging.info("No seed tokens configured")
            return 0
        
        logging.info(f"Processing {len(seed_tokens)} seed tokens")
        total_processed = 0
        
        for token in seed_tokens:
            if not TokenExtractor.validate_token(token):
                continue
                
            if self.dry_run:
                logging.info(f"[DRY RUN] Would process seed token: {token}")
                total_processed += 1
                continue
            
            try:
                company_name, job_count, locations, departments, job_titles, work_type_counts = \
                    self.board_parser.parse_board(token, self.config.getint('scraping', 'max_retries', 3))
                
                if company_name:
                    success = self.db_manager.upsert_token(
                        token, "seed_token", company_name, job_count, 
                        locations, departments, job_titles, work_type_counts
                    )
                    if success:
                        total_processed += 1
                        logging.info(f"✅ Processed {token}: {company_name} ({job_count} jobs - "
                                   f"Remote:{work_type_counts.get('remote', 0)} "
                                   f"Hybrid:{work_type_counts.get('hybrid', 0)} "
                                   f"On-site:{work_type_counts.get('onsite', 0)})")
                else:
                    logging.warning(f"Failed to parse seed token: {token}")
                
                # Rate limiting
                delay = random.uniform(
                    self.config.getint('scraping', 'min_delay', 5),
                    self.config.getint('scraping', 'max_delay', 15)
                )
                time.sleep(delay)
                
            except Exception as e:
                logging.error(f"Error processing seed token {token}: {e}")
        
        return total_processed
    
    def run(self) -> bool:
        start_time = datetime.utcnow()
        logging.info(f"🚀 Starting Greenhouse token collection {'(DRY RUN)' if self.dry_run else ''}")
        
        total_tokens = 0
        
        # Process seed tokens
        seed_tokens_processed = self.process_seed_tokens()
        total_tokens += seed_tokens_processed
        logging.info(f"Processed {seed_tokens_processed} seed tokens")
        
        # Send email summary if enabled
        if (self.email_reporter and 
            not self.dry_run and 
            self.config.getboolean('email', 'enabled', True)):
            
            recipient = os.getenv('EMAIL_RECIPIENT')
            if recipient:
                tokens_data = self.db_manager.get_all_tokens()
                self.email_reporter.send_summary(tokens_data, recipient)
        
        end_time = datetime.utcnow()
        duration = (end_time - start_time).total_seconds()
        
        logging.info(f"✅ Collection completed in {duration:.2f} seconds. "
                    f"Total tokens processed: {total_tokens}")
        
        return True


def main():
    import argparse
    
    parser = argparse.ArgumentParser(description='Greenhouse Token Collector')
    parser.add_argument('--dry-run', action='store_true', help='Run without making changes')
    
    args = parser.parse_args()
    
    try:
        collector = GreenhouseCollector(dry_run=args.dry_run)
        success = collector.run()
        return 0 if success else 1
    except Exception as e:
        logging.error(f"Fatal error: {e}")
        return 1


if __name__ == "__main__":
    exit(main())
