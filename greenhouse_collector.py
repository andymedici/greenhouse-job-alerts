"""
Enhanced Greenhouse Metadata Collector with Historical Tracking
--------------------------------------------------------------
A comprehensive system for collecting Greenhouse job board tokens with:
- Monthly historical job count tracking
- Work type classification (remote/hybrid/onsite)
- Extensive token discovery from 150+ seed URLs
- Market intelligence and trend analysis
- Automated email reporting with historical insights

Features:
- 53 seed tokens for immediate data
- 150+ discovery URLs for finding new companies
- Monthly snapshots for trend analysis
- Smart change detection and historical tracking
- Email reports with month-over-month comparisons
- Market intelligence analytics
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
from datetime import datetime, timedelta
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
            'user_agent': 'JobMarketBot/2.0 (Research; contact: researcher@example.com)',
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
        
        # Comprehensive seed URLs for discovering new tokens
        self.config['seed_urls'] = {
            'urls': '''https://github.com/poteto/hiring-without-whiteboards
https://github.com/donnemartin/awesome-interview-questions
https://github.com/jwasham/coding-interview-university
https://github.com/remoteintech/remote-jobs
https://github.com/lukasz-madon/awesome-remote-job
https://github.com/yanirs/established-remote
https://github.com/hng/tech-companies-in-singapore
https://github.com/tramcar/awesome-job-list
https://github.com/blakeembrey/remote-jobs
https://github.com/jessicard/remote-jobs
https://themuse.com/companies
https://builtin.com/companies
https://builtin.com/jobs
https://www.keyvalues.com/companies
https://angel.co/jobs
https://angel.co/companies
https://www.wellfound.com/jobs
https://www.wellfound.com/companies
https://worklist.fyi/
https://www.ycombinator.com/companies
https://www.ycombinator.com/topcompanies
https://www.crunchbase.com/discover/organization.companies
https://techcrunch.com/startups/
https://www.producthunt.com/topics/hiring-and-recruiting
https://jobs.ashbyhq.com/
https://remote.co/remote-jobs/
https://remote.co/remote-companies/
https://remoteok.io/
https://remoteok.io/remote-companies
https://weworkremotely.com/
https://weworkremotely.com/companies
https://flexjobs.com/
https://justremote.co/
https://justremote.co/remote-companies
https://remotehub.io/
https://remotehub.io/companies
https://whoishiring.io/
https://whoishiring.io/search/
https://startup.jobs/
https://startup.jobs/companies
https://unicornhunt.io/
https://unicornhunt.io/companies
https://www.glassdoor.com/Job/startup-jobs-SRCH_KO0,7.htm
https://www.indeed.com/q-startup-jobs.html
https://stackoverflow.com/jobs/companies
https://stackoverflow.blog/
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
https://apply.workable.com/
https://jobs.smartrecruiters.com/
https://www.comparably.com/companies
https://www.builtinnyc.com/companies
https://www.builtinla.com/companies
https://www.builtinaustin.com/companies
https://www.builtinchicago.com/companies
https://www.builtinboston.com/companies
https://www.builtinseattle.com/companies
https://www.builtincolorado.com/companies
https://www.failory.com/startups
https://www.owler.com/
https://craft.co/startups
https://techstars.com/portfolio
https://www.500.co/
https://a16z.com/portfolio/
https://www.sequoiacap.com/companies/
https://www.kleinerperkins.com/companies
https://www.benchmark.com/companies/
https://greylock.com/portfolio/
https://accel.com/companies/
https://www.gv.com/portfolio/
https://www.nea.com/portfolio
https://lightspeed.com/portfolio/
https://foundrygroup.com/portfolio/
https://www.sparkcapital.com/portfolio
https://www.mayfield.com/portfolio/
https://matrix.com/portfolio/
https://initialized.com/portfolio/
https://www.firstround.com/portfolio/
https://www.usv.com/portfolio/
https://www.bessemer.com/portfolio
https://www.insightpartners.com/portfolio/
https://www.generalcatalyst.com/portfolio/
https://www.andreessen.org/portfolio/
https://www.cbinsights.com/research-unicorn-companies
https://pitchbook.com/
https://www.startupranking.com/
https://angel.co/job-collections/remote
https://angel.co/job-collections/startup-jobs
https://angel.co/job-collections/engineering
https://angel.co/job-collections/product
https://angel.co/job-collections/marketing
https://angel.co/job-collections/sales
https://angel.co/job-collections/design
https://angel.co/job-collections/data-science
https://techcrunch.com/category/startups/
https://venturebeat.com/category/entrepreneur/
https://www.entrepreneur.com/topic/startups
https://techstartups.com/
https://startupsavant.com/
https://www.rocketship.fm/companies
https://www.f6s.com/companies
https://www.startupcrawler.com/
https://www.startupstash.com/
https://www.producthunt.com/collections/remote-work-tools
https://nomadlist.com/remote-work-tools
https://remote.tools/
https://www.toptal.com/companies
https://triplebyte.com/
https://hired.com/
https://vettery.com/
https://www.otta.com/companies
https://jobs.github.com/companies
https://careers.stackoverflow.com/companies
https://hired.com/companies
https://www.levels.fyi/companies/
https://www.teamblind.com/companies
https://candor.co/companies
https://www.fishbowlapp.com/companies
https://www.comparably.com/
https://www.glassdoor.com/Reviews/
https://www.indeed.com/companies
https://www.ziprecruiter.com/Companies
https://www.dice.com/jobs
https://cyberseek.org/heatmap.html
https://stackoverflow.com/tags
https://github.com/trending
https://github.com/topics/startup
https://github.com/topics/jobs
https://github.com/topics/careers
https://github.com/collections/choosing-projects
https://dev.to/t/career
https://www.reddit.com/r/startups/
https://www.reddit.com/r/cscareerquestions/
https://www.reddit.com/r/jobs/
https://www.reddit.com/r/remotework/
https://www.reddit.com/r/digitalnomad/
https://www.indiehackers.com/
https://www.producthunt.com/
https://betalist.com/
https://www.crunchbase.com/hub/startup-companies
https://www.eu-startups.com/directory/
https://www.startups.com/
https://www.startupblink.com/
https://www.startupgenome.com/
https://dealroom.co/
https://www.tracxn.com/
https://www.wellfound.com/startup-jobs
https://jobs.techstars.com/
https://jobs.ycombinator.com/
https://500.co/jobs/
https://techcrunch.com/events/
https://disrupt.techcrunch.com/
https://saastr.com/
https://firstround.com/review/
https://a16z.com/
https://news.ycombinator.com/
https://www.techmeme.com/
https://techcrunch.com/
https://venturebeat.com/
https://www.theverge.com/
https://arstechnica.com/
https://www.wired.com/
https://techreport.com/
https://www.engadget.com/
https://gizmodo.com/
https://www.fastcompany.com/
https://www.inc.com/'''
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
            # Main tokens table
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
            
            # Monthly historical data
            conn.execute("""
                CREATE TABLE IF NOT EXISTS monthly_job_history (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    token TEXT,
                    year INTEGER,
                    month INTEGER,
                    job_count INTEGER,
                    remote_count INTEGER,
                    hybrid_count INTEGER,
                    onsite_count INTEGER,
                    snapshot_date TIMESTAMP,
                    FOREIGN KEY (token) REFERENCES greenhouse_tokens (token)
                )
            """)
            
            # Discovery tracking
            conn.execute("""
                CREATE TABLE IF NOT EXISTS discovery_history (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    url TEXT,
                    tokens_found INTEGER,
                    new_tokens INTEGER,
                    success BOOLEAN,
                    error_message TEXT,
                    crawl_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            
            # Add new columns to existing tables if they don't exist
            try:
                conn.execute("ALTER TABLE greenhouse_tokens ADD COLUMN remote_jobs_count INTEGER DEFAULT 0")
                conn.execute("ALTER TABLE greenhouse_tokens ADD COLUMN hybrid_jobs_count INTEGER DEFAULT 0") 
                conn.execute("ALTER TABLE greenhouse_tokens ADD COLUMN onsite_jobs_count INTEGER DEFAULT 0")
            except sqlite3.OperationalError:
                pass
            
            # Create indexes for performance
            conn.execute("CREATE INDEX IF NOT EXISTS idx_monthly_token_date ON monthly_job_history(token, year, month)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_tokens_last_seen ON greenhouse_tokens(last_seen)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_discovery_time ON discovery_history(crawl_time)")
            
            conn.commit()
            logging.info("Database initialized successfully")
    
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
    
    def create_monthly_snapshot(self, token: str, company_name: str, job_count: int, work_type_counts: Dict[str, int]) -> bool:
        """Create monthly snapshot if this is the first time seeing this company this month."""
        try:
            now = datetime.utcnow()
            
            with self.get_connection() as conn:
                # Check if we already have a snapshot for this month
                cursor = conn.execute("""
                    SELECT COUNT(*) FROM monthly_job_history 
                    WHERE token = ? AND year = ? AND month = ?
                """, (token, now.year, now.month))
                
                existing_count = cursor.fetchone()[0]
                
                # Create snapshot if this is first time seeing company this month
                if existing_count == 0:
                    conn.execute("""
                        INSERT INTO monthly_job_history 
                        (token, year, month, job_count, remote_count, 
                         hybrid_count, onsite_count, snapshot_date)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    """, (token, now.year, now.month, job_count,
                          work_type_counts.get('remote', 0),
                          work_type_counts.get('hybrid', 0),
                          work_type_counts.get('onsite', 0), now))
                    
                    conn.commit()
                    logging.info(f"📅 Monthly snapshot created for {token} - {now.strftime('%B %Y')}")
                    return True
                return False
        except Exception as e:
            logging.error(f"Error creating monthly snapshot for {token}: {e}")
            return False
    
    def log_discovery(self, url: str, tokens_found: int, new_tokens: int, success: bool, error_message: str = None):
        """Log URL crawling results."""
        try:
            with self.get_connection() as conn:
                conn.execute("""
                    INSERT INTO discovery_history (url, tokens_found, new_tokens, success, error_message)
                    VALUES (?, ?, ?, ?, ?)
                """, (url, tokens_found, new_tokens, success, error_message))
                conn.commit()
        except Exception as e:
            logging.error(f"Failed to log discovery history: {e}")
    
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
    
    def get_monthly_trends(self) -> Dict[str, Any]:
        """Get month-over-month trends for email reporting."""
        try:
            with self.get_connection() as conn:
                now = datetime.utcnow()
                
                # Current month totals
                current_cursor = conn.execute("""
                    SELECT 
                        COUNT(*) as companies,
                        SUM(job_count) as total_jobs,
                        SUM(remote_count) as remote_jobs,
                        SUM(hybrid_count) as hybrid_jobs,
                        SUM(onsite_count) as onsite_jobs
                    FROM monthly_job_history 
                    WHERE year = ? AND month = ?
                """, (now.year, now.month))
                current = current_cursor.fetchone()
                
                # Previous month totals
                prev_month = now.month - 1 if now.month > 1 else 12
                prev_year = now.year if now.month > 1 else now.year - 1
                
                prev_cursor = conn.execute("""
                    SELECT 
                        COUNT(*) as companies,
                        SUM(job_count) as total_jobs,
                        SUM(remote_count) as remote_jobs,
                        SUM(hybrid_count) as hybrid_jobs,
                        SUM(onsite_count) as onsite_jobs
                    FROM monthly_job_history 
                    WHERE year = ? AND month = ?
                """, (prev_year, prev_month))
                previous = prev_cursor.fetchone()
                
                # New companies this month
                new_companies_cursor = conn.execute("""
                    SELECT token, snapshot_date FROM monthly_job_history 
                    WHERE year = ? AND month = ?
                    AND token NOT IN (
                        SELECT DISTINCT token FROM monthly_job_history 
                        WHERE year = ? AND month = ?
                    )
                    ORDER BY snapshot_date
                """, (now.year, now.month, prev_year, prev_month))
                new_companies = new_companies_cursor.fetchall()
                
                return {
                    'current': current,
                    'previous': previous,
                    'new_companies': new_companies,
                    'current_month': now.strftime('%B %Y'),
                    'previous_month': f"{['Jan','Feb','Mar','Apr','May','Jun','Jul','Aug','Sep','Oct','Nov','Dec'][prev_month-1]} {prev_year}"
                }
        except Exception as e:
            logging.error(f"Failed to retrieve monthly trends: {e}")
            return {}


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
                    logging.warning(f"Rate limited for {token}, waiting {wait_time} seconds...")
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
    
    def send_summary(self, tokens_data: List[sqlite3.Row], trends_data: Dict[str, Any], recipient: str) -> bool:
        try:
            body = self._format_summary(tokens_data, trends_data)
            html_body = self._format_html_summary(tokens_data, trends_data)
            
            msg = MIMEMultipart('alternative')
            msg["Subject"] = f"📊 Greenhouse Market Intelligence - {datetime.utcnow().strftime('%Y-%m-%d')}"
            msg["From"] = self.smtp_user
            msg["To"] = recipient
            
            text_part = MIMEText(body, 'plain')
            html_part = MIMEText(html_body, 'html')
            
            msg.attach(text_part)
            msg.attach(html_part)
            
            with smtplib.SMTP(self.smtp_server, self.smtp_port) as server:
                server.starttls()
                server.login(self.smtp_user, self.smtp_pass)
                server.sendmail(self.smtp_user, recipient, msg.as_string())
            
            logging.info("📧 Enhanced email summary sent successfully")
            return True
        except Exception as e:
            logging.error(f"Error sending email: {e}")
            return False
    
    def _format_summary(self, tokens_data: List[sqlite3.Row], trends_data: Dict[str, Any]) -> str:
        if not tokens_data:
            return "No tokens collected yet."
        
        total_jobs = sum(row.get('job_count', 0) or 0 for row in tokens_data)
        total_remote = sum(row.get('remote_jobs_count', 0) or 0 for row in tokens_data)
        total_hybrid = sum(row.get('hybrid_jobs_count', 0) or 0 for row in tokens_data)
        total_onsite = sum(row.get('onsite_jobs_count', 0) or 0 for row in tokens_data)
        
        lines = [
            f"🚀 Greenhouse Market Intelligence Report",
            f"=" * 60,
            f"📊 CURRENT MARKET SNAPSHOT",
            f"Total Companies Tracked: {len(tokens_data)}",
            f"Total Job Openings: {total_jobs:,}",
            f"  🏠 Remote: {total_remote:,} ({total_remote/total_jobs*100:.1f}%)" if total_jobs > 0 else "  🏠 Remote: 0",
            f"  🏢 Hybrid: {total_hybrid:,} ({total_hybrid/total_jobs*100:.1f}%)" if total_jobs > 0 else "  🏢 Hybrid: 0", 
            f"  🏢 On-site: {total_onsite:,} ({total_onsite/total_jobs*100:.1f}%)" if total_jobs > 0 else "  🏢 On-site: 0",
            ""
        ]
        
        # Add trends if available
        if trends_data and trends_data.get('current') and trends_data.get('previous'):
            current = trends_data['current']
            previous = trends_data['previous']
            
            if previous['total_jobs'] and previous['total_jobs'] > 0:
                job_change = current['total_jobs'] - previous['total_jobs']
                job_change_pct = (job_change / previous['total_jobs']) * 100
                
                company_change = current['companies'] - previous['companies']
                
                lines.extend([
                    f"📈 MONTH-OVER-MONTH TRENDS ({trends_data.get('current_month', '')})",
                    f"Jobs Change: {job_change:+,} ({job_change_pct:+.1f}%) vs {trends_data.get('previous_month', '')}",
                    f"New Companies Discovered: +{company_change} companies",
                    ""
                ])
                
                # New companies this month
                if trends_data.get('new_companies'):
                    lines.extend([
                        f"🆕 NEW COMPANIES DISCOVERED THIS MONTH:",
                        f"Found {len(trends_data['new_companies'])} new companies hiring on Greenhouse",
                        ""
                    ])
        
        lines.extend([
            f"🏆 TOP HIRING COMPANIES:",
            f"=" * 30
        ])
        
        # Sort companies by job count and show top 15
        sorted_companies = sorted(tokens_data, key=lambda x: x.get('job_count', 0) or 0, reverse=True)
        for i, row in enumerate(sorted_companies[:15], 1):
            try:
                job_titles_list = json.loads(row.get('job_titles', '[]')) if row.get('job_titles') else []
            except:
                job_titles_list = []
            
            remote_count = row.get('remote_jobs_count', 0) or 0
            hybrid_count = row.get('hybrid_jobs_count', 0) or 0
            onsite_count = row.get('onsite_jobs_count', 0) or 0
            
            titles_preview = ", ".join(job_titles_list[:2]) + ("..." if len(job_titles_list) > 2 else "")
            
            lines.append(
                f"{i:2d}. {row.get('company_name', 'Unknown')} ({row.get('token', '')})\n"
                f"     Jobs: {row.get('job_count', 0):,} | Remote:{remote_count} Hybrid:{hybrid_count} On-site:{onsite_count}\n"
                f"     Sample roles: {titles_preview}\n"
                f"     Locations: {(row.get('locations', 'Not specified') or 'Not specified')[:100]}"
            )
        
        lines.extend([
            "",
            f"📱 Dashboard: Visit your Railway app URL for real-time data",
            f"🔄 Next update: Automatic collection every 6 hours",
            f"📅 Report generated: {datetime.utcnow().strftime('%Y-%m-%d %H:%M UTC')}"
        ])
        
        return "\n".join(lines)
    
    def _format_html_summary(self, tokens_data: List[sqlite3.Row], trends_data: Dict[str, Any]) -> str:
        if not tokens_data:
            return "<p>No tokens collected yet.</p>"
        
        total_jobs = sum(row.get('job_count', 0) or 0 for row in tokens_data)
        total_remote = sum(row.get('remote_jobs_count', 0) or 0 for row in tokens_data)
        total_hybrid = sum(row.get('hybrid_jobs_count', 0) or 0 for row in tokens_data)
        total_onsite = sum(row.get('onsite_jobs_count', 0) or 0 for row in tokens_data)
        
        # Calculate trends
        trend_html = ""
        if trends_data and trends_data.get('current') and trends_data.get('previous'):
            current = trends_data['current']
            previous = trends_data['previous']
            
            if previous['total_jobs'] and previous['total_jobs'] > 0:
                job_change = current['total_jobs'] - previous['total_jobs']
                job_change_pct = (job_change / previous['total_jobs']) * 100
                company_change = current['companies'] - previous['companies']
                
                trend_color = "#28a745" if job_change >= 0 else "#dc3545"
                trend_icon = "📈" if job_change >= 0 else "📉"
                
                trend_html = f"""
                <div style="background-color: #e8f4fd; padding: 15px; border-radius: 5px; margin: 20px 0;">
                    <h3>{trend_icon} Month-over-Month Trends</h3>
                    <p><strong>Jobs Change:</strong> <span style="color: {trend_color};">{job_change:+,} ({job_change_pct:+.1f}%)</span> vs {trends_data.get('previous_month', '')}</p>
                    <p><strong>New Companies:</strong> +{company_change} companies discovered</p>
                    {f"<p><strong>New This Month:</strong> {len(trends_data.get('new_companies', []))} companies</p>" if trends_data.get('new_companies') else ""}
                </div>
                """
        
        html = f"""
        <html>
        <head>
            <style>
                body {{ font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Arial, sans-serif; margin: 20px; line-height: 1.6; }}
                .header {{ background: linear-gradient(135deg, #667eea 0%, #764ba2 100%); color: white; padding: 20px; border-radius: 10px; text-align: center; }}
                .stats {{ display: flex; justify-content: space-around; margin: 20px 0; }}
                .stat-box {{ background: #f8f9fa; padding: 15px; border-radius: 8px; text-align: center; min-width: 120px; }}
                .stat-number {{ font-size: 24px; font-weight: bold; color: #333; }}
                .stat-label {{ font-size: 12px; color: #666; margin-top: 5px; }}
                table {{ border-collapse: collapse; width: 100%; margin-top: 20px; }}
                th, td {{ border: 1px solid #ddd; padding: 12px; text-align: left; }}
                th {{ background-color: #f2f2f2; font-weight: bold; }}
                tr:nth-child(even) {{ background-color: #f9f9f9; }}
                .company-name {{ font-weight: bold; color: #0066cc; }}
                .job-count {{ font-weight: bold; color: #28a745; }}
                .work-types {{ font-size: 11px; color: #666; }}
                .footer {{ margin-top: 30px; padding: 15px; background-color: #f8f9fa; border-radius: 5px; font-size: 12px; color: #666; }}
            </style>
        </head>
        <body>
            <div class="header">
                <h1>🚀 Greenhouse Market Intelligence</h1>
                <p>Real-time job market analysis • {datetime.utcnow().strftime('%B %d, %Y')}</p>
            </div>
            
            <div class="stats">
                <div class="stat-box">
                    <div class="stat-number">{len(tokens_data):,}</div>
                    <div class="stat-label">Companies Tracked</div>
                </div>
                <div class="stat-box">
                    <div class="stat-number">{total_jobs:,}</div>
                    <div class="stat-label">Total Jobs</div>
                </div>
                <div class="stat-box">
                    <div class="stat-number">{total_remote/total_jobs*100:.1f}%</div>
                    <div class="stat-label">Remote Jobs</div>
                </div>
                <div class="stat-box">
                    <div class="stat-number">{total_hybrid/total_jobs*100:.1f}%</div>
                    <div class="stat-label">Hybrid Jobs</div>
                </div>
            </div>
            
            {trend_html}
            
            <h2>🏆 Top Hiring Companies</h2>
            <table>
                <tr>
                    <th>Rank</th>
                    <th>Company</th>
                    <th>Total Jobs</th>
                    <th>🏠 Remote</th>
                    <th>🏢 Hybrid</th>
                    <th>🏢 On-site</th>
                    <th>Locations</th>
                </tr>
        """
        
        # Sort companies by job count and show top 20
        sorted_companies = sorted(tokens_data, key=lambda x: x.get('job_count', 0) or 0, reverse=True)
        for i, row in enumerate(sorted_companies[:20], 1):
            remote_count = row.get('remote_jobs_count', 0) or 0
            hybrid_count = row.get('hybrid_jobs_count', 0) or 0
            onsite_count = row.get('onsite_jobs_count', 0) or 0
            
            locations = (row.get('locations', '') or '')[:100]
            if len(locations) >= 100:
                locations += "..."
            
            html += f"""
            <tr>
                <td>{i}</td>
                <td class="company-name">
                    <a href="https://boards.greenhouse.io/{row.get('token', '')}" target="_blank">
                        {row.get('company_name', 'Unknown')}
                    </a>
                    <br><span style="font-size: 11px; color: #666;">({row.get('token', '')})</span>
                </td>
                <td class="job-count">{row.get('job_count', 0):,}</td>
                <td>{remote_count}</td>
                <td>{hybrid_count}</td>
                <td>{onsite_count}</td>
                <td style="font-size: 11px;">{locations or 'Not specified'}</td>
            </tr>
            """
        
        html += f"""
            </table>
            
            <div class="footer">
                <p><strong>🤖 Automated Collection:</strong> Data refreshed every 6 hours from 150+ discovery sources</p>
                <p><strong>📊 Market Coverage:</strong> Tracking {len(tokens_data)} companies across all tech sectors</p>
                <p><strong>🔗 Live Data:</strong> Click company names to view current job postings on Greenhouse</p>
                <p><strong>⏰ Generated:</strong> {datetime.utcnow().strftime('%Y-%m-%d %H:%M UTC')}</p>
            </div>
        </body>
        </html>
        """
        return html


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
        
        logging.info(f"🚀 Processing {len(seed_tokens)} seed tokens")
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
                    # Update main table
                    success = self.db_manager.upsert_token(
                        token, "seed_token", company_name, job_count, 
                        locations, departments, job_titles, work_type_counts
                    )
                    
                    # Create monthly snapshot
                    if success:
                        self.db_manager.create_monthly_snapshot(token, company_name, job_count, work_type_counts)
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
    
    def crawl_page(self, url: str) -> Tuple[int, int]:
        """Crawl a single page for Greenhouse tokens. Returns (total_found, new_tokens)."""
        tokens_found = 0
        new_tokens = 0
        error_message = None
        
        try:
            logging.info(f"🔍 Crawling: {url}")
            
            headers = {"User-Agent": self.config.get('scraping', 'user_agent')}
            response = requests.get(url, headers=headers, 
                                 timeout=self.config.getint('scraping', 'timeout', 15))
            
            if response.status_code != 200:
                error_message = f"HTTP {response.status_code}"
                logging.warning(f"Failed to fetch {url}: {error_message}")
                self.db_manager.log_discovery(url, 0, 0, False, error_message)
                return 0, 0
            
            soup = BeautifulSoup(response.text, "html.parser")
            
            # Find all links that might contain Greenhouse tokens
            for link in soup.find_all("a", href=True):
                token = TokenExtractor.extract_token(link["href"])
                if token:
                    tokens_found += 1
                    
                    if self.dry_run:
                        logging.info(f"[DRY RUN] Would process discovered token: {token}")
                        new_tokens += 1
                        continue
                    
                    # Check if this is a new token
                    with self.db_manager.get_connection() as conn:
                        cursor = conn.execute("SELECT COUNT(*) FROM greenhouse_tokens WHERE token = ?", (token,))
                        is_new = cursor.fetchone()[0] == 0
                    
                    # Parse the board
                    company_name, job_count, locations, departments, job_titles, work_type_counts = \
                        self.board_parser.parse_board(token, self.config.getint('scraping', 'max_retries', 3))
                    
                    if company_name:
                        # Update main table
                        success = self.db_manager.upsert_token(
                            token, url, company_name, job_count, 
                            locations, departments, job_titles, work_type_counts
                        )
                        
                        # Create monthly snapshot
                        if success:
                            self.db_manager.create_monthly_snapshot(token, company_name, job_count, work_type_counts)
                            
                            if is_new:
                                new_tokens += 1
                                logging.info(f"🆕 Discovered {token}: {company_name} ({job_count} jobs - "
                                           f"Remote:{work_type_counts.get('remote', 0)} "
                                           f"Hybrid:{work_type_counts.get('hybrid', 0)} "
                                           f"On-site:{work_type_counts.get('onsite', 0)})")
                            else:
                                logging.debug(f"♻️ Updated {token}: {company_name} ({job_count} jobs)")
                    
                    # Rate limiting
                    delay = random.uniform(
                        self.config.getint('scraping', 'min_delay', 5),
                        self.config.getint('scraping', 'max_delay', 15)
                    )
                    time.sleep(delay)
        
        except Exception as e:
            error_message = str(e)
            logging.error(f"Error crawling {url}: {e}")
        
        finally:
            # Log the crawl attempt
            if not self.dry_run:
                self.db_manager.log_discovery(url, tokens_found, new_tokens, error_message is None, error_message)
        
        return tokens_found, new_tokens
    
    def run(self) -> bool:
        start_time = datetime.utcnow()
        logging.info(f"🚀 Starting Greenhouse token collection {'(DRY RUN)' if self.dry_run else ''}")
        
        total_tokens_processed = 0
        total_new_discoveries = 0
        
        # Phase 1: Process known seed tokens
        seed_tokens_processed = self.process_seed_tokens()
        total_tokens_processed += seed_tokens_processed
        logging.info(f"✅ Phase 1 Complete: Processed {seed_tokens_processed} seed tokens")
        
        # Phase 2: Crawl seed URLs for new token discovery
        seed_urls = [url.strip() for url in 
                    self.config.get('seed_urls', 'urls', '').split('\n') if url.strip()]
        
        if seed_urls:
            logging.info(f"🔍 Phase 2: Crawling {len(seed_urls)} seed URLs for token discovery")
            
            for i, url in enumerate(seed_urls, 1):
                tokens_found, new_tokens = self.crawl_page(url)
                total_tokens_processed += tokens_found
                total_new_discoveries += new_tokens
                
                if tokens_found > 0:
                    logging.info(f"📊 URL {i}/{len(seed_urls)}: Found {tokens_found} tokens ({new_tokens} new) from {url[:50]}...")
                
                # Add delay between pages
                delay = random.uniform(
                    self.config.getint('scraping', 'min_delay', 5),
                    self.config.getint('scraping', 'max_delay', 15)
                )
                time.sleep(delay)
            
            logging.info(f"✅ Phase 2 Complete: Discovered {total_new_discoveries} new companies from URL crawling")
        else:
            logging.info("No seed URLs configured, skipping discovery crawling")
        
        # Phase 3: Send enhanced email summary
        if (self.email_reporter and 
            not self.dry_run and 
            self.config.getboolean('email', 'enabled', True)):
            
            recipient = os.getenv('EMAIL_RECIPIENT')
            if recipient:
                tokens_data = self.db_manager.get_all_tokens()
                trends_data = self.db_manager.get_monthly_trends()
                self.email_reporter.send_summary(tokens_data, trends_data, recipient)
        
        end_time = datetime.utcnow()
        duration = (end_time - start_time).total_seconds()
        
        # Final summary
        with self.db_manager.get_connection() as conn:
            cursor = conn.execute("SELECT COUNT(*) FROM greenhouse_tokens")
            total_companies = cursor.fetchone()[0]
            
            cursor = conn.execute("SELECT SUM(job_count) FROM greenhouse_tokens")
            total_jobs = cursor.fetchone()[0] or 0
        
        logging.info(f"🎉 Collection completed in {duration/60:.1f} minutes")
        logging.info(f"📊 Final Stats: {total_companies:,} companies | {total_jobs:,} jobs | {total_new_discoveries} new discoveries")
        
        return True


def main():
    import argparse
    
    parser = argparse.ArgumentParser(description='Enhanced Greenhouse Token Collector')
    parser.add_argument('--dry-run', action='store_true', help='Run without making changes')
    parser.add_argument('--verbose', '-v', action='store_true', help='Verbose logging')
    
    args = parser.parse_args()
    
    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)
    
    try:
        collector = GreenhouseCollector(dry_run=args.dry_run)
        success = collector.run()
        return 0 if success else 1
    except Exception as e:
        logging.error(f"Fatal error: {e}")
        return 1


if __name__ == "__main__":
    exit(main())
