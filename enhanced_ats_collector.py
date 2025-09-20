"""
Enhanced Multi-ATS Collector with Historical Tracking and Market Intelligence
---------------------------------------------------------------------------
A comprehensive system supporting both Greenhouse and Lever ATS with:
- Monthly historical job count tracking
- Geographic expansion detection
- Work type classification (remote/hybrid/onsite)
- Market intelligence and trend analysis
- Automated email reporting with historical insights using Resend API
- Backward compatibility with existing Greenhouse database

Features:
- Multi-ATS support (Greenhouse + Lever)
- Monthly snapshots for trend analysis
- Location expansion tracking
- Smart change detection and historical tracking
- Email reports with month-over-month comparisons
- Market intelligence analytics
- Fortune 500 company discovery URLs
"""

import requests
import sqlite3
import time
import random
import json
import logging
import os
import urllib.robotparser
from contextlib import contextmanager
from datetime import datetime, timedelta
from configparser import ConfigParser
from typing import Tuple, List, Optional, Dict, Any
from urllib.parse import urlparse, urljoin
from bs4 import BeautifulSoup

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass


class Config:
    """Enhanced configuration management for multi-ATS collector."""
    
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
            'user_agent': 'MultiATSBot/2.0 (Research; contact: researcher@example.com)',
            'respect_robots_txt': 'true'
        }
        
        self.config['email'] = {
            'enabled': 'true'
        }
        
        # Known tokens for both ATS systems
        self.config['seed_tokens'] = {
            'greenhouse_tokens': 'stripe,notion,figma,discord,dropbox,zoom,doordash,instacart,robinhood,coinbase,plaid,openai,anthropic,airbnb,reddit,gitlab,hashicorp,mongodb,elastic,salesforce,snowflake,databricks,atlassian,asana,slack,okta,twilio,brex,mercury,ramp,checkr,chime,affirm,canva,flexport,benchling,retool,vercel,linear,23andme,shopify,tesla,netflix,spotify,unity,cloudflare,docker,intel,nvidia,apple,meta,google,microsoft',
            'lever_tokens': 'lever,uber,netflix,postmates,box,github,thumbtack,lyft,twitch,palantir,atlassian,asana,slack,zoom,calendly,linear,mixpanel,sendgrid,twilio,okta,auth0,datadog,newrelic,pagerduty,mongodb,elastic,cockroachlabs,brex,mercury,ramp,checkr,chime,affirm,klarna,stripe,square,adyen,marqeta'
        }
        
        # Fortune 500 and comprehensive discovery URLs (truncated for brevity)
        self.config['seed_urls'] = {
            'urls': '''https://careers.walmart.com/
https://www.amazon.jobs/
https://careers.apple.com/
https://careers.google.com/
https://careers.microsoft.com/
https://jobs.netflix.com/
https://careers.meta.com/
https://careers.salesforce.com/
https://jobs.tesla.com/
https://github.com/poteto/hiring-without-whiteboards
https://github.com/remoteintech/remote-jobs
https://themuse.com/companies
https://builtin.com/companies
https://www.keyvalues.com/companies
https://angel.co/jobs
https://www.wellfound.com/jobs
https://www.ycombinator.com/companies
https://jobs.lever.co/
https://boards.greenhouse.io/'''
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
    """Enhanced database manager with multi-ATS support and backward compatibility."""
    
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
        """Initialize database with backward compatibility for existing Greenhouse data."""
        with self.get_connection() as conn:
            # Keep existing greenhouse_tokens table structure for backward compatibility
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
            
            # New unified ATS companies table
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
                    first_seen TIMESTAMP,
                    last_seen TIMESTAMP,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(ats_type, token)
                )
            """)
            
            # Enhanced monthly historical data with ATS type support
            conn.execute("""
                CREATE TABLE IF NOT EXISTS monthly_job_history (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    ats_type TEXT DEFAULT 'greenhouse',
                    token TEXT,
                    year INTEGER,
                    month INTEGER,
                    job_count INTEGER,
                    remote_count INTEGER,
                    hybrid_count INTEGER,
                    onsite_count INTEGER,
                    snapshot_date TIMESTAMP,
                    company_name TEXT
                )
            """)
            
            # Enhanced discovery tracking
            conn.execute("""
                CREATE TABLE IF NOT EXISTS discovery_history (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    url TEXT,
                    ats_type TEXT DEFAULT 'greenhouse',
                    tokens_found INTEGER,
                    new_tokens INTEGER,
                    success BOOLEAN,
                    error_message TEXT,
                    crawl_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            
            # Enhanced location expansion tracking
            conn.execute("""
                CREATE TABLE IF NOT EXISTS location_expansions (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    ats_type TEXT DEFAULT 'greenhouse',
                    token TEXT,
                    company_name TEXT,
                    new_location TEXT,
                    first_seen_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            
            # Add new columns to existing tables if they don't exist (backward compatibility)
            try:
                conn.execute("ALTER TABLE greenhouse_tokens ADD COLUMN remote_jobs_count INTEGER DEFAULT 0")
                conn.execute("ALTER TABLE greenhouse_tokens ADD COLUMN hybrid_jobs_count INTEGER DEFAULT 0") 
                conn.execute("ALTER TABLE greenhouse_tokens ADD COLUMN onsite_jobs_count INTEGER DEFAULT 0")
                conn.execute("ALTER TABLE monthly_job_history ADD COLUMN ats_type TEXT DEFAULT 'greenhouse'")
                conn.execute("ALTER TABLE monthly_job_history ADD COLUMN company_name TEXT")
                conn.execute("ALTER TABLE discovery_history ADD COLUMN ats_type TEXT DEFAULT 'greenhouse'")
                conn.execute("ALTER TABLE location_expansions ADD COLUMN ats_type TEXT DEFAULT 'greenhouse'")
            except sqlite3.OperationalError:
                pass  # Columns already exist
            
            # Create indexes for performance
            conn.execute("CREATE INDEX IF NOT EXISTS idx_monthly_token_date ON monthly_job_history(ats_type, token, year, month)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_tokens_last_seen ON greenhouse_tokens(last_seen)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_ats_companies_type ON ats_companies(ats_type)")
            
            conn.commit()
            logging.info("Database initialized successfully with multi-ATS support")
    
    def upsert_company(self, ats_type: str, token: str, source: str, company_name: str, 
                      job_count: int, locations: List[str], departments: List[str], 
                      job_titles: List[str], work_type_counts: Dict[str, int] = None,
                      job_details: List[Dict] = None) -> bool:
        """Insert or update company in appropriate table based on ATS type."""
        try:
            now = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
            titles_json = json.dumps(job_titles[:50])
            locs_str = ", ".join(locations[:20])
            depts_str = ", ".join(departments[:10])
            
            if work_type_counts is None:
                work_type_counts = {'remote': 0, 'hybrid': 0, 'onsite': 0}
            
            with self.get_connection() as conn:
                # Handle backward compatibility for Greenhouse
                if ats_type == 'greenhouse':
                    # Update both old and new tables for Greenhouse
                    cursor = conn.execute("SELECT locations FROM greenhouse_tokens WHERE token=?", (token,))
                    existing_row = cursor.fetchone()
                    
                    # Track location expansions
                    if existing_row:
                        self._track_location_expansions(conn, ats_type, token, company_name, existing_row['locations'], locs_str)
                    
                    # Update greenhouse_tokens table (backward compatibility)
                    if existing_row:
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
                
                # Update unified ats_companies table
                company_id = f"{ats_type}_{token}"
                cursor = conn.execute("SELECT id, locations FROM ats_companies WHERE id=?", (company_id,))
                existing_unified = cursor.fetchone()
                
                # Track location expansions for non-Greenhouse ATS or if not tracked above
                if ats_type != 'greenhouse' and existing_unified:
                    self._track_location_expansions(conn, ats_type, token, company_name, 
                                                  existing_unified['locations'], locs_str)
                
                if existing_unified:
                    conn.execute("""
                        UPDATE ats_companies
                        SET source_url=?, company_name=?, job_count=?, locations=?, 
                            departments=?, job_titles=?, remote_jobs_count=?, hybrid_jobs_count=?,
                            onsite_jobs_count=?, last_seen=?, updated_at=?
                        WHERE id=?
                    """, (source, company_name, job_count, locs_str, depts_str, 
                         titles_json, work_type_counts.get('remote', 0), 
                         work_type_counts.get('hybrid', 0), work_type_counts.get('onsite', 0),
                         now, now, company_id))
                else:
                    conn.execute("""
                        INSERT INTO ats_companies
                        (id, ats_type, token, source_url, company_name, job_count, locations, 
                         departments, job_titles, remote_jobs_count, hybrid_jobs_count,
                         onsite_jobs_count, first_seen, last_seen)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """, (company_id, ats_type, token, source, company_name, job_count, 
                         locs_str, depts_str, titles_json, work_type_counts.get('remote', 0),
                         work_type_counts.get('hybrid', 0), work_type_counts.get('onsite', 0),
                         now, now))
                
                conn.commit()
                logging.info(f"Updated {ats_type.upper()} company: {token}")
                return True
        except Exception as e:
            logging.error(f"Database error for {ats_type} token {token}: {e}")
            return False
    
    def _track_location_expansions(self, conn, ats_type: str, token: str, company_name: str, 
                                 old_locations: str, new_locations: str):
        """Track when companies expand to new geographic locations."""
        try:
            old_locs = set()
            new_locs = set()
            
            if old_locations:
                old_locs = {loc.strip().lower() for loc in old_locations.split(',') if loc.strip()}
            if new_locations:
                new_locs = {loc.strip().lower() for loc in new_locations.split(',') if loc.strip()}
            
            added_locations = new_locs - old_locs
            
            meaningful_additions = []
            for loc in added_locations:
                if self._is_meaningful_location(loc):
                    meaningful_additions.append(loc.title())
            
            for new_location in meaningful_additions:
                cursor = conn.execute("""
                    SELECT COUNT(*) FROM location_expansions 
                    WHERE ats_type = ? AND token = ? AND new_location = ? 
                    AND first_seen_date > datetime('now', '-30 days')
                """, (ats_type, token, new_location))
                
                if cursor.fetchone()[0] == 0:
                    conn.execute("""
                        INSERT INTO location_expansions (ats_type, token, company_name, new_location)
                        VALUES (?, ?, ?, ?)
                    """, (ats_type, token, company_name, new_location))
                    
                    logging.info(f"Location expansion: {company_name} ({ats_type.upper()}) now hiring in {new_location}")
        
        except Exception as e:
            logging.error(f"Error tracking location expansions for {ats_type} {token}: {e}")
    
    def _is_meaningful_location(self, location: str) -> bool:
        """Filter out generic locations to focus on meaningful geographic expansions."""
        location_lower = location.lower().strip()
        
        skip_terms = {
            'remote', 'anywhere', 'global', 'worldwide', 'various', 'multiple',
            'tbd', 'flexible', 'distributed', 'virtual', 'n/a', 'not specified',
            'usa', 'us', 'united states', 'europe', 'asia', 'north america'
        }
        
        return (location_lower not in skip_terms and 
                len(location_lower) >= 3 and 
                any(c.isalpha() for c in location_lower))
    
    def create_monthly_snapshot(self, ats_type: str, token: str, company_name: str, 
                              job_count: int, work_type_counts: Dict[str, int]) -> bool:
        """Create monthly snapshot with ATS type support."""
        try:
            now = datetime.utcnow()
            
            with self.get_connection() as conn:
                cursor = conn.execute("""
                    SELECT COUNT(*) FROM monthly_job_history 
                    WHERE ats_type = ? AND token = ? AND year = ? AND month = ?
                """, (ats_type, token, now.year, now.month))
                
                existing_count = cursor.fetchone()[0]
                
                if existing_count == 0:
                    conn.execute("""
                        INSERT INTO monthly_job_history 
                        (ats_type, token, year, month, job_count, remote_count, 
                         hybrid_count, onsite_count, snapshot_date, company_name)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """, (ats_type, token, now.year, now.month, job_count,
                          work_type_counts.get('remote', 0),
                          work_type_counts.get('hybrid', 0),
                          work_type_counts.get('onsite', 0), now, company_name))
                    
                    conn.commit()
                    logging.info(f"Monthly snapshot created for {ats_type.upper()} {token} - {now.strftime('%B %Y')}")
                    return True
                return False
        except Exception as e:
            logging.error(f"Error creating monthly snapshot for {ats_type} {token}: {e}")
            return False
    
    def get_all_companies(self) -> List[Dict]:
        """Retrieve all companies from both old and new tables."""
        try:
            companies = []
            
            with self.get_connection() as conn:
                # Get Greenhouse companies from original table (backward compatibility)
                cursor = conn.execute("""
                    SELECT 'greenhouse' as ats_type, token, company_name, job_count, locations, departments, 
                           job_titles, remote_jobs_count, hybrid_jobs_count, onsite_jobs_count,
                           first_seen, last_seen
                    FROM greenhouse_tokens 
                    ORDER BY company_name
                """)
                greenhouse_companies = [dict(row) for row in cursor.fetchall()]
                companies.extend(greenhouse_companies)
                
                # Get non-Greenhouse companies from unified table
                cursor = conn.execute("""
                    SELECT ats_type, token, company_name, job_count, locations, departments, 
                           job_titles, remote_jobs_count, hybrid_jobs_count, onsite_jobs_count,
                           first_seen, last_seen
                    FROM ats_companies 
                    WHERE ats_type != 'greenhouse'
                    ORDER BY ats_type, company_name
                """)
                other_companies = [dict(row) for row in cursor.fetchall()]
                companies.extend(other_companies)
                
                return companies
        except Exception as e:
            logging.error(f"Failed to retrieve companies: {e}")
            return []
    
    def get_monthly_trends(self) -> Dict[str, Any]:
        """Get month-over-month trends for email reporting with multi-ATS support."""
        try:
            with self.get_connection() as conn:
                now = datetime.utcnow()
                
                # Current month totals across all ATS types
                current_cursor = conn.execute("""
                    SELECT 
                        ats_type,
                        COUNT(*) as companies,
                        SUM(job_count) as total_jobs,
                        SUM(remote_count) as remote_jobs,
                        SUM(hybrid_count) as hybrid_jobs,
                        SUM(onsite_count) as onsite_jobs
                    FROM monthly_job_history 
                    WHERE year = ? AND month = ?
                    GROUP BY ats_type
                """, (now.year, now.month))
                current_by_ats = {row['ats_type']: dict(row) for row in current_cursor.fetchall()}
                
                # Calculate overall current totals
                current = {
                    'companies': sum(data['companies'] for data in current_by_ats.values()),
                    'total_jobs': sum(data['total_jobs'] or 0 for data in current_by_ats.values()),
                    'remote_jobs': sum(data['remote_jobs'] or 0 for data in current_by_ats.values()),
                    'hybrid_jobs': sum(data['hybrid_jobs'] or 0 for data in current_by_ats.values()),
                    'onsite_jobs': sum(data['onsite_jobs'] or 0 for data in current_by_ats.values()),
                    'by_ats': current_by_ats
                }
                
                # Previous month totals
                prev_month = now.month - 1 if now.month > 1 else 12
                prev_year = now.year if now.month > 1 else now.year - 1
                
                prev_cursor = conn.execute("""
                    SELECT 
                        ats_type,
                        COUNT(*) as companies,
                        SUM(job_count) as total_jobs,
                        SUM(remote_count) as remote_jobs,
                        SUM(hybrid_count) as hybrid_jobs,
                        SUM(onsite_count) as onsite_jobs
                    FROM monthly_job_history 
                    WHERE year = ? AND month = ?
                    GROUP BY ats_type
                """, (prev_year, prev_month))
                prev_by_ats = {row['ats_type']: dict(row) for row in prev_cursor.fetchall()}
                
                previous = {
                    'companies': sum(data['companies'] for data in prev_by_ats.values()),
                    'total_jobs': sum(data['total_jobs'] or 0 for data in prev_by_ats.values()),
                    'remote_jobs': sum(data['remote_jobs'] or 0 for data in prev_by_ats.values()),
                    'hybrid_jobs': sum(data['hybrid_jobs'] or 0 for data in prev_by_ats.values()),
                    'onsite_jobs': sum(data['onsite_jobs'] or 0 for data in prev_by_ats.values()),
                    'by_ats': prev_by_ats
                }
                
                # New companies this month by ATS type
                new_companies_cursor = conn.execute("""
                    SELECT ats_type, token, company_name, snapshot_date FROM monthly_job_history 
                    WHERE year = ? AND month = ?
                    AND (ats_type, token) NOT IN (
                        SELECT ats_type, token FROM monthly_job_history 
                        WHERE year = ? AND month = ?
                    )
                    ORDER BY ats_type, snapshot_date
                """, (now.year, now.month, prev_year, prev_month))
                new_companies_rows = new_companies_cursor.fetchall()
                new_companies = [dict(row) for row in new_companies_rows]
                
                # Location expansions in the last 30 days across all ATS types
                expansions_cursor = conn.execute("""
                    SELECT le.ats_type, le.company_name, le.new_location, le.first_seen_date,
                           COALESCE(gt.job_count, ac.job_count) as job_count, le.token
                    FROM location_expansions le
                    LEFT JOIN greenhouse_tokens gt ON le.token = gt.token AND le.ats_type = 'greenhouse'
                    LEFT JOIN ats_companies ac ON le.token = ac.token AND le.ats_type = ac.ats_type
                    WHERE le.first_seen_date > datetime('now', '-30 days')
                    ORDER BY le.first_seen_date DESC
                """)
                expansion_rows = expansions_cursor.fetchall()
                location_expansions = [dict(row) for row in expansion_rows]
                
                return {
                    'current': current,
                    'previous': previous,
                    'new_companies': new_companies,
                    'location_expansions': location_expansions,
                    'current_month': now.strftime('%B %Y'),
                    'previous_month': f"{['Jan','Feb','Mar','Apr','May','Jun','Jul','Aug','Sep','Oct','Nov','Dec'][prev_month-1]} {prev_year}"
                }
        except Exception as e:
            logging.error(f"Failed to retrieve monthly trends: {e}")
            return {}


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
        except Exception:
            pass
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
            elif "lever.co" in parsed.netloc:
                subdomain = parsed.netloc.split('.')[0]
                if subdomain != "jobs" and TokenExtractor.validate_token(subdomain):
                    return subdomain
        except Exception:
            pass
        return None
    
    @staticmethod
    def validate_token(token: str) -> bool:
        """Validate token format."""
        return (token and 
                len(token) >= 2 and 
                len(token) <= 100 and 
                token.replace('-', '').replace('_', '').replace('.', '').isalnum())


class GreenhouseBoardParser:
    """Parse Greenhouse job board pages."""
    
    def __init__(self, user_agent: str, timeout: int = 15):
        self.user_agent = user_agent
        self.timeout = timeout
        self.session = requests.Session()
        self.session.headers.update({'User-Agent': user_agent})
    
    def parse_board(self, token: str, max_retries: int = 3) -> Tuple[Optional[str], int, List[str], List[str], List[str], Dict[str, int], List[Dict]]:
        """Fetch and parse metadata from Greenhouse board."""
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
                    return None, 0, [], [], [], {}, []
                elif response.status_code != 200:
                    return None, 0, [], [], [], {}, []
                
                return self._parse_html(response.text, token)
                
            except requests.RequestException as e:
                if attempt == max_retries - 1:
                    return None, 0, [], [], [], {}, []
                time.sleep(random.uniform(5, 15))
        
        return None, 0, [], [], [], {}, []
    
    def _parse_html(self, html: str, token: str) -> Tuple[str, int, List[str], List[str], List[str], Dict[str, int], List[Dict]]:
        """Parse HTML content to extract job information."""
        soup = BeautifulSoup(html, "html.parser")
        
        company_name = self._extract_company_name(soup, token)
        jobs_data = self._extract_jobs_data(soup)
        
        job_titles = []
        locations = set()
        departments = set()
        work_type_counts = {'remote': 0, 'hybrid': 0, 'onsite': 0}
        detailed_jobs = []
        
        for job_data in jobs_data:
            if job_data.get('title'):
                job_titles.append(job_data['title'][:100])
                
                work_type = self._classify_work_type(job_data.get('location', ''), job_data.get('title', ''))
                work_type_counts[work_type] += 1
                
                detailed_job = {
                    'title': job_data['title'][:100],
                    'url': job_data.get('url', ''),
                    'location': job_data.get('location', '')[:50],
                    'department': job_data.get('department', '')[:50],
                    'work_type': work_type,
                    'posted_date': job_data.get('posted_date', ''),
                    'description_snippet': job_data.get('description', '')[:500]
                }
                detailed_jobs.append(detailed_job)
            
            if job_data.get('location'):
                locations.add(job_data['location'][:50])
            if job_data.get('department'):
                departments.add(job_data['department'][:50])
        
        return (company_name, len(job_titles), 
                sorted(list(locations)), sorted(list(departments)), job_titles,
                work_type_counts, detailed_jobs)
    
    def _extract_company_name(self, soup: BeautifulSoup, token: str) -> str:
        """Extract company name from page."""
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
        """Extract job data from page."""
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
        """Extract job information from element."""
        job_data = {}
        
        title_element = element if element.name in ['a', 'h1', 'h2', 'h3'] else element.find(['a', 'h1', 'h2', 'h3'])
        if title_element:
            job_data['title'] = title_element.get_text(strip=True)
            if title_element.name == 'a' and title_element.get('href'):
                job_data['url'] = title_element.get('href')
                if job_data['url'].startswith('/'):
                    job_data['url'] = f"https://boards.greenhouse.io{job_data['url']}"
        
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
        """Classify job work type."""
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


class LeverBoardParser:
    """Parse Lever job board pages."""
    
    def __init__(self, user_agent: str, timeout: int = 15):
        self.user_agent = user_agent
        self.timeout = timeout
        self.session = requests.Session()
        self.session.headers.update({'User-Agent': user_agent})
    
    def parse_board(self, token: str, max_retries: int = 3) -> Tuple[Optional[str], int, List[str], List[str], List[str], Dict[str, int], List[Dict]]:
        """Fetch and parse metadata from Lever board."""
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
                        break
                    elif response.status_code != 200:
                        break
                    
                    result = self._parse_html(response.text, token)
                    if result[0]:
                        return result
                    
                except requests.RequestException as e:
                    if attempt == max_retries - 1:
                        break
                    time.sleep(random.uniform(5, 15))
        
        return None, 0, [], [], [], {}, []
    
    def _parse_html(self, html: str, token: str) -> Tuple[str, int, List[str], List[str], List[str], Dict[str, int], List[Dict]]:
        """Parse HTML content to extract job information."""
        soup = BeautifulSoup(html, "html.parser")
        
        company_name = self._extract_company_name(soup, token)
        jobs_data = self._extract_jobs_data(soup)
        
        job_titles = []
        locations = set()
        departments = set()
        work_type_counts = {'remote': 0, 'hybrid': 0, 'onsite': 0}
        detailed_jobs = []
        
        for job_data in jobs_data:
            if job_data.get('title'):
                job_titles.append(job_data['title'][:100])
                
                work_type = self._classify_work_type(job_data.get('location', ''), job_data.get('title', ''))
                work_type_counts[work_type] += 1
                
                detailed_job = {
                    'title': job_data['title'][:100],
                    'url': job_data.get('url', ''),
                    'location': job_data.get('location', '')[:50],
                    'department': job_data.get('department', '')[:50],
                    'work_type': work_type,
                    'posted_date': job_data.get('posted_date', ''),
                    'description_snippet': job_data.get('description', '')[:500]
                }
                detailed_jobs.append(detailed_job)
            
            if job_data.get('location'):
                locations.add(job_data['location'][:50])
            if job_data.get('department'):
                departments.add(job_data['department'][:50])
        
        return (company_name, len(job_titles), 
                sorted(list(locations)), sorted(list(departments)), job_titles,
                work_type_counts, detailed_jobs)
    
    def _extract_company_name(self, soup: BeautifulSoup, token: str) -> str:
        """Extract company name from Lever page."""
        selectors = ['.company-name', '[data-qa="company-name"]', 'h1', 'title', '.header-company-name']
        
        for selector in selectors:
            element = soup.select_one(selector)
            if element:
                text = element.get_text(strip=True)
                if text and len(text) > 2:
                    text = text.replace(' Jobs', '').replace(' Careers', '').strip()
                    return text[:100]
        
        return token.replace('-', ' ').replace('_', ' ').title()
    
    def _extract_jobs_data(self, soup: BeautifulSoup) -> List[Dict[str, str]]:
        """Extract job data from Lever page."""
        jobs_data = []
        job_selectors = ['.posting', '[data-qa="posting"]', '.posting-card', '.job-posting', 'a[href*="/jobs/"]']
        
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
        
        title_element = element if element.name in ['a', 'h1', 'h2', 'h3'] else element.find(['a', 'h1', 'h2', 'h3'])
        if title_element:
            job_data['title'] = title_element.get_text(strip=True)
            if title_element.name == 'a' and title_element.get('href'):
                job_data['url'] = title_element.get('href')
                if job_data['url'].startswith('/'):
                    job_data['url'] = f"https://jobs.lever.co{job_data['url']}"
        
        location_selectors = ['.posting-location', '.location', '[data-qa="posting-location"]']
        for sel in location_selectors:
            loc_elem = element.find(class_=sel.replace('.', '')) or element.find_next(class_=sel.replace('.', ''))
            if loc_elem:
                job_data['location'] = loc_elem.get_text(strip=True)
                break
        
        dept_selectors = ['.posting-category', '.department', '[data-qa="posting-category"]']
        for sel in dept_selectors:
            dept_elem = element.find(class_=sel.replace('.', '')) or element.find_next(class_=sel.replace('.', ''))
            if dept_elem:
                job_data['department'] = dept_elem.get_text(strip=True)
                break
        
        return job_data if job_data.get('title') else None
    
    def _classify_work_type(self, location: str, title: str) -> str:
        """Classify job work type."""
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
    """Enhanced email reporting with Resend API instead of SMTP."""
    
    def __init__(self):
        self.resend_api_key = os.getenv('RESEND_API_KEY')
        self.from_email = os.getenv('FROM_EMAIL', 'greenhouse-reports@resend.dev')
    
    def send_summary(self, companies_data: List[Dict], trends_data: Dict[str, Any], recipient: str) -> bool:
        """Send enhanced email summary using Resend API."""
        try:
            if not self.resend_api_key:
                logging.error("RESEND_API_KEY not found in environment variables")
                return False
            
            subject = f"📊 Multi-ATS Market Intelligence - {datetime.utcnow().strftime('%Y-%m-%d')}"
            html_body = self._format_html_summary(companies_data, trends_data)
            
            return self._send_via_resend(subject, html_body, recipient)
            
        except Exception as e:
            logging.error(f"Error sending email: {e}")
            return False
    
    def _send_via_resend(self, subject: str, html_content: str, to_email: str) -> bool:
        """Send email using Resend API."""
        url = "https://api.resend.com/emails"
        
        payload = {
            "from": self.from_email,
            "to": [to_email],
            "subject": subject,
            "html": html_content
        }
        
        headers = {
            "Authorization": f"Bearer {self.resend_api_key}",
            "Content-Type": "application/json"
        }
        
        try:
            response = requests.post(url, json=payload, headers=headers)
            
            if response.status_code == 200:
                logging.info("📧 Enhanced email summary sent successfully via Resend")
                return True
            else:
                logging.error(f"❌ Email failed: {response.status_code} - {response.text}")
                return False
                
        except Exception as e:
            logging.error(f"❌ Email error: {str(e)}")
            return False
    
    def _format_html_summary(self, companies_data: List[Dict], trends_data: Dict[str, Any]) -> str:
        """Format enhanced HTML summary with multi-ATS breakdown."""
        if not companies_data:
            return "<p>No companies collected yet.</p>"
        
        # Calculate basic stats
        greenhouse_companies = [c for c in companies_data if c.get('ats_type') == 'greenhouse']
        lever_companies = [c for c in companies_data if c.get('ats_type') == 'lever']
        total_jobs = sum(row.get('job_count', 0) or 0 for row in companies_data)
        total_remote = sum(row.get('remote_jobs_count', 0) or 0 for row in companies_data)
        total_hybrid = sum(row.get('hybrid_jobs_count', 0) or 0 for row in companies_data)
        total_onsite = sum(row.get('onsite_jobs_count', 0) or 0 for row in companies_data)
        
        html = f"""
        <html>
        <head>
            <style>
                body {{ font-family: Arial, sans-serif; margin: 20px; }}
                .header {{ background: #667eea; color: white; padding: 20px; border-radius: 10px; text-align: center; }}
                .stats {{ display: flex; justify-content: space-around; margin: 20px 0; flex-wrap: wrap; }}
                .stat-box {{ background: #f8f9fa; padding: 15px; border-radius: 8px; text-align: center; margin: 5px; }}
                .stat-number {{ font-size: 24px; font-weight: bold; color: #333; }}
                .stat-label {{ color: #666; margin-top: 5px; }}
                table {{ border-collapse: collapse; width: 100%; margin-top: 20px; }}
                th, td {{ border: 1px solid #ddd; padding: 12px; text-align: left; }}
                th {{ background-color: #f2f2f2; }}
                .ats-badge {{ padding: 3px 8px; border-radius: 3px; color: white; font-weight: bold; font-size: 11px; }}
                .greenhouse {{ background-color: #0066cc; }}
                .lever {{ background-color: #ff6600; }}
                .work-type-summary {{ background: #e8f4f8; padding: 15px; border-radius: 8px; margin: 20px 0; }}
            </style>
        </head>
        <body>
            <div class="header">
                <h1>🚀 Multi-ATS Market Intelligence</h1>
                <p>Real-time job market analysis • {datetime.utcnow().strftime('%B %d, %Y')}</p>
            </div>
            
            <div class="stats">
                <div class="stat-box">
                    <div class="stat-number">{len(companies_data):,}</div>
                    <div class="stat-label">Total Companies</div>
                </div>
                <div class="stat-box">
                    <div class="stat-number">{total_jobs:,}</div>
                    <div class="stat-label">Total Jobs</div>
                </div>
                <div class="stat-box">
                    <div class="stat-number">{len(greenhouse_companies)}</div>
                    <div class="stat-label">Greenhouse</div>
                </div>
                <div class="stat-box">
                    <div class="stat-number">{len(lever_companies)}</div>
                    <div class="stat-label">Lever</div>
                </div>
            </div>
            
            <div class="work-type-summary">
                <h3>Work Type Distribution</h3>
                <p>🏠 <strong>Remote:</strong> {total_remote:,} jobs ({total_remote/total_jobs*100:.1f}%)</p>
                <p>🏢 <strong>Hybrid:</strong> {total_hybrid:,} jobs ({total_hybrid/total_jobs*100:.1f}%)</p>
                <p>🏢 <strong>On-site:</strong> {total_onsite:,} jobs ({total_onsite/total_jobs*100:.1f}%)</p>
            </div>
            
            <h2>🏆 Top Hiring Companies</h2>
            <table>
                <tr>
                    <th>Rank</th>
                    <th>ATS</th>
                    <th>Company</th>
                    <th>Jobs</th>
                    <th>Remote</th>
                    <th>Hybrid</th>
                    <th>On-site</th>
                </tr>
        """
        
        # Sort companies by job count and show top 20
        sorted_companies = sorted(companies_data, key=lambda x: x.get('job_count', 0) or 0, reverse=True)
        for i, row in enumerate(sorted_companies[:20], 1):
            ats_type = row.get('ats_type', 'unknown')
            ats_class = 'greenhouse' if ats_type == 'greenhouse' else 'lever'
            
            # Create appropriate URL based on ATS type
            if ats_type == 'greenhouse':
                job_board_url = f"https://boards.greenhouse.io/{row.get('token', '')}"
            elif ats_type == 'lever':
                job_board_url = f"https://jobs.lever.co/{row.get('token', '')}"
            else:
                job_board_url = "#"
            
            html += f"""
            <tr>
                <td>{i}</td>
                <td><span class="ats-badge {ats_class}">{ats_type.upper()}</span></td>
                <td><a href="{job_board_url}" target="_blank">{row.get('company_name', 'Unknown')}</a></td>
                <td>{row.get('job_count', 0):,}</td>
                <td>{row.get('remote_jobs_count', 0) or 0}</td>
                <td>{row.get('hybrid_jobs_count', 0) or 0}</td>
                <td>{row.get('onsite_jobs_count', 0) or 0}</td>
            </tr>
            """
        
        # Add trends section if available
        trends_section = ""
        if trends_data and trends_data.get('current') and trends_data.get('previous'):
            current = trends_data['current']
            previous = trends_data['previous']
            
            if previous.get('total_jobs') and previous['total_jobs'] > 0:
                job_change = current.get('total_jobs', 0) - previous['total_jobs']
                job_change_pct = (job_change / previous['total_jobs']) * 100
                company_change = current.get('companies', 0) - previous.get('companies', 0)
                
                trends_section = f"""
                <div style="background: #f0f8ff; padding: 15px; border-radius: 8px; margin: 20px 0;">
                    <h3>📈 Month-over-Month Trends</h3>
                    <p><strong>Job Change:</strong> {job_change:+,} ({job_change_pct:+.1f}%) vs {trends_data.get('previous_month', '')}</p>
                    <p><strong>New Companies:</strong> +{company_change} companies discovered</p>
                </div>
                """
        
        html += f"""
            </table>
            
            {trends_section}
            
            <div style="margin-top: 30px; font-size: 12px; color: #666; border-top: 1px solid #eee; padding-top: 15px;">
                <p><strong>Generated:</strong> {datetime.utcnow().strftime('%Y-%m-%d %H:%M UTC')}</p>
                <p><strong>Next Collection:</strong> Automatic update in 6 hours</p>
                <p>Click company names to view current job postings • Data collected from {len(companies_data)} companies across Greenhouse and Lever ATS platforms</p>
            </div>
        </body>
        </html>
        """
        return html


class EnhancedATSCollector:
    """Enhanced collector supporting multiple ATS systems with full intelligence features."""
    
    def __init__(self, config_file: str = "config.ini", dry_run: bool = False):
        self.dry_run = dry_run
        self.config = Config(config_file)
        self._setup_logging()
        
        # Initialize components
        db_path = os.getenv('DB_PATH', 'greenhouse_tokens.db')
        self.db_manager = DatabaseManager(db_path)
        
        # Initialize ATS parsers
        self.greenhouse_parser = GreenhouseBoardParser(
            self.config.get('scraping', 'user_agent'),
            self.config.getint('scraping', 'timeout', 15)
        )
        self.lever_parser = LeverBoardParser(
            self.config.get('scraping', 'user_agent'),
            self.config.getint('scraping', 'timeout', 15)
        )
        
        # Setup email if enabled
        if self.config.getboolean('email', 'enabled', True) and os.getenv('RESEND_API_KEY'):
            self.email_reporter = EmailReporter()
        else:
            self.email_reporter = None
    
    def _setup_logging(self):
        """Configure logging."""
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[logging.StreamHandler()]
        )
    
    def process_seed_tokens(self) -> Tuple[int, int]:
        """Process known ATS tokens directly. Returns (greenhouse_processed, lever_processed)."""
        greenhouse_tokens = [token.strip() for token in 
                           self.config.get('seed_tokens', 'greenhouse_tokens', '').split(',') if token.strip()]
        lever_tokens = [token.strip() for token in 
                       self.config.get('seed_tokens', 'lever_tokens', '').split(',') if token.strip()]
        
        total_greenhouse = 0
        total_lever = 0
        
        # Process Greenhouse tokens
        if greenhouse_tokens:
            logging.info(f"Processing {len(greenhouse_tokens)} Greenhouse seed tokens")
            for token in greenhouse_tokens:
                if not TokenExtractor.validate_token(token):
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
                            self.db_manager.create_monthly_snapshot('greenhouse', token, company_name, job_count, work_type_counts)
                            total_greenhouse += 1
                            logging.info(f"Processed Greenhouse {token}: {company_name} ({job_count} jobs)")
                    
                    # Rate limiting
                    delay = random.uniform(
                        self.config.getint('scraping', 'min_delay', 5),
                        self.config.getint('scraping', 'max_delay', 15)
                    )
                    time.sleep(delay)
                    
                except Exception as e:
                    logging.error(f"Error processing Greenhouse seed token {token}: {e}")
        
        # Process Lever tokens
        if lever_tokens:
            logging.info(f"Processing {len(lever_tokens)} Lever seed tokens")
            for token in lever_tokens:
                if not TokenExtractor.validate_token(token):
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
                            self.db_manager.create_monthly_snapshot('lever', token, company_name, job_count, work_type_counts)
                            total_lever += 1
                            logging.info(f"Processed Lever {token}: {company_name} ({job_count} jobs)")
                    
                    # Rate limiting
                    delay = random.uniform(
                        self.config.getint('scraping', 'min_delay', 5),
                        self.config.getint('scraping', 'max_delay', 15)
                    )
                    time.sleep(delay)
                    
                except Exception as e:
                    logging.error(f"Error processing Lever seed token {token}: {e}")
        
        return total_greenhouse, total_lever
    
    def crawl_page(self, url: str) -> Tuple[int, int, int, int]:
        """Crawl a single page for ATS tokens. Returns (gh_found, gh_new, lv_found, lv_new)."""
        gh_found = gh_new = lv_found = lv_new = 0
        
        try:
            logging.info(f"Crawling: {url}")
            
            headers = {"User-Agent": self.config.get('scraping', 'user_agent')}
            response = requests.get(url, headers=headers, 
                                 timeout=self.config.getint('scraping', 'timeout', 15))
            
            if response.status_code != 200:
                logging.warning(f"Failed to fetch {url}: HTTP {response.status_code}")
                return 0, 0, 0, 0
            
            soup = BeautifulSoup(response.text, "html.parser")
            
            # Find all links that might contain ATS tokens
            for link in soup.find_all("a", href=True):
                href = link["href"]
                
                # Check for Greenhouse tokens
                greenhouse_token = TokenExtractor.extract_greenhouse_token(href)
                if greenhouse_token:
                    gh_found += 1
                    
                    if self.dry_run:
                        gh_new += 1
                        continue
                    
                    # Check if this is a new token
                    with self.db_manager.get_connection() as conn:
                        cursor = conn.execute("SELECT COUNT(*) FROM greenhouse_tokens WHERE token = ?", (greenhouse_token,))
                        is_new = cursor.fetchone()[0] == 0
                    
                    # Parse the board
                    company_name, job_count, locations, departments, job_titles, work_type_counts, job_details = \
                        self.greenhouse_parser.parse_board(greenhouse_token, self.config.getint('scraping', 'max_retries', 3))
                    
                    if company_name:
                        success = self.db_manager.upsert_company(
                            'greenhouse', greenhouse_token, url, company_name, job_count, 
                            locations, departments, job_titles, work_type_counts, job_details
                        )
                        
                        if success:
                            self.db_manager.create_monthly_snapshot('greenhouse', greenhouse_token, company_name, job_count, work_type_counts)
                            
                            if is_new:
                                gh_new += 1
                                logging.info(f"Discovered Greenhouse {greenhouse_token}: {company_name}")
                
                # Check for Lever tokens
                lever_token = TokenExtractor.extract_lever_token(href)
                if lever_token:
                    lv_found += 1
                    
                    if self.dry_run:
                        lv_new += 1
                        continue
                    
                    # Check if this is a new token
                    with self.db_manager.get_connection() as conn:
                        cursor = conn.execute("SELECT COUNT(*) FROM ats_companies WHERE ats_type = 'lever' AND token = ?", (lever_token,))
                        is_new = cursor.fetchone()[0] == 0
                    
                    # Parse the board
                    company_name, job_count, locations, departments, job_titles, work_type_counts, job_details = \
                        self.lever_parser.parse_board(lever_token, self.config.getint('scraping', 'max_retries', 3))
                    
                    if company_name:
                        success = self.db_manager.upsert_company(
                            'lever', lever_token, url, company_name, job_count, 
                            locations, departments, job_titles, work_type_counts, job_details
                        )
                        
                        if success:
                            self.db_manager.create_monthly_snapshot('lever', lever_token, company_name, job_count, work_type_counts)
                            
                            if is_new:
                                lv_new += 1
                                logging.info(f"Discovered Lever {lever_token}: {company_name}")
                
                # Rate limiting
                if greenhouse_token or lever_token:
                    delay = random.uniform(
                        self.config.getint('scraping', 'min_delay', 5),
                        self.config.getint('scraping', 'max_delay', 15)
                    )
                    time.sleep(delay)
        
        except Exception as e:
            logging.error(f"Error crawling {url}: {e}")
        
        return gh_found, gh_new, lv_found, lv_new
    
    def run(self) -> bool:
        """Run the complete enhanced collection process."""
        start_time = datetime.utcnow()
        logging.info(f"Starting enhanced multi-ATS collection {'(DRY RUN)' if self.dry_run else ''}")
        
        total_gh_processed = total_lv_processed = 0
        total_gh_discoveries = total_lv_discoveries = 0
        
        # Phase 1: Process known seed tokens
        seed_gh, seed_lv = self.process_seed_tokens()
        total_gh_processed += seed_gh
        total_lv_processed += seed_lv
        logging.info(f"Phase 1 Complete: Processed {seed_gh} Greenhouse and {seed_lv} Lever seed tokens")
        
        # Phase 2: Crawl seed URLs for new token discovery
        seed_urls = [url.strip() for url in 
                    self.config.get('seed_urls', 'urls', '').split('\n') if url.strip()]
        
        if seed_urls:
            logging.info(f"Phase 2: Crawling {len(seed_urls)} discovery URLs")
            
            for i, url in enumerate(seed_urls, 1):
                gh_found, gh_new, lv_found, lv_new = self.crawl_page(url)
                total_gh_processed += gh_found
                total_lv_processed += lv_found
                total_gh_discoveries += gh_new
                total_lv_discoveries += lv_new
                
                if gh_found > 0 or lv_found > 0:
                    logging.info(f"URL {i}/{len(seed_urls)}: Found {gh_found} Greenhouse ({gh_new} new), {lv_found} Lever ({lv_new} new)")
                
                # Add delay between pages
                delay = random.uniform(
                    self.config.getint('scraping', 'min_delay', 5),
                    self.config.getint('scraping', 'max_delay', 15)
                )
                time.sleep(delay)
            
            logging.info(f"Phase 2 Complete: Discovered {total_gh_discoveries} new Greenhouse and {total_lv_discoveries} new Lever companies")
        
        # Phase 3: Send enhanced email summary
        if (self.email_reporter and 
            not self.dry_run and 
            self.config.getboolean('email', 'enabled', True)):
            
            recipient = "andy.medici@gmail.com"
            companies_data = self.db_manager.get_all_companies()
            trends_data = self.db_manager.get_monthly_trends()
            self.email_reporter.send_summary(companies_data, trends_data, recipient)
        
        end_time = datetime.utcnow()
        duration = (end_time - start_time).total_seconds()
        
        logging.info(f"Enhanced collection completed in {duration/60:.1f} minutes")
        logging.info("Collection completed. Success: True")
        
        return True


def main():
    """Main entry point."""
    import argparse
    
    parser = argparse.ArgumentParser(description='Enhanced Multi-ATS Collector')
    parser.add_argument('--dry-run', action='store_true', help='Run without making changes')
    parser.add_argument('--verbose', '-v', action='store_true', help='Verbose logging')
    
    args = parser.parse_args()
    
    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)
    
    try:
        collector = EnhancedATSCollector(dry_run=args.dry_run)
        success = collector.run()
        return 0 if success else 1
    except Exception as e:
        logging.error(f"Fatal error: {e}")
        return 1


if __name__ == "__main__":
    exit(main())
