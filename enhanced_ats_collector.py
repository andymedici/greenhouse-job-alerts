"""
Enhanced Multi-ATS Collector with Historical Tracking and Market Intelligence
---------------------------------------------------------------------------
A comprehensive system supporting both Greenhouse and Lever ATS with:
- Monthly historical job count tracking
- Geographic expansion detection
- Work type classification (remote/hybrid/onsite)
- Market intelligence and trend analysis
- Automated email reporting with historical insights
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
import smtplib
import json
import logging
import os
import urllib.robotparser
from contextlib import contextmanager
from datetime import datetime, timedelta
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
            'smtp_server': 'smtp.gmail.com',
            'smtp_port': '587',
            'enabled': 'true'
        }
        
        # Known tokens for both ATS systems
        self.config['seed_tokens'] = {
            'greenhouse_tokens': 'stripe,notion,figma,discord,dropbox,zoom,doordash,instacart,robinhood,coinbase,plaid,openai,anthropic,airbnb,reddit,gitlab,hashicorp,mongodb,elastic,salesforce,snowflake,databricks,atlassian,asana,slack,okta,twilio,brex,mercury,ramp,checkr,chime,affirm,canva,flexport,benchling,retool,vercel,linear,23andme,shopify,tesla,netflix,spotify,unity,cloudflare,docker,intel,nvidia,apple,meta,google,microsoft',
            'lever_tokens': 'lever,uber,netflix,postmates,box,github,thumbtack,lyft,twitch,palantir,atlassian,asana,slack,zoom,calendly,linear,mixpanel,sendgrid,twilio,okta,auth0,datadog,newrelic,pagerduty,mongodb,elastic,cockroachlabs,brex,mercury,ramp,checkr,chime,affirm,klarna,stripe,square,adyen,marqeta'
        }
        
        # Fortune 500 and comprehensive discovery URLs
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
https://github.com/poteto/hiring-without-whiteboards
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
                    company_name TEXT,
                    FOREIGN KEY (token) REFERENCES greenhouse_tokens (token)
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
                    first_seen_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (token) REFERENCES greenhouse_tokens (token)
                )
            """)
            
            # Job details table for both ATS systems
            conn.execute("""
                CREATE TABLE IF NOT EXISTS job_details (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    ats_type TEXT DEFAULT 'greenhouse',
                    token TEXT,
                    job_title TEXT,
                    job_url TEXT,
                    location TEXT,
                    department TEXT,
                    work_type TEXT,
                    posted_date TEXT,
                    job_description_snippet TEXT,
                    first_seen TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    last_seen TIMESTAMP DEFAULT CURRENT_TIMESTAMP
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
                conn.execute("ALTER TABLE job_details ADD COLUMN ats_type TEXT DEFAULT 'greenhouse'")
            except sqlite3.OperationalError:
                pass  # Columns already exist
            
            # Create indexes for performance
            conn.execute("CREATE INDEX IF NOT EXISTS idx_monthly_token_date ON monthly_job_history(ats_type, token, year, month)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_tokens_last_seen ON greenhouse_tokens(last_seen)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_ats_companies_type ON ats_companies(ats_type)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_ats_companies_last_seen ON ats_companies(last_seen)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_discovery_time ON discovery_history(crawl_time)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_location_expansions ON location_expansions(ats_type, token, first_seen_date)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_job_details_ats ON job_details(ats_type, token)")
            
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
                
                # Store detailed job information
                if job_details:
                    conn.execute("DELETE FROM job_details WHERE ats_type=? AND token=?", (ats_type, token))
                    
                    for job in job_details[:100]:
                        conn.execute("""
                            INSERT INTO job_details 
                            (ats_type, token, job_title, job_url, location, 
                             department, work_type, posted_date, job_description_snippet)
                            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """, (ats_type, token, job.get('title', ''), job.get('url', ''), 
                             job.get('location', ''), job.get('department', ''),
                             job.get('work_type', ''), job.get('posted_date', ''),
                             job.get('description_snippet', '')))
                
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
                    
                    logging.info(f"🌍 Location expansion: {company_name} ({ats_type.upper()}) now hiring in {new_location}")
        
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
                    logging.info(f"📅 Monthly snapshot created for {ats_type.upper()} {token} - {now.strftime('%B %Y')}")
                    return True
                return False
        except Exception as e:
            logging.error(f"Error creating monthly snapshot for {ats_type} {token}: {e}")
            return False
    
    def log_discovery(self, url: str, ats_type: str, tokens_found: int, new_tokens: int, 
                     success: bool, error_message: str = None):
        """Log URL crawling results with ATS type."""
        try:
            with self.get_connection() as conn:
                conn.execute("""
                    INSERT INTO discovery_history (url, ats_type, tokens_found, new_tokens, success, error_message)
                    VALUES (?, ?, ?, ?, ?, ?)
                """, (url, ats_type, tokens_found, new_tokens, success, error_message))
                conn.commit()
        except Exception as e:
            logging.error(f"Failed to log discovery history: {e}")
    
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
            return True


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
    """Enhanced email reporting with multi-ATS support."""
    
    def __init__(self, smtp_server: str, smtp_port: int, smtp_user: str, smtp_pass: str):
        self.smtp_server = smtp_server
        self.smtp_port = smtp_port
        self.smtp_user = smtp_user
        self.smtp_pass = smtp_pass
    
    def send_summary(self, companies_data: List[Dict], trends_data: Dict[str, Any], recipient: str) -> bool:
        """Send enhanced email summary with multi-ATS support."""
        try:
            body = self._format_summary(companies_data, trends_data)
            html_body = self._format_html_summary(companies_data, trends_data)
            
            msg = MIMEMultipart('alternative')
            msg["Subject"] = f"Multi-ATS Market Intelligence - {datetime.utcnow().strftime('%Y-%m-%d')}"
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
            
            logging.info("Enhanced multi-ATS email summary sent successfully")
            return True
        except Exception as e:
            logging.error(f"Error sending email: {e}")
            return False
    
    def _format_summary(self, companies_data: List[Dict], trends_data: Dict[str, Any]) -> str:
        """Format enhanced text summary with multi-ATS breakdown."""
        if not companies_data:
            return "No companies collected yet."
        
        # Separate by ATS type
        greenhouse_companies = [c for c in companies_data if c.get('ats_type') == 'greenhouse']
        lever_companies = [c for c in companies_data if c.get('ats_type') == 'lever']
        
        total_jobs = sum(row.get('job_count', 0) or 0 for row in companies_data)
        total_remote = sum(row.get('remote_jobs_count', 0) or 0 for row in companies_data)
        total_hybrid = sum(row.get('hybrid_jobs_count', 0) or 0 for row in companies_data)
        total_onsite = sum(row.get('onsite_jobs_count', 0) or 0 for row in companies_data)
        
        lines = [
            f"Multi-ATS Market Intelligence Report",
            f"=" * 60,
            f"CURRENT MARKET SNAPSHOT",
            f"Total Companies Tracked: {len(companies_data):,}",
            f"  Greenhouse: {len(greenhouse_companies):,}",
            f"  Lever: {len(lever_companies):,}",
            f"Total Job Openings: {total_jobs:,}",
            f"  Remote: {total_remote:,} ({total_remote/total_jobs*100:.1f}%)" if total_jobs > 0 else "  Remote: 0",
            f"  Hybrid: {total_hybrid:,} ({total_hybrid/total_jobs*100:.1f}%)" if total_jobs > 0 else "  Hybrid: 0", 
            f"  On-site: {total_onsite:,} ({total_onsite/total_jobs*100:.1f}%)" if total_jobs > 0 else "  On-site: 0",
            ""
        ]
        
        # Add trends if available
        if trends_data and trends_data.get('current') and trends_data.get('previous'):
            current = trends_data['current']
            previous = trends_data['previous']
            
            if previous.get('total_jobs') and previous['total_jobs'] > 0:
                job_change = current.get('total_jobs', 0) - previous['total_jobs']
                job_change_pct = (job_change / previous['total_jobs']) * 100
                company_change = current.get('companies', 0) - previous.get('companies', 0)
                
                lines.extend([
                    f"MONTH-OVER-MONTH TRENDS ({trends_data.get('current_month', '')})",
                    f"Jobs Change: {job_change:+,} ({job_change_pct:+.1f}%) vs {trends_data.get('previous_month', '')}",
                    f"New Companies Discovered: +{company_change} companies",
                    ""
                ])
                
                # ATS breakdown
                if current.get('by_ats'):
                    lines.append("ATS Breakdown:")
                    for ats_type, data in current['by_ats'].items():
                        lines.append(f"  {ats_type.upper()}: {data.get('companies', 0)} companies, {data.get('total_jobs', 0):,} jobs")
                    lines.append("")
                
                # Location expansions
                if trends_data.get('location_expansions'):
                    lines.extend([
                        f"GEOGRAPHIC EXPANSIONS (Last 30 Days):",
                        f"Companies expanding to new locations:"
                    ])
                    for expansion in trends_data['location_expansions'][:10]:
                        ats_badge = f"[{row.get('ats_type', 'unknown').upper()}]"
            titles_preview = ", ".join(job_titles_list[:2]) + ("..." if len(job_titles_list) > 2 else "")
            
            lines.append(
                f"{i:2d}. {ats_badge} {row.get('company_name', 'Unknown')} ({row.get('token', '')})\n"
                f"     Jobs: {row.get('job_count', 0):,} | Remote:{remote_count} Hybrid:{hybrid_count} On-site:{onsite_count}\n"
                f"     Sample roles: {titles_preview}\n"
                f"     Locations: {(row.get('locations', 'Not specified') or 'Not specified')[:100]}"
            )
        
        lines.extend([
            "",
            f"Dashboard: Visit your Railway app URL for real-time data",
            f"Next update: Automatic collection every 6 hours",
            f"Report generated: {datetime.utcnow().strftime('%Y-%m-%d %H:%M UTC')}"
        ])
        
        return "\n".join(lines)
    
    def _format_html_summary(self, companies_data: List[Dict], trends_data: Dict[str, Any]) -> str:
        """Format enhanced HTML summary with multi-ATS breakdown."""
        if not companies_data:
            return "<p>No companies collected yet.</p>"
        
        # Separate by ATS type
        greenhouse_companies = [c for c in companies_data if c.get('ats_type') == 'greenhouse']
        lever_companies = [c for c in companies_data if c.get('ats_type') == 'lever']
        
        total_jobs = sum(row.get('job_count', 0) or 0 for row in companies_data)
        total_remote = sum(row.get('remote_jobs_count', 0) or 0 for row in companies_data)
        total_hybrid = sum(row.get('hybrid_jobs_count', 0) or 0 for row in companies_data)
        total_onsite = sum(row.get('onsite_jobs_count', 0) or 0 for row in companies_data)
        
        # Calculate trends
        trend_html = ""
        if trends_data and trends_data.get('current') and trends_data.get('previous'):
            current = trends_data['current']
            previous = trends_data['previous']
            
            if previous.get('total_jobs') and previous['total_jobs'] > 0:
                job_change = current.get('total_jobs', 0) - previous['total_jobs']
                job_change_pct = (job_change / previous['total_jobs']) * 100
                company_change = current.get('companies', 0) - previous.get('companies', 0)
                
                trend_color = "#28a745" if job_change >= 0 else "#dc3545"
                trend_icon = "📈" if job_change >= 0 else "📉"
                
                # ATS breakdown for trends
                ats_breakdown_html = ""
                if current.get('by_ats'):
                    ats_breakdown_html = "<h4>ATS Breakdown:</h4><ul>"
                    for ats_type, data in current['by_ats'].items():
                        ats_color = "#0066cc" if ats_type == 'greenhouse' else "#ff6600"
                        ats_breakdown_html += f"""
                        <li><span style="color: {ats_color}; font-weight: bold;">{ats_type.upper()}</span>: 
                        {data.get('companies', 0)} companies, {data.get('total_jobs', 0):,} jobs</li>
                        """
                    ats_breakdown_html += "</ul>"
                
                trend_html = f"""
                <div style="background-color: #e8f4fd; padding: 15px; border-radius: 5px; margin: 20px 0;">
                    <h3>{trend_icon} Month-over-Month Trends</h3>
                    <p><strong>Jobs Change:</strong> <span style="color: {trend_color};">{job_change:+,} ({job_change_pct:+.1f}%)</span> vs {trends_data.get('previous_month', '')}</p>
                    <p><strong>New Companies:</strong> +{company_change} companies discovered</p>
                    {ats_breakdown_html}
                    {f"<p><strong>New This Month:</strong> {len(trends_data.get('new_companies', []))} companies</p>" if trends_data.get('new_companies') else ""}
                </div>
                """
        
        # Location expansions section
        expansion_html = ""
        if trends_data.get('location_expansions'):
            expansion_html = f"""
            <div style="background-color: #f8f9fa; padding: 15px; border-radius: 5px; margin: 20px 0; border-left: 4px solid #17a2b8;">
                <h3>🌍 Geographic Expansions (Last 30 Days)</h3>
                <p>Companies expanding to new hiring locations:</p>
                <ul style="margin: 10px 0; padding-left: 20px;">
            """
            for expansion in trends_data['location_expansions'][:8]:
                ats_type = expansion.get('ats_type', 'unknown')
                ats_color = "#0066cc" if ats_type == 'greenhouse' else "#ff6600"
                expansion_html += f"""
                    <li style="margin: 5px 0;">
                        <span style="background-color: {ats_color}; color: white; padding: 2px 6px; border-radius: 3px; font-size: 10px; font-weight: bold;">{ats_type.upper()}</span>
                        <strong>{expansion.get('company_name', 'Unknown')}</strong> → {expansion.get('new_location', 'Unknown')} 
                        <span style="color: #666; font-size: 12px;">({expansion.get('job_count', 0)} total jobs)</span>
                    </li>
                """
            expansion_html += "</ul></div>"
        
        html = f"""
        <html>
        <head>
            <style>
                body {{ font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Arial, sans-serif; margin: 20px; line-height: 1.6; }}
                .header {{ background: linear-gradient(135deg, #667eea 0%, #764ba2 100%); color: white; padding: 20px; border-radius: 10px; text-align: center; }}
                .stats {{ display: flex; justify-content: space-around; margin: 20px 0; flex-wrap: wrap; }}
                .stat-box {{ background: #f8f9fa; padding: 15px; border-radius: 8px; text-align: center; min-width: 120px; margin: 5px; }}
                .stat-number {{ font-size: 24px; font-weight: bold; color: #333; }}
                .stat-label {{ font-size: 12px; color: #666; margin-top: 5px; }}
                .ats-breakdown {{ display: flex; justify-content: center; gap: 20px; margin: 15px 0; }}
                .ats-badge {{ padding: 8px 16px; border-radius: 5px; color: white; font-weight: bold; text-align: center; }}
                .greenhouse {{ background-color: #0066cc; }}
                .lever {{ background-color: #ff6600; }}
                table {{ border-collapse: collapse; width: 100%; margin-top: 20px; }}
                th, td {{ border: 1px solid #ddd; padding: 12px; text-align: left; }}
                th {{ background-color: #f2f2f2; font-weight: bold; }}
                tr:nth-child(even) {{ background-color: #f9f9f9; }}
                .company-name {{ font-weight: bold; color: #0066cc; }}
                .job-count {{ font-weight: bold; color: #28a745; }}
                .footer {{ margin-top: 30px; padding: 15px; background-color: #f8f9fa; border-radius: 5px; font-size: 12px; color: #666; }}
            </style>
        </head>
        <body>
            <div class="header">
                <h1>🚀 Multi-ATS Market Intelligence</h1>
                <p>Real-time job market analysis across Greenhouse & Lever • {datetime.utcnow().strftime('%B %d, %Y')}</p>
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
                    <div class="stat-number">{total_remote/total_jobs*100:.1f}%</div>
                    <div class="stat-label">Remote Jobs</div>
                </div>
                <div class="stat-box">
                    <div class="stat-number">{total_hybrid/total_jobs*100:.1f}%</div>
                    <div class="stat-label">Hybrid Jobs</div>
                </div>
            </div>
            
            <div class="ats-breakdown">
                <div class="ats-badge greenhouse">
                    Greenhouse: {len(greenhouse_companies)} companies
                </div>
                <div class="ats-badge lever">
                    Lever: {len(lever_companies)} companies
                </div>
            </div>
            
            {trend_html}
            {expansion_html}
            
            <h2>🏆 Top Hiring Companies</h2>
            <table>
                <tr>
                    <th>Rank</th>
                    <th>ATS</th>
                    <th>Company</th>
                    <th>Total Jobs</th>
                    <th>🏠 Remote</th>
                    <th>🏢 Hybrid</th>
                    <th>🏢 On-site</th>
                    <th>Locations</th>
                </tr>
        """
        
        # Sort companies by job count and show top 20
        sorted_companies = sorted(companies_data, key=lambda x: x.get('job_count', 0) or 0, reverse=True)
        for i, row in enumerate(sorted_companies[:20], 1):
            ats_type = row.get('ats_type', 'unknown')
            ats_color = "#0066cc" if ats_type == 'greenhouse' else "#ff6600"
            
            remote_count = row.get('remote_jobs_count', 0) or 0
            hybrid_count = row.get('hybrid_jobs_count', 0) or 0
            onsite_count = row.get('onsite_jobs_count', 0) or 0
            
            locations = (row.get('locations', '') or '')[:100]
            if len(locations) >= 100:
                locations += "..."
            
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
                <td><span style="background-color: {ats_color}; color: white; padding: 3px 8px; border-radius: 3px; font-size: 11px; font-weight: bold;">{ats_type.upper()}</span></td>
                <td class="company-name">
                    <a href="{job_board_url}" target="_blank">
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
                <p><strong>🤖 Multi-ATS Collection:</strong> Data refreshed every 6 hours from 150+ discovery sources</p>
                <p><strong>📊 Market Coverage:</strong> Tracking {len(companies_data)} companies across Greenhouse & Lever platforms</p>
                <p><strong>🔗 Live Data:</strong> Click company names to view current job postings</p>
                <p><strong>⏰ Generated:</strong> {datetime.utcnow().strftime('%Y-%m-%d %H:%M UTC')}</p>
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
        
        # Initialize robots checker
        if self.config.getboolean('scraping', 'respect_robots_txt', True):
            self.robots_checker = RobotsChecker(self.config.get('scraping', 'user_agent'))
        else:
            self.robots_checker = None
        
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
            logging.info(f"🚀 Processing {len(greenhouse_tokens)} Greenhouse seed tokens")
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
                            logging.info(f"✅ Processed Greenhouse {token}: {company_name} ({job_count} jobs - "
                                       f"Remote:{work_type_counts.get('remote', 0)} "
                                       f"Hybrid:{work_type_counts.get('hybrid', 0)} "
                                       f"On-site:{work_type_counts.get('onsite', 0)})")
                    else:
                        logging.warning(f"Failed to parse Greenhouse seed token: {token}")
                    
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
            logging.info(f"🚀 Processing {len(lever_tokens)} Lever seed tokens")
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
                            logging.info(f"✅ Processed Lever {token}: {company_name} ({job_count} jobs - "
                                       f"Remote:{work_type_counts.get('remote', 0)} "
                                       f"Hybrid:{work_type_counts.get('hybrid', 0)} "
                                       f"On-site:{work_type_counts.get('onsite', 0)})")
                    else:
                        logging.warning(f"Failed to parse Lever seed token: {token}")
                    
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
        if self.robots_checker and not self.robots_checker.can_fetch(url):
            logging.warning(f"Skipping {url} due to robots.txt restrictions")
            return 0, 0, 0, 0
        
        gh_found = gh_new = lv_found = lv_new = 0
        error_message = None
        
        try:
            logging.info(f"🔍 Crawling: {url}")
            
            headers = {"User-Agent": self.config.get('scraping', 'user_agent')}
            response = requests.get(url, headers=headers, 
                                 timeout=self.config.getint('scraping', 'timeout', 15))
            
            if response.status_code != 200:
                error_message = f"HTTP {response.status_code}"
                logging.warning(f"Failed to fetch {url}: {error_message}")
                if not self.dry_run:
                    self.db_manager.log_discovery(url, 'greenhouse', 0, 0, False, error_message)
                    self.db_manager.log_discovery(url, 'lever', 0, 0, False, error_message)
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
                        logging.info(f"[DRY RUN] Would process discovered Greenhouse token: {greenhouse_token}")
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
                                logging.info(f"🆕 Discovered Greenhouse {greenhouse_token}: {company_name} ({job_count} jobs)")
                            else:
                                logging.debug(f"♻️ Updated Greenhouse {greenhouse_token}: {company_name} ({job_count} jobs)")
                
                # Check for Lever tokens
                lever_token = TokenExtractor.extract_lever_token(href)
                if lever_token:
                    lv_found += 1
                    
                    if self.dry_run:
                        logging.info(f"[DRY RUN] Would process discovered Lever token: {lever_token}")
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
                                logging.info(f"🆕 Discovered Lever {lever_token}: {company_name} ({job_count} jobs)")
                            else:
                                logging.debug(f"♻️ Updated Lever {lever_token}: {company_name} ({job_count} jobs)")
                
                # Rate limiting between token processing
                if greenhouse_token or lever_token:
                    delay = random.uniform(
                        self.config.getint('scraping', 'min_delay', 5),
                        self.config.getint('scraping', 'max_delay', 15)
                    )
                    time.sleep(delay)
        
        except Exception as e:
            error_message = str(e)
            logging.error(f"Error crawling {url}: {e}")
        
        finally:
            # Log the crawl attempts
            if not self.dry_run:
                if gh_found > 0:
                    self.db_manager.log_discovery(url, 'greenhouse', gh_found, gh_new, error_message is None, error_message)
                if lv_found > 0:
                    self.db_manager.log_discovery(url, 'lever', lv_found, lv_new, error_message is None, error_message)
        
        return gh_found, gh_new, lv_found, lv_new
    
    def run(self) -> bool:
        """Run the complete enhanced collection process."""
        start_time = datetime.utcnow()
        logging.info(f"🚀 Starting enhanced multi-ATS collection {'(DRY RUN)' if self.dry_run else ''}")
        
        total_gh_processed = total_lv_processed = 0
        total_gh_discoveries = total_lv_discoveries = 0
        
        # Phase 1: Process known seed tokens
        seed_gh, seed_lv = self.process_seed_tokens()
        total_gh_processed += seed_gh
        total_lv_processed += seed_lv
        logging.info(f"✅ Phase 1 Complete: Processed {seed_gh} Greenhouse and {seed_lv} Lever seed tokens")
        
        # Phase 2: Crawl seed URLs for new token discovery
        seed_urls = [url.strip() for url in 
                    self.config.get('seed_urls', 'urls', '').split('\n') if url.strip()]
        
        if seed_urls:
            logging.info(f"🔍 Phase 2: Crawling {len(seed_urls)} discovery URLs")
            
            for i, url in enumerate(seed_urls, 1):
                gh_found, gh_new, lv_found, lv_new = self.crawl_page(url)
                total_gh_processed += gh_found
                total_lv_processed += lv_found
                total_gh_discoveries += gh_new
                total_lv_discoveries += lv_new
                
                if gh_found > 0 or lv_found > 0:
                    logging.info(f"📊 URL {i}/{len(seed_urls)}: Found {gh_found} Greenhouse ({gh_new} new), {lv_found} Lever ({lv_new} new)")
                
                # Add delay between pages
                delay = random.uniform(
                    self.config.getint('scraping', 'min_delay', 5),
                    self.config.getint('scraping', 'max_delay', 15)
                )
                time.sleep(delay)
            
            logging.info(f"✅ Phase 2 Complete: Discovered {total_gh_discoveries} new Greenhouse and {total_lv_discoveries} new Lever companies")
        else:
            logging.info("No seed URLs configured, skipping discovery crawling")
        
        # Phase 3: Send enhanced email summary
        if (self.email_reporter and 
            not self.dry_run and 
            self.config.getboolean('email', 'enabled', True)):
            
            recipient = os.getenv('EMAIL_RECIPIENT')
            if recipient:
                companies_data = self.db_manager.get_all_companies()
                trends_data = self.db_manager.get_monthly_trends()
                self.email_reporter.send_summary(companies_data, trends_data, recipient)
        
        end_time = datetime.utcnow()
        duration = (end_time - start_time).total_seconds()
        
        # Final summary
        with self.db_manager.get_connection() as conn:
            cursor = conn.execute("SELECT COUNT(*) FROM greenhouse_tokens")
            total_greenhouse_companies = cursor.fetchone()[0]
            
            cursor = conn.execute("SELECT COUNT(*) FROM ats_companies WHERE ats_type = 'lever'")
            total_lever_companies = cursor.fetchone()[0]
            
            cursor = conn.execute("""
                SELECT SUM(job_count) FROM greenhouse_tokens 
                UNION ALL 
                SELECT SUM(job_count) FROM ats_companies WHERE ats_type = 'lever'
            """)
            job_counts = cursor.fetchall()
            total_jobs = sum(row[0] or 0 for row in job_counts)
        
        total_companies = total_greenhouse_companies + total_lever_companies
        total_discoveries = total_gh_discoveries + total_lv_discoveries
        
        logging.info(f"🎉 Enhanced collection completed in {duration/60:.1f} minutes")
        logging.info(f"📊 Final Stats: {total_companies:,} companies ({total_greenhouse_companies} Greenhouse, {total_lever_companies} Lever) | {total_jobs:,} jobs | {total_discoveries} new discoveries")
        
        return True


def main():
    """Main entry point."""
    import argparse
    
    parser = argparse.ArgumentParser(description='Enhanced Multi-ATS Collector with Intelligence Features')
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
    exit(main())"[{expansion.get('ats_type', 'unknown').upper()}]"
                        lines.append(f"  {ats_badge} {expansion.get('company_name', 'Unknown')} -> {expansion.get('new_location', 'Unknown')} ({expansion.get('job_count', 0)} total jobs)")
                    lines.append("")
        
        lines.extend([
            f"TOP HIRING COMPANIES:",
            f"=" * 30
        ])
        
        # Sort companies by job count and show top 15
        sorted_companies = sorted(companies_data, key=lambda x: x.get('job_count', 0) or 0, reverse=True)
        for i, row in enumerate(sorted_companies[:15], 1):
            try:
                job_titles_list = json.loads(row.get('job_titles', '[]')) if row.get('job_titles') else []
            except:
                job_titles_list = []
            
            remote_count = row.get('remote_jobs_count', 0) or 0
            hybrid_count = row.get('hybrid_jobs_count', 0) or 0
            onsite_count = row.get('onsite_jobs_count', 0) or 0
            ats_badge = f
