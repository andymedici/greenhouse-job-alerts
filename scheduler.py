"""
Simplified Multi-ATS Scheduler
-----------------------------
Direct database reads, no complex status tracking, prevents concurrent runs
"""

import os
import time
import threading
import schedule
import sqlite3
from datetime import datetime
from flask import Flask, jsonify, render_template_string
import logging

app = Flask(__name__)

# Simple status tracking
collection_running = False
last_collection_start = None

def get_db_stats():
    """Get statistics directly from database - simple and reliable."""
    try:
        db_path = os.getenv('DB_PATH', 'greenhouse_tokens.db')
        conn = sqlite3.connect(db_path, timeout=10)
        conn.row_factory = sqlite3.Row
        
        # Get Greenhouse companies
        cursor = conn.execute("SELECT COUNT(*) as count, SUM(job_count) as jobs FROM greenhouse_tokens")
        gh_data = cursor.fetchone()
        gh_companies = gh_data['count'] if gh_data else 0
        gh_jobs = gh_data['jobs'] if gh_data and gh_data['jobs'] else 0
        
        # Get Lever companies  
        cursor = conn.execute("SELECT COUNT(*) as count, SUM(job_count) as jobs FROM ats_companies WHERE ats_type = 'lever'")
        lv_data = cursor.fetchone()
        lv_companies = lv_data['count'] if lv_data else 0
        lv_jobs = lv_data['jobs'] if lv_data and lv_data['jobs'] else 0
        
        # Get recent activity
        cursor = conn.execute("SELECT COUNT(*) FROM greenhouse_tokens WHERE last_seen > datetime('now', '-1 day')")
        recent_updates = cursor.fetchone()[0]
        
        conn.close()
        
        return {
            'greenhouse_companies': gh_companies,
            'lever_companies': lv_companies,
            'total_companies': gh_companies + lv_companies,
            'greenhouse_jobs': gh_jobs,
            'lever_jobs': lv_jobs,
            'total_jobs': gh_jobs + lv_jobs,
            'recent_updates': recent_updates,
            'last_updated': datetime.utcnow().isoformat()
        }
    except Exception as e:
        logging.error(f"Database read error: {e}")
        return {
            'greenhouse_companies': 0,
            'lever_companies': 0,
            'total_companies': 0,
            'greenhouse_jobs': 0,
            'lever_jobs': 0,
            'total_jobs': 0,
            'recent_updates': 0,
            'last_updated': datetime.utcnow().isoformat(),
            'error': str(e)
        }

def run_collection():
    """Run collection with simple concurrency protection."""
    global collection_running, last_collection_start
    
    if collection_running:
        logging.info("Collection already running, skipping...")
        return
    
    try:
        collection_running = True
        last_collection_start = datetime.utcnow()
        
        logging.info("Starting collection...")
        
        # Import and run collector
        from enhanced_ats_collector import EnhancedATSCollector
        collector = EnhancedATSCollector()
        success = collector.run()
        
        logging.info(f"Collection completed. Success: {success}")
        
    except Exception as e:
        logging.error(f"Collection failed: {e}")
    finally:
        collection_running = False

def start_scheduler():
    """Start the background scheduler."""
    # Schedule every 6 hours
    schedule.every(6).hours.do(run_collection)
    
    # Daily at 9 AM UTC
    schedule.every().day.at("09:00").do(run_collection)
    
    # Run once on startup (after a delay)
    time.sleep(10)  # Let Flask start first
    run_collection()
    
    # Main loop
    while True:
        schedule.run_pending()
        time.sleep(60)

@app.route('/')
def dashboard():
    """Simple dashboard that reads directly from database."""
    stats = get_db_stats()
    
    return render_template_string("""
    <!DOCTYPE html>
    <html>
    <head>
        <title>Multi-ATS Dashboard</title>
        <meta http-equiv="refresh" content="60">
        <style>
            body { 
                font-family: Arial, sans-serif; 
                margin: 40px; 
                background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
                min-height: 100vh;
            }
            .container { 
                max-width: 1000px; 
                margin: 0 auto; 
                background: white; 
                padding: 30px; 
                border-radius: 15px;
                box-shadow: 0 10px 30px rgba(0,0,0,0.2);
            }
            .header { 
                text-align: center; 
                margin-bottom: 40px; 
                color: #333;
            }
            .metrics { 
                display: grid; 
                grid-template-columns: repeat(auto-fit, minmax(200px, 1fr)); 
                gap: 20px; 
                margin-bottom: 40px; 
            }
            .metric { 
                background: #f8f9fa; 
                padding: 20px; 
                border-radius: 10px; 
                text-align: center;
                border-left: 4px solid #667eea;
            }
            .metric-number { 
                font-size: 2.5em; 
                font-weight: bold; 
                color: #667eea; 
                margin: 0;
            }
            .metric-label { 
                color: #666; 
                margin-top: 10px; 
                font-weight: 500;
            }
            .status { 
                background: #e8f5e8; 
                padding: 20px; 
                border-radius: 10px; 
                margin-bottom: 20px;
                border-left: 4px solid #28a745;
            }
            .status.running { 
                background: #fff3cd; 
                border-left-color: #ffc107; 
            }
            .status.error { 
                background: #f8d7da; 
                border-left-color: #dc3545; 
            }
            .footer { 
                text-align: center; 
                color: #666; 
                margin-top: 30px; 
                font-size: 0.9em;
            }
            .ats-breakdown {
                display: grid;
                grid-template-columns: 1fr 1fr;
                gap: 20px;
                margin-top: 20px;
            }
            .ats-card {
                background: #f0f0f0;
                padding: 15px;
                border-radius: 8px;
                text-align: center;
            }
            .greenhouse { border-left: 4px solid #0066cc; }
            .lever { border-left: 4px solid #ff6600; }
        </style>
    </head>
    <body>
        <div class="container">
            <div class="header">
                <h1>Multi-ATS Intelligence Dashboard</h1>
                <p>Real-time job market monitoring</p>
            </div>
            
            <div class="status {{ 'running' if collection_running else 'error' if stats.get('error') else '' }}">
                <h3>System Status</h3>
                <p><strong>Collection Status:</strong> {{ 'Running' if collection_running else 'Idle' }}</p>
                <p><strong>Last Collection:</strong> {{ last_collection_start.strftime('%Y-%m-%d %H:%M UTC') if last_collection_start else 'Not started' }}</p>
                <p><strong>Database:</strong> {{ 'Connected' if not stats.get('error') else 'Error: ' + stats.get('error', '') }}</p>
                <p><strong>Recent Updates:</strong> {{ stats.recent_updates }} companies updated in last 24h</p>
            </div>
            
            <div class="metrics">
                <div class="metric">
                    <div class="metric-number">{{ stats.total_companies }}</div>
                    <div class="metric-label">Total Companies</div>
                </div>
                <div class="metric">
                    <div class="metric-number">{{ "{:,}".format(stats.total_jobs) }}</div>
                    <div class="metric-label">Total Jobs</div>
                </div>
                <div class="metric">
                    <div class="metric-number">{{ stats.greenhouse_companies }}</div>
                    <div class="metric-label">Greenhouse Companies</div>
                </div>
                <div class="metric">
                    <div class="metric-number">{{ stats.lever_companies }}</div>
                    <div class="metric-label">Lever Companies</div>
                </div>
            </div>
            
            <div class="ats-breakdown">
                <div class="ats-card greenhouse">
                    <h4>Greenhouse Platform</h4>
                    <p><strong>{{ stats.greenhouse_companies }}</strong> companies</p>
                    <p><strong>{{ "{:,}".format(stats.greenhouse_jobs) }}</strong> job openings</p>
                </div>
                <div class="ats-card lever">
                    <h4>Lever Platform</h4>
                    <p><strong>{{ stats.lever_companies }}</strong> companies</p>
                    <p><strong>{{ "{:,}".format(stats.lever_jobs) }}</strong> job openings</p>
                </div>
            </div>
            
            <div class="footer">
                <p>Auto-refresh every 60 seconds | Last updated: {{ stats.last_updated }}</p>
                <p><a href="/api/stats">View Raw Data</a> | <a href="/manual-run">Manual Collection</a></p>
            </div>
        </div>
    </body>
    </html>
    """, 
    stats=stats, 
    collection_running=collection_running, 
    last_collection_start=last_collection_start
    )

@app.route('/api/stats')
def api_stats():
    """Return raw statistics as JSON."""
    stats = get_db_stats()
    stats['collection_running'] = collection_running
    stats['last_collection_start'] = last_collection_start.isoformat() if last_collection_start else None
    return jsonify(stats)

@app.route('/manual-run')
def manual_run():
    """Trigger manual collection (only if not already running)."""
    if collection_running:
        return jsonify({
            'status': 'error',
            'message': 'Collection already running',
            'collection_running': True
        })
    
    # Start collection in background
    threading.Thread(target=run_collection, daemon=True).start()
    
    return jsonify({
        'status': 'success',
        'message': 'Collection started',
        'timestamp': datetime.utcnow().isoformat()
    })

@app.route('/health')
def health():
    """Health check for Railway."""
    stats = get_db_stats()
    return jsonify({
        'status': 'healthy',
        'timestamp': datetime.utcnow().isoformat(),
        'database_connected': not bool(stats.get('error')),
        'total_companies': stats['total_companies']
    })

if __name__ == '__main__':
    # Setup logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )
    
    logging.info("Starting simplified Multi-ATS scheduler...")
    
    # Start scheduler in background
    scheduler_thread = threading.Thread(target=start_scheduler, daemon=True)
    scheduler_thread.start()
    
    # Start Flask
    port = int(os.environ.get('PORT', 8080))
    app.run(host='0.0.0.0', port=port, debug=False)
