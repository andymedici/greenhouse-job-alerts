"""
Railway Scheduler for Greenhouse Collector
Runs the collector on a schedule and provides a web interface
"""

import os
import time
import threading
import schedule
from datetime import datetime
from flask import Flask, jsonify, render_template_string
from greenhouse_collector import GreenhouseCollector
import logging

app = Flask(__name__)

# Track last run status
last_run_status = {
    'timestamp': None,
    'success': False,
    'tokens_processed': 0,
    'error': None
}

def run_collector():
    """Run the greenhouse collector"""
    global last_run_status
    
    try:
        logging.info("Starting scheduled collection run...")
        collector = GreenhouseCollector()
        success = collector.run()
        
        # Get total tokens from database for reporting
        tokens_data = collector.db_manager.get_all_tokens()
        
        last_run_status = {
            'timestamp': datetime.utcnow().isoformat(),
            'success': success,
            'tokens_processed': len(tokens_data),
            'error': None
        }
        
        logging.info(f"Collection completed. Success: {success}, Tokens: {len(tokens_data)}")
        
    except Exception as e:
        logging.error(f"Collection failed: {e}")
        last_run_status = {
            'timestamp': datetime.utcnow().isoformat(),
            'success': False,
            'tokens_processed': 0,
            'error': str(e)
        }

def start_scheduler():
    """Start the background scheduler"""
    # Schedule runs every 6 hours
    schedule.every(6).hours.do(run_collector)
    
    # Also run daily at 9 AM UTC
    schedule.every().day.at("09:00").do(run_collector)
    
    # Run immediately on startup
    run_collector()
    
    while True:
        schedule.run_pending()
        time.sleep(60)  # Check every minute

@app.route('/')
def status():
    """Status endpoint"""
    return render_template_string("""
    <!DOCTYPE html>
    <html>
    <head>
        <title>Greenhouse Collector Status</title>
        <meta http-equiv="refresh" content="300">
        <style>
            body { font-family: Arial, sans-serif; margin: 40px; }
            .status { padding: 20px; border-radius: 5px; margin: 20px 0; }
            .success { background-color: #d4edda; border: 1px solid #c3e6cb; }
            .error { background-color: #f8d7da; border: 1px solid #f5c6cb; }
            .info { background-color: #d1ecf1; border: 1px solid #bee5eb; }
        </style>
    </head>
    <body>
        <h1>🔍 Greenhouse Token Collector</h1>
        
        <div class="status {{ 'success' if last_run.success else 'error' if last_run.timestamp else 'info' }}">
            <h3>Last Run Status</h3>
            {% if last_run.timestamp %}
                <p><strong>Time:</strong> {{ last_run.timestamp }}</p>
                <p><strong>Status:</strong> {{ '✅ Success' if last_run.success else '❌ Failed' }}</p>
                <p><strong>Tokens Processed:</strong> {{ last_run.tokens_processed }}</p>
                {% if last_run.error %}
                    <p><strong>Error:</strong> {{ last_run.error }}</p>
                {% endif %}
            {% else %}
                <p>No runs completed yet. Collection starting soon...</p>
            {% endif %}
        </div>
        
        <div class="info">
            <h3>Schedule</h3>
            <p>🕘 Every 6 hours</p>
            <p>🌅 Daily at 9:00 AM UTC</p>
            <p>🔄 Page auto-refreshes every 5 minutes</p>
        </div>
        
        <div class="info">
            <h3>Environment</h3>
            <p><strong>Database:</strong> {{ db_path }}</p>
            <p><strong>Email Enabled:</strong> {{ email_enabled }}</p>
            <p><strong>Current Time:</strong> {{ current_time }}</p>
        </div>
    </body>
    </html>
    """, 
    last_run=last_run_status,
    db_path=os.getenv('DB_PATH', 'greenhouse_tokens.db'),
    email_enabled='✅' if os.getenv('SMTP_USER') else '❌',
    current_time=datetime.utcnow().isoformat()
    )

@app.route('/api/status')
def api_status():
    """JSON status endpoint"""
    return jsonify({
        'last_run': last_run_status,
        'environment': {
            'db_path': os.getenv('DB_PATH', 'greenhouse_tokens.db'),
            'email_enabled': bool(os.getenv('SMTP_USER')),
            'current_time': datetime.utcnow().isoformat()
        }
    })

@app.route('/api/run')
def manual_run():
    """Manually trigger a collection run"""
    def run_async():
        run_collector()
    
    threading.Thread(target=run_async).start()
    return jsonify({'message': 'Collection started', 'timestamp': datetime.utcnow().isoformat()})

if __name__ == '__main__':
    # Start scheduler in background thread
    scheduler_thread = threading.Thread(target=start_scheduler, daemon=True)
    scheduler_thread.start()
    
    # Start Flask web server
    port = int(os.environ.get('PORT', 8080))
    app.run(host='0.0.0.0', port=port)