"""
Robust Railway Scheduler for Enhanced Multi-ATS Collector
--------------------------------------------------------
Includes comprehensive error handling to prevent crashes
"""

import os
import time
import threading
import schedule
import traceback
from datetime import datetime
from flask import Flask, jsonify, render_template_string
import logging

app = Flask(__name__)

# Track last run status
last_run_status = {
    'timestamp': None,
    'success': False,
    'companies_processed': 0,
    'greenhouse_count': 0,
    'lever_count': 0,
    'total_jobs': 0,
    'new_discoveries': 0,
    'location_expansions': 0,
    'error': None,
    'collector_type': 'Unknown',
    'import_status': 'Checking...'
}

# Global collector instance
collector_instance = None
COLLECTOR_TYPE = "Unknown"

def initialize_collector():
    """Initialize collector with comprehensive error handling."""
    global collector_instance, COLLECTOR_TYPE, last_run_status
    
    try:
        # Try enhanced collector first
        try:
            from enhanced_ats_collector import EnhancedATSCollector
            collector_instance = EnhancedATSCollector()
            COLLECTOR_TYPE = "Enhanced Multi-ATS"
            last_run_status['import_status'] = 'Enhanced Multi-ATS Loaded'
            logging.info("Successfully loaded Enhanced Multi-ATS Collector")
            return True
        except ImportError as e:
            logging.warning(f"Enhanced collector not available: {e}")
        
        # Try original enhanced version
        try:
            from greenhouse_collector import GreenhouseCollector
            collector_instance = GreenhouseCollector()
            COLLECTOR_TYPE = "Greenhouse Intelligence"
            last_run_status['import_status'] = 'Greenhouse Intelligence Loaded'
            logging.info("Using Greenhouse Intelligence collector")
            return True
        except ImportError as e:
            logging.error(f"No collector available: {e}")
        
        # If all fails, create a dummy collector
        collector_instance = None
        COLLECTOR_TYPE = "No Collector Available"
        last_run_status['import_status'] = 'No collector modules found'
        logging.error("No collector modules found!")
        return False
        
    except Exception as e:
        logging.error(f"Critical error initializing collector: {e}")
        logging.error(traceback.format_exc())
        collector_instance = None
        COLLECTOR_TYPE = "Initialization Failed"
        last_run_status['import_status'] = f'Initialization failed: {str(e)}'
        return False

def run_collector():
    """Run the collector with comprehensive error handling."""
    global last_run_status
    
    if collector_instance is None:
        logging.error("No collector available - skipping run")
        last_run_status = {
            'timestamp': datetime.utcnow().isoformat(),
            'success': False,
            'companies_processed': 0,
            'greenhouse_count': 0,
            'lever_count': 0,
            'total_jobs': 0,
            'new_discoveries': 0,
            'location_expansions': 0,
            'error': 'No collector available',
            'collector_type': COLLECTOR_TYPE,
            'import_status': last_run_status.get('import_status', 'Unknown')
        }
        return
    
    try:
        logging.info("Starting scheduled collection run...")
        
        # Run the collector
        success = collector_instance.run()
        
        # Try to get statistics - with error handling for each step
        companies_data = []
        trends_data = {}
        
        try:
            companies_data = collector_instance.db_manager.get_all_companies()
        except Exception as e:
            logging.error(f"Error getting companies data: {e}")
        
        try:
            trends_data = collector_instance.db_manager.get_monthly_trends()
        except Exception as e:
            logging.error(f"Error getting trends data: {e}")
        
        # Calculate statistics safely
        greenhouse_companies = [c for c in companies_data if c.get('ats_type') == 'greenhouse']
        lever_companies = [c for c in companies_data if c.get('ats_type') == 'lever']
        total_jobs = sum(row.get('job_count', 0) or 0 for row in companies_data)
        
        new_discoveries = len(trends_data.get('new_companies', []))
        location_expansions = len(trends_data.get('location_expansions', []))
        
        last_run_status = {
            'timestamp': datetime.utcnow().isoformat(),
            'success': success,
            'companies_processed': len(companies_data),
            'greenhouse_count': len(greenhouse_companies),
            'lever_count': len(lever_companies),
            'total_jobs': total_jobs,
            'new_discoveries': new_discoveries,
            'location_expansions': location_expansions,
            'error': None,
            'collector_type': COLLECTOR_TYPE,
            'import_status': last_run_status.get('import_status', 'Loaded')
        }
        
        logging.info(f"Collection completed successfully. "
                    f"Companies: {len(companies_data)}, "
                    f"Jobs: {total_jobs:,}")
        
    except Exception as e:
        error_msg = f"Collection failed: {str(e)}"
        logging.error(error_msg)
        logging.error(traceback.format_exc())
        
        last_run_status = {
            'timestamp': datetime.utcnow().isoformat(),
            'success': False,
            'companies_processed': 0,
            'greenhouse_count': 0,
            'lever_count': 0,
            'total_jobs': 0,
            'new_discoveries': 0,
            'location_expansions': 0,
            'error': error_msg,
            'collector_type': COLLECTOR_TYPE,
            'import_status': last_run_status.get('import_status', 'Unknown')
        }

def start_scheduler():
    """Start the background scheduler with error handling."""
    try:
        # Initialize collector first
        if not initialize_collector():
            logging.error("Collector initialization failed - scheduler will run but collections will fail")
        
        # Schedule runs every 6 hours
        schedule.every(6).hours.do(run_collector)
        
        # Also run daily at 9 AM UTC
        schedule.every().day.at("09:00").do(run_collector)
        
        # Run immediately on startup (with error handling)
        try:
            run_collector()
        except Exception as e:
            logging.error(f"Initial collection run failed: {e}")
            logging.error(traceback.format_exc())
        
        # Main scheduler loop
        while True:
            try:
                schedule.run_pending()
                time.sleep(60)  # Check every minute
            except Exception as e:
                logging.error(f"Scheduler loop error: {e}")
                time.sleep(60)  # Continue running even if there's an error
        
    except Exception as e:
        logging.error(f"Critical scheduler error: {e}")
        logging.error(traceback.format_exc())
        # Don't exit - keep the web server running

@app.route('/')
def status():
    """Status dashboard with error handling."""
    try:
        return render_template_string("""
        <!DOCTYPE html>
        <html>
        <head>
            <title>Multi-ATS Intelligence Dashboard</title>
            <meta http-equiv="refresh" content="300">
            <style>
                body { 
                    font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Arial, sans-serif; 
                    margin: 0; 
                    background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
                    min-height: 100vh;
                    color: #333;
                }
                .container { 
                    max-width: 1200px; 
                    margin: 0 auto; 
                    padding: 20px;
                }
                .header { 
                    background: rgba(255, 255, 255, 0.95); 
                    color: #333; 
                    padding: 30px; 
                    border-radius: 15px; 
                    margin-bottom: 30px; 
                    text-align: center;
                    box-shadow: 0 8px 32px rgba(0, 0, 0, 0.1);
                }
                .status-card { 
                    background: rgba(255, 255, 255, 0.95); 
                    padding: 25px; 
                    border-radius: 15px; 
                    margin-bottom: 20px;
                    box-shadow: 0 8px 32px rgba(0, 0, 0, 0.1);
                }
                .metrics-grid { 
                    display: grid; 
                    grid-template-columns: repeat(auto-fit, minmax(200px, 1fr)); 
                    gap: 20px; 
                    margin-bottom: 30px; 
                }
                .metric-card { 
                    background: rgba(255, 255, 255, 0.95); 
                    padding: 20px; 
                    border-radius: 15px; 
                    text-align: center;
                    box-shadow: 0 8px 32px rgba(0, 0, 0, 0.1);
                }
                .metric-number { 
                    font-size: 2em; 
                    font-weight: bold; 
                    color: #667eea; 
                    margin: 0;
                }
                .metric-label { 
                    color: #666; 
                    margin-top: 8px; 
                    font-size: 0.9em;
                }
                .status-indicator { 
                    width: 12px; 
                    height: 12px; 
                    border-radius: 50%; 
                    display: inline-block; 
                    margin-right: 8px;
                }
                .status-indicator.success { background: #28a745; }
                .status-indicator.error { background: #dc3545; }
                .status-indicator.warning { background: #ffc107; }
                .error-details { 
                    background: #f8d7da; 
                    color: #721c24; 
                    padding: 15px; 
                    border-radius: 8px; 
                    margin-top: 15px; 
                    font-family: monospace; 
                    font-size: 0.9em;
                    word-break: break-word;
                }
                .footer { 
                    text-align: center; 
                    margin-top: 40px; 
                    color: rgba(255, 255, 255, 0.8); 
                    font-size: 0.9em;
                }
                .footer a { 
                    color: rgba(255, 255, 255, 0.9); 
                    text-decoration: none; 
                    margin: 0 10px;
                    padding: 5px 10px;
                    border-radius: 5px;
                    background: rgba(255, 255, 255, 0.1);
                }
            </style>
        </head>
        <body>
            <div class="container">
                <div class="header">
                    <h1>Multi-ATS Intelligence Dashboard</h1>
                    <p>Status: {{ last_run.import_status }}</p>
                </div>
                
                <div class="metrics-grid">
                    <div class="metric-card">
                        <div class="metric-number">{{ last_run.companies_processed or 0 }}</div>
                        <div class="metric-label">Total Companies</div>
                    </div>
                    <div class="metric-card">
                        <div class="metric-number">{{ "{:,}".format(last_run.total_jobs or 0) }}</div>
                        <div class="metric-label">Job Openings</div>
                    </div>
                    <div class="metric-card">
                        <div class="metric-number">{{ last_run.greenhouse_count or 0 }}</div>
                        <div class="metric-label">Greenhouse</div>
                    </div>
                    <div class="metric-card">
                        <div class="metric-number">{{ last_run.lever_count or 0 }}</div>
                        <div class="metric-label">Lever</div>
                    </div>
                </div>
                
                <div class="status-card">
                    <h3>
                        <span class="status-indicator {{ 'success' if last_run.success else 'error' if last_run.timestamp else 'warning' }}"></span>
                        Collection Status
                    </h3>
                    
                    <p><strong>Collector Type:</strong> {{ last_run.collector_type }}</p>
                    <p><strong>Import Status:</strong> {{ last_run.import_status }}</p>
                    
                    {% if last_run.timestamp %}
                        <p><strong>Last Run:</strong> {{ last_run.timestamp }}</p>
                        <p><strong>Status:</strong> {{ 'Success' if last_run.success else 'Failed' }}</p>
                        
                        {% if last_run.success %}
                            <p><strong>Companies:</strong> {{ last_run.companies_processed }}</p>
                            <p><strong>Jobs:</strong> {{ "{:,}".format(last_run.total_jobs or 0) }}</p>
                            {% if last_run.new_discoveries > 0 %}
                                <p><strong>New Discoveries:</strong> {{ last_run.new_discoveries }}</p>
                            {% endif %}
                        {% endif %}
                        
                        {% if last_run.error %}
                            <div class="error-details">
                                <strong>Error Details:</strong><br>
                                {{ last_run.error }}
                            </div>
                        {% endif %}
                    {% else %}
                        <p>Collection starting soon...</p>
                    {% endif %}
                </div>
                
                <div class="status-card">
                    <h3>System Information</h3>
                    <p><strong>Database:</strong> {{ db_path }}</p>
                    <p><strong>Email Reports:</strong> {{ email_enabled }}</p>
                    <p><strong>Server Time:</strong> {{ current_time }}</p>
                </div>
                
                <div class="footer">
                    <p>Multi-ATS Intelligence Collector</p>
                    <a href="/api/status">JSON API</a>
                    <a href="/api/run">Manual Run</a>
                    <a href="/health">Health Check</a>
                </div>
            </div>
        </body>
        </html>
        """, 
        last_run=last_run_status,
        db_path=os.getenv('DB_PATH', 'greenhouse_tokens.db'),
        email_enabled='Enabled' if os.getenv('SMTP_USER') else 'Disabled',
        current_time=datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S UTC')
        )
    except Exception as e:
        logging.error(f"Error rendering status page: {e}")
        return f"<h1>Dashboard Error</h1><p>Error: {str(e)}</p>"

@app.route('/api/status')
def api_status():
    """JSON status endpoint."""
    try:
        return jsonify({
            'last_run': last_run_status,
            'environment': {
                'db_path': os.getenv('DB_PATH', 'greenhouse_tokens.db'),
                'email_enabled': bool(os.getenv('SMTP_USER')),
                'current_time': datetime.utcnow().isoformat(),
                'collector_available': collector_instance is not None,
                'collector_type': COLLECTOR_TYPE
            }
        })
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/run')
def manual_run():
    """Manually trigger a collection run."""
    try:
        def run_async():
            run_collector()
        
        threading.Thread(target=run_async).start()
        return jsonify({
            'message': 'Collection started', 
            'timestamp': datetime.utcnow().isoformat(),
            'collector_type': COLLECTOR_TYPE
        })
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/health')
def health_check():
    """Health check endpoint for Railway."""
    return jsonify({
        'status': 'healthy',
        'timestamp': datetime.utcnow().isoformat(),
        'service': 'multi-ats-collector',
        'collector_available': collector_instance is not None,
        'collector_type': COLLECTOR_TYPE,
        'last_run_success': last_run_status.get('success', False)
    })

@app.route('/debug')
def debug_info():
    """Debug information."""
    try:
        import sys
        import os
        
        debug_data = {
            'python_version': sys.version,
            'working_directory': os.getcwd(),
            'environment_vars': {
                'DB_PATH': os.getenv('DB_PATH'),
                'SMTP_USER': 'Set' if os.getenv('SMTP_USER') else 'Not set',
                'EMAIL_RECIPIENT': 'Set' if os.getenv('EMAIL_RECIPIENT') else 'Not set'
            },
            'files_in_directory': os.listdir('.'),
            'collector_status': {
                'instance_created': collector_instance is not None,
                'type': COLLECTOR_TYPE,
                'import_status': last_run_status.get('import_status')
            }
        }
        
        return jsonify(debug_data)
    except Exception as e:
        return jsonify({'error': str(e)}), 500

if __name__ == '__main__':
    # Setup comprehensive logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.StreamHandler()
        ]
    )
    
    logging.info("Starting Multi-ATS Intelligence Scheduler...")
    
    try:
        # Start scheduler in background thread with error handling
        scheduler_thread = threading.Thread(target=start_scheduler, daemon=True)
        scheduler_thread.start()
        logging.info("Scheduler thread started")
        
        # Start Flask web server
        port = int(os.environ.get('PORT', 8080))
        logging.info(f"Starting Flask server on port {port}")
        app.run(host='0.0.0.0', port=port, debug=False)
        
    except Exception as e:
        logging.error(f"Critical startup error: {e}")
        logging.error(traceback.format_exc())
        # Still try to start Flask even if scheduler fails
        try:
            port = int(os.environ.get('PORT', 8080))
            app.run(host='0.0.0.0', port=port, debug=False)
        except Exception as e2:
            logging.error(f"Flask startup failed: {e2}")
