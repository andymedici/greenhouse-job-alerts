"""
Railway Scheduler for Enhanced Multi-ATS Collector with Intelligence Features
---------------------------------------------------------------------------
Runs the enhanced collector on a schedule and provides a comprehensive web interface
Supports both Greenhouse and Lever ATS systems with market intelligence analytics
"""

import os
import time
import threading
import schedule
from datetime import datetime
from flask import Flask, jsonify, render_template_string
import logging

# Import the enhanced collector with fallback
try:
    from enhanced_ats_collector import EnhancedATSCollector
    COLLECTOR_TYPE = "Enhanced Multi-ATS"
    logging.info("Loaded Enhanced Multi-ATS Collector")
except ImportError:
    try:
        # Fallback to the enhanced version from the document
        from greenhouse_collector import GreenhouseCollector as EnhancedATSCollector
        COLLECTOR_TYPE = "Greenhouse Intelligence"
        logging.warning("Enhanced collector not found, using Greenhouse Intelligence collector")
    except ImportError:
        # Final fallback to basic collector
        try:
            from greenhouse_collector import GreenhouseCollector as EnhancedATSCollector
            COLLECTOR_TYPE = "Basic Greenhouse"
            logging.warning("Using basic Greenhouse collector")
        except ImportError:
            logging.error("No collector module found!")
            raise

app = Flask(__name__)

# Enhanced tracking for multi-ATS status
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
    'collector_type': COLLECTOR_TYPE
}

def run_collector():
    """Run the enhanced multi-ATS collector"""
    global last_run_status
    
    try:
        logging.info("Starting scheduled multi-ATS collection run...")
        collector = EnhancedATSCollector()
        success = collector.run()
        
        # Get comprehensive statistics from database
        companies_data = collector.db_manager.get_all_companies()
        trends_data = collector.db_manager.get_monthly_trends()
        
        # Calculate detailed statistics
        greenhouse_companies = [c for c in companies_data if c.get('ats_type') == 'greenhouse']
        lever_companies = [c for c in companies_data if c.get('ats_type') == 'lever']
        total_jobs = sum(row.get('job_count', 0) or 0 for row in companies_data)
        
        # Count recent discoveries and expansions
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
            'collector_type': COLLECTOR_TYPE
        }
        
        logging.info(f"Collection completed. Success: {success}, "
                    f"Companies: {len(companies_data)} "
                    f"(GH: {len(greenhouse_companies)}, Lever: {len(lever_companies)}), "
                    f"Jobs: {total_jobs:,}, New: {new_discoveries}, Expansions: {location_expansions}")
        
    except Exception as e:
        logging.error(f"Collection failed: {e}")
        last_run_status = {
            'timestamp': datetime.utcnow().isoformat(),
            'success': False,
            'companies_processed': 0,
            'greenhouse_count': 0,
            'lever_count': 0,
            'total_jobs': 0,
            'new_discoveries': 0,
            'location_expansions': 0,
            'error': str(e),
            'collector_type': COLLECTOR_TYPE
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
    """Enhanced status dashboard with multi-ATS intelligence features"""
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
                max-width: 1400px; 
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
                backdrop-filter: blur(10px);
            }
            .header h1 { margin: 0; font-size: 2.5em; font-weight: 300; }
            .header p { margin: 10px 0 0 0; opacity: 0.8; }
            
            .status-grid { 
                display: grid; 
                grid-template-columns: repeat(auto-fit, minmax(300px, 1fr)); 
                gap: 20px; 
                margin-bottom: 30px; 
            }
            .status-card { 
                background: rgba(255, 255, 255, 0.95); 
                padding: 25px; 
                border-radius: 15px; 
                box-shadow: 0 8px 32px rgba(0, 0, 0, 0.1);
                backdrop-filter: blur(10px);
            }
            .status-card h3 { 
                margin: 0 0 20px 0; 
                color: #667eea; 
                font-size: 1.3em; 
                display: flex; 
                align-items: center; 
                gap: 10px;
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
                backdrop-filter: blur(10px);
            }
            .metric-number { 
                font-size: 2.5em; 
                font-weight: bold; 
                color: #667eea; 
                margin: 0;
                line-height: 1;
            }
            .metric-label { 
                color: #666; 
                margin-top: 8px; 
                font-size: 0.9em;
                text-transform: uppercase;
                letter-spacing: 1px;
            }
            .metric-change { 
                font-size: 0.8em; 
                margin-top: 5px; 
                padding: 3px 8px;
                border-radius: 12px;
                display: inline-block;
            }
            .metric-change.positive { background: #d4edda; color: #155724; }
            .metric-change.negative { background: #f8d7da; color: #721c24; }
            
            .ats-breakdown { 
                display: flex; 
                gap: 15px; 
                margin: 15px 0; 
                justify-content: center;
                flex-wrap: wrap;
            }
            .ats-badge { 
                padding: 8px 16px; 
                border-radius: 20px; 
                color: white; 
                font-weight: bold; 
                font-size: 0.9em;
                display: flex;
                align-items: center;
                gap: 8px;
            }
            .ats-badge.greenhouse { background: linear-gradient(135deg, #0066cc, #004499); }
            .ats-badge.lever { background: linear-gradient(135deg, #ff6600, #cc5200); }
            
            .status-indicator { 
                width: 12px; 
                height: 12px; 
                border-radius: 50%; 
                display: inline-block; 
            }
            .status-indicator.success { background: #28a745; }
            .status-indicator.error { background: #dc3545; }
            .status-indicator.running { background: #ffc107; animation: pulse 1.5s infinite; }
            
            @keyframes pulse {
                0% { opacity: 1; }
                50% { opacity: 0.5; }
                100% { opacity: 1; }
            }
            
            .info-grid { 
                display: grid; 
                grid-template-columns: repeat(auto-fit, minmax(400px, 1fr)); 
                gap: 20px; 
            }
            .info-card { 
                background: rgba(255, 255, 255, 0.95); 
                padding: 25px; 
                border-radius: 15px;
                box-shadow: 0 8px 32px rgba(0, 0, 0, 0.1);
                backdrop-filter: blur(10px);
            }
            .info-card h3 { 
                margin: 0 0 15px 0; 
                color: #667eea; 
                font-size: 1.2em;
            }
            .info-card ul { 
                margin: 0; 
                padding-left: 20px; 
            }
            .info-card li { 
                margin: 8px 0; 
                line-height: 1.5;
            }
            
            .feature-list { 
                list-style: none; 
                padding: 0; 
            }
            .feature-list li { 
                padding: 8px 0; 
                border-bottom: 1px solid #eee; 
                display: flex; 
                align-items: center; 
                gap: 10px;
            }
            .feature-list li:last-child { border-bottom: none; }
            .feature-icon { 
                width: 20px; 
                height: 20px; 
                border-radius: 50%; 
                background: #667eea; 
                color: white; 
                display: flex; 
                align-items: center; 
                justify-content: center; 
                font-size: 12px; 
                font-weight: bold;
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
                transition: background 0.3s;
            }
            .footer a:hover { background: rgba(255, 255, 255, 0.2); }
            
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
        </style>
    </head>
    <body>
        <div class="container">
            <div class="header">
                <h1>Multi-ATS Intelligence Dashboard</h1>
                <p>Advanced job market analysis across Greenhouse & Lever platforms</p>
                <div class="ats-breakdown">
                    <div class="ats-badge greenhouse">
                        <span class="status-indicator {{ 'success' if last_run.greenhouse_count > 0 else 'error' }}"></span>
                        Greenhouse
                    </div>
                    <div class="ats-badge lever">
                        <span class="status-indicator {{ 'success' if last_run.lever_count > 0 else 'error' }}"></span>
                        Lever
                    </div>
                </div>
            </div>
            
            <div class="metrics-grid">
                <div class="metric-card">
                    <div class="metric-number">{{ last_run.companies_processed or 0 }}</div>
                    <div class="metric-label">Total Companies</div>
                    {% if last_run.new_discoveries > 0 %}
                        <div class="metric-change positive">+{{ last_run.new_discoveries }} new</div>
                    {% endif %}
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
                <div class="metric-card">
                    <div class="metric-number">{{ last_run.location_expansions or 0 }}</div>
                    <div class="metric-label">Geographic Expansions</div>
                    <div class="metric-change positive">Last 30 days</div>
                </div>
            </div>
            
            <div class="status-grid">
                <div class="status-card">
                    <h3>
                        <span class="status-indicator {{ 'success' if last_run.success else 'error' if last_run.timestamp else 'running' }}"></span>
                        Collection Status
                    </h3>
                    {% if last_run.timestamp %}
                        <p><strong>Last Run:</strong> {{ last_run.timestamp }}</p>
                        <p><strong>Status:</strong> {{ 'Success' if last_run.success else 'Failed' }}</p>
                        <p><strong>Collector:</strong> {{ last_run.collector_type }}</p>
                        
                        {% if last_run.success %}
                            <ul class="feature-list">
                                <li>
                                    <span class="feature-icon">✓</span>
                                    Processed {{ last_run.companies_processed }} companies
                                </li>
                                <li>
                                    <span class="feature-icon">🏢</span>
                                    {{ last_run.greenhouse_count }} Greenhouse + {{ last_run.lever_count }} Lever
                                </li>
                                <li>
                                    <span class="feature-icon">💼</span>
                                    {{ "{:,}".format(last_run.total_jobs or 0) }} total job openings
                                </li>
                                {% if last_run.new_discoveries > 0 %}
                                <li>
                                    <span class="feature-icon">🆕</span>
                                    {{ last_run.new_discoveries }} new companies discovered
                                </li>
                                {% endif %}
                                {% if last_run.location_expansions > 0 %}
                                <li>
                                    <span class="feature-icon">🌍</span>
                                    {{ last_run.location_expansions }} location expansions tracked
                                </li>
                                {% endif %}
                            </ul>
                        {% endif %}
                        
                        {% if last_run.error %}
                            <div class="error-details">
                                <strong>Error Details:</strong><br>
                                {{ last_run.error }}
                            </div>
                        {% endif %}
                    {% else %}
                        <p>🚀 Collection starting soon...</p>
                        <p>First run will begin automatically after startup.</p>
                    {% endif %}
                </div>
                
                <div class="status-card">
                    <h3>🔄 Automation Schedule</h3>
                    <ul class="feature-list">
                        <li>
                            <span class="feature-icon">⏰</span>
                            Every 6 hours - Continuous monitoring
                        </li>
                        <li>
                            <span class="feature-icon">🌅</span>
                            Daily at 9:00 AM UTC - Scheduled collection
                        </li>
                        <li>
                            <span class="feature-icon">📧</span>
                            Automated email reports with trends
                        </li>
                        <li>
                            <span class="feature-icon">🔄</span>
                            Auto-refresh every 5 minutes
                        </li>
                    </ul>
                </div>
            </div>
            
            <div class="info-grid">
                <div class="info-card">
                    <h3>🎯 Intelligence Features</h3>
                    <ul>
                        <li><strong>Monthly Snapshots:</strong> Historical job count tracking</li>
                        <li><strong>Geographic Expansion:</strong> New location detection</li>
                        <li><strong>Market Trends:</strong> Month-over-month analysis</li>
                        <li><strong>Work Type Classification:</strong> Remote/Hybrid/On-site</li>
                        <li><strong>Discovery Analytics:</strong> New company identification</li>
                        <li><strong>Email Intelligence:</strong> Automated market reports</li>
                    </ul>
                </div>
                
                <div class="info-card">
                    <h3>🏢 ATS Platform Coverage</h3>
                    <ul>
                        <li><strong>Greenhouse:</strong> Leading ATS for tech companies</li>
                        <li><strong>Lever:</strong> Popular with growth-stage startups</li>
                        <li><strong>Fortune 500:</strong> Enterprise company coverage</li>
                        <li><strong>Startup Ecosystem:</strong> YC, Techstars, VCs</li>
                        <li><strong>Discovery Sources:</strong> 150+ seed URLs</li>
                        <li><strong>Compliance:</strong> robots.txt respected</li>
                    </ul>
                </div>
                
                <div class="info-card">
                    <h3>⚙️ System Configuration</h3>
                    <ul>
                        <li><strong>Database:</strong> {{ db_path }}</li>
                        <li><strong>Email Reports:</strong> {{ email_enabled }}</li>
                        <li><strong>Rate Limiting:</strong> Respectful crawling</li>
                        <li><strong>Error Handling:</strong> Automatic retries</li>
                        <li><strong>Data Validation:</strong> Quality assurance</li>
                        <li><strong>Monitoring:</strong> Comprehensive logging</li>
                    </ul>
                </div>
                
                <div class="info-card">
                    <h3>📊 Market Analytics</h3>
                    <ul>
                        <li><strong>Job Market Trends:</strong> Growth tracking</li>
                        <li><strong>Remote Work Analysis:</strong> Work type distribution</li>
                        <li><strong>Geographic Insights:</strong> Expansion patterns</li>
                        <li><strong>Company Growth:</strong> Hiring velocity</li>
                        <li><strong>Industry Coverage:</strong> Cross-sector analysis</li>
                        <li><strong>Competitive Intelligence:</strong> Market positioning</li>
                    </ul>
                </div>
            </div>
            
            <div class="footer">
                <p>Multi-ATS Intelligence Collector • Real-time job market analysis</p>
                <a href="/api/status">JSON API</a>
                <a href="/api/run">Manual Run</a>
                <a href="/api/stats">Statistics</a>
                <p>Current Server Time: {{ current_time }}</p>
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

@app.route('/api/status')
def api_status():
    """Enhanced JSON status endpoint with multi-ATS details"""
    return jsonify({
        'last_run': last_run_status,
        'environment': {
            'db_path': os.getenv('DB_PATH', 'greenhouse_tokens.db'),
            'email_enabled': bool(os.getenv('SMTP_USER')),
            'current_time': datetime.utcnow().isoformat(),
            'supported_ats': ['greenhouse', 'lever'],
            'collector_type': COLLECTOR_TYPE
        },
        'collection_info': {
            'schedule': ['Every 6 hours', 'Daily at 9:00 AM UTC'],
            'coverage': ['Fortune 500 companies', 'Tech startups', 'YC companies', 'Venture portfolios'],
            'features': ['Monthly snapshots', 'Location tracking', 'Market intelligence', 'Trend analysis']
        },
        'ats_breakdown': {
            'greenhouse': {
                'companies': last_run_status.get('greenhouse_count', 0),
                'platform': 'boards.greenhouse.io'
            },
            'lever': {
                'companies': last_run_status.get('lever_count', 0),
                'platform': 'jobs.lever.co'
            }
        }
    })

@app.route('/api/run')
def manual_run():
    """Manually trigger a collection run"""
    def run_async():
        run_collector()
    
    threading.Thread(target=run_async).start()
    return jsonify({
        'message': 'Multi-ATS collection started', 
        'timestamp': datetime.utcnow().isoformat(),
        'systems': ['greenhouse', 'lever'],
        'features': ['intelligence_tracking', 'location_expansion', 'monthly_snapshots']
    })

@app.route('/api/stats')
def api_stats():
    """Get detailed statistics with market intelligence"""
    try:
        stats = {
            'companies': {
                'total': last_run_status.get('companies_processed', 0),
                'greenhouse': last_run_status.get('greenhouse_count', 0),
                'lever': last_run_status.get('lever_count', 0)
            },
            'jobs': {
                'total': last_run_status.get('total_jobs', 0)
            },
            'intelligence': {
                'new_discoveries': last_run_status.get('new_discoveries', 0),
                'location_expansions': last_run_status.get('location_expansions', 0)
            },
            'collection': {
                'last_updated': last_run_status.get('timestamp'),
                'success_rate': 1.0 if last_run_status.get('success') else 0.0,
                'collector_type': last_run_status.get('collector_type', 'Unknown')
            },
            'coverage': {
                'ats_platforms': 2,
                'discovery_sources': '150+',
                'market_segments': ['Enterprise', 'Startups', 'Venture-backed', 'Public companies']
            }
        }
        return jsonify(stats)
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/trends')
def api_trends():
    """Get market trends and intelligence data"""
    try:
        # This would require access to the collector instance for real trends
        # For now, return structured format that matches the enhanced collector
        trends = {
            'monthly_growth': {
                'companies': last_run_status.get('new_discoveries', 0),
                'jobs': 'Calculated in email reports'
            },
            'geographic_expansion': {
                'recent_expansions': last_run_status.get('location_expansions', 0),
                'timeframe': 'Last 30 days'
            },
            'ats_distribution': {
                'greenhouse_share': last_run_status.get('greenhouse_count', 0),
                'lever_share': last_run_status.get('lever_count', 0)
            },
            'data_freshness': {
                'last_collection': last_run_status.get('timestamp'),
                'next_scheduled': 'Within 6 hours'
            }
        }
        return jsonify(trends)
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/health')
def health_check():
    """Health check endpoint for Railway"""
    return jsonify({
        'status': 'healthy',
        'timestamp': datetime.utcnow().isoformat(),
        'service': 'multi-ats-intelligence-collector',
        'collector_type': COLLECTOR_TYPE,
        'last_run_success': last_run_status.get('success', False),
        'companies_tracked': last_run_status.get('companies_processed', 0)
    })

if __name__ == '__main__':
    # Setup logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler('scheduler.log'),
            logging.StreamHandler()
        ]
    )
    
    # Start scheduler in background thread
    scheduler_thread = threading.Thread(target=start_scheduler, daemon=True)
    scheduler_thread.start()
    
    # Start Flask web server
    port = int(os.environ.get('PORT', 8080))
    app.run(host='0.0.0.0', port=port, debug=False)
