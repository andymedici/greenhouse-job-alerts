# 🔍 Greenhouse Token Collector

An automated system for discovering and monitoring job opportunities from companies using Greenhouse ATS (Applicant Tracking System).

## 🎯 Features

- **Token Discovery**: Crawls seed URLs to find Greenhouse job boards
- **Work Type Classification**: Automatically categorizes jobs as remote, hybrid, or onsite
- **Comprehensive Data**: Collects company info, job titles, locations, departments
- **Email Reports**: Automated daily summaries with job statistics
- **Web Dashboard**: Real-time status monitoring (Railway deployment)
- **Scheduled Runs**: Automatic data collection every 6 hours

## 📊 What It Collects

For each company:
- 🏢 Company name and Greenhouse token
- 📈 Total job count and breakdown by work type
- 📍 Job locations and departments
- 💼 Individual job titles and URLs
- 📅 First/last seen timestamps

## 🚀 Quick Start

### Local Usage

1. **Install dependencies:**
```bash
pip install requests beautifulsoup4 python-dotenv
```

2. **Set up environment variables:**
```bash
# Create .env file
SMTP_USER=your_email@gmail.com
SMTP_PASS=your_gmail_app_password
EMAIL_RECIPIENT=recipient@example.com
```

3. **Run the collector:**
```bash
# Test run (no changes made)
python greenhouse_collector.py --dry-run

# Full run
python greenhouse_collector.py
```

### Railway Deployment

1. **Fork this repository**
2. **Connect to Railway**: [railway.app](https://railway.app)
3. **Deploy from GitHub**
4. **Set environment variables** in Railway dashboard
5. **Add persistent volume**: `/app/data`

## 📧 Email Setup (Gmail)

1. Enable 2-factor authentication on Gmail
2. Generate an App Password: Google Account → Security → App passwords
3. Use the app password (not your regular password) for `SMTP_PASS`

## 🗄️ Database Schema

### greenhouse_tokens table
- Company metadata and job counts
- Work type breakdown (remote/hybrid/onsite)
- Location and department information

### job_details table  
- Individual job listings
- Work type classification
- Job URLs and descriptions

## 🔧 Configuration

The system auto-generates `config.ini` with:
- **200+ seed tokens** from major tech companies
- **40+ seed URLs** for token discovery  
- **Configurable delays** and retry logic
- **Email and database settings**

## 📈 Sample Output

```
📊 Summary Statistics
Total Companies: 150
Total Jobs: 2,847
  🏠 Remote: 1,203 (42.3%)
  🏢 Hybrid: 487 (17.1%) 
  🏢 On-site: 1,157 (40.6%)
```

## 🤖 Future: Job Alert System

This collector provides the foundation for a job alert system that can:
- Monitor job changes in real-time
- Send personalized job notifications
- Filter by work type, location, keywords
- Track application opportunities

## 📝 License

MIT License - feel free to use and modify!

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Submit a pull request

## ⚖️ Compliance

- Respects robots.txt
- Implements rate limiting
- Uses appropriate delays between requests
- Targets publicly available job boards only

## 📞 Support

For issues or questions, please open a GitHub issue.