"""
Slack Notification Script - NYC Taxi ETL Pipeline
Sends pipeline completion notifications via Slack webhook.
"""
import requests
import os
import json
from datetime import datetime
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Slack webhook URL from environment
SLACK_WEBHOOK_URL = os.environ.get('SLACK_WEBHOOK_URL', '')

# Paths for metrics
METRICS_PATH = '/opt/airflow/models/model_metrics.txt'
REPORT_DIR = '/opt/airflow/data/reports'


def get_pipeline_stats():
    """Gather pipeline statistics for notification."""
    stats = {
        'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
        'model_metrics': None,
        'report_generated': False
    }
    
    # Check for model metrics
    if os.path.exists(METRICS_PATH):
        with open(METRICS_PATH, 'r') as f:
            stats['model_metrics'] = f.read()
    
    # Check for report
    report_date = datetime.now().strftime('%Y-%m-%d')
    report_path = os.path.join(REPORT_DIR, f'taxi_report_{report_date}.pdf')
    stats['report_generated'] = os.path.exists(report_path)
    
    return stats


def send_slack_notification(success=True, error_message=None):
    """Send Slack notification about pipeline status."""
    
    if not SLACK_WEBHOOK_URL:
        logger.warning("SLACK_WEBHOOK_URL not configured. Skipping notification.")
        logger.info("To enable Slack notifications, set SLACK_WEBHOOK_URL in .env file")
        return False
    
    stats = get_pipeline_stats()
    
    if success:
        # Success message
        message = {
            "blocks": [
                {
                    "type": "header",
                    "text": {
                        "type": "plain_text",
                        "text": "✅ NYC Taxi ETL Pipeline Complete",
                        "emoji": True
                    }
                },
                {
                    "type": "section",
                    "fields": [
                        {
                            "type": "mrkdwn",
                            "text": f"*Completed:*\n{stats['timestamp']}"
                        },
                        {
                            "type": "mrkdwn",
                            "text": f"*Report Generated:*\n{'Yes ✅' if stats['report_generated'] else 'No ❌'}"
                        }
                    ]
                },
                {
                    "type": "section",
                    "text": {
                        "type": "mrkdwn",
                        "text": "*Pipeline Tasks Completed:*\n• Data cleaned and filtered\n• Weather data enriched\n• ML model trained\n• Data loaded to PostgreSQL\n• PDF report generated"
                    }
                },
                {
                    "type": "divider"
                },
                {
                    "type": "context",
                    "elements": [
                        {
                            "type": "mrkdwn",
                            "text": "📊 View dashboards in Power BI | 🔍 Check Airflow UI at http://localhost:8080"
                        }
                    ]
                }
            ]
        }
    else:
        # Error message
        message = {
            "blocks": [
                {
                    "type": "header",
                    "text": {
                        "type": "plain_text",
                        "text": "❌ NYC Taxi ETL Pipeline Failed",
                        "emoji": True
                    }
                },
                {
                    "type": "section",
                    "text": {
                        "type": "mrkdwn",
                        "text": f"*Error:*\n```{error_message or 'Unknown error'}```"
                    }
                },
                {
                    "type": "section",
                    "text": {
                        "type": "mrkdwn",
                        "text": f"*Timestamp:* {stats['timestamp']}"
                    }
                },
                {
                    "type": "context",
                    "elements": [
                        {
                            "type": "mrkdwn",
                            "text": "🔍 Check Airflow logs for details: http://localhost:8080"
                        }
                    ]
                }
            ]
        }
    
    try:
        response = requests.post(
            SLACK_WEBHOOK_URL,
            json=message,
            headers={'Content-Type': 'application/json'},
            timeout=10
        )
        response.raise_for_status()
        logger.info("Slack notification sent successfully!")
        return True
        
    except requests.exceptions.RequestException as e:
        logger.error(f"Failed to send Slack notification: {e}")
        return False


if __name__ == '__main__':
    # Test notification
    send_slack_notification(success=True)
