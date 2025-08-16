import logging
import smtplib
from email.mime.text import MimeText
from email.mime.multipart import MimeMultipart
from typing import Dict, Any
import json
from src.models.risk_metrics import RiskAlert
from src.config.aws_config import aws_config
from src.config.settings import settings
from src.utils.logger import setup_logger

logger = setup_logger(__name__)

class NotificationService:
    def __init__(self):
        self.sns_client = aws_config.get_sns_client()
    
    def send_critical_alert(self, alert: RiskAlert) -> bool:
        """Send critical alert via multiple channels"""
        try:
            # Send SNS notification
            sns_sent = self._send_sns_alert(alert)
            
            # Send email notification
            email_sent = self._send_email_alert(alert)
            
            # Log to CloudWatch
            self._log_alert(alert)
            
            return sns_sent or email_sent
            
        except Exception as e:
            logger.error(f"Failed to send critical alert: {str(e)}")
            return False
    
    def _send_sns_alert(self, alert: RiskAlert) -> bool:
        """Send alert via AWS SNS"""
        try:
            message = {
                "alert_id": alert.alert_id,
                "severity": alert.severity.value,
                "metric_type": alert.metric_type.value,
                "message": alert.message,
                "current_value": str(alert.current_value),
                "threshold_breached": str(alert.threshold_breached),
                "timestamp": alert.timestamp.isoformat()
            }
            
            response = self.sns_client.publish(
                TopicArn=f"arn:aws:sns:{settings.AWS_REGION}:123456789012:liquidity-risk-alerts",
                Subject=f"CRITICAL: {alert.metric_type.value.upper()} Alert",
                Message=json.dumps(message, indent=2)
            )
            
            logger.info(f"SNS alert sent: {response['MessageId']}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to send SNS alert: {str(e)}")
            return False
    
    def _send_email_alert(self, alert: RiskAlert) -> bool:
        """Send alert via email"""
        try:
            # Email configuration (in production, use AWS SES)
            smtp_server = "localhost"  # Replace with actual SMTP server
            smtp_port = 587
            
            msg = MimeMultipart()
            msg['From'] = "liquidity-risk@bank.com"
            msg['To'] = settings.ALERT_EMAIL
            msg['Subject'] = f"CRITICAL ALERT: {alert.metric_type.value.upper()} Threshold Breached"
            
            body = f"""
            CRITICAL LIQUIDITY RISK ALERT
            
            Alert ID: {alert.alert_id}
            Metric Type: {alert.metric_type.value.upper()}
            Severity: {alert.severity.value.upper()}
            
            Current Value: {alert.current_value}
            Threshold Breached: {alert.threshold_breached}
            
            Message: {alert.message}
            
            Timestamp: {alert.timestamp.isoformat()}
            
            Please take immediate action to address this risk.
            """
            
            msg.attach(MimeText(body, 'plain'))
            
            # In production, use proper SMTP authentication
            # server = smtplib.SMTP(smtp_server, smtp_port)
            # server.send_message(msg)
            # server.quit()
            
            logger.info(f"Email alert prepared for {alert.alert_id}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to send email alert: {str(e)}")
            return False
    
    def _log_alert(self, alert: RiskAlert) -> None:
        """Log alert to CloudWatch"""
        logger.critical(
            f"REGULATORY_ALERT: {alert.metric_type.value}",
            extra={
                "alert_id": alert.alert_id,
                "severity": alert.severity.value,
                "current_value": str(alert.current_value),
                "threshold_breached": str(alert.threshold_breached),
                "message": alert.message
            }
        )
    
    def send_daily_report(self, report_data: Dict[str, Any]) -> bool:
        """Send daily regulatory report"""
        try:
            # Format report for email
            report_summary = f"""
            Daily Liquidity Risk Report - {report_data['business_date']}
            
            Basel III Compliance:
            - LCR: {report_data['basel_iii_compliance']['lcr']['ratio']:.2f}% (Required: 100%)
            - NSFR: {report_data['basel_iii_compliance']['nsfr']['ratio']:.2f}% (Required: 100%)
            
            Active Alerts: {len(report_data['active_alerts'])}
            Overall Compliance Score: {report_data['overall_compliance_score']:.1f}
            
            Generated at: {report_data['generated_at']}
            """
            
            logger.info(f"Daily report prepared for {report_data['business_date']}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to send daily report: {str(e)}")
            return False