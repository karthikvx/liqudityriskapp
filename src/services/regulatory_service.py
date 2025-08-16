import logging
from typing import List, Dict, Any
from decimal import Decimal
from datetime import datetime
from src.models.risk_metrics import RiskAlert, RiskMetrics, AlertSeverity
from src.config.regulatory_config import ALERT_THRESHOLDS
from src.services.notification_service import NotificationService
from src.utils.logger import setup_logger

logger = setup_logger(__name__)

class RegulatoryService:
    def __init__(self):
        self.notification_service = NotificationService()
    
    def check_regulatory_compliance(self, metrics: RiskMetrics) -> List[RiskAlert]:
        """Check all regulatory compliance and generate alerts"""
        alerts = []
        
        # Check LCR compliance
        lcr_alerts = self._check_lcr_compliance(metrics)
        alerts.extend(lcr_alerts)
        
        # Check NSFR compliance
        nsfr_alerts = self._check_nsfr_compliance(metrics)
        alerts.extend(nsfr_alerts)
        
        # Check concentration limits
        concentration_alerts = self._check_concentration_limits(metrics)
        alerts.extend(concentration_alerts)
        
        # Send critical alerts
        for alert in alerts:
            if alert.severity == AlertSeverity.CRITICAL:
                self.notification_service.send_critical_alert(alert)
        
        logger.info(f"Generated {len(alerts)} regulatory alerts")
        return alerts
    
    def _check_lcr_compliance(self, metrics: RiskMetrics) -> List[RiskAlert]:
        """Check LCR compliance and generate alerts"""
        alerts = []
        lcr_ratio = metrics.lcr_metrics.lcr_ratio
        thresholds = ALERT_THRESHOLDS['lcr']
        
        if lcr_ratio < thresholds['critical']:
            alerts.append(RiskAlert(
                alert_id=f"LCR_CRITICAL_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}",
                metric_type="lcr",
                severity=AlertSeverity.CRITICAL,
                threshold_breached=thresholds['critical'],
                current_value=lcr_ratio,
                message=f"LCR critically low at {lcr_ratio:.2f}% (threshold: {thresholds['critical']:.2f}%)"
            ))
        elif lcr_ratio < thresholds['warning']:
            alerts.append(RiskAlert(
                alert_id=f"LCR_WARNING_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}",
                metric_type="lcr",
                severity=AlertSeverity.WARNING,
                threshold_breached=thresholds['warning'],
                current_value=lcr_ratio,
                message=f"LCR below warning threshold at {lcr_ratio:.2f}% (threshold: {thresholds['warning']:.2f}%)"
            ))
        
        return alerts
    
    def _check_nsfr_compliance(self, metrics: RiskMetrics) -> List[RiskAlert]:
        """Check NSFR compliance and generate alerts"""
        alerts = []
        nsfr_ratio = metrics.nsfr_metrics.nsfr_ratio
        thresholds = ALERT_THRESHOLDS['nsfr']
        
        if nsfr_ratio < thresholds['critical']:
            alerts.append(RiskAlert(
                alert_id=f"NSFR_CRITICAL_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}",
                metric_type="nsfr",
                severity=AlertSeverity.CRITICAL,
                threshold_breached=thresholds['critical'],
                current_value=nsfr_ratio,
                message=f"NSFR critically low at {nsfr_ratio:.2f}% (threshold: {thresholds['critical']:.2f}%)"
            ))
        elif nsfr_ratio < thresholds['warning']:
            alerts.append(RiskAlert(
                alert_id=f"NSFR_WARNING_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}",
                metric_type="nsfr",
                severity=AlertSeverity.WARNING,
                threshold_breached=thresholds['warning'],
                current_value=nsfr_ratio,
                message=f"NSFR below warning threshold at {nsfr_ratio:.2f}% (threshold: {thresholds['warning']:.2f}%)"
            ))
        
        return alerts
    
    def _check_concentration_limits(self, metrics: RiskMetrics) -> List[RiskAlert]:
        """Check concentration limits and generate alerts"""
        alerts = []
        concentration = metrics.concentration_metrics
        thresholds = ALERT_THRESHOLDS['concentration']
        
        # Single counterparty concentration
        if concentration.counterparty_concentration_ratio > thresholds['single_counterparty'] * 100:
            alerts.append(RiskAlert(
                alert_id=f"CONC_COUNTERPARTY_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}",
                metric_type="concentration",
                severity=AlertSeverity.WARNING,
                threshold_breached=Decimal(str(thresholds['single_counterparty'] * 100)),
                current_value=concentration.counterparty_concentration_ratio,
                message=f"Counterparty concentration at {concentration.counterparty_concentration_ratio:.2f}% exceeds limit"
            ))
        
        # Sector concentration
        if concentration.sector_concentration_ratio > thresholds['sector'] * 100:
            alerts.append(RiskAlert(
                alert_id=f"CONC_SECTOR_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}",
                metric_type="concentration",
                severity=AlertSeverity.WARNING,
                threshold_breached=Decimal(str(thresholds['sector'] * 100)),
                current_value=concentration.sector_concentration_ratio,
                message=f"Sector concentration at {concentration.sector_concentration_ratio:.2f}% exceeds limit"
            ))
        
        return alerts
    
    def generate_regulatory_report(self, metrics: RiskMetrics) -> Dict[str, Any]:
        """Generate comprehensive regulatory report"""
        report = {
            "report_id": f"REG_REPORT_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}",
            "institution_id": metrics.institution_id,
            "business_date": metrics.business_date.isoformat(),
            "generated_at": datetime.utcnow().isoformat(),
            "basel_iii_compliance": {
                "lcr": {
                    "ratio": float(metrics.lcr_metrics.lcr_ratio),
                    "minimum_required": 100.0,
                    "compliant": metrics.lcr_metrics.lcr_ratio >= 100,
                    "buffer": float(metrics.lcr_metrics.lcr_ratio - 100)
                },
                "nsfr": {
                    "ratio": float(metrics.nsfr_metrics.nsfr_ratio),
                    "minimum_required": 100.0,
                    "compliant": metrics.nsfr_metrics.nsfr_ratio >= 100,
                    "buffer": float(metrics.nsfr_metrics.nsfr_ratio - 100)
                }
            },
            "concentration_risk": {
                "counterparty_max": float(metrics.concentration_metrics.counterparty_concentration_ratio),
                "sector_max": float(metrics.concentration_metrics.sector_concentration_ratio),
                "geography_max": float(metrics.concentration_metrics.geography_concentration_ratio)
            },
            "active_alerts": [
                {
                    "alert_id": alert.alert_id,
                    "severity": alert.severity.value,
                    "metric": alert.metric_type.value,
                    "message": alert.message
                } for alert in metrics.active_alerts
            ],
            "overall_compliance_score": float(metrics.get_overall_risk_score())
        }
        
        return report