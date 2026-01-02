#!/usr/bin/env python3
"""
Tests for Enterprise Security Scanner
Author: uldyssian-sh
"""

import pytest
import asyncio
from datetime import datetime
from unittest.mock import patch, MagicMock

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from enterprise_security_scanner import EnterpriseSecurityScanner, SecurityScanResult

class TestEnterpriseSecurityScanner:
    """Test suite for Enterprise Security Scanner"""
    
    @pytest.fixture
    def scanner(self):
        """Create scanner instance for testing"""
        return EnterpriseSecurityScanner()
    
    def test_scanner_initialization(self, scanner):
        """Test scanner initialization"""
        assert scanner is not None
        assert scanner.config is not None
        assert scanner.logger is not None
        assert scanner.scan_results == []
    
    def test_config_loading(self):
        """Test configuration loading"""
        scanner = EnterpriseSecurityScanner()
        
        # Test default configuration
        assert scanner.config["scan_depth"] == "comprehensive"
        assert "nist_csf" in scanner.config["compliance_frameworks"]
        assert scanner.config["severity_threshold"] == "medium"
    
    @pytest.mark.asyncio
    async def test_scan_infrastructure(self, scanner):
        """Test infrastructure scanning"""
        targets = ["test-server-01.example.com", "test-db.example.com"]
        
        results = await scanner.scan_infrastructure(targets)
        
        # Verify results
        assert len(results) > 0
        assert all(isinstance(result, SecurityScanResult) for result in results)
        assert len(scanner.scan_results) == len(results)
    
    @pytest.mark.asyncio
    async def test_scan_target(self, scanner):
        """Test individual target scanning"""
        target = "test-server.example.com"
        
        results = await scanner._scan_target(target)
        
        # Verify results structure
        assert len(results) >= 3  # network, application, compliance
        assert all(isinstance(result, SecurityScanResult) for result in results)
        
        # Verify result properties
        for result in results:
            assert result.scan_id is not None
            assert result.timestamp is not None
            assert result.severity in ["critical", "high", "medium", "low"]
            assert result.category is not None
            assert result.description is not None
            assert target in result.affected_resources
    
    @pytest.mark.asyncio
    async def test_network_security_scan(self, scanner):
        """Test network security scanning"""
        target = "test-server.example.com"
        
        results = await scanner._scan_network_security(target)
        
        assert len(results) == 1
        result = results[0]
        assert result.category == "network_security"
        assert result.severity == "medium"
        assert "nist_csf" in result.compliance_impact
    
    @pytest.mark.asyncio
    async def test_application_security_scan(self, scanner):
        """Test application security scanning"""
        target = "test-app.example.com"
        
        results = await scanner._scan_application_security(target)
        
        assert len(results) == 1
        result = results[0]
        assert result.category == "application_security"
        assert result.severity == "high"
        assert "SQL injection" in result.description
    
    @pytest.mark.asyncio
    async def test_compliance_check(self, scanner):
        """Test compliance checking"""
        target = "test-service.example.com"
        
        results = await scanner._check_compliance(target)
        
        assert len(results) == 1
        result = results[0]
        assert result.category == "compliance"
        assert result.severity == "low"
        assert "security headers" in result.description
    
    def test_generate_compliance_report_empty(self, scanner):
        """Test compliance report generation with no results"""
        report = scanner.generate_compliance_report()
        
        assert "error" in report
        assert report["error"] == "No scan results available"
    
    @pytest.mark.asyncio
    async def test_generate_compliance_report_with_results(self, scanner):
        """Test compliance report generation with scan results"""
        # Perform a scan first
        targets = ["test-server.example.com"]
        await scanner.scan_infrastructure(targets)
        
        # Generate report
        report = scanner.generate_compliance_report()
        
        # Verify report structure
        assert "scan_summary" in report
        assert "framework_analysis" in report
        assert "recommendations" in report
        assert "generated_at" in report
        
        # Verify scan summary
        summary = report["scan_summary"]
        assert "total_issues" in summary
        assert "severity_breakdown" in summary
        assert "compliance_score" in summary
        
        # Verify severity breakdown
        severity_breakdown = summary["severity_breakdown"]
        assert all(severity in severity_breakdown for severity in ["critical", "high", "medium", "low"])
        
        # Verify compliance score is reasonable
        assert 0 <= summary["compliance_score"] <= 100
    
    def test_generate_recommendations(self, scanner):
        """Test recommendation generation"""
        # Add some mock results
        scanner.scan_results = [
            SecurityScanResult(
                scan_id="test_1",
                timestamp=datetime.now(),
                severity="high",
                category="test",
                description="Test issue",
                affected_resources=["test"],
                remediation="Test remediation",
                compliance_impact={"nist_csf": "PR.AC-1"}
            )
        ]
        
        recommendations = scanner._generate_recommendations()
        
        assert len(recommendations) > 0
        assert isinstance(recommendations, list)
        assert all(isinstance(rec, str) for rec in recommendations)
        
        # Should include high-severity recommendation
        assert any("high-severity" in rec.lower() for rec in recommendations)
    
    def test_security_scan_result_dataclass(self):
        """Test SecurityScanResult dataclass"""
        result = SecurityScanResult(
            scan_id="test_scan_001",
            timestamp=datetime.now(),
            severity="medium",
            category="network_security",
            description="Test security issue",
            affected_resources=["server1", "server2"],
            remediation="Apply security patch",
            compliance_impact={"nist_csf": "PR.AC-1", "iso_27001": "A.9.1.1"}
        )
        
        assert result.scan_id == "test_scan_001"
        assert result.severity == "medium"
        assert result.category == "network_security"
        assert len(result.affected_resources) == 2
        assert len(result.compliance_impact) == 2

if __name__ == "__main__":
    pytest.main([__file__, "-v"])