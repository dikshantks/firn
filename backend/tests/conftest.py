"""Pytest configuration and fixtures."""

import pytest
from fastapi.testclient import TestClient

from app.main import app


@pytest.fixture
def client():
    """Create a test client for the FastAPI app."""
    return TestClient(app)


@pytest.fixture
def mock_catalog_config():
    """Sample catalog configuration for testing."""
    return {
        "name": "test-hive",
        "type": "hive",
        "properties": {
            "uri": "thrift://localhost:9083",
            "s3.endpoint": "http://localhost:9000",
            "s3.access-key-id": "minioadmin",
            "s3.secret-access-key": "minioadmin",
        },
    }
