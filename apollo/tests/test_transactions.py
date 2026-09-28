"""
    unit testing script for TransactionGenerator in transactions.py
    v0.1 - test fixtures and synthetic data setup for user pool management, schema validation, and streaming
    NOTE: SOME PARTS ARE AI ASSISTED
"""

import pytest
import logging
import asyncio
from decimal import Decimal
from uuid import UUID, uuid4
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import patch, MagicMock

from apollo.scrapers.transactions import TransactionGenerator, USER_POOL_FILE
from apollo.schemas import TransactionPayload

# ----------------------------------------------------------------------------------------------------------------------------------------------------------

# fixtures & synthetic test data

@pytest.fixture
def anyio_backend():
    return "asyncio"

@pytest.fixture
def sample_user_uuids() -> list[UUID]:
    """synthetic list of 1,000 deterministically generated UUID instances for user pool testing"""
    return [uuid4() for _ in range(1000)]

@pytest.fixture
def sample_valid_user_pool_file(tmp_path: Path, sample_user_uuids: list[UUID]) -> Path:
    """synthetic valid user_pool.txt file containing exactly 1,000 UUID strings separated by newlines"""
    pool_file = tmp_path / "data" / "user_pool.txt"
    pool_file.parent.mkdir(parents=True, exist_ok=True)
    with open(pool_file, "w", encoding="utf-8") as f:
        for u in sample_user_uuids:
            f.write(f"{u}\n")
    return pool_file

@pytest.fixture
def sample_empty_user_pool_file(tmp_path: Path) -> Path:
    """synthetic empty (0-byte) user_pool.txt file to test boundary handling when file exists but has no data"""
    pool_file = tmp_path / "data" / "user_pool.txt"
    pool_file.parent.mkdir(parents=True, exist_ok=True)
    pool_file.touch()
    return pool_file

@pytest.fixture
def sample_corrupted_user_pool_file(tmp_path: Path) -> Path:
    """synthetic corrupted user_pool.txt containing non-UUID lines to test exception handling"""
    pool_file = tmp_path / "data" / "user_pool.txt"
    pool_file.parent.mkdir(parents=True, exist_ok=True)
    with open(pool_file, "w", encoding="utf-8") as f:
        f.write("not-a-valid-uuid-record\nanother-invalid-line\n")
    return pool_file

@pytest.fixture
def sample_unwritable_user_pool_dir(tmp_path: Path) -> Path:
    """synthetic path inside a non-existent unwritable directory to test disk failure handling"""
    return tmp_path / "read_only_dir" / "user_pool.txt"

@pytest.fixture
def mock_transaction_generator(sample_valid_user_pool_file: Path, monkeypatch) -> TransactionGenerator:
    """pre-initialized TransactionGenerator instance isolated from production user_pool.txt"""
    monkeypatch.setattr("apollo.scrapers.transactions.USER_POOL_FILE", sample_valid_user_pool_file)
    return TransactionGenerator()