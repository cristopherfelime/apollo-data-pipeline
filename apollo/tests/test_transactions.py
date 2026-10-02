"""
    unit testing script for TransactionGenerator in transactions.py
    v0.1 - test fixtures and synthetic data setup for user pool management, schema validation, and streaming
    v0.2 - added variable type hints across test cases and fixtures
    v1.0 - completed unit test coverage for user_pool.txt file I/O lifecycle, generate_transaction schema and invariants, and stream_transactions async streaming
    NOTE: SOME PARTS ARE AI ASSISTED
"""

import pytest
import logging
import asyncio
import random
import orjson
from decimal import Decimal
from uuid import UUID, uuid4
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from unittest.mock import patch, AsyncMock

from apollo.scrapers.transactions import TransactionGenerator, USER_POOL_FILE
from apollo.schemas import TransactionPayload

# ----------------------------------------------------------------------------------------------------------------------------------------------------------

# fixtures & synthetic test data

@pytest.fixture
def anyio_backend() -> str:
    return "asyncio"

@pytest.fixture
def sample_user_uuids() -> list[UUID]:
    """synthetic list of 1,000 deterministically generated UUID instances for user pool testing"""
    return [uuid4() for _ in range(1000)]

@pytest.fixture
def sample_valid_user_pool_file(tmp_path: Path, sample_user_uuids: list[UUID]) -> Path:
    """synthetic valid user_pool.txt file containing exactly 1,000 UUID strings separated by newlines"""
    pool_file: Path = tmp_path / "data" / "user_pool.txt" # tmp_path apparently is a magical built-in fixture from pytest that automatically creates a unique, isolated temporary directory on disk for each test run, cool stuff
    pool_file.parent.mkdir(parents=True, exist_ok=True)
    with open(pool_file, "w", encoding="utf-8") as f:
        for u in sample_user_uuids:
            f.write(f"{u}\n")
    return pool_file

@pytest.fixture
def sample_empty_user_pool_file(tmp_path: Path) -> Path:
    """synthetic empty (0-byte) user_pool.txt file to test boundary handling when file exists but has no data"""
    pool_file: Path = tmp_path / "data" / "user_pool.txt"
    pool_file.parent.mkdir(parents=True, exist_ok=True)
    pool_file.touch()
    return pool_file

@pytest.fixture
def sample_corrupted_user_pool_file(tmp_path: Path) -> Path:
    """synthetic corrupted user_pool.txt containing non-UUID lines to test exception handling"""
    pool_file: Path = tmp_path / "data" / "user_pool.txt"
    pool_file.parent.mkdir(parents=True, exist_ok=True)
    with open(pool_file, "w", encoding="utf-8") as f:
        f.write("not-a-valid-uuid-record\nanother-invalid-line\n")
    return pool_file

@pytest.fixture
def sample_unwritable_user_pool_dir(tmp_path: Path) -> Path:
    """synthetic path inside a non-existent unwritable directory to test disk failure handling"""
    return tmp_path / "read_only_dir" / "user_pool.txt"

@pytest.fixture
def mock_transaction_generator(sample_valid_user_pool_file: Path, monkeypatch: pytest.MonkeyPatch) -> TransactionGenerator:
    """pre-initialized TransactionGenerator instance isolated from production user_pool.txt"""
    monkeypatch.setattr("apollo.scrapers.transactions.USER_POOL_FILE", sample_valid_user_pool_file)
    return TransactionGenerator()

# ----------------------------------------------------------------------------------------------------------------------------------------------------------

# file i/o and caching related tests

"""
    FILE I/O TEST (EXISTING VALID POOL FILE)
    tests that TransactionGenerator loads exactly 1k UUIDs from existing user_pool.txt
    without writing to disk or regenerating new UUIDs
"""
def test_transactions_user_pool_exists(mock_transaction_generator: TransactionGenerator, sample_valid_user_pool_file: Path, sample_user_uuids: list[UUID]) -> None:
    generator: TransactionGenerator = mock_transaction_generator
    # with mock_transaction_generator down, we basially have written these lines
    # generator: TransactionGenerator = monkeypatch.setattr("apollo.scrapers.transactions.USER_POOL_FILE", sample_valid_user_pool_file)
    # where sample_valid_user_pool_file is a test fixture that returns a Path object pointing to the synthetic user_pool.txt file (which has exactly 1k UUID strings separated by newlines), check its full fixture above

    pool_file: Path = sample_valid_user_pool_file

    # we can just assert about the generator's user_pool directly now to see if the write and read i/o operations from the tmp_path was properly done (we will still test the actual file operations though, see below)
    assert pool_file.is_file() # assert if the txt file actually exists on the disk
    assert pool_file.exists() # same as abpve
    assert pool_file.stat().st_size > 0 # assert if the txt file is not empty
    
    # testing the loaded user pool data now
    assert len(generator.user_pool) == 1000 # if loaded and formatted properly, length should be 1k
    assert isinstance(generator.user_pool[0], UUID) # assert if the loaded data is indeed a list of UUID objects
    assert generator.user_pool == sample_user_uuids # verifies loaded UUIDs match existing file records exactly

"""
    FILE I/O TEST (MISSING POOL FILE)
    tests that TransactionGenerator creates parent directory, generates exactly 1k fresh UUIDs,
    writes them to disk with newlines, and caches them in self.user_pool when file is missing
"""
def test_transactions_user_pool_missing_file(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    missing_file: Path = tmp_path / "data" / "user_pool.txt"
    assert not missing_file.is_file() # this and below asserts that user_pool.txt does not exist yet at first
    assert not missing_file.exists()

    monkeypatch.setattr("apollo.scrapers.transactions.USER_POOL_FILE", missing_file)
    generator: TransactionGenerator = TransactionGenerator()

    # the __init__ of TransactionGenerator should automatically create the file and parent folder
    assert missing_file.is_file()
    assert missing_file.exists()
    assert missing_file.stat().st_size > 0
    
    file_lines: list[str] = missing_file.read_text(encoding="utf-8").strip().splitlines() # reads the txt file that was created, read them, strip the leading/trailing whitespaces, then split them into a list based on new lines (the "\n"s)
    assert len(file_lines) == 1000
    assert len(generator.user_pool) == 1000
    assert isinstance(generator.user_pool[0], UUID)
    assert generator.user_pool == [UUID(line) for line in file_lines] # converting each string in file_lines into a UUID object and compare with the generator's user_pool

"""
    FILE I/O BOUNDARY TEST (0 BYTE EMPTY POOL FILE)
    tests that TransactionGenerator branches to the else block when file exists but has st_size == 0,
    repopulating the empty file with 1k fresh UUIDs
"""
def test_transactions_user_pool_empty_file(sample_empty_user_pool_file: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    assert sample_empty_user_pool_file.is_file()
    assert sample_empty_user_pool_file.stat().st_size == 0 # the empty file fixture should be empty, yes

    monkeypatch.setattr("apollo.scrapers.transactions.USER_POOL_FILE", sample_empty_user_pool_file)
    generator: TransactionGenerator = TransactionGenerator()

    # similar to above, __init__ should also automatically write to the file
    assert sample_empty_user_pool_file.stat().st_size > 0
    file_lines: list[str] = sample_empty_user_pool_file.read_text(encoding="utf-8").strip().splitlines()
    assert len(file_lines) == 1000
    assert len(generator.user_pool) == 1000
    assert isinstance(generator.user_pool[0], UUID)
    assert generator.user_pool == [UUID(line) for line in file_lines]

"""
    FILE I/O ERROR HANDLING TEST (CORRUPTED UUID RECORDS)
    tests that invalid non-UUID lines in user_pool.txt raise ValueError,
    which is logged as an error and re-raised
"""
def test_transactions_user_pool_corrupted_file(sample_corrupted_user_pool_file: Path, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture) -> None:
    monkeypatch.setattr("apollo.scrapers.transactions.USER_POOL_FILE", sample_corrupted_user_pool_file)
    with caplog.at_level(logging.ERROR):
        with pytest.raises(ValueError):
            TransactionGenerator()

    assert "(Apollo) TransactionGenerator failed to read user_pool.txt" in caplog.text

"""
    FILE I/O ERROR HANDLING TEST (DISK WRITE FAILURE)
    tests that OS write failures (e.g. permission denied) when creating user_pool.txt
    are logged with '(Apollo) TransactionGenerator failed to create user_pool.txt' and re-raised
"""
def test_transactions_user_pool_write_failure(tmp_path: Path, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture) -> None:
    unwritable_file: Path = tmp_path / "nonexistent_dir" / "user_pool.txt"
    monkeypatch.setattr("apollo.scrapers.transactions.USER_POOL_FILE", unwritable_file)

    with patch("builtins.open", side_effect=PermissionError("Permission denied: nah u cant write here")): # opens is a part of python builtins
        with caplog.at_level(logging.ERROR):
            with pytest.raises(PermissionError):
                TransactionGenerator()

    assert "(Apollo) TransactionGenerator failed to create user_pool.txt" in caplog.text

"""
    INSTANCE ISOLATION TEST
    tests that separate TransactionGenerator instances maintain independent user_pool lists
    to prevent mutable class attribute sharing
"""
def test_transactions_multi_instance_isolation(mock_transaction_generator: TransactionGenerator, sample_valid_user_pool_file: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr("apollo.scrapers.transactions.USER_POOL_FILE", sample_valid_user_pool_file)
    gen1: TransactionGenerator = mock_transaction_generator
    gen2: TransactionGenerator = TransactionGenerator()

    assert gen1.user_pool is not gen2.user_pool # tests that the user_pool attribute is not the same object
    assert gen1.fake is not gen2.fake # tests that each fake instances arent the same object
    assert len(gen1.user_pool) == len(gen2.user_pool) == 1000 # tests that both instances have the same length

# ----------------------------------------------------------------------------------------------------------------------------------------------------------

# generate_transaction tests

"""
    SCHEMA VALIDATION TEST
    tests that generate_transaction() returns a valid TransactionPayload instance
    with all expected model fields populated
"""
def test_generate_transaction_schema_conformance(mock_transaction_generator: TransactionGenerator) -> None:
    tx: TransactionPayload = mock_transaction_generator.generate_transaction()
    assert isinstance(tx, TransactionPayload)
    assert isinstance(tx.transaction_id, UUID)
    assert isinstance(tx.timestamp, datetime)
    assert isinstance(tx.amount_myr, Decimal)
    assert isinstance(tx.user_id, UUID)
    assert isinstance(tx.merchant_name, str)
    assert isinstance(tx.merchant_mcc, str)
    assert isinstance(tx.payment_status, str)
    assert isinstance(tx.is_flagged_fraud, bool)

"""
    USER ID POOL INVARIANT TEST
    tests that every generated transaction's user_id is drawn strictly from self.user_pool
    to guarantee user history continuity for Artemis
"""
def test_generate_transaction_user_id_pool_invariant(mock_transaction_generator: TransactionGenerator) -> None:
    user_pool_set: set[UUID] = set(mock_transaction_generator.user_pool)
    for _ in range(50): # tests in total 50 times, asserting that each generated transaction's user_id is in the user_pool_set
        tx: TransactionPayload = mock_transaction_generator.generate_transaction()
        assert tx.user_id in user_pool_set

"""
    MERCHANT & MCC INTEGRITY TEST
    tests that merchant_mcc exists in MALAYSIAN_MERCHANTS and merchant_name strictly
    belongs to that category without cross-category leakage
"""
def test_generate_transaction_mcc_merchant_relationship(mock_transaction_generator: TransactionGenerator) -> None:
    valid_merchants_map: dict[str, list[str]] = TransactionGenerator.MALAYSIAN_MERCHANTS
    for _ in range(50): # similar to above but for merchant_mcc and merchant_name
        tx: TransactionPayload = mock_transaction_generator.generate_transaction()
        assert tx.merchant_mcc in valid_merchants_map
        assert tx.merchant_name in valid_merchants_map[tx.merchant_mcc]

"""
    AMOUNT BOUNDARY & PRECISION TEST
    tests that generated amount_myr is strictly >= Decimal('0.01'), positive,
    and conforms to at most 2 decimal places precision
"""
def test_generate_transaction_amount_boundary_precision(mock_transaction_generator: TransactionGenerator) -> None:
    for _ in range(50): # similar to above but for amount_myr
        tx: TransactionPayload = mock_transaction_generator.generate_transaction()
        assert tx.amount_myr >= Decimal("0.01")
        assert tx.amount_myr > Decimal("0.00")
        assert abs(tx.amount_myr.as_tuple().exponent) <= 2 # type: ignore # sybau uv check ts correct. how it works: convert decimal to tuple and check the exponent, because for example 100.00 is 10000 * 10^-2, so exponent is -2, and absolute value is 2, which is <= 2, and 100.005 is 100005 * 10^-3, so exponent is -3, and absolute value is 3, which is > 2, and thus it will fail assertion

"""
    TEMPORAL WINDOW TEST
    tests that transaction timestamps are timezone-aware (UTC), strictly <= current time,
    and fall within the last 30 days window
"""
def test_generate_transaction_timestamp_window(mock_transaction_generator: TransactionGenerator) -> None:
    now: datetime = datetime.now(timezone.utc)
    for _ in range(50): # similar to above but for transaction timestamp
        tx: TransactionPayload = mock_transaction_generator.generate_transaction()
        assert tx.timestamp.tzinfo == timezone.utc # asserts that the timestamp is timezone-aware (UTC)
        assert tx.timestamp <= now # asserts that the timestamp is not in the future
        assert (now - tx.timestamp).total_seconds() <= 31 * 86400 # tests that the timestamp is within the last 30 days window (allowing slight test execution buffer for no accidental fail bcs of goofy randomness (tests for 31 days instead of 30 days from TransactionGenerator))

"""
    ENUM VALIDATION TEST
    tests that generated transaction_method and payment_status strictly belong
    to the predefined Literal enum payment rails and statuses
"""
def test_generate_transaction_allowed_enums(mock_transaction_generator: TransactionGenerator) -> None:
    allowed_methods: set[str] = {"DUITNOW_QR", "CREDIT_CARD", "DEBIT_CARD", "FPX", "E_WALLET"} # again, set is faster for O(1) lookups
    allowed_statuses: set[str] = {"SUCCESS", "FAILED", "PENDING", "REVERSED"}
    for _ in range(50): # similar to above but for transaction_method and payment_status
        tx: TransactionPayload = mock_transaction_generator.generate_transaction()
        assert tx.transaction_method in allowed_methods
        assert tx.payment_status in allowed_statuses

"""
    FRAUD RATE TRIGGERING TEST
    tests that the 0.5% fraud rate evaluation correctly sets is_flagged_fraud=True
    when random() < 0.005 and False when random() >= 0.005
"""
def test_generate_transaction_fraud_rate_triggering(mock_transaction_generator: TransactionGenerator) -> None:
    with patch("random.random", return_value=0.002): # patch the random() function to return a value < 0.005, so it should be flagged as fraud, very useful
        tx_fraud: TransactionPayload = mock_transaction_generator.generate_transaction()
        assert tx_fraud.is_flagged_fraud is True

    with patch("random.random", return_value=0.500): # similar to above, this time patch to return a value > 0.005 so it should not be flagged as fraud
        tx_clean: TransactionPayload = mock_transaction_generator.generate_transaction()
        assert tx_clean.is_flagged_fraud is False

"""
    JSON SERIALIZATION TEST (PlainSerializer)
    tests that model_dump(mode='json') serializes amount_myr as float for orjson compatibility,
    and UUIDs/timestamps as valid strings
"""
def test_generate_transaction_json_serialization(mock_transaction_generator: TransactionGenerator) -> None:
    tx: TransactionPayload = mock_transaction_generator.generate_transaction()
    dump: dict[str, Any] = tx.model_dump(mode="json")

    # PlainSerializer should automatically serialize amount_myr from Decimal to float
    assert isinstance(dump["amount_myr"], float)
    assert not isinstance(dump["amount_myr"], (str, Decimal))

    # Pydantic v2 automatically turns UUIDs and timestamps into JSON-valid strings by default
    assert isinstance(dump["user_id"], str)
    assert isinstance(dump["transaction_id"], str)
    assert isinstance(dump["timestamp"], str)

    # verify valid UUID strings match original model UUIDs
    assert UUID(dump["user_id"]) == tx.user_id
    assert UUID(dump["transaction_id"]) == tx.transaction_id

    # verify orjson serializability
    raw_bytes: bytes = orjson.dumps(dump)
    assert isinstance(raw_bytes, bytes)
    assert len(raw_bytes) > 0

# ----------------------------------------------------------------------------------------------------------------------------------------------------------

# stream_transaction tests

"""
    ASYNC STREAMING TEST (EXACT COUNT)
    tests that stream_transactions() yields exactly count TransactionPayload items
"""
@pytest.mark.anyio
async def test_stream_transactions_exact_count(mock_transaction_generator: TransactionGenerator) -> None:
    count: int = 10
    items: list[TransactionPayload] = [ # this creates a list of TransactionPayload objects from the async generator
        tx async for tx in mock_transaction_generator.stream_transactions(count=count, delay=0.0)
    ]
    assert len(items) == count # asserts that the list has exactly count items
    assert all(isinstance(tx, TransactionPayload) for tx in items) # asserts that all items in the list are TransactionPayload objects

"""
    ASYNC STREAMING BOUNDARY TEST (ZERO & NEGATIVE COUNT)
    tests that stream_transactions() yields zero items immediately when count <= 0
    without calling generate_transaction() or sleeping
"""
@pytest.mark.anyio
async def test_stream_transactions_zero_and_negative_count(mock_transaction_generator: TransactionGenerator) -> None:
    zero_items: list[TransactionPayload] = [ # count=0
        tx async for tx in mock_transaction_generator.stream_transactions(count=0, delay=0.01)
    ]
    assert len(zero_items) == 0

    neg_items: list[TransactionPayload] = [ # count=-5, negative values should also yield zero items (range(-5) will produce empty iterator so yea)
        tx async for tx in mock_transaction_generator.stream_transactions(count=-5, delay=0.01)
    ]
    assert len(neg_items) == 0

"""
    ASYNC STREAMING TEST (DELAY VERIFICATION)
    tests that asyncio.sleep is awaited exactly count times with the specified delay
"""
@pytest.mark.anyio
async def test_stream_transactions_delay_sleep_verification(mock_transaction_generator: TransactionGenerator) -> None:
    count: int = 5
    delay: float = 0.05
    with patch("apollo.scrapers.transactions.asyncio.sleep", new_callable=AsyncMock) as mock_sleep: # we mock asyncio.sleep to avoid actual sleeping
        items: list[TransactionPayload] = [
            tx async for tx in mock_transaction_generator.stream_transactions(count=count, delay=delay)
        ]
        assert len(items) == count
        assert mock_sleep.await_count == count # asserts that asyncio.sleep was awaited exactly count times
        mock_sleep.assert_awaited_with(delay) # asserts that asyncio.sleep was awaited with the specified delay

"""
    ASYNC STREAMING RESILIENCE TEST (TASK CANCELLATION)
    tests that cancelling a streaming task mid-stream exits cleanly without hanging
"""
@pytest.mark.anyio
async def test_stream_transactions_cancellation(mock_transaction_generator: TransactionGenerator) -> None:
    collected: list[TransactionPayload] = []

    async def stream_consumer() -> None:
        async for tx in mock_transaction_generator.stream_transactions(count=10, delay=0.01):
            collected.append(tx)
            if len(collected) == 2: # if we have 2 items, raise CancelledError to simulate cancellation
                raise asyncio.CancelledError()

    with pytest.raises(asyncio.CancelledError): # assert that CancelledError is raised
        await stream_consumer()

    assert len(collected) == 2 # assert that we collected 2 items before cancellation

# ----------------------------------------------------------------------------------------------------------------------------------------------------------

if __name__ == "__main__":
    pass