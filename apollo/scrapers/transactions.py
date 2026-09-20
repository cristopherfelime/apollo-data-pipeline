"""
    not actually a scraper but its placed here anyways
    artificial finance transaction generator using Faker and stuff
    NOTE: since this aint a scaper, TransactionGenerator will not inherit/implement BaseScraper
    v0.1
"""

import logging
import asyncio
import random
from faker import Faker
from decimal import Decimal
from datetime import datetime, timezone
from uuid import uuid4
from collections.abc import AsyncIterator # apparently this is used for async generators like stream_transactions()

from apollo.schemas import TransactionPayload

logger = logging.getLogger(__name__)

# ----------------------------------------------------------------------------------------------------------------------------------------------------------

"""
    class placeholder docstring
"""
class TransactionGenerator:
    fake: Faker

    """
        method docstring placeholder
    """
    def __init__(self) -> None:
        self.fake = Faker("en_MS")

    """
        method docstring placeholder
    """
    async def generate_transaction(self) -> TransactionPayload:
        r"""
            transaction_id: Annotated[UUID, Field(default_factory=uuid4)]
            timestamp: Annotated[datetime, Field(description="exact UTC timestamp of when transaction was conducted")] # yyeeee
            transaction_method: Annotated[Literal["DUITNOW_QR", "CREDIT_CARD", "DEBIT_CARD", "FPX", "E_WALLET"], Field(max_length=100, description="transaction method used")]
            amount_myr: Annotated[Decimal, Field(gt=Decimal("0.00"), decimal_places=2), PlainSerializer(lambda x: float(x), when_used="json")] # transaction amount in rm, plainserializer to ensure that when dumped using mode="json" it would be float and not str, to also avoid JSONEncodeError by orjson downstream
            user_id: Annotated[UUID, Field(description="user ID of the user who conducted the transaction type shi")]
            merchant_name: Annotated[str, Field(min_length=1, max_length=500, description="name of the merchant or business")]
            merchant_mcc: Annotated[str, Field(max_length=4, pattern=r"^\d{4}$", description="merchant category code")] # merchant category code has 4 digits 
            payment_status: Annotated[Literal["SUCCESS", "FAILED", "PENDING", "REVERSED"], Field(max_length=100)] # transaction payment status as of logging
            ingested_at: Annotated[datetime, Field(default_factory=lambda: datetime.now(timezone.utc))]
            is_flagged_fraud: Annotated[bool, Field(description="boolean flag indicating if the transaction is flagged as fraud (ts primarily for artemis later)")]
        """
        timestamp = self.fake.date_time_between( # returns timezone aware date time object (up to the microsecond!) ranging from 30 days ago to now
            start_date="-30d",
            end_date="now",
            tzinfo=timezone.utc
        )
        transaction_method = ["DUITNOW_QR", "CREDIT_CARD", "DEBIT_CARD", "FPX", "E_WALLET"]
        amount_myr = self.fake.pydecimal(
            left_digits=4,
            right_digits=2,
            positive=True
        ) # returns a Decimal object, 4 digits to the left of decimal and 2 digits to the right (so 2 decimal places, example: 9999.99), always positive too
        user_id = uuid4()
        merchant_name = self.fake.company() # returns a random company name as a string, since we localized them above as ms_MY, it will actually return a fake malaysian style-ish or whatever the word is company
        merchant_mcc = self.fake.bothify(text="####") # bothify is useful to generate random set of number according to the hashtags in the given tags, so ts useful to mimic MCC
        payment_status = ["SUCCESS", "FAILED", "PENDING", "REVERSED"]
        is_flagged_fraud = self.fake.boolean(chance_of_getting_true=5) # kinda self-explanatory

        return TransactionPayload(
            timestamp=timestamp,
            transaction_method=random.choices( # random.choices() gives us the option to modify the weights (chances) of each item getting selected, useful to mimic or simulate which transaction methods are more oftenly used
                transaction_method,
                weights=[40, 20, 15, 15, 10],
                k=1
            )[0], # from here we also return 1 object inside a collection, so we take the first index
            amount_myr=amount_myr,
            user_id=user_id,
            merchant_name=merchant_name,
            merchant_mcc=merchant_mcc,
            payment_status=random.choices(
                payment_status,
                weights=[80, 10, 5, 5],
                k=1
            )[0],
            is_flagged_fraud=is_flagged_fraud
        )
    
    """
        method placeholder docstring
    """
    async def stream_transactions(self, count: int, delay: float=0.01) -> AsyncIterator[TransactionPayload]:
        for _ in range(count):
            yield await self.generate_transaction()
            await asyncio.sleep(delay)



"""
    function placeholder docstring
"""
async def main() -> None:
    generator = TransactionGenerator()
    
    tx = await generator.generate_transaction()
    print(f"test generating a fake transaction:\n {tx}")
    print(f"and its json dump (see if PlainSerializer works):\n {tx.model_dump(mode='json')}")

    print("="*100) # line shi

    count = 5
    async for tx_stream in generator.stream_transactions(count=count, delay=0.1):
        print(f"test streaming transactions:\n {tx_stream}")
    

# ----------------------------------------------------------------------------------------------------------------------------------------------------------

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("transactions.py keyboard interrupt test")
        
