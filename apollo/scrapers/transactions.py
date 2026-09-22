"""
    not actually a scraper but its placed here anyways
    artificial finance transaction generator using Faker and stuff
    NOTE: since this aint a scaper, TransactionGenerator will not inherit/implement BaseScraper
    v1.0 - docstrings are finished, generate_transaction() was made synchronous due to actually not having to await any async coroutines innit, Faker malaysian locale stuff apparently do not exist so I had to just write my own locale, odds of fraudulent transactions are now properly evaluated to 0.5% cuz apparently Faker.boolean chance_of_getting_true is unable to evaluate floats, user_id are no longer fully randomly generated (for artemis!)
    v1.0.1 - added min_value constraint to amount_myr generation, user_pool are now generated in __init__ instead of as a class attribute to avoid the class attribute being shared across multiple instances (should there be more than one)
"""

import logging
import asyncio
import random
from faker import Faker
from decimal import Decimal
from datetime import datetime, timezone
from uuid import UUID, uuid4
from collections.abc import AsyncIterator # apparently this is used for async generators like stream_transactions()

from apollo.schemas import TransactionPayload

logger = logging.getLogger(__name__)

# ----------------------------------------------------------------------------------------------------------------------------------------------------------

"""
    the transaction generator class
    attributes:
        fake (Faker): standard Faker generator instance for timestamps and decimals
        user_pool (list[UUID]): pool of 1,000 unique user UUIDs to simulate recurring user transaction patterns for Artemis
        MALAYSIAN_MERCHANTS (dict[str, list[str]]): mapping of 4-digit ISO MCC codes to authentic Malaysian merchant brands
    methods:
        __init__ -> initializes the generator with Faker and generates user_pool for consistent user IDs
        generate_transaction -> generates a single synthetic TransactionPayload with weighted payment methods, status, and 0.5% fraud probability
        stream_transactions -> asynchronously streams generated transaction payloads using an async generator
"""
class TransactionGenerator:
    fake: Faker
    user_pool: list[UUID]
    MALAYSIAN_MERCHANTS: dict[str, list[str]] = {
        "5411": [  # grocery stores and supermarket
            "99 Speedmart",
            "Jaya Grocer",
            "Village Grocer",
            "Lotus's Malaysia", # ts goated btw
            "Econsave",
            "Mydin",
            "HeroMarket",
            "Giant Hypermarket",
            "NSK Trade City",
        ],
        "5814": [  # fnb
            "ZUS Coffee",
            "Tealive",
            "Gigi Coffee",
            "Marrybrown",
            "Richiamo Coffee",
            "The Alley Malaysia",
            "KyoChon 1991 Malaysia",
            "Rotiboy",
        ],
        "5812": [  # restaurants or warung
            "Secret Recipe",
            "The Chicken Rice Shop",
            "Nando's Malaysia",
            "Sushi King Malaysia",
            "OldTown White Coffee",
            "PappaRich",
            "Oriental Kopi",
            "Madam Kwan's",
        ],
        "5499": [  # convenience store
            "FamilyMart Malaysia",
            "CU Malaysia",
            "7-Eleven Malaysia",
            "myNEWS",
            "emart24 Malaysia",
        ],
        "5541": [  # services or petrol station
            "Petronas",
            "Shell Malaysia",
            "Petron Malaysia",
            "Caltex Malaysia",
            "BHPetrol",
        ],
        "5912": [  # drug stores and pharmacies
            "Watsons Malaysia",
            "Guardian Malaysia",
            "Caring Pharmacy",
            "Big Pharmacy",
            "Alpro Pharmacy",
        ],
        "5311": [  # department stores
            "AEON Malaysia",
            "Parkson",
            "SOGO Malaysia",
            "Metrojaya",
        ],
        "5651": [  # family clothing and fashion stores
            "Padini Concept Store",
            "Brands Outlet",
            "Bonia",
            "Carlo Rino",
            "British India",
        ],
        "5331": [  # variety stores and retail
            "MR. D.I.Y.",
            "MR. Dollar",
            "Daiso Malaysia",
            "Miniso Malaysia",
        ],
        "5732": [  # electronics
            "All IT Hypermarket",
            "Senheng",
            "Thunder Match Technology (TMT)",
            "Harvey Norman Malaysia",
        ],
        "4814": [  # telecommunications
            "Maxis",
            "CelcomDigi",
            "U Mobile",
            "Unifi Mobile",
            "Yes 5G",
        ],
        "4900": [  # utilities including electric, water, and sewerage
            "Tenaga Nasional Berhad",
            "Pengurusan Air Selangor",
            "Indah Water Konsortium",
            "Sabah Electricity",
            "Sarawak Energy",
        ],
        "4121": [  # taxicabs and ride hailing services
            "Grab Malaysia",
            "Maxim Malaysia",
            "AirAsia Ride",
        ],
        "7832": [  # movie theatres
            "Golden Screen Cinemas (GSC)",
            "TGV Cinemas",
            "MBO Cinemas",
        ],
    }

    """
        initializes the transaction generator with Faker and a pool of 1,000 user UUIDs
        arguments: self
        EXPECTED TO return: None
    """
    def __init__(self) -> None:
        self.fake = Faker()
        self.user_pool: list[UUID] = [uuid4() for _ in range(1000)] # rather than randomly generating unique uuid for every user in transaction, we can use 1k generated uuid instead so that artemis may be able to learn each user patterns

    """
        generates a single fake financial transaction payload, this serves as the core method of the generator
        arguments: self
        EXPECTED TO return: TransactionPayload
    """
    def generate_transaction(self) -> TransactionPayload:
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
            positive=True,
            min_value=Decimal("0.01") # to not violate pydantic model validation
        ) # returns a Decimal object, 4 digits to the left of decimal and 2 digits to the right (so 2 decimal places, example: 9999.99), always positive too
        user_id = random.choice(self.user_pool)
        merchant_mcc = random.choice(list(self.MALAYSIAN_MERCHANTS.keys())) # randomly select an MCC category key from our Malaysian merchants dictionary above, convert dict_keys to list then pick randomly
        merchant_name = random.choice(self.MALAYSIAN_MERCHANTS[merchant_mcc]) # pick a real Malaysian merchant matching that exact MCC, from the selected mcc key above it will randomly pick from the list of merchant names
        payment_status = ["SUCCESS", "FAILED", "PENDING", "REVERSED"]
        is_flagged_fraud = random.random() < 0.005 # returns True if the random number between 0.0 and 1.0 returns less than 0.005, which should properly simulates 0.5% fraudulent transactions rate

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
        asynchronously streams generated transaction payloads using an async generator
        arguments: self, count (int: number of transactions to generate and stream), delay (float: delay between each generated transaction in seconds, default is 0.01)
        EXPECTED TO return: AsyncIterator of TransactionPayload
    """
    async def stream_transactions(self, count: int, delay: float=0.01) -> AsyncIterator[TransactionPayload]:
        for _ in range(count):
            yield self.generate_transaction()
            await asyncio.sleep(delay)



"""
    main function just for testing directly in the terminal
"""
async def main() -> None:
    generator: TransactionGenerator = TransactionGenerator()
    
    tx: TransactionPayload = generator.generate_transaction()
    print(f"test generating a fake transaction:\n {tx}")
    print(f"and its json dump (see if PlainSerializer works):\n {tx.model_dump(mode='json')}\n")

    print("\n" + "=" * 100 + "\n") # line shi

    count: int = 5
    async for tx_stream in generator.stream_transactions(count=count, delay=0.1):
        print(f"test streaming transactions:\n {tx_stream}")
        print(f"their json dumps:\n {tx_stream.model_dump(mode='json')}\n")
    

# ----------------------------------------------------------------------------------------------------------------------------------------------------------

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("transactions.py keyboard interrupt test")
        
