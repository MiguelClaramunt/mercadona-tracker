import abc
import hashlib
import logging
import sys
import typing
from datetime import date
from decimal import Decimal

import pandas as pd

LOGGER = logging.getLogger()

# The hash function to use to create hashes over columns.
# All functions in hashlib should work, but there are performance trade-offs.
HASH_FUNCTION: typing.Callable = hashlib.md5
# Used as a separator when concating fields for hashing, ensures there are no
# mistakes if fields are empty.
HASH_FIELD_SEPARATOR: str = ":"


class AbstractHasher(abc.ABC):
    """Interface for structured testing."""

    dataframe: pd.DataFrame
    target_column_name: str
    columns_to_hash: typing.List[str]
    num_records: int

    def __init__(
        self,
        dataframe: pd.DataFrame,
        columns_to_hash: typing.List[str],
        target_column_name: str = "hash",
    ) -> "AbstractHasher":

        self.dataframe = dataframe.copy()
        self.target_column_name = target_column_name
        self.columns_to_hash = columns_to_hash
        self.num_records = len(dataframe)

    @abc.abstractmethod
    def hash(self) -> pd.DataFrame:
        """Hash the columns"""


class Hasher(AbstractHasher):
    """
    Like PythonHasherV2 but using itertuples instead of converting values to a list
    """

    def hash(self) -> pd.DataFrame:
        def hash_string_iterable(string_iterable: typing.Iterable[str]) -> str:
            input_str = HASH_FIELD_SEPARATOR.join(string_iterable)
            return HASH_FUNCTION(input_str.encode("utf-8")).hexdigest()

        # Apply the hash_string_iterable to the stringified list of row values
        hashed_series = pd.Series(
            map(
                hash_string_iterable,
                self.dataframe[self.columns_to_hash]
                .astype(str)
                .itertuples(index=False, name=None),
            ),
            index=self.dataframe.index,
        )

        self.dataframe[self.target_column_name] = hashed_series

        return self.dataframe


def assert_hasher_is_correct(hasher_class: typing.Type[AbstractHasher]) -> None:
    """Assert that a given hasher computes the output we expect for known inputs."""

    # Arrange

    known_input = pd.DataFrame(
        {
            "int": [1, 2, 3],
            "decimal": [Decimal("1.33"), Decimal("7"), Decimal("23.5")],
            "bool": [True, False, True],
            "date": [
                date.fromisoformat("2022-09-16"),
                date.fromisoformat("2022-04-13"),
                date.fromisoformat("2022-09-16"),
            ],
            "str": ["This", "Hasher", "Is_Correct"],
        }
    )

    expected_df = known_input.copy()
    expected_df["hashed"] = [
        "c1551c906a6e8ebeecb91abbfd90db87264f1335cd4e855d3c8521b7c02c8c65",
        "0afe31f8a139e9dfe7466989f7b4ffdb6504b150c81949f72bd96875b0ae91c4",
        "073ccefe4bc389dd6b97321f5467305c910f14dfba58f4d75c9f9a67eed43514",
    ]

    columns_to_hash = ["int", "decimal", "bool", "date", "str"]
    target_column_name = "hashed"

    # Act

    hasher = hasher_class(
        dataframe=known_input,
        columns_to_hash=columns_to_hash,
        target_column_name=target_column_name,
    )

    actual_df = hasher.hash()

    # Assert

    pd.testing.assert_frame_equal(actual_df, expected_df)
    LOGGER.info("Hasher %s is correct", hasher_class.__name__)


HASHERS_TO_TEST: typing.List[typing.Type[AbstractHasher]] = [
    Hasher,
]


def main() -> None:
    """Function that orchestrates the tests."""
    LOGGER.addHandler(logging.StreamHandler(sys.stdout))
    LOGGER.setLevel(logging.INFO)

    for hasher in HASHERS_TO_TEST:
        assert_hasher_is_correct(hasher)


if __name__ == "__main__":
    main()
