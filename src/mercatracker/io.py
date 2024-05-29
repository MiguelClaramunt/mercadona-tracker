import pathlib
from dataclasses import dataclass
from typing import Iterable, List

import pandas as pd


def df2csv(
    df: pd.DataFrame,
    path: str,
    columns: list | None,
    force_replace: bool = False,
) -> None:
    # if not os.path.isfile(path) or force_replace:
    #     df.to_csv(path, header=True, index=False, columns=columns)
    # else:  # else it exists so append without writing the header
    df.to_csv(path, mode="a", header=False, index=False, columns=columns)


def read_multiple(paths: list[str]) -> pd.DataFrame:
    return pd.concat((pd.read_csv(file) for file in paths), ignore_index=True)


@dataclass
class File:
    levels: Iterable[str,]
    extension: str
    path: str = None

    def __post_init__(self):
        self.path = self.build_path(self.levels, self.extension)

    @classmethod
    def build_path(cls, levels: Iterable[str,], extension: str) -> str:
        if not all(isinstance(i, str) for i in levels):
            levels = (*map(str, levels),)
        return pathlib.Path(*levels).with_suffix(extension)

    def read(self, dtypes: dict = {"id": str}) -> List[str,]:
        try:
            values = (
                pd.read_csv(self.path, usecols=dtypes.keys(), dtype=dtypes)[
                    dtypes.keys()
                ]
                .iloc[:, 0]
                .values.tolist()
            )
        except FileNotFoundError:
            values = list()

        return values

    def write_header(self, text: str, mode: str = "x") -> None:
        try:
            with open(self.path, mode=mode) as f:
                f.write(text)
        except FileExistsError:
            pass
