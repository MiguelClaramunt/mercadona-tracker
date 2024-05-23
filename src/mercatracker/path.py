import pathlib
from typing import Iterable

from mercatracker.config import Config

config = Config().load()


def build(levels: Iterable[str,], extension: str) -> str:
    if not all(isinstance(i, str) for i in levels):
        levels = (*map(str, levels),)
    return pathlib.Path(*levels).with_suffix(extension)
