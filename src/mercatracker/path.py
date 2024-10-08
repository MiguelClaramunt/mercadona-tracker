import os
from pathlib import Path
from typing import Iterable


def build(levels: Iterable[str,], extension: str) -> str:
    if not all(isinstance(i, str) for i in levels):
        levels = (*map(str, levels),)
    return str(Path(*levels).with_suffix(extension))


def get_project_dir() -> str:
    try:
        dir = [
            p
            for p in Path(os.path.abspath("")).parents
            if p.parts[-1] == "mercadona-tracker"
        ][0]
    except IndexError or TypeError:
        dir = os.path.abspath("")
    return str(dir)
