import os

import pandas as pd


def df2csv(
    df: pd.DataFrame,
    path: str,
    columns: list | None,
    force_replace: bool = False,
) -> None:

    if not os.path.isfile(path) or force_replace:
        df.to_csv(path, header=True, index=False, columns=columns)
    else:  # else it exists so append without writing the header
        df.to_csv(path, mode="a", header=False, index=False, columns=columns)


