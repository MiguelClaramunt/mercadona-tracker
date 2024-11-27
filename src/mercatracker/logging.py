import datetime
from datetime import timedelta


def performance(times: tuple[float, float], ids: list[list[str]] | list[int]) -> None:
    """_summary_

    Args:
        times (tuple[float, float]): tuple[start, end]; len(start) < len(end)
        ids (list[list[str]]): _description_
    """
    start, end = times
    time_delta = timedelta(seconds=end - start)

    start_len, total_len = (len(subset) for subset in ids)

    print(f"Items recovered: {total_len - start_len}")
    if not total_len - start_len:
        print(f"Elapsed: {time_delta}")
    else:
        print(
            f"Elapsed: {time_delta} ({(time_delta / (total_len - start_len)).total_seconds():.3f}) s/it"
        )


def lastmod_date(lastmod: int, to_date: bool = False) -> None:
    if to_date:
        lastmod = datetime.strptime(str(lastmod), r"%Y%m%d")
    print(f"Last mod. date retieved: {lastmod}")


def lengths_status(lenghts: tuple[int, int]) -> None:
    start_len, total_len = lenghts
    print(f"Items current/total: {start_len}/{total_len}")


def soup(
    lastmod: int,
    ids: list[list[str]] = None,
) -> None:
    lastmod_date(lastmod=lastmod)
    if ids:
        lengths_status(lenghts=(len(subset) for subset in ids))


# def etl_db() -> None:
