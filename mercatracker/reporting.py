from datetime import timedelta


def performance(
    times: tuple[float, float], lenghts: tuple[int, int], ids: list[list[str]]
) -> None:
    start, end = times
    time_delta = timedelta(seconds=end - start)

    if ids:
        lengths = (len(subset) for subset in ids)
    start_len, total_len = lenghts
    len_delta = total_len - start_len

    print(f"Elapsed: {time_delta} ({(time_delta / (len_delta)).total_seconds()}) s/it")


def last_mod_date(lastmod: int) -> None:
    print(f"Last mod. date retieved: {lastmod}")


def lengths_status(lenghts: tuple[int, int]) -> None:
    start_len, total_len = lenghts
    print(f"Items current/total: {start_len}/{total_len}")


def soup(
    lastmod: int,
    lenghts: tuple[int, int] | None = None,
    ids: list[list[str]] | None = None,
) -> None:
    last_mod_date(lastmod=lastmod)
    if ids:
        lenghts = (len(subset) for subset in ids)
    lengths_status(lenghts=lenghts)
