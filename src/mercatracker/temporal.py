import datetime

from mercatracker.config import Config

config = Config().load()


def iso_date2custom_format(iso_date: str, custom_format: str) -> str:
    date = datetime.date.fromisoformat(iso_date)
    return date.strftime(custom_format)


def updated_ids() -> bool:
    """Checks if ids in `.env.temp` file are updated, avoiding unnecesary requests to API.

    Returns:
        bool: True if ids in `.env.temp` are updated
    """
    return iso_date2custom_format(
        str(datetime.date.today()), custom_format=config["FORMAT_DATE"]
    ) == str(config["LASTMOD_DATE"])
