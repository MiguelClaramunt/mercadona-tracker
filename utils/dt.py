import datetime

from dotenv import dotenv_values
from mercatracker.globals import load_dotenv

config = load_dotenv(".env.shared")


def iso_date2custom_format(iso_date: str, custom_format: str) -> str:
    date = datetime.date.fromisoformat(iso_date)
    return date.strftime(custom_format)
