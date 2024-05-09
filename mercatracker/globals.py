import ast
import os

import dotenv


def load_dotenv(
    dotenv_shared: str = "",
    dotenv_secret: str = "",
    os_environ: bool = False,
) -> dict:

    if dotenv_shared:
        env_shared = {
            **dotenv.dotenv_values(dotenv_path=dotenv_shared),
        }  # load shared development variables
    else:
        env_shared = dict()

    if dotenv_secret:
        env_secret = {
            **dotenv.dotenv_values(dotenv_path=dotenv_secret),
        }
    else:
        env_secret = dict()

    if os_environ:
        os_environ = {**os.environ}
    else:
        os_environ = dict()

    config = refresh_variables(config=env_shared | env_secret | os_environ)
    config = concatenate_config_columns(
        config=config, variable="COLUMNS_TO_CONCATENATE"
    )

    return config


# config = load_dotenv(dotenv_shared=".env.shared")


def update_variable(variable: str, value: int, dotenv_file: str) -> None:
    dotenv_file = dotenv.find_dotenv(dotenv_file)

    # Write changes to .env file.
    dotenv.set_key(dotenv_file, variable, value)


def refresh_variables(config: dict) -> list:
    for variable in config.keys():
        try:
            config[variable] = ast.literal_eval(config[variable])
        except (ValueError, SyntaxError):
            continue

    return config


def concatenate_config_columns(
    config: dict, variable: str = "COLUMNS_TO_CONCATENATE"
) -> dict:
    for var in config[variable]:
        config[var] = sum([config[column] for column in config[var]], [])

    return config
