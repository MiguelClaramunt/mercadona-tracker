import ast
import os

import dotenv


def load_dotenv(
    dotenv_shared: str = "shared.env",
    dotenv_temp: str = "temp.env",
    dotenv_secret: str = "",
    os_environ: bool = False,
) -> dict:
    if dotenv_shared:
        env_shared = {
            **dotenv.dotenv_values(dotenv_path=dotenv_shared),
        }  # load shared development variables
    else:
        env_shared = dict()

    if dotenv_temp:
        env_temp = {
            **dotenv.dotenv_values(dotenv_path=dotenv_temp),
        }
    else:
        env_temp = dict()

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

    config = dotenv_pipeline(env_shared | env_temp | env_secret | os_environ)

    return config


def update_variable(variables: dict, file: str) -> None:
    file = dotenv.find_dotenv(file)

    # Write changes to .env file.
    for variable, value in variables.items():
        dotenv.set_key(dotenv_path=file, key_to_set=variable, value_to_set=value)


def refresh_variables(config: dict) -> list:
    for variable in config.keys():
        try:
            config[variable] = ast.literal_eval(config[variable])
        except (ValueError, SyntaxError):
            continue

    return config


def concatenate_config_columns(
    config: dict, to_concatenate: str = "COLUMNS_TO_CONCATENATE"
) -> dict:
    for var in config[to_concatenate]:
        config[var] = sum([config[column] for column in config[var]], [])

    return config


def generate_etl_parameters(
    config: dict, to_concatenate: str = "ETL_PARAMETERS"
) -> dict:
    for i, parameters in enumerate(config[to_concatenate]):
        for column in ("columns_to_select", "columns_to_hash"):
            try:
                config[to_concatenate][i][column] = config[parameters[column]]
            except TypeError:
                continue

    return config


def dotenv_pipeline(dicts: dict) -> dict:
    config = refresh_variables(config=dicts)
    config = concatenate_config_columns(
        config=config, to_concatenate="COLUMNS_TO_CONCATENATE"
    )
    config = generate_etl_parameters(config, to_concatenate="ETL_PARAMETERS")

    return config
