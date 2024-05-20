import ast
import os
from dataclasses import dataclass, field

import dotenv


@dataclass
class Config:
    shared: str
    temp: str = field(init=False)
    secret: str = field(init=False)
    os: bool = field(init=False)
    _config: dict = field(init=False)

    def _refresh(self) -> "Config":
        for variable in self._config.keys():
            try:
                self._config[variable] = ast.literal_eval(self._config[variable])
            except (ValueError, SyntaxError):
                continue

        return self

    def _concatenate_cols(self, var_name: str = "COLUMNS_TO_CONCATENATE") -> "Config":
        for var in self._config[var_name]:
            self._config[var] = sum(
                [self._config[column] for column in self._config[var]], []
            )

        return self

    def _generate_etl_parameters(self, var_name: str = "ETL_PARAMETERS") -> "Config":
        for i, parameters in enumerate(self._config[var_name]):
            for column in ("columns_to_select", "columns_to_hash"):
                try:
                    self._config[var_name][i][column] = self._config[parameters[column]]
                except TypeError:
                    continue

        return self

    def load(self) -> dict:
        env_shared = load_dotenv(self.shared)
        env_temp = load_dotenv(self.temp)
        env_secret = load_dotenv(self.secret)
        env_os = dict(os.environ) if self.os else {}

        self._config = {**env_shared, **env_temp, **env_secret, **env_os}
        self._refresh()._concatenate_cols(
            var_name="COLUMNS_TO_CONCATENATE"
        )._generate_etl_parameters(var_name="ETL_PARAMETERS")

        return self._config


def load_dotenv(path: str) -> dict:
    return dotenv.dotenv_values(dotenv_path=path) if path else {}
