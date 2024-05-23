import ast
import glob
import os
from dataclasses import dataclass, field
from itertools import chain
from pathlib import Path
from typing import Dict, Iterable, List, Union

import dotenv


@dataclass
class Config:
    paths: Union[Dict[str, str], None] = None
    os: Union[bool, None] = None
    dict_: dict = field(init=False)

    def __getitem__(self, item: Union[str, Iterable[str,]]):
        if isinstance(item, str):
            return self.dict_[item]
        elif isinstance(item, Iterable) and all(isinstance(i, str) for i in item):
            return *(self.dict_[key] for key in item),
        else:
            raise TypeError("Key must be a string or a list of strings.")

    @classmethod
    def _search(cls, path: str) -> list:
        return {Path(path).stem: path for path in glob.glob(pathname=path)}

    @classmethod
    def _load_dotenv(cls, paths: List[str,]) -> Dict[str, str | None]:
        return dict(
            chain.from_iterable(
                dotenv.dotenv_values(dotenv_path=path).items()
                for path in paths.values()
            )
        )

    def __post_init__(self):
        if self.paths is None:
            self.paths = self._search(
                os.path.expanduser("~/git/mercadona-tracker/*.env")
            )
        self.dict_ = self._load_dotenv(self.paths)

    def _refresh(self) -> "Config":
        for variable in self.dict_.keys():
            try:
                self.dict_[variable] = ast.literal_eval(self.dict_[variable])
            except (ValueError, SyntaxError):
                continue

        return self

    def load(self, postprocess: bool = True) -> "Config":
        self.dict_ = self._load_dotenv(self.paths)
        self._refresh()

        if postprocess:
            self._convert_aliases(
                meta_var="COLS_WITH_ALIASES"
            )._generate_etl_parameters(meta_var="ETL_PARAMETERS")

        return self

    def _convert_aliases(self, meta_var: str = "COLS_WITH_ALIASES") -> "Config":
        """Convert aliases into its values from a meta-variable.

        From aliases `YMD`, `CATEGORIES_FINAL`; and the meta-variable
        `COLS_WITH_ALIASES` contained in `shared.env` file:
        - `YMD = ["ymd"]`
        - `CATEGORIES_FINAL = ["level", "name"]`
        - `COLS_WITH_ALIASES = [..., "_CATEGORIES_COLS", ...]`

        We process the aliases in `_CATEGORIES_COLS`, resulting in the final
        list of variables:
        - `_CATEGORIES_COLS = ["CATEGORIES_FINAL", "YMD"] -> ["level", "name", "ymd"]`

        Args:
            meta_var (str, optional): Meta-variable, contains all variables
            to convert. Defaults to `"COLS_WITH_ALIASES"`.
        """
        for var in self.dict_[meta_var]:
            self.dict_[var] = sum(
                [self.dict_[column] for column in self.dict_[var]], []
            )

        return self

    def _generate_etl_parameters(self, meta_var: str = "ETL_PARAMETERS") -> "Config":
        for i, parameters in enumerate(self.dict_[meta_var]):
            for column in ("cols_to_select", "cols_to_hash"):
                try:
                    self.dict_[meta_var][i][column] = self.dict_[parameters[column]]
                except TypeError:
                    continue

        return self

    def update(self, var: Dict[str, str]) -> None:
        for k, v in var.items():
            dotenv.set_key(dotenv_path=self.paths["temp"], key_to_set=k, value_to_set=v)
