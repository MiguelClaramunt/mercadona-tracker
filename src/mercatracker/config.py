import ast
import glob
import os
from dataclasses import dataclass
from itertools import chain
from typing import Dict, List, Tuple, Union

import dotenv


@dataclass
class Config:
    paths: Union[List[str,], Tuple[str,], None] = None
    os: Union[bool, None] = None

    def __getitem__(self, item):
        return self.dict_.__getitem__(item)

    @classmethod
    def _search(cls, path: str) -> list:
        return glob.glob(pathname=path)

    @classmethod
    def _load_dotenv(cls, paths: List[str,]) -> Dict[str, str | None]:
        return dict(
            chain.from_iterable(
                dotenv.dotenv_values(dotenv_path=path).items() for path in paths
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

    def load(self, postprocess: bool = True) -> dict:
        self.dict_ = self._load_dotenv(self.paths)
        self._refresh()

        if postprocess:
            self._convert_aliases(
                meta_var="COLS_WITH_ALIASES"
            )._generate_etl_parameters(meta_var="ETL_PARAMETERS")

        return self.dict_

    def _convert_aliases(self, meta_var: str = "COLS_WITH_ALIASES") -> "Config":
        """Convert aliases into its values from a meta-variable.

        From this aliases `YMD`, `CATEGORIES_FINAL`; and the meta-variable `COLS_WITH_ALIASES`:
        * `YMD = ["ymd"]`
        * `CATEGORIES_FINAL = ["level", "name"]`
        * `COLS_WITH_ALIASES = [..., "_CATEGORIES_COLS", ...]`

        We process the aliases in `_CATEGORIES_COLS`, resulting in the final list of variables:
        * `_CATEGORIES_COLS = ["CATEGORIES_FINAL", "YMD"] -> ["level", "name", "ymd"]`

        Args:
            meta_var (str, optional): Meta-variable, contains all variables to convert. Defaults to "COLS_WITH_ALIASES".
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
