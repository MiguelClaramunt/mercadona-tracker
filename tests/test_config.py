import pytest
from mercatracker.config import Config


@pytest.fixture
def mock_config():
    return Config().load()


def test_load(mock_config):
    config = mock_config
    assert config['ITEMS_FOLDER'] == 'items'
    # assert temp_values == {"TEMP_VAR": "temp_value"}
    # assert secret_values == {"SECRET_VAR": "secret_value"}


# def test_load_method(mock_config):
#     config = mock_config
#     config._config = {"COLUMNS_TO_CONCATENATE": {}, "ETL_PARAMETERS": []}
#     config.load()

#     assert config._config.get("SHARED_VAR") == "shared_value"
#     assert config._config.get("TEMP_VAR") == "temp_value"
#     assert config._config.get("SECRET_VAR") == "secret_value"


# def test_refresh_method(mock_config):
#     config = mock_config
#     config._config = {
#         "INT_VAR": "123",
#         "FLOAT_VAR": "123.45",
#         "STR_VAR": "some_string",
#         "INVALID_VAR": "invalid_literal",
#     }
#     config._refresh()

#     assert config._config["INT_VAR"] == 123
#     assert config._config["FLOAT_VAR"] == 123.45
#     assert config._config["STR_VAR"] == "some_string"
#     assert config._config["INVALID_VAR"] == "invalid_literal"


# def test_concatenate_cols_method(mock_config):
#     config = mock_config
#     config._config = {
#         "COLUMNS_TO_CONCATENATE": {"col1": ["a", "b"], "col2": ["c", "d"]},
#         "VAR_TO_CONCATENATE": ["col1", "col2"],
#     }
#     config._concatenate_cols(var_name="COLUMNS_TO_CONCATENATE")

#     assert config._config["col1"] == ["a", "b", "c", "d"]
#     assert config._config["col2"] == ["c", "d"]


# def test_generate_etl_parameters_method(mock_config):
#     config = mock_config
#     config._config = {
#         "ETL_PARAMETERS": [{"columns_to_select": "col1", "columns_to_hash": "col2"}],
#         "col1": ["a", "b"],
#         "col2": ["c", "d"],
#     }
#     config._generate_etl_parameters(var_name="ETL_PARAMETERS")

#     assert config._config["ETL_PARAMETERS"][0]["columns_to_select"] == ["a", "b"]
#     assert config._config["ETL_PARAMETERS"][0]["columns_to_hash"] == ["c", "d"]


if __name__ == "__main__":
    pytest.main()
