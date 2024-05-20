import pytest

from mercatracker import config


def test_load_dotenv_with_valid_path(tmp_path):
    # Create a temporary .env file
    env_file = tmp_path / ".env"
    env_file.write_text("VAR1=value1\nVAR2=value2")

    # Call the load_dotenv function with the path to the temporary .env file
    env_vars = config.load_dotenv(str(env_file))

    # Assert that the function correctly loads the environment variables
    assert env_vars == {"VAR1": "value1", "VAR2": "value2"}


def test_load_dotenv_with_empty_file(tmp_path):
    # Create a temporary empty .env file
    env_file = tmp_path / ".env"
    env_file.write_text("")

    # Call the load_dotenv function with the path to the temporary .env file
    env_vars = config.load_dotenv(str(env_file))

    # Assert that the function returns an empty dictionary
    assert env_vars == {}


def test_load_dotenv_with_no_path():
    # Call the load_dotenv function with no path
    env_vars = config.load_dotenv(None)

    # Assert that the function returns an empty dictionary
    assert env_vars == {}


def test_load_dotenv_with_nonexistent_file():
    # Call the load_dotenv function with a nonexistent file path
    env_vars = config.load_dotenv("nonexistent.env")

    # Assert that the function returns an empty dictionary
    assert env_vars == {}


def test_load_dotenv_with_test_env(tmp_path):
    # Create a temporary .env file with the specified content
    env_content = (
        "TEST_STR = 'string'\n"
        "TEST_DICT = {'lang': 'es', 'wh': 'vlc1'}\n"
        "TEST_LIST = ['list']\n"
    )
    env_file = tmp_path / "test.env"
    env_file.write_text(env_content)

    # Call the load_dotenv function with the path to the temporary .env file
    env_vars = config.load_dotenv(str(env_file))

    # Assert that the function correctly loads the environment variables
    assert env_vars == {
        "TEST_STR": "'string'",
        "TEST_DICT": "{'lang': 'es', 'wh': 'vlc1'}",
        "TEST_LIST": "['list']",
    }


def test_config_load_with_test_env(tmp_path):
    # Create a temporary .env file with the specified content
    env_content = (
        "TEST_STR = 'string'\n"
        "TEST_DICT = {'lang': 'es', 'wh': 'vlc1'}\n"
        "TEST_LIST = ['list']\n"
    )
    env_file = tmp_path / "test.env"
    env_file.write_text(env_content)

    # Initialize the Config object
    conf = config.Config(shared=str(env_file))

    # Load the configuration
    conf.load()

    # Assert that the configuration is correctly loaded and processed
    assert conf._config["TEST_STR"] == "string"
    assert conf._config["TEST_DICT"] == {"lang": "es", "wh": "vlc1"}
    assert conf._config["TEST_LIST"] == ["list"]


@pytest.mark.parametrize(
    "env_var, expected_value",
    [
        ("TEST_STR", "string"),
        ("TEST_DICT", {"lang": "es", "wh": "vlc1"}),
        ("TEST_LIST", ["list"]),
    ],
)
def test_config_variables(env_var, expected_value, tmp_path):
    # Create a temporary .env file with the specified content
    env_content = (
        "TEST_STR = 'string'\n"
        "TEST_DICT = {'lang': 'es', 'wh': 'vlc1'}\n"
        "TEST_LIST = ['list']\n"
    )
    env_file = tmp_path / "test.env"
    env_file.write_text(env_content)

    # Initialize the Config object
    conf = config.Config(shared=str(env_file))

    # Load the configuration
    conf.load()

    # Assert that the specified environment variable is correctly processed
    assert config._config[env_var] == expected_value


# def test_load_dotenv():
#     # config = Config(shared='.env.test')
#     assert (config.load_dotenv('.env.test')['TEST_STR'] == 'string')

if __name__ == "__main__":
    config.Config(shared=".env.test")
