from os.path import join


def build(folder: str, file: str | int, extension: str) -> str:
    if isinstance(file, int):
        file = str(file)
    return join(".", folder, f"{file}.{extension}")
