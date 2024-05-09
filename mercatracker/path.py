from os import path


def build(folder: str, file: str | int, extension: str) -> str:
    if isinstance(file, int):
        file = str(file)
    return path.join(".", folder, f"{file}.{extension}")
