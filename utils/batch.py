def batch_split(elements: list, batch_size: int) -> list[set[str]]:
    return [set(elements[i:i+batch_size]) for i in range(0, len(elements), batch_size)]