def str_to_bool(value: str) -> bool:
    if value is None:
        return True

    return False if value.lower() == "false" else True
