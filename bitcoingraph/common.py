import platform


def is_pypy():
    if platform.python_implementation() == "PyPy":
        return True
    return False