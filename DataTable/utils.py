def are_contained(set1, set2):
    ls1 = extract_match(set1, set2)
    ls2 = extract_match(set2, set1)
    return set(ls1) == set(ls2)


def extract_match(list1, list2):
    return [element for element in list1 if element in list2]


def should_be_type(d_type, var_value, var_name="", source=""):
    if not isinstance(var_value, d_type):
        raise TypeError(f"<{source}> '{var_name}' is not type {d_type}")


def assertion(statement, text):
    if not statement:
        raise AssertionError(text)


def new_file(path, content=''):
    with open(path, 'w+') as file:
        file.write(content)


def read_file(path):
    with open(path) as file:
        return file.read()


def normalize_path(path):
    if '\\' in path:
        path = path.replace('\\', '/')
    if path[-1] == '/':
        path = path[: -1]
    return path
