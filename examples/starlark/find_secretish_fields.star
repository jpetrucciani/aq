def is_secretish(path, value):
    leaf = path[len(path) - 1]
    return type(leaf) == "string" and leaf in ["password", "token", "secret"]

def describe(path, value):
    if is_secretish(path, value):
        return {"path": path, "value": value}
    return None

def main(data):
    return {
        "paths": aq.find_paths(data, is_secretish, leaves_only = True),
        "matches": aq.drop_nulls(
            aq.collect_paths(data, describe, leaves_only = True)
        ),
    }
