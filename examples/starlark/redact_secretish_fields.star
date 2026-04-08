def is_secretish(path, value):
    if len(path) == 0:
        return False
    leaf = path[len(path) - 1]
    return type(leaf) == "string" and leaf in ["password", "token", "secret"]


def main(data):
    return aq.omit_where(data, is_secretish, leaves_only = True)
