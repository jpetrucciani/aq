def trim_and_patch(path, value):
    if type(value) == "string":
        value = value.strip()
    if path == ["metadata", "labels", "tier"]:
        return value.upper()
    return value

def main(data):
    return aq.walk_paths(data, trim_and_patch)
