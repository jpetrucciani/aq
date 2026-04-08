def main(data):
    merged = aq.merge_all([data["base"], data["overlay"]], deep = True)
    cleaned = aq.drop_nulls(merged, recursive = True)
    return aq.sort_keys(cleaned, recursive = True)
