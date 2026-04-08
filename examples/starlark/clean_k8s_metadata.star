def main(data):
    cleaned = aq.clean_k8s_metadata(data)
    return aq.set_path(cleaned, ["metadata", "labels", "managed-by"], "aq")
