def main(data):
    docs = aq.read_glob_all("data/manifests/**/*.yaml")
    return [
        {
            "path": doc["path"],
            "index": doc["index"],
            "kind": doc["value"]["kind"],
            "name": doc["value"]["metadata"]["name"],
        }
        for doc in docs
    ]
