def main(data):
    docs = aq.read_glob_all("data/manifests/**/*.yaml")
    entries = [
        {
            "path": data["out_dir"] + "/" + doc["value"]["metadata"]["name"] + ".json",
            "value": {
                "path": doc["path"],
                "index": doc["index"],
                "kind": doc["value"]["kind"],
                "name": doc["value"]["metadata"]["name"],
            },
        }
        for doc in docs
    ]
    return aq.write_batch(entries, "json", compact = True, parents = True)
