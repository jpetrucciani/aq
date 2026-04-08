def main(data):
    return [
        {"path": path, "sha256": aq.hash_file(path)}
        for path in aq.glob("lib/**/*.star")
    ]
