def main(data):
    files = aq.glob("lib/**/*.star")
    return {
        "files": files,
        "resolved": [aq.resolve_path(path) for path in files],
        "relative_from_lib": aq.relative_path("lib/k8s.star", start = "lib"),
    }
