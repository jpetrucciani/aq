def main(data):
    return {
        "files": aq.walk_files(path = "lib"),
        "has_lib": aq.is_dir("lib"),
    }
