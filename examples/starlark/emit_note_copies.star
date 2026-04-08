def main(data):
    entries = aq.read_text_glob("data/notes/**/*.txt")
    return aq.write_text_batch(
        [
            {
                "path": data["out_dir"] + "/" + entry["path"],
                "text": "# Source: " + entry["path"] + "\n\n" + entry["text"],
            }
            for entry in entries
        ],
        parents = True,
    )
