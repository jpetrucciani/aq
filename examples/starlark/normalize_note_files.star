def annotate(path, text):
    return "# Source: " + path.split("/")[-1] + "\n\n" + text.upper()


def main(data):
    return [
        {"path": path, "bytes": aq.rewrite_text(path, annotate)}
        for path in data["paths"]
    ]
