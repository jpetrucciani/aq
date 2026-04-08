def main(data):
    aq.mkdir(data["out_dir"], parents = True)
    report_path = data["out_dir"] + "/summary.json"
    aq.write(report_path, {"name": data["name"]}, "json", compact = True)
    return {
        "report": aq.stat(report_path),
    }
