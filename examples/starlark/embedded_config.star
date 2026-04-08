def main(data):
    config = aq.parse(data["config"], "yaml")
    config["port"] = 8443
    return {
        "config": aq.render(config, "yaml"),
    }
