def main(data):
    config_maps = aq.query_all('.[] | select(.kind == "ConfigMap") | .metadata.name', data)
    return {
        "format": aq.format(),
        "config_maps": config_maps,
    }
