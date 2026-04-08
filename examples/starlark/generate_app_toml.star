def main(data):
    return {
        "app": {
            "name": "aq",
            "port": 8443,
            "features": ["query", "starlark"],
        },
        "database": {
            "host": "db.internal",
            "pool": 16,
        },
    }
