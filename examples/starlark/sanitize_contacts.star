def main(data):
    return [
        {
            "name": user["name"],
            "email": aq.regex_replace("^[^@]+@", "***@", user["email"]),
            "token": aq.base64_encode(user["email"]),
            "fingerprint": aq.hash(user["email"], algorithm = "blake3"),
        }
        for user in data["users"]
    ]
