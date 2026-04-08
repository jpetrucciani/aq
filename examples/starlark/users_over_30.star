def main(data):
    users = aq.query_all(".users[]", data)
    adults = [user for user in users if user["age"] > 30]
    return sorted(
        [{"name": user["name"], "age": user["age"]} for user in adults],
        key = lambda user: user["name"],
    )
