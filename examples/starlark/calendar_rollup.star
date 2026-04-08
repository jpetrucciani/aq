def main(data):
    release_day = aq.date("2026-03-30").replace(day = 31)
    ship_at = release_day.at(hour = 9, minute = 15).replace(hour = 17, minute = 0, second = 0)
    return {
        "day": release_day,
        "weekday": release_day.weekday(),
        "ordinal": release_day.ordinal,
        "ship_at": ship_at,
        "ship_day": ship_at.date(),
    }
