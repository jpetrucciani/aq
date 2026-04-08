def main(data):
    start = aq.datetime("2026-03-30T12:30:00Z")
    return {
        "release": {
            "day": aq.date("2026-03-30") + aq.timedelta(days = 1),
            "start": start,
            "cutoff": start - aq.timedelta(hours = 2),
        },
        "delay_seconds": (start + aq.timedelta(days = 1) - start).total_seconds(),
    }
