def main(data):
    return {
        "same_day": aq.now().date() == aq.today(),
        "next_week": (aq.date("2026-03-30") + aq.timedelta(weeks = 1)).isoformat(),
        "grace_seconds": aq.timedelta(milliseconds = 250, microseconds = 500, nanoseconds = 600).total_seconds(),
    }
