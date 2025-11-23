import datetime


class Logger:
    """Simple logger following SRP"""

    @staticmethod
    def log(msg: str):
        print(f"[{datetime.datetime.now()}] {msg}")