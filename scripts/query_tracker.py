import csv
import os
import threading

class QueryTracker:
    def __init__(self, filename="allqueries.csv"):
        self.filename = filename
        self.lock = threading.Lock()  # For thread-safe writing
        self._initialize_file()

    def _initialize_file(self):
        with open(self.filename, mode='w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            writer.writerow(["Query", "Exit Reason"])

    def log(self, query, exit_reason):
        with self.lock:
            with open(self.filename, mode='a', newline='', encoding='utf-8') as f:
                writer = csv.writer(f)
                writer.writerow([query, exit_reason])