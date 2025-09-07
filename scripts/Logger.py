import threading

class Logger:
    def __init__(self):
        self.lock = threading.Lock()
        self.serper_credits = 0
        self.icypeas_profiles = 0
        self.openai_tokens = 0
        self.findymail_credits = 0  # Assuming credits equal to number of emails found
        self.total_queries_processed = 0
        self.total_urls_found = 0
        self.total_after_dedup = 0
        self.total_matches = 0
        self.total_emails_found = 0
        self.total_input_companies = 0

    def add_found_email(self, count):
        with self.lock:
            self.total_emails_found += count

    def add_deduplicated(self, count):
        with self.lock:
            self.total_after_dedup += count

    def add_findmymail_credit(self, count):
        with self.lock:
            self.findymail_credits += count

    def add_queries(self, count):
        with self.lock:
            self.total_queries_processed += count

    def add_serper(self, count):
        with self.lock:
            self.serper_credits += count

    def add_icypeas(self, count):
        with self.lock:
            self.icypeas_profiles += count

    def add_openai(self, tokens):
        with self.lock:
            self.openai_tokens += tokens

    def add_urls_found(self, count):
        with self.lock:
            self.total_urls_found += count

    def add_after_dedup(self, count):
        with self.lock:
            self.total_after_dedup += count

    def add_matches(self, count):
        with self.lock:
            self.total_matches += count

    def add_emails(self, count):
        with self.lock:
            self.total_emails_found += count
            self.findymail_credits += count  # Assuming 1 credit per found email

    def output(self):
        print(f"Total input companies: {self.total_input_companies}")
        print(f"Total queries processed: {self.total_queries_processed}")
        serper_cost = (self.serper_credits / 1000) * 0.30
        print(f"Serper.dev credits used & cost: {self.serper_credits} credits (${serper_cost:.2f})")
        print(f"Total LinkedIn URLs found: {self.total_urls_found}")
        print(f"Total after deduplication: {self.total_after_dedup}")
        icypeas_credits = self.icypeas_profiles * 1.5
        icypeas_cost = self.icypeas_profiles * 0.0025
        print(f"Icypeas credits used & cost: {icypeas_credits} credits (${icypeas_cost:.4f})")
        openai_cost = (self.openai_tokens / 1000000) * 0.40
        print(f"OpenAI tokens used (model 4.1 Nano only): {self.openai_tokens} tokens (${openai_cost:.4f})")
        print(f"Total LinkedIn matches: {self.total_matches}")
        print(f"Total emails found: {self.total_emails_found}")
        findymail_cost = self.findymail_credits * 0.00599625
        print(f"Findymail credits used & cost: {self.findymail_credits} credits (${findymail_cost:.4f})")
        
        print("---------------------------------")
        print(f"Total cost: ${serper_cost + icypeas_cost + openai_cost + findymail_cost:.4f}")