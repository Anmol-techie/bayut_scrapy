import scrapy
import csv
from w3lib.url import canonicalize_url, url_query_cleaner


class BayutCrawlSpider(scrapy.Spider):
    name = "bayut_crawl"
    allowed_domains = ["bayut.com"]
    start_urls = ["https://www.bayut.com/"]

    custom_settings = {
        "ROBOTSTXT_OBEY": True,
        "AUTOTHROTTLE_ENABLED": True,
        "AUTOTHROTTLE_START_DELAY": 0.25,
        "AUTOTHROTTLE_MAX_DELAY": 5.0,
        "CONCURRENT_REQUESTS_PER_DOMAIN": 8,
        "DOWNLOAD_TIMEOUT": 20,
        # Removed DEPTH_LIMIT to allow unlimited depth
        "DEFAULT_REQUEST_HEADERS": {
            "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
                          "(KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.9",
        },
        "FEEDS": {},  # disable Scrapy's feed exporters
    }

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.seen_urls = set()
        # open CSV file in append mode
        self.csv_file = open("bayut_urls.csv", "a", newline="", encoding="utf-8")
        self.writer = csv.writer(self.csv_file)
        # optional: write header only if file is empty
        if self.csv_file.tell() == 0:
            self.writer.writerow(["url"])

    def parse(self, response):
        canonical = response.css('link[rel="canonical"]::attr(href)').get()
        cur = canonical or response.url

        cleaned = url_query_cleaner(cur, parameterlist=None, remove=True)
        normalized = canonicalize_url(cleaned, keep_fragments=False)

        # Only process English URLs (skip Arabic URLs with /ar/)
        if normalized.startswith("https://www.bayut.com/") and "/ar/" not in normalized:
            if normalized not in self.seen_urls:
                self.seen_urls.add(normalized)
                # write immediately to CSV
                self.writer.writerow([normalized])
                self.csv_file.flush()  # make sure it's written

                yield {"url": normalized}

        for href in response.css("a::attr(href)").getall():
            u = response.urljoin(href)
            if not u.startswith("https://www.bayut.com/"):
                continue
            # Skip Arabic language URLs (containing /ar/)
            if "/ar/" in u:
                continue
            if any(u.endswith(ext) for ext in (".jpg", ".jpeg", ".png", ".gif", ".svg", ".webp", ".pdf")):
                continue
            if u.startswith("mailto:") or u.startswith("tel:") or "javascript:" in u:
                continue
            yield response.follow(u, callback=self.parse)

    def closed(self, reason):
        self.csv_file.close()
        self.logger.info(f"✅ Wrote {len(self.seen_urls)} unique URLs to bayut_urls.csv")
