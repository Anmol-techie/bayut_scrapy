import scrapy
from scrapy.spiders import SitemapSpider

class BayutSitemapSpider(SitemapSpider):
    name = "bayut_sitemap"
    allowed_domains = ["bayut.com"]

    # Auto-discover sitemaps from robots.txt
    sitemap_urls = ["https://www.bayut.com/robots.txt"]
    sitemap_follow = [r".*"]                 # follow all sitemap indexes
    sitemap_alternate_links = True

    custom_settings = {
        "ROBOTSTXT_OBEY": True,
        "AUTOTHROTTLE_ENABLED": True,
        "AUTOTHROTTLE_START_DELAY": 0.25,
        "AUTOTHROTTLE_MAX_DELAY": 5.0,
        "CONCURRENT_REQUESTS_PER_DOMAIN": 8,
        "DOWNLOAD_TIMEOUT": 20,
        # Light UA to look like a browser
        "DEFAULT_REQUEST_HEADERS": {
            "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
                          "(KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.9",
        },
        # Export one URL per line (jsonlines)
        "FEEDS": {
            "sitemap_urls.jl": {"format": "jsonlines", "overwrite": True},
        },
    }

    def sitemap_filter(self, entries):
        """Keep only https://www.bayut.com/* URLs (skip cdn/subdomains if any)."""
        for entry in entries:
            loc = entry.get("loc", "")
            if loc.startswith("https://www.bayut.com/"):
                yield entry

    def parse(self, response):
        # Prefer canonical URL if present
        canonical = response.css('link[rel="canonical"]::attr(href)').get()
        url = canonical or response.url
        if url.startswith("https://www.bayut.com/"):
            yield {"url": url}
