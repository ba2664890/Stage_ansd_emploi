BOT_NAME = "Emploi_senegal"

SPIDER_MODULES = ["Emploi_senegal.spiders"]
NEWSPIDER_MODULE = "Emploi_senegal.spiders"

# Playwright
TWISTED_REACTOR = "twisted.internet.asyncioreactor.AsyncioSelectorReactor"
DOWNLOAD_HANDLERS = {
    "http":  "scrapy_playwright.handler.ScrapyPlaywrightDownloadHandler",
    "https": "scrapy_playwright.handler.ScrapyPlaywrightDownloadHandler",
}
PLAYWRIGHT_BROWSER_TYPE = "firefox"
PLAYWRIGHT_LAUNCH_OPTIONS = {"headless": True, "timeout": 30_000}
PLAYWRIGHT_CONTEXT_ARGS = {"viewport": {"width": 1366, "height": 768}}

# Politesse
ROBOTSTXT_OBEY = False
DOWNLOAD_DELAY = 2
CONCURRENT_REQUESTS = 1




    
# --- pipelines génériques ----------------------------------------------------
ITEM_PIPELINES = {
   'Emploi_senegal.pipelines.DuplicatesPipeline': 100,
  # 'Emploi_senegal.pipelines.EmploiSenegalPostgreSQLPipeline': 200,
   #'Emploi_senegal.pipelines.senjobPipeline': 300,
   #'Emploi_senegal.pipelines.SQLAlchemyPipeline': 400,
    'Emploi_senegal.pipelines.ExpatDakarPipeline': 500,
}


# --- configuration par pipeline ---------------------------------------------
PIPELINES_CONFIG = {
    "GenericPostgreSQLPipeline": {
        "table": "jobs",
        "fields": ["title", "company", "description", "contract",
                   "region", "education", "experience", "skills",
                   "posted", "url", "source"],
    }
}

DATABASE = {
    "database": "scrapy_immo",
    "user":     "Cardan",
    "password": "Fatimata05?",
    "host":     "localhost",
    "port":     5432,
}

# Désactivez les logs trop verbeux
LOG_LEVEL = "INFO"