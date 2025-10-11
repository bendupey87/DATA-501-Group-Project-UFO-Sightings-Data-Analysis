"""
NUFORC UFO Sightings Highlights Scraper with Logging
-----------------------------------------------------

This script scrapes UFO sighting highlights from the NUFORC (National UFO Reporting Center) website.
Because the site loads its "Highlights" table dynamically with JavaScript, we can’t just use requests
or pandas.read_html() — we need Selenium to act like a real browser.

We then parse the rendered HTML with BeautifulSoup, convert it into pandas DataFrames, and finally save
the combined data into a CSV file for later analysis.

On top of that, we use Python's logging module to keep track of everything that happens (progress,
errors, completion). This way we have a permanent record of the run, which is *critical* for reproducibility
and debugging.

Teaching Notes:
---------------
- This script is a nice example of "ETL in the wild": Extract (scrape data), Transform (combine it),
  Load (save to CSV).
- It demonstrates the interaction between Selenium (browser automation), BeautifulSoup (HTML parsing),
  pandas (data handling), and logging (workflow tracking).
"""

# ---------------------------------------------------------------------
# 0. Import required libraries
# ---------------------------------------------------------------------
# - time: lets us pause so the page can fully load.
# - logging: keeps detailed notes of what happened during the run.
# - pandas: handles tables and saves to CSV.
# - BeautifulSoup: parses rendered HTML into something we can search.
# - selenium: controls a real web browser.
# - WebDriverException: special error type we might see if ChromeDriver fails.
# ---------------------------------------------------------------------
import time
import logging
import pandas as pd
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.common.exceptions import WebDriverException

# ---------------------------------------------------------------------
# 1. Configure logging
# ---------------------------------------------------------------------
# Logging is like the "black box recorder" of our program.
# - filename: where the logs are saved permanently.
# - level: INFO means we log progress messages, warnings, and errors.
# - format: defines what each log line looks like (timestamp, level, message).
#
# We ALSO add a StreamHandler so students can see the logs live
# in their Jupyter output/terminal, not just hidden in a file.
# ---------------------------------------------------------------------
log_path = r"C:\git\DATA-501-Group-Project-UFO-Sightings-Data-Analysis\notebooks\ben\logging\nuforc_web_scrape.log"

logging.basicConfig(
    filename=log_path,            # permanent log file
    level=logging.INFO,           # INFO = normal + warnings + errors
    format="%(asctime)s - %(levelname)s - %(message)s",
)

# Add a console handler so logs show up in real time
console = logging.StreamHandler()
console.setLevel(logging.INFO)    # same level as above
console.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s - %(message)s"))
logging.getLogger().addHandler(console)

logging.info("NUFORC Highlights web scraping started.")  # first log entry

# ---------------------------------------------------------------------
# 2. Start the Chrome WebDriver
# ---------------------------------------------------------------------
# Selenium needs a "driver" to control the Chrome browser. If ChromeDriver
# is not installed or doesn't match your Chrome version, this will fail.
# ---------------------------------------------------------------------
try:
    driver = webdriver.Chrome()
    logging.info("Chrome WebDriver started successfully.")
except WebDriverException as e:
    logging.error("Failed to start Chrome WebDriver: %s", e)
    raise  # stops the script if Chrome doesn’t start

# ---------------------------------------------------------------------
# 3. Loop through paginated results
# ---------------------------------------------------------------------
# The NUFORC Highlights page is split into multiple pages (pagination).
# Example: ?pg=0, ?pg=1, ?pg=2, etc.
#
# We loop through pages until we stop finding tables.
# This is safer than guessing the exact number of pages.
#
# all_data: a list that will store one pandas DataFrame per page.
# ---------------------------------------------------------------------
all_data = []

for page in range(0, 2000):  # big upper bound, breaks automatically when no more pages
    url = f"https://nuforc.org/subndx/?id=highlights&pg={page}"
    try:
        # Tell Selenium to load this page
        driver.get(url)

        # Sleep (2 seconds) so the page’s JavaScript has time to finish
        time.sleep(2)

        # Get the fully rendered HTML and parse with BeautifulSoup
        soup = BeautifulSoup(driver.page_source, "html.parser")

        # Grab the first <table> element on the page
        table = soup.find("table")

        # If no table found, we’ve reached the end → stop looping
        if not table:
            logging.info("No table found on page %s. Ending scrape.", page)
            break

        # Convert the HTML table into a pandas DataFrame
        df = pd.read_html(str(table))[0]

        # Add this DataFrame to our list
        all_data.append(df)

        # Log success for this page
        logging.info("Scraped page %s with %s rows.", page, len(df))

    except Exception as e:
        # If something goes wrong (e.g., network error, parsing error),
        # log it but keep going to the next page.
        logging.error("Error scraping page %s: %s", page, e)
        continue

# ---------------------------------------------------------------------
# 4. Close the browser
# ---------------------------------------------------------------------
# Always quit the browser so it doesn’t stay open in the background.
# ---------------------------------------------------------------------
driver.quit()
logging.info("Browser closed.")

# ---------------------------------------------------------------------
# 5. Combine and Save Results
# ---------------------------------------------------------------------
# Once all pages are scraped, we combine the DataFrames into one big table
# (axis=0 = stack rows) and save to a CSV file.
#
# If no pages worked (all_data = empty), we log a warning instead.
# ---------------------------------------------------------------------
if all_data:
    final = pd.concat(all_data, ignore_index=True)

    output_path = r"C:\git\DATA-501-Group-Project-UFO-Sightings-Data-Analysis\data\nuforc_highlights.csv"
    final.to_csv(output_path, index=False)

    logging.info("Scraping complete! Saved %s records to %s", len(final), output_path)
else:
    logging.warning("No data scraped. CSV not created.")

