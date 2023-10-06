import time
import pandas as pd
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import NoSuchElementException, TimeoutException
from loguru import logger

OUT_PATH = f"/Users/benjaminjones/repos/llm-experiments/data/aws_talks_{time.strftime('%Y%m%d_%H%M%S')}.csv"

DRIVER = webdriver.Chrome()


def accept_cookies(driver):
    wait = WebDriverWait(driver, 5)
    try:
        cookie_accept_button = wait.until(
            EC.element_to_be_clickable(
                (By.XPATH, '//button[@aria-label="Accept all cookies"]')
            )
        )
        cookie_accept_button.click()
    except TimeoutException as e:
        logger.exception(f"Failed to accept cookies: {e}")


def get_talk_data(button, driver):
    title, description = None, None
    wait = WebDriverWait(driver, 5)

    try:
        button.click()
        # wait to load modal
        time.sleep(0.2)

        # Try to get title
        try:
            title = wait.until(
                EC.presence_of_element_located((By.CSS_SELECTOR, "p.fs-4.fw-700.mb-2"))
            ).text.strip()
        except NoSuchElementException as e:
            logger.exception(
                f"An error occurred getting the description: {e}", backtrace=False
            )
            time.sleep(1)
            pass  # If title not found, title remains None

        # Try to get the description
        try:
            description = wait.until(
                EC.presence_of_element_located(
                    (
                        By.CSS_SELECTOR,
                        "div.session-description p, div.session-description div",
                    )
                )
            ).text
        except NoSuchElementException as e:
            logger.exception(
                f"An error occurred getting the description: {e}", backtrace=False
            )
            time.sleep(1)
            pass  # If description not found, description remains None

        close_button = wait.until(
            EC.element_to_be_clickable(
                (By.CSS_SELECTOR, "button.btn.btn-link.flex-end")
            )
        )
        close_button.click()
        time.sleep(0.5)

    except (NoSuchElementException, TimeoutException) as e:
        logger.exception(f"Failed to get talk data: {e}")
        time.sleep(1)

    return title, description


def append_to_csv(row, path):
    df = pd.DataFrame([row], columns=["Title", "Description"])
    df.to_csv(path, mode="a", header=False, index=False)
    logger.info(f"Appended {row}")


def scrape_aws_talks(url, driver=DRIVER):
    logger.add("run.log", level="ERROR")
    driver.get(url)
    accept_cookies(driver)
    wait = WebDriverWait(driver, 5)

    details_buttons = wait.until(
        EC.presence_of_all_elements_located(
            (By.CSS_SELECTOR, "button.btn.d-flex.align-items-center.flex-grow-1")
        )
    )

    for button in details_buttons:
        for ix in range(3):  # Retry logic
            title, description = get_talk_data(button, driver)
            if title or description:
                append_to_csv([title, description], OUT_PATH)
                break
            logger.info(f"Retrying for {ix+1}th time... on button {button.text}")
            time.sleep(1)  # Wait before retrying

    driver.quit()


url = "https://hub.reinvent.awsevents.com/attendee-portal/catalog/"
scrape_aws_talks(url)
