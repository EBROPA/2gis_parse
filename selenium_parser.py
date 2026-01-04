import time
import random
import json
import logging
import re
import os
import concurrent.futures
from typing import List, Dict, Optional, Set, Tuple
from urllib.parse import quote, urlencode
import requests
from selenium import webdriver
from selenium.webdriver.chrome.service import Service as ChromeService
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.action_chains import ActionChains
from webdriver_manager.chrome import ChromeDriverManager
from fake_useragent import UserAgent
import pandas as pd
from tqdm import tqdm
import threading

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


class AntiDetection:
    """Anti-detection measures."""

    USER_AGENTS = [
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:121.0) Gecko/20100101 Firefox/121.0",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.2 Safari/605.1.15",
    ]

    RESOLUTIONS = [(1920, 1080), (1440, 900), (1536, 864), (1680, 1050)]

    def __init__(self):
        try:
            self.ua = UserAgent()
        except:
            self.ua = None

    def get_user_agent(self) -> str:
        if self.ua:
            try:
                return self.ua.random
            except:
                pass
        return random.choice(self.USER_AGENTS)

    def get_resolution(self) -> Tuple[int, int]:
        return random.choice(self.RESOLUTIONS)

    @staticmethod
    def delay(min_s: float = 0.5, max_s: float = 1.5):
        time.sleep(random.uniform(min_s, max_s))


class TwoGisParser:
    """
    2GIS Parser using Selenium with proper virtual scroll handling.
    2GIS loads results dynamically as you scroll inside the results panel.
    """

    def __init__(self, headless: bool = True):
        self.anti = AntiDetection()
        self.headless = headless
        self.driver = None

    def _create_driver(self) -> webdriver.Chrome:
        """Create a new Chrome driver instance."""
        options = Options()

        ua = self.anti.get_user_agent()
        options.add_argument(f'user-agent={ua}')
        options.add_argument("--lang=ru-RU")

        w, h = self.anti.get_resolution()
        options.add_argument(f'--window-size={w},{h}')

        if self.headless:
            options.add_argument("--headless=new")

        # Anti-detection
        options.add_argument("--disable-blink-features=AutomationControlled")
        options.add_experimental_option("excludeSwitches", ["enable-automation"])
        options.add_experimental_option('useAutomationExtension', False)
        options.add_argument("--no-sandbox")
        options.add_argument("--disable-dev-shm-usage")
        options.add_argument("--disable-gpu")
        options.add_argument("--log-level=3")

        prefs = {
            "profile.default_content_setting_values.notifications": 2,
            "credentials_enable_service": False,
        }
        options.add_experimental_option("prefs", prefs)

        service = ChromeService(ChromeDriverManager().install())
        driver = webdriver.Chrome(service=service, options=options)

        # Stealth JS
        driver.execute_cdp_cmd("Page.addScriptToEvaluateOnNewDocument", {
            "source": """
                Object.defineProperty(navigator, 'webdriver', { get: () => undefined });
                Object.defineProperty(navigator, 'plugins', { get: () => [1, 2, 3] });
            """
        })

        driver.set_page_load_timeout(30)
        driver.implicitly_wait(3)

        return driver

    def _close_driver(self):
        if self.driver:
            try:
                self.driver.quit()
            except:
                pass
            self.driver = None

    def collect_links_with_scroll(self, city: str, query: str, max_items: int = 500) -> List[str]:
        """
        Collect company links by scrolling inside the results panel.
        2GIS uses virtual scroll with Intersection Observer - needs special handling.
        """
        self.driver = self._create_driver()
        unique_links: Set[str] = set()

        try:
            # Build URL
            encoded_query = quote(query, safe='')
            url = f"https://2gis.ru/{city}/search/{encoded_query}"

            logger.info(f"Opening: {url}")
            self.driver.get(url)
            time.sleep(4)

            # Wait for results to load
            try:
                WebDriverWait(self.driver, 15).until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, "a[href*='/firm/']"))
                )
            except:
                logger.warning("No results found or page didn't load")
                return []

            # Get total count if available
            try:
                count_elem = self.driver.find_element(By.XPATH,
                    "//*[contains(text(), 'найден') or contains(text(), 'Найден')]")
                logger.info(f"Search result info: {count_elem.text}")
            except:
                pass

            # Find the scrollable results container using JavaScript
            # This is more reliable than CSS selectors
            scroll_js = """
            function findScrollableParent() {
                // Find all links to firms
                var links = document.querySelectorAll('a[href*="/firm/"]');
                if (links.length === 0) return null;

                // Get the first link and traverse up to find scrollable parent
                var el = links[0];
                while (el && el !== document.body) {
                    var style = window.getComputedStyle(el);
                    var overflowY = style.overflowY;

                    if ((overflowY === 'auto' || overflowY === 'scroll') &&
                        el.scrollHeight > el.clientHeight + 10) {
                        return el;
                    }
                    el = el.parentElement;
                }

                // Fallback: find any scrollable div with significant height
                var divs = document.querySelectorAll('div');
                for (var i = 0; i < divs.length; i++) {
                    var div = divs[i];
                    if (div.scrollHeight > div.clientHeight + 100 &&
                        div.clientHeight > 300) {
                        var style = window.getComputedStyle(div);
                        if (style.overflowY === 'auto' || style.overflowY === 'scroll') {
                            return div;
                        }
                    }
                }
                return null;
            }
            return findScrollableParent();
            """

            scroll_container = self.driver.execute_script(scroll_js)

            if scroll_container:
                container_class = scroll_container.get_attribute("class") or "unknown"
                logger.info(f"Found scroll container (class: {container_class[:50]}...)")
            else:
                logger.warning("No scroll container found, using body scroll")

            # Scroll and collect
            no_new_count = 0
            max_no_new = 15  # More attempts
            last_count = 0
            scroll_position = 0

            logger.info(f"Starting scroll collection (target: {max_items})...")

            while len(unique_links) < max_items and no_new_count < max_no_new:
                # Collect current visible links
                links = self.driver.find_elements(By.CSS_SELECTOR, "a[href*='/firm/']")

                for link in links:
                    try:
                        href = link.get_attribute("href")
                        if href and "/firm/" in href:
                            # Clean URL - remove query params and hash
                            clean = href.split("?")[0].split("#")[0]
                            if clean not in unique_links:
                                unique_links.add(clean)
                    except:
                        continue

                current_count = len(unique_links)

                if current_count > last_count:
                    logger.info(f"Collected: {current_count} links (+{current_count - last_count})")
                    last_count = current_count
                    no_new_count = 0
                else:
                    no_new_count += 1

                if current_count >= max_items:
                    break

                # Multiple scroll strategies
                try:
                    # Strategy 1: Scroll container directly
                    if scroll_container:
                        scroll_position += 600
                        self.driver.execute_script(
                            f"arguments[0].scrollTop = {scroll_position};",
                            scroll_container
                        )

                    # Strategy 2: Scroll last link into view
                    if links:
                        last_link = links[-1]
                        self.driver.execute_script(
                            "arguments[0].scrollIntoView({behavior: 'smooth', block: 'center'});",
                            last_link
                        )

                    # Strategy 3: Keyboard scroll
                    body = self.driver.find_element(By.TAG_NAME, "body")
                    body.send_keys(Keys.PAGE_DOWN)

                    # Strategy 4: Wheel event simulation
                    if scroll_container:
                        self.driver.execute_script("""
                            var evt = new WheelEvent('wheel', {
                                deltaY: 500,
                                bubbles: true
                            });
                            arguments[0].dispatchEvent(evt);
                        """, scroll_container)

                except Exception as e:
                    logger.debug(f"Scroll error: {e}")

                # Wait for lazy load
                time.sleep(random.uniform(0.8, 1.5))

            logger.info(f"Total collected: {len(unique_links)} links")
            return list(unique_links)[:max_items]

        except Exception as e:
            logger.error(f"Error collecting links: {e}")
            return list(unique_links)

        finally:
            self._close_driver()

    def parse_company(self, url: str, city: str = "") -> Optional[Dict]:
        """Parse a single company page."""
        if not self.driver:
            self.driver = self._create_driver()

        max_retries = 2
        for attempt in range(max_retries):
            try:
                self.driver.get(url)
                time.sleep(random.uniform(1.5, 2.5))

                # Wait for name
                try:
                    WebDriverWait(self.driver, 8).until(
                        EC.presence_of_element_located((By.TAG_NAME, "h1"))
                    )
                except:
                    pass

                data = {
                    "name": None,
                    "city": city.capitalize() if city else "",
                    "country": "Россия",
                    "address": None,
                    "phones": [],
                    "websites": [],
                    "emails": [],
                    "url": url
                }

                # Name
                try:
                    data["name"] = self.driver.find_element(By.TAG_NAME, "h1").text.strip()
                except:
                    pass

                # Address
                try:
                    for sel in ["a[href*='/geo/']", "[class*='address']"]:
                        try:
                            elem = self.driver.find_element(By.CSS_SELECTOR, sel)
                            text = elem.text.strip()
                            if text and len(text) > 5:
                                data["address"] = text
                                break
                        except:
                            continue
                except:
                    pass

                # Click "Show phone" if exists
                try:
                    btns = self.driver.find_elements(By.XPATH,
                        "//*[contains(text(), 'Показать') and contains(text(), 'телефон')]"
                    )
                    for btn in btns[:2]:
                        try:
                            btn.click()
                            time.sleep(0.3)
                        except:
                            pass
                except:
                    pass

                # Phones
                try:
                    phones = self.driver.find_elements(By.CSS_SELECTOR, "a[href^='tel:']")
                    for p in phones:
                        href = p.get_attribute("href")
                        if href:
                            raw = href.replace("tel:", "")
                            norm = self._normalize_phone(raw)
                            if norm and norm not in data["phones"]:
                                data["phones"].append(norm)
                except:
                    pass

                # Emails
                try:
                    emails = self.driver.find_elements(By.CSS_SELECTOR, "a[href^='mailto:']")
                    for e in emails:
                        href = e.get_attribute("href")
                        if href:
                            email = href.replace("mailto:", "").split("?")[0]
                            if email and email not in data["emails"]:
                                data["emails"].append(email)
                except:
                    pass

                # Websites
                try:
                    data["websites"] = self._extract_websites()
                except:
                    pass

                return data

            except Exception as e:
                if attempt < max_retries - 1:
                    logger.warning(f"Retry {attempt + 1} for {url}")
                    time.sleep(2)
                else:
                    logger.error(f"Failed to parse {url}: {e}")
                    return None

        return None

    def _extract_websites(self) -> List[str]:
        """Extract website URLs."""
        ignored = [
            "2gis.", "google.", "yandex.", "vk.com", "t.me",
            "instagram.com", "facebook.com", "twitter.com", "ok.ru",
            "youtube.com", "whatsapp.com", "wa.me", "apple.com",
            "play.google.com", "apps.apple.com", "booking.com",
        ]

        websites = []
        try:
            links = self.driver.find_elements(By.CSS_SELECTOR, "a[href^='http']")
            for link in links:
                try:
                    href = link.get_attribute("href")
                    if not href:
                        continue

                    href_lower = href.lower()
                    if any(d in href_lower for d in ignored):
                        continue

                    text = link.text.lower().strip()
                    if any(kw in text for kw in ["скачать", "app", "приложение"]):
                        continue

                    # Priority for explicit website links
                    if text in ["сайт", "website"] or ("." in text and " " not in text):
                        if href not in websites:
                            websites.insert(0, href)
                    elif len(websites) < 3:
                        if href not in websites:
                            websites.append(href)
                except:
                    continue
        except:
            pass

        return websites[:3]

    def _normalize_phone(self, phone: str) -> str:
        """Normalize phone to +7 format."""
        if not phone:
            return ""
        digits = re.sub(r'\D', '', phone)
        if not digits:
            return ""
        if len(digits) == 11:
            if digits.startswith('7') or digits.startswith('8'):
                return f"+7{digits[1:]}"
        elif len(digits) == 10:
            return f"+7{digits}"
        return f"+{digits}" if not phone.startswith('+') else phone

    def close(self):
        self._close_driver()


def parse_companies_batch(links: List[str], city: str, worker_id: int = 0) -> List[Dict]:
    """Parse a batch of company links."""
    results = []
    parser = TwoGisParser(headless=True)

    try:
        for i, url in enumerate(links):
            try:
                data = parser.parse_company(url, city)
                if data and data.get("name"):
                    results.append(data)

                # Small delay between pages
                if i < len(links) - 1:
                    time.sleep(random.uniform(0.3, 0.8))

            except Exception as e:
                logger.error(f"[Worker {worker_id}] Error parsing {url}: {e}")
                continue

    finally:
        parser.close()

    return results


def save_to_excel(data: List[Dict], filename: str = "companies.xlsx"):
    """Save data to Excel."""
    if not data:
        logger.warning("No data to save.")
        return

    processed = []
    for item in data:
        p = item.copy()
        p['phones'] = ", ".join(item.get('phones', [])) if item.get('phones') else ""
        p['emails'] = ", ".join(item.get('emails', [])) if item.get('emails') else ""
        p['websites'] = ", ".join(item.get('websites', [])) if item.get('websites') else ""
        processed.append(p)

    df = pd.DataFrame(processed)

    columns = {
        "name": "Название",
        "city": "Город",
        "country": "Страна",
        "address": "Адрес",
        "phones": "Телефоны",
        "websites": "Сайты",
        "emails": "Email",
        "url": "Ссылка 2ГИС"
    }
    df.rename(columns=columns, inplace=True)

    order = ["Название", "Город", "Страна", "Адрес", "Телефоны", "Email", "Сайты", "Ссылка 2ГИС"]
    cols = [c for c in order if c in df.columns]
    df = df[cols]

    try:
        df.to_excel(filename, index=False, engine='openpyxl')
        logger.info(f"Saved {len(data)} records to {filename}")
    except Exception as e:
        logger.error(f"Excel save error: {e}")
        csv_name = filename.replace('.xlsx', '.csv')
        df.to_csv(csv_name, index=False, encoding='utf-8-sig')
        logger.info(f"Saved as CSV: {csv_name}")


def run_parser(
    city: str,
    niches: List[str],
    max_items_per_niche: int = 500,
    max_workers: int = 3,
    output_file: str = "companies.xlsx"
) -> List[Dict]:
    """
    Main parser function.

    Args:
        city: City name (e.g., 'moscow', 'novosibirsk')
        niches: List of search queries
        max_items_per_niche: Max items per query
        max_workers: Parallel workers for parsing details
        output_file: Output filename
    """
    all_links = []

    logger.info("=" * 50)
    logger.info(f"2GIS Parser - City: {city}, Queries: {niches}")
    logger.info(f"Max items: {max_items_per_niche}, Workers: {max_workers}")
    logger.info("=" * 50)

    # Phase 1: Collect all links
    logger.info("\n--- Phase 1: Collecting Links ---")

    for niche in niches:
        logger.info(f"\nSearching: '{niche}'")
        parser = TwoGisParser(headless=True)
        try:
            links = parser.collect_links_with_scroll(city, niche, max_items_per_niche)
            all_links.extend(links)
            logger.info(f"Found {len(links)} links for '{niche}'")
        finally:
            parser.close()

        # Delay between niches
        if len(niches) > 1:
            time.sleep(random.uniform(2, 4))

    # Deduplicate
    unique_links = list(set(all_links))
    logger.info(f"\nTotal unique links: {len(unique_links)}")

    if not unique_links:
        logger.warning("No links found!")
        return []

    # Phase 2: Parse company details
    logger.info(f"\n--- Phase 2: Parsing {len(unique_links)} Companies ---")

    all_results = []

    # Split into chunks for parallel processing
    chunk_size = max(5, len(unique_links) // max_workers)
    chunks = [unique_links[i:i + chunk_size] for i in range(0, len(unique_links), chunk_size)]

    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {
            executor.submit(parse_companies_batch, chunk, city, idx): idx
            for idx, chunk in enumerate(chunks)
        }

        with tqdm(total=len(unique_links), desc="Parsing", unit="company") as pbar:
            for future in concurrent.futures.as_completed(futures):
                try:
                    results = future.result()
                    all_results.extend(results)
                    pbar.update(len(chunks[futures[future]]))
                except Exception as e:
                    logger.error(f"Worker error: {e}")

    # Deduplicate by URL
    seen = set()
    final = []
    for r in all_results:
        url = r.get("url", "")
        if url and url not in seen:
            seen.add(url)
            final.append(r)

    # Save
    if final:
        save_to_excel(final, output_file)

    logger.info(f"\n{'=' * 50}")
    logger.info(f"DONE: {len(final)} companies collected")
    logger.info(f"{'=' * 50}")

    return final


def interactive_cli():
    """Interactive CLI."""
    print("\n" + "=" * 50)
    print("     2GIS Parser v3.0 (Virtual Scroll)")
    print("=" * 50)

    # City
    city = input("\nГород (moscow/novosibirsk/etc): ").strip() or "moscow"

    # Queries
    print("\nВведите запросы через запятую")
    print("Например: стоматология, аптека, салон красоты")
    queries = input("Запросы: ").strip()

    if queries:
        niches = [q.strip() for q in queries.split(",") if q.strip()]
    else:
        niches = ["кафе"]

    # Max items
    try:
        max_items = int(input("\nМакс. компаний на запрос (100-2000): ").strip() or "200")
        max_items = min(max(50, max_items), 5000)
    except:
        max_items = 200

    # Workers
    try:
        workers = int(input("Потоков для парсинга (1-5): ").strip() or "3")
        workers = min(max(1, workers), 5)
    except:
        workers = 3

    # Output
    output = input("Файл (companies.xlsx): ").strip() or "companies.xlsx"

    print(f"\n{'=' * 50}")
    print(f"Город: {city}")
    print(f"Запросы: {niches}")
    print(f"Лимит: {max_items}")
    print(f"Потоков: {workers}")
    print(f"Файл: {output}")
    print(f"{'=' * 50}")

    if input("\nНачать? (Y/n): ").strip().lower() == 'n':
        print("Отменено.")
        return

    print("\nЗапуск... (Ctrl+C для остановки)\n")

    start = time.time()

    try:
        results = run_parser(
            city=city,
            niches=niches,
            max_items_per_niche=max_items,
            max_workers=workers,
            output_file=output
        )

        elapsed = time.time() - start
        print(f"\n✓ Готово! {len(results)} компаний за {elapsed:.1f} сек.")

    except KeyboardInterrupt:
        print("\n\nОстановлено пользователем.")
    except Exception as e:
        logger.error(f"Ошибка: {e}")


if __name__ == "__main__":
    interactive_cli()
