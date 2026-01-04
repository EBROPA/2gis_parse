import time
import random
import json
import logging
import re
import os
import concurrent.futures
from typing import List, Dict, Optional, Set, Tuple
from urllib.parse import quote, urlencode, urlparse
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
    """Advanced anti-detection and anti-ban measures."""

    # Pool of realistic User Agents (updated 2024)
    USER_AGENTS = [
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:121.0) Gecko/20100101 Firefox/121.0",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.2 Safari/605.1.15",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36 Edg/120.0.0.0",
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/118.0.0.0 Safari/537.36",
    ]

    RESOLUTIONS = [
        (1920, 1080), (1366, 768), (1440, 900), (1536, 864),
        (1280, 720), (1280, 800), (1600, 900), (1680, 1050),
        (2560, 1440), (1920, 1200)
    ]

    LANGUAGES = ["ru-RU,ru;q=0.9,en-US;q=0.8,en;q=0.7", "ru,en;q=0.9", "ru-RU,ru;q=0.9"]

    def __init__(self, proxy_list: List[str] = None):
        self.proxy_list = proxy_list or []
        self.current_proxy_index = 0
        self._lock = threading.Lock()
        try:
            self.ua = UserAgent()
        except:
            self.ua = None

    def get_random_user_agent(self) -> str:
        """Returns a random realistic user agent."""
        if self.ua:
            try:
                return self.ua.random
            except:
                pass
        return random.choice(self.USER_AGENTS)

    def get_random_resolution(self) -> Tuple[int, int]:
        return random.choice(self.RESOLUTIONS)

    def get_random_language(self) -> str:
        return random.choice(self.LANGUAGES)

    def get_next_proxy(self) -> Optional[str]:
        """Returns next proxy from the pool (round-robin)."""
        if not self.proxy_list:
            return None
        with self._lock:
            proxy = self.proxy_list[self.current_proxy_index]
            self.current_proxy_index = (self.current_proxy_index + 1) % len(self.proxy_list)
        return proxy

    @staticmethod
    def human_delay(min_sec: float = 0.5, max_sec: float = 2.0):
        """Random delay simulating human behavior."""
        time.sleep(random.uniform(min_sec, max_sec))

    @staticmethod
    def smart_delay(page_num: int, base_min: float = 1.0, base_max: float = 2.5):
        """Adaptive delay that increases with page number to avoid rate limiting."""
        # Add extra delay every N pages
        extra = (page_num // 10) * 0.5
        delay = random.uniform(base_min + extra, base_max + extra)
        time.sleep(delay)


class TwoGisAPI:
    """
    Direct API access to 2GIS for faster and more reliable data extraction.
    Uses the same API endpoints that the website uses internally.
    """

    BASE_API_URL = "https://catalog.api.2gis.com/3.0/items"
    SEARCH_API_URL = "https://catalog.api.2gis.com/3.0/items"

    # API keys found in 2GIS web app (public keys)
    API_KEYS = [
        "rurbbn3446",  # Main key
        "runjvb6743",
        "rutnpt6224",
    ]

    def __init__(self, anti_detection: AntiDetection = None):
        self.anti_detection = anti_detection or AntiDetection()
        self.session = requests.Session()
        self._update_session_headers()

    def _update_session_headers(self):
        """Update session with new random headers."""
        self.session.headers.update({
            "User-Agent": self.anti_detection.get_random_user_agent(),
            "Accept": "application/json, text/plain, */*",
            "Accept-Language": self.anti_detection.get_random_language(),
            "Origin": "https://2gis.ru",
            "Referer": "https://2gis.ru/",
        })

    def _get_api_key(self) -> str:
        return random.choice(self.API_KEYS)

    def get_region_id(self, city: str) -> Optional[int]:
        """Get region ID for a city."""
        city_map = {
            "moscow": 32, "novosibirsk": 1, "saint-petersburg": 2, "spb": 2,
            "krasnoyarsk": 4, "ekaterinburg": 3, "kazan": 12, "nizhny_novgorod": 7,
            "samara": 8, "omsk": 5, "chelyabinsk": 6, "rostov-on-don": 9,
            "ufa": 10, "volgograd": 11, "perm": 13, "voronezh": 14,
            "krasnodar": 15, "saratov": 16, "tyumen": 17, "tolyatti": 18,
            "izhevsk": 19, "barnaul": 20, "irkutsk": 21, "ulyanovsk": 22,
            "khabarovsk": 23, "vladivostok": 24, "yaroslavl": 25, "tomsk": 26,
            "orenburg": 27, "kemerovo": 28, "ryazan": 29, "naberezhnye_chelny": 30,
            "penza": 31, "almaty": 33, "astana": 34, "nur-sultan": 34,
            "minsk": 35, "kiev": 36, "kyiv": 36, "dubai": 37,
        }
        city_lower = city.lower().replace(" ", "_").replace("-", "_")
        return city_map.get(city_lower)

    def search_items(self, city: str, query: str, page: int = 1, page_size: int = 50) -> Dict:
        """
        Search for items using 2GIS API.
        Returns raw API response with items and total count.
        """
        region_id = self.get_region_id(city)

        params = {
            "q": query,
            "page": page,
            "page_size": page_size,
            "key": self._get_api_key(),
            "fields": "items.point,items.adm_div,items.contact_groups,items.schedule,items.org,items.name_ex,items.external_content",
            "sort": "relevance",
            "type": "branch,org",
        }

        if region_id:
            params["region_id"] = region_id
        else:
            # Try using city name directly
            params["region_id"] = 32  # Default to Moscow
            params["q"] = f"{query} {city}"

        try:
            self._update_session_headers()
            AntiDetection.human_delay(0.3, 0.8)

            response = self.session.get(
                self.SEARCH_API_URL,
                params=params,
                timeout=15
            )

            if response.status_code == 200:
                return response.json()
            elif response.status_code == 429:
                logger.warning("API rate limit hit, waiting...")
                time.sleep(random.uniform(5, 10))
                return {"result": {"items": [], "total": 0}}
            else:
                logger.warning(f"API returned status {response.status_code}")
                return {"result": {"items": [], "total": 0}}

        except Exception as e:
            logger.error(f"API request failed: {e}")
            return {"result": {"items": [], "total": 0}}

    def parse_api_item(self, item: Dict, city: str) -> Dict:
        """Parse a single item from API response."""
        data = {
            "name": item.get("name", ""),
            "city": city.capitalize(),
            "country": "Россия",
            "address": "",
            "phones": [],
            "websites": [],
            "emails": [],
            "url": f"https://2gis.ru/firm/{item.get('id', '')}"
        }

        # Address
        if "address_name" in item:
            data["address"] = item["address_name"]
        elif "adm_div" in item:
            parts = []
            for div in item.get("adm_div", []):
                if div.get("type") in ["city", "street"]:
                    parts.append(div.get("name", ""))
            if parts:
                data["address"] = ", ".join(parts)

        # Contacts
        for group in item.get("contact_groups", []):
            for contact in group.get("contacts", []):
                ctype = contact.get("type", "")
                value = contact.get("value", "")

                if ctype == "phone":
                    normalized = self._normalize_phone(value)
                    if normalized and normalized not in data["phones"]:
                        data["phones"].append(normalized)

                elif ctype == "email":
                    if value and value not in data["emails"]:
                        data["emails"].append(value)

                elif ctype == "website":
                    if value and not self._is_ignored_domain(value):
                        if value not in data["websites"]:
                            data["websites"].append(value)

        return data

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

    def _is_ignored_domain(self, url: str) -> bool:
        """Check if URL should be ignored."""
        ignored = [
            "2gis.", "google.", "yandex.", "vk.com", "t.me",
            "instagram.com", "facebook.com", "twitter.com", "ok.ru",
            "youtube.com", "whatsapp.com", "wa.me", "apple.com",
            "play.google.com", "apps.apple.com", "booking.com",
            "tripadvisor.", "delivery-club.ru", "eda.yandex",
            "zoon.ru", "yell.ru", "onelink.me"
        ]
        url_lower = url.lower()
        return any(d in url_lower for d in ignored)


class SeleniumParser:
    """Enhanced Selenium parser with pagination support and anti-ban measures."""

    def __init__(self, headless: bool = True, proxy: str = None, anti_detection: AntiDetection = None):
        self.anti_detection = anti_detection or AntiDetection()
        self.proxy = proxy
        self.headless = headless
        self.driver = None
        self._setup_driver()

    def _setup_driver(self):
        """Setup Chrome driver with stealth settings."""
        options = Options()

        # User Agent
        user_agent = self.anti_detection.get_random_user_agent()
        options.add_argument(f'user-agent={user_agent}')

        # Language
        options.add_argument(f"--lang={self.anti_detection.get_random_language().split(',')[0]}")

        # Window size
        width, height = self.anti_detection.get_random_resolution()
        options.add_argument(f'--window-size={width},{height}')

        # Headless mode
        if self.headless:
            options.add_argument("--headless=new")

        # Anti-detection flags
        options.add_argument("--disable-blink-features=AutomationControlled")
        options.add_experimental_option("excludeSwitches", ["enable-automation"])
        options.add_experimental_option('useAutomationExtension', False)
        options.add_argument("--no-sandbox")
        options.add_argument("--disable-dev-shm-usage")
        options.add_argument("--disable-gpu")
        options.add_argument("--log-level=3")
        options.add_argument("--disable-logging")

        # Disable images for faster loading (optional, can be enabled)
        # options.add_argument("--blink-settings=imagesEnabled=false")

        # Proxy support
        if self.proxy:
            options.add_argument(f'--proxy-server={self.proxy}')

        # Additional stealth
        options.add_argument("--disable-extensions")
        options.add_argument("--disable-plugins-discovery")
        options.add_argument("--disable-infobars")

        # Preferences
        prefs = {
            "profile.default_content_setting_values.notifications": 2,
            "credentials_enable_service": False,
            "profile.password_manager_enabled": False,
            "webrtc.ip_handling_policy": "disable_non_proxied_udp",
            "webrtc.multiple_routes_enabled": False,
            "webrtc.nonproxied_udp_enabled": False
        }
        options.add_experimental_option("prefs", prefs)

        # Initialize driver
        try:
            service = ChromeService(ChromeDriverManager().install())
            self.driver = webdriver.Chrome(service=service, options=options)
        except Exception as e:
            logger.error(f"Failed to initialize driver: {e}")
            raise

        # Execute stealth scripts
        self._inject_stealth_scripts()

        # Set timeouts
        self.driver.set_page_load_timeout(30)
        self.driver.implicitly_wait(5)

    def _inject_stealth_scripts(self):
        """Inject JavaScript to mask automation indicators."""
        stealth_js = """
            // Remove webdriver flag
            Object.defineProperty(navigator, 'webdriver', {
                get: () => undefined
            });

            // Mock plugins
            Object.defineProperty(navigator, 'plugins', {
                get: () => [1, 2, 3, 4, 5]
            });

            // Mock languages
            Object.defineProperty(navigator, 'languages', {
                get: () => ['ru-RU', 'ru', 'en-US', 'en']
            });

            // Mock permissions
            const originalQuery = window.navigator.permissions.query;
            window.navigator.permissions.query = (parameters) => (
                parameters.name === 'notifications' ?
                    Promise.resolve({ state: Notification.permission }) :
                    originalQuery(parameters)
            );

            // Mock chrome runtime
            window.chrome = {
                runtime: {}
            };

            // Override toString for functions
            const originalToString = Function.prototype.toString;
            Function.prototype.toString = function() {
                if (this === navigator.webdriver) {
                    return 'function webdriver() { [native code] }';
                }
                return originalToString.call(this);
            };
        """
        try:
            self.driver.execute_cdp_cmd("Page.addScriptToEvaluateOnNewDocument", {"source": stealth_js})
        except:
            pass

    def close(self):
        """Close the driver."""
        if self.driver:
            try:
                self.driver.quit()
            except:
                pass

    def get_search_links_with_pagination(self, city: str, query: str, max_items: int = 1000) -> List[str]:
        """
        Collect company links using proper pagination.
        2GIS uses page parameter in URL: /search/query?page=N
        """
        unique_links: Set[str] = set()
        page = 1
        max_pages = (max_items // 12) + 5  # ~12 items per page + buffer
        consecutive_empty = 0
        max_consecutive_empty = 3

        logger.info(f"Collecting links for '{query}' in '{city}' (max: {max_items})...")

        while len(unique_links) < max_items and page <= max_pages:
            try:
                # Build URL with page parameter
                encoded_query = quote(query, safe='')
                if page == 1:
                    url = f"https://2gis.ru/{city}/search/{encoded_query}"
                else:
                    url = f"https://2gis.ru/{city}/search/{encoded_query}?page={page}"

                self.driver.get(url)

                # Smart delay based on page number
                AntiDetection.smart_delay(page, 1.5, 3.0)

                # Wait for results
                try:
                    WebDriverWait(self.driver, 10).until(
                        EC.presence_of_element_located((By.CSS_SELECTOR, "a[href*='/firm/']"))
                    )
                except:
                    # No results on this page
                    consecutive_empty += 1
                    if consecutive_empty >= max_consecutive_empty:
                        logger.info(f"No more results after page {page}")
                        break
                    page += 1
                    continue

                # Extract links
                links = self.driver.find_elements(By.CSS_SELECTOR, "a[href*='/firm/']")

                initial_count = len(unique_links)

                for link in links:
                    try:
                        href = link.get_attribute("href")
                        if href and "/firm/" in href:
                            # Clean URL - remove any query parameters
                            clean_href = href.split("?")[0]
                            unique_links.add(clean_href)
                    except:
                        continue

                new_count = len(unique_links)
                new_on_page = new_count - initial_count

                if new_on_page > 0:
                    consecutive_empty = 0
                    logger.info(f"Page {page}: +{new_on_page} new links (total: {new_count})")
                else:
                    consecutive_empty += 1
                    if consecutive_empty >= max_consecutive_empty:
                        logger.info(f"No new links for {consecutive_empty} pages, stopping")
                        break

                # Check for "end of results" indicators
                try:
                    no_results = self.driver.find_elements(By.XPATH,
                        "//*[contains(text(), 'ничего не найдено') or contains(text(), 'Ничего не нашлось')]")
                    if no_results:
                        logger.info("End of results reached")
                        break
                except:
                    pass

                page += 1

            except Exception as e:
                logger.warning(f"Error on page {page}: {e}")
                consecutive_empty += 1
                if consecutive_empty >= max_consecutive_empty:
                    break
                page += 1
                continue

        result = list(unique_links)[:max_items]
        logger.info(f"Total collected: {len(result)} unique links")
        return result

    def parse_company_page(self, url: str, city: str = "") -> Optional[Dict]:
        """Parse a single company page."""
        max_retries = 3

        for attempt in range(max_retries):
            try:
                self.driver.get(url)
                AntiDetection.human_delay(1.5, 3.0)

                # Wait for page load
                WebDriverWait(self.driver, 10).until(
                    EC.presence_of_element_located((By.TAG_NAME, "h1"))
                )

                data = self._extract_company_details(city)
                data["url"] = url
                return data

            except Exception as e:
                if attempt < max_retries - 1:
                    logger.warning(f"Retry {attempt + 1} for {url}: {e}")
                    time.sleep(random.uniform(2, 4))
                else:
                    logger.error(f"Failed to parse {url}: {e}")
                    return None

        return None

    def _extract_company_details(self, city: str = "") -> Dict:
        """Extract company details from current page."""
        data = {
            "name": None,
            "city": city.capitalize() if city else "",
            "country": "Россия",
            "address": None,
            "phones": [],
            "websites": [],
            "emails": []
        }

        # Name
        try:
            h1 = self.driver.find_element(By.TAG_NAME, "h1")
            data["name"] = h1.text.strip()
        except:
            pass

        # Address
        try:
            addr_selectors = [
                "a[href*='/geo/']",
                "[class*='address']",
                "[data-name='AdditionalInfo'] a",
            ]
            for sel in addr_selectors:
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

        # Click "Show phone" buttons
        try:
            buttons = self.driver.find_elements(By.XPATH,
                "//*[contains(text(), 'Показать телефон') or contains(text(), 'Show phone') or contains(text(), 'показать')]")
            for btn in buttons[:3]:  # Limit clicks
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
                try:
                    href = p.get_attribute("href")
                    if href:
                        raw = href.replace("tel:", "")
                        normalized = self._normalize_phone(raw)
                        if normalized and normalized not in data["phones"]:
                            data["phones"].append(normalized)
                except:
                    pass
        except:
            pass

        # Emails
        try:
            emails = self.driver.find_elements(By.CSS_SELECTOR, "a[href^='mailto:']")
            for e in emails:
                try:
                    href = e.get_attribute("href")
                    if href:
                        email = href.replace("mailto:", "").split("?")[0]
                        if email and email not in data["emails"]:
                            data["emails"].append(email)
                except:
                    pass
        except:
            pass

        # Websites
        try:
            data["websites"] = self._extract_websites()
        except:
            pass

        # Fallback: website from email domain
        if not data["websites"] and data["emails"]:
            generic = ["gmail.com", "yandex.ru", "mail.ru", "bk.ru", "list.ru",
                      "inbox.ru", "yahoo.com", "hotmail.com", "outlook.com", "rambler.ru"]
            for email in data["emails"]:
                try:
                    domain = email.split("@")[1]
                    if domain.lower() not in generic:
                        data["websites"].append(f"https://{domain}")
                        break
                except:
                    pass

        return data

    def _extract_websites(self) -> List[str]:
        """Extract website URLs from the page."""
        ignored_domains = [
            "2gis.", "google.", "yandex.", "vk.com", "t.me",
            "instagram.com", "facebook.com", "twitter.com", "ok.ru",
            "youtube.com", "whatsapp.com", "wa.me", "apple.com",
            "play.google.com", "apps.apple.com", "booking.com", "tripadvisor.",
            "delivery-club.ru", "eda.yandex", "zoon.ru", "yell.ru"
        ]

        websites = []
        seen = set()

        try:
            links = self.driver.find_elements(By.CSS_SELECTOR, "a[href^='http']")

            for link in links:
                try:
                    href = link.get_attribute("href")
                    text = link.text.lower().strip()

                    if not href:
                        continue

                    href_lower = href.lower()

                    # Check blacklist
                    if any(d in href_lower for d in ignored_domains):
                        continue

                    # Skip app links
                    if any(kw in text for kw in ["скачать", "download", "app", "приложение"]):
                        continue

                    # Good indicators
                    is_likely_website = (
                        text in ["сайт", "website", "веб-сайт"] or
                        ("." in text and " " not in text and len(text) > 3)
                    )

                    if is_likely_website or len(websites) < 5:
                        if href not in seen:
                            seen.add(href)
                            if is_likely_website:
                                websites.insert(0, href)  # Priority
                            else:
                                websites.append(href)

                except:
                    continue

        except:
            pass

        return websites[:5]  # Limit to 5 websites

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


class HybridParser:
    """
    Hybrid approach: Use API for listing, Selenium for details.
    This is faster and more reliable than pure Selenium.
    """

    def __init__(self, anti_detection: AntiDetection = None, use_api: bool = True):
        self.anti_detection = anti_detection or AntiDetection()
        self.use_api = use_api
        self.api = TwoGisAPI(self.anti_detection) if use_api else None

    def collect_items_via_api(self, city: str, query: str, max_items: int = 1000) -> List[Dict]:
        """Collect items using the API (fast method)."""
        if not self.api:
            return []

        items = []
        page = 1
        page_size = 50  # Max allowed by API
        consecutive_empty = 0

        logger.info(f"[API] Collecting '{query}' in '{city}' (max: {max_items})...")

        total_found = None

        while len(items) < max_items:
            response = self.api.search_items(city, query, page, page_size)

            result = response.get("result", {})
            page_items = result.get("items", [])

            if total_found is None:
                total_found = result.get("total", 0)
                logger.info(f"[API] Total available: {total_found}")

            if not page_items:
                consecutive_empty += 1
                if consecutive_empty >= 2:
                    break
                page += 1
                continue

            consecutive_empty = 0

            for item in page_items:
                if len(items) >= max_items:
                    break
                parsed = self.api.parse_api_item(item, city)
                if parsed and parsed.get("name"):
                    items.append(parsed)

            logger.info(f"[API] Page {page}: collected {len(items)} items")

            # Check if we've got all available items
            if len(items) >= total_found:
                break

            page += 1
            AntiDetection.human_delay(0.5, 1.5)

        logger.info(f"[API] Total collected: {len(items)} items")
        return items

    def collect_links_via_selenium(self, city: str, query: str, max_items: int = 1000) -> List[str]:
        """Fallback: collect links using Selenium with pagination."""
        parser = SeleniumParser(headless=True, anti_detection=self.anti_detection)
        try:
            return parser.get_search_links_with_pagination(city, query, max_items)
        finally:
            parser.close()


def process_links_chunk(links: List[str], city: str, anti_detection: AntiDetection = None) -> List[Dict]:
    """Worker function to process a chunk of links."""
    results = []
    proxy = anti_detection.get_next_proxy() if anti_detection else None
    parser = SeleniumParser(headless=True, proxy=proxy, anti_detection=anti_detection)

    try:
        for url in links:
            data = parser.parse_company_page(url, city=city)
            if data:
                results.append(data)
            # Random delay between requests
            AntiDetection.human_delay(0.5, 1.5)
    finally:
        parser.close()

    return results


def save_to_excel(data: List[Dict], filename: str = "companies.xlsx"):
    """Save data to Excel file."""
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

    column_map = {
        "name": "Название",
        "city": "Город",
        "country": "Страна",
        "address": "Адрес",
        "phones": "Телефоны",
        "websites": "Сайты",
        "emails": "Email",
        "url": "Ссылка 2ГИС"
    }

    df.rename(columns=column_map, inplace=True)

    order = ["Название", "Город", "Страна", "Адрес", "Телефоны", "Email", "Сайты", "Ссылка 2ГИС"]
    cols = [c for c in order if c in df.columns]
    df = df[cols]

    try:
        df.to_excel(filename, index=False, engine='openpyxl')
        logger.info(f"Saved {len(data)} records to {filename}")
    except Exception as e:
        logger.error(f"Error saving: {e}")
        # Fallback to CSV
        csv_name = filename.replace('.xlsx', '.csv')
        df.to_csv(csv_name, index=False, encoding='utf-8-sig')
        logger.info(f"Saved as CSV: {csv_name}")


def save_incremental(data: List[Dict], filename: str = "companies.xlsx"):
    """Save data incrementally (append if file exists)."""
    if not data:
        return

    processed = []
    for item in data:
        p = item.copy()
        p['phones'] = ", ".join(item.get('phones', [])) if item.get('phones') else ""
        p['emails'] = ", ".join(item.get('emails', [])) if item.get('emails') else ""
        p['websites'] = ", ".join(item.get('websites', [])) if item.get('websites') else ""
        processed.append(p)

    new_df = pd.DataFrame(processed)

    column_map = {
        "name": "Название",
        "city": "Город",
        "country": "Страна",
        "address": "Адрес",
        "phones": "Телефоны",
        "websites": "Сайты",
        "emails": "Email",
        "url": "Ссылка 2ГИС"
    }
    new_df.rename(columns=column_map, inplace=True)

    try:
        if os.path.exists(filename):
            existing_df = pd.read_excel(filename)
            combined = pd.concat([existing_df, new_df], ignore_index=True)
            # Remove duplicates by URL
            if "Ссылка 2ГИС" in combined.columns:
                combined.drop_duplicates(subset=["Ссылка 2ГИС"], keep='first', inplace=True)
            combined.to_excel(filename, index=False, engine='openpyxl')
        else:
            new_df.to_excel(filename, index=False, engine='openpyxl')
        logger.info(f"Saved/updated {filename}")
    except Exception as e:
        logger.error(f"Incremental save error: {e}")


def run_parser(
    city: str,
    niches: List[str],
    max_items_per_niche: int = 500,
    max_workers: int = 4,
    use_api: bool = True,
    output_file: str = "companies.xlsx",
    proxy_list: List[str] = None
) -> List[Dict]:
    """
    Main parser function.

    Args:
        city: City name (e.g., 'moscow', 'novosibirsk')
        niches: List of search queries
        max_items_per_niche: Maximum items to collect per niche
        max_workers: Number of parallel workers for Selenium
        use_api: Try API first (faster), fallback to Selenium
        output_file: Output Excel filename
        proxy_list: List of proxy servers (optional)

    Returns:
        List of collected company data
    """
    anti_detection = AntiDetection(proxy_list)
    hybrid = HybridParser(anti_detection, use_api=use_api)

    all_results = []
    all_links_to_parse = []

    logger.info("=" * 50)
    logger.info(f"Starting parser: city={city}, niches={niches}")
    logger.info(f"Max items per niche: {max_items_per_niche}, Workers: {max_workers}")
    logger.info("=" * 50)

    # Phase 1: Collect data (API) or links (Selenium)
    for niche in niches:
        logger.info(f"\n--- Processing niche: '{niche}' ---")

        if use_api:
            # Try API first
            items = hybrid.collect_items_via_api(city, niche, max_items_per_niche)
            if items:
                all_results.extend(items)
                logger.info(f"[API] Got {len(items)} items for '{niche}'")
            else:
                # Fallback to Selenium
                logger.info(f"[API] Failed, falling back to Selenium...")
                links = hybrid.collect_links_via_selenium(city, niche, max_items_per_niche)
                all_links_to_parse.extend(links)
        else:
            # Pure Selenium mode
            links = hybrid.collect_links_via_selenium(city, niche, max_items_per_niche)
            all_links_to_parse.extend(links)

    # Phase 2: Parse individual pages (if we have links from Selenium)
    if all_links_to_parse:
        unique_links = list(set(all_links_to_parse))
        # Remove links we already have from API
        existing_urls = {r.get("url") for r in all_results}
        links_to_parse = [l for l in unique_links if l not in existing_urls]

        if links_to_parse:
            logger.info(f"\n--- Phase 2: Parsing {len(links_to_parse)} company pages ---")

            # Split into chunks
            chunk_size = max(5, len(links_to_parse) // max_workers)
            chunks = [links_to_parse[i:i+chunk_size] for i in range(0, len(links_to_parse), chunk_size)]

            with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
                futures = {
                    executor.submit(process_links_chunk, chunk, city, anti_detection): chunk
                    for chunk in chunks
                }

                with tqdm(total=len(links_to_parse), desc="Parsing pages", unit="page") as pbar:
                    for future in concurrent.futures.as_completed(futures):
                        try:
                            chunk_results = future.result()
                            all_results.extend(chunk_results)
                            pbar.update(len(futures[future]))
                        except Exception as e:
                            logger.error(f"Worker error: {e}")

    # Deduplicate results
    seen_urls = set()
    unique_results = []
    for r in all_results:
        url = r.get("url", "")
        if url and url not in seen_urls:
            seen_urls.add(url)
            unique_results.append(r)

    # Save results
    if unique_results:
        save_to_excel(unique_results, output_file)

    logger.info(f"\n{'=' * 50}")
    logger.info(f"COMPLETED: {len(unique_results)} unique companies collected")
    logger.info(f"{'=' * 50}")

    return unique_results


def interactive_cli():
    """Interactive command-line interface."""
    print("\n" + "=" * 50)
    print("  2GIS Parser v2.0 (with API + Pagination)")
    print("=" * 50)

    # City
    city = input("\nГород (например: moscow, novosibirsk): ").strip() or "moscow"

    # Niches
    print("\nВведите ниши через запятую (например: стоматология, аптека)")
    print("Или 'all' для популярных ниш")
    niches_input = input("Ниши: ").strip()

    if niches_input.lower() == 'all':
        niches = ["кафе", "ресторан", "аптека", "салон красоты", "стоматология",
                  "автосервис", "фитнес", "отель", "магазин продуктов"]
    elif niches_input:
        niches = [n.strip() for n in niches_input.split(",")]
    else:
        niches = ["кафе"]

    # Max items
    try:
        max_items = int(input("\nМаксимум компаний на нишу (по умолчанию: 500): ").strip() or "500")
    except:
        max_items = 500

    # Workers
    try:
        workers = int(input("Число параллельных потоков (по умолчанию: 4): ").strip() or "4")
    except:
        workers = 4

    # API mode
    api_input = input("Использовать быстрый API режим? (Y/n): ").strip().lower()
    use_api = api_input != 'n'

    # Output file
    output = input("Имя файла (по умолчанию: companies.xlsx): ").strip() or "companies.xlsx"

    print(f"\n{'=' * 50}")
    print(f"Город: {city}")
    print(f"Ниши: {niches}")
    print(f"Лимит: {max_items} на нишу")
    print(f"Потоков: {workers}")
    print(f"API режим: {'Да' if use_api else 'Нет'}")
    print(f"Файл: {output}")
    print(f"{'=' * 50}")

    confirm = input("\nНачать? (Y/n): ").strip().lower()
    if confirm == 'n':
        print("Отменено.")
        return

    print("\nЗапуск парсера... (Ctrl+C для остановки)\n")

    start_time = time.time()

    try:
        results = run_parser(
            city=city,
            niches=niches,
            max_items_per_niche=max_items,
            max_workers=workers,
            use_api=use_api,
            output_file=output
        )

        elapsed = time.time() - start_time
        print(f"\n✓ Готово! Собрано {len(results)} компаний за {elapsed:.1f} сек.")

    except KeyboardInterrupt:
        print("\n\nОстановлено пользователем.")
    except Exception as e:
        logger.error(f"Ошибка: {e}")


if __name__ == "__main__":
    interactive_cli()
