import time
import random
import json
import logging
import re
import concurrent.futures
from typing import List, Dict, Optional, Set
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

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class Randomizer:
    """Helper class to manage randomization of browser parameters."""
    
    def __init__(self):
        self.ua = UserAgent()
        
    def get_random_user_agent(self) -> str:
        """Returns a random user agent string."""
        return self.ua.random

    def get_random_window_size(self) -> tuple:
        """Returns a random common window size."""
        resolutions = [
            (1920, 1080),
            (1366, 768),
            (1440, 900),
            (1536, 864),
            (1280, 720),
            (1280, 800)
        ]
        return random.choice(resolutions)

    def random_sleep(self, min_seconds=1.0, max_seconds=3.0):
        """Sleeps for a random duration."""
        time.sleep(random.uniform(min_seconds, max_seconds))

class SeleniumParser:
    def __init__(self, headless=False):
        self.randomizer = Randomizer()
        self.driver = self._setup_driver(headless)
        self.headless = headless
        
    def _setup_driver(self, headless: bool) -> webdriver.Chrome:
        """Sets up the Chrome WebDriver with stealth settings."""
        options = Options()
        
        # 1. Random User Agent
        user_agent = self.randomizer.get_random_user_agent()
        options.add_argument(f'user-agent={user_agent}')
        options.add_argument("--lang=ru-RU")  # Force Russian language
        # logger.info(f"Using User-Agent: {user_agent}")
        
        # 2. Window Size
        width, height = self.randomizer.get_random_window_size()
        options.add_argument(f'--window-size={width},{height}')
        # logger.info(f"Using Window Size: {width}x{height}")
        
        # 3. Headless Mode (if requested)
        if headless:
            options.add_argument("--headless=new")

        # 4. Stealth / Anti-Detection Flags
        options.add_argument("--disable-blink-features=AutomationControlled")
        options.add_experimental_option("excludeSwitches", ["enable-automation"])
        options.add_experimental_option('useAutomationExtension', False)
        
        # Disable generic automation flags
        options.add_argument("--no-sandbox")
        options.add_argument("--disable-dev-shm-usage")
        options.add_argument("--disable-gpu")
        # Suppress logging
        options.add_argument("--log-level=3")
        
        # Initialize Driver
        service = ChromeService(ChromeDriverManager().install())
        driver = webdriver.Chrome(service=service, options=options)
        
        # 5. CDP Command to remove navigator.webdriver flag (Critical for stealth)
        driver.execute_cdp_cmd("Page.addScriptToEvaluateOnNewDocument", {
            "source": """
                Object.defineProperty(navigator, 'webdriver', {
                    get: () => undefined
                });
            """
        })
        
        return driver

    def close(self):
        """Closes the driver."""
        try:
            self.driver.quit()
        except:
            pass

    def get_smart_suggestions(self, city: str, query: str) -> List[str]:
        print(f"DEBUG: get_smart_suggestions called for {query}")
        """
        Interacts with the search bar to find related categories/rubrics.
        Returns a list of category names (e.g. 'Стоматологические поликлиники').
        If no categories found, returns an empty list.
        """
        logger.info(f"Checking smart suggestions for '{query}' in '{city}'...")
        
        try:
            # New Strategy: Go directly to the search page and scrape "Rubrics" or "Did you mean"
            # This is more reliable than the dropdown for "broad" queries like "stomatology"
            url = f"https://2gis.ru/{city}/search/{query}"
            self.driver.get(url)
            self.randomizer.random_sleep(3, 5)
            
            # Wait for body
            WebDriverWait(self.driver, 10).until(
                EC.presence_of_element_located((By.TAG_NAME, "body"))
            )

            # 1. Look for "Rubrics" / "Categories" chips or links
            # Usually they appear as chips at the top or a list
            # Selectors for 2GIS categories/rubrics on search page
            selectors = [
                "div[class*='_1e8064'] a", # Chips
                "a[href*='/rubric/']", # Links to rubrics
                "div[class*='rubric']",
                "//div[contains(text(), 'Rubrics') or contains(text(), 'Рубрики')]/following-sibling::div//a",
                "//div[contains(@class, 'suggest')]//div[contains(@role, 'button')]" # Suggestions in result list
            ]
            
            items = []
            for selector in selectors:
                try:
                    if selector.startswith("//"):
                        found = self.driver.find_elements(By.XPATH, selector)
                    else:
                        found = self.driver.find_elements(By.CSS_SELECTOR, selector)
                    items.extend(found)
                except:
                    pass
            
            # 2. Also check if there are "Did you mean" suggestions
            # "Возможно, вы искали..."
            try:
                did_mean = self.driver.find_elements(By.XPATH, "//div[contains(text(), 'озможно, вы искали')]/..//a")
                items.extend(did_mean)
            except:
                pass

            suggestions_found = set()
            for item in items:
                try:
                    text = item.text.strip()
                    if not text:
                        continue
                    # Clean up text
                    if any(char.isdigit() for char in text) and "," in text: continue # Address
                    
                    if len(text) > 3 and text.lower() != query.lower():
                        suggestions_found.add(text)
                except:
                    continue

            results = list(suggestions_found)
            logger.info(f"Found suggestions via search page: {results}")
            return results

        except Exception as e:
            logger.warning(f"Smart suggestion failed: {e}")
            return []

    def _find_scrollable_parent(self, element):
        """Finds the first scrollable parent of an element."""
        try:
            # 2GIS Specific: The scrollable container is often the one with class containing '_1itmoss' or similar
            # Or we can find it by height.
            return self.driver.execute_script("""
                var el = arguments[0];
                // Try to find a parent that looks like the sidebar list
                while (el && el !== document.body) {
                    var style = window.getComputedStyle(el);
                    // 2GIS Sidebar often has these characteristics:
                    // Height is significant (e.g., > 50% of window)
                    // Overflow is auto/scroll OR it has a specific class
                    
                    if ((style.overflowY === 'auto' || style.overflowY === 'scroll') && el.scrollHeight > el.clientHeight) {
                        return el;
                    }
                    
                    // Fallback: Check if it's the main list container by class heuristic
                    if (el.className && typeof el.className === 'string' && (el.className.includes('scroll') || el.className.includes('List'))) {
                         if (el.scrollHeight > el.clientHeight) return el;
                    }
                    
                    el = el.parentElement;
                }
                return null;
            """, element)
        except:
            return None

    def get_search_links(self, city: str, query: str, max_items: int = 20) -> List[str]:
        """Searches for a query in a city and returns a list of company profile URLs."""
        # If query looks like a URL, use it directly
        if query.startswith("http"):
            url = query
        else:
            url = f"https://2gis.ru/{city}/search/{query}"
            
        logger.info(f"Collecting links for '{query}'...")
        
        try:
            self.driver.get(url)
            self.randomizer.random_sleep(3, 5) # Wait for initial load
            
            # Zoom out to see more items (helps with loading)
            try:
                self.driver.execute_script("document.body.style.zoom = '0.5'")
            except:
                pass

            # Allow page to settle
            WebDriverWait(self.driver, 10).until(
                EC.presence_of_element_located((By.TAG_NAME, "body"))
            )
            
            unique_links = set()
            
            # Improved scrolling logic
            no_new_items_attempts = 0
            max_no_new_attempts = 15 # Increased retries
            
            scrollable_container = None
            
            while len(unique_links) < max_items:
                # 1. Parse visible items
                links = self.driver.find_elements(By.CSS_SELECTOR, "a[href*='/firm/']")
                
                initial_count = len(unique_links)
                
                for link in links:
                    href = link.get_attribute("href")
                    if href and "firm" in href:
                        if href not in unique_links:
                            unique_links.add(href)
                
                new_count = len(unique_links)
                if new_count > initial_count:
                    logger.info(f"Collected {new_count} links so far (+{new_count - initial_count})...")
                
                if new_count >= max_items:
                    break
                
                if new_count == initial_count:
                    no_new_items_attempts += 1
                else:
                    no_new_items_attempts = 0 # Reset if we found something
                    
                    # If we found links, try to find their scrollable container
                    if not scrollable_container and links:
                        scrollable_container = self._find_scrollable_parent(links[0])
                        if scrollable_container:
                            logger.info("Found scrollable result container.")

                if no_new_items_attempts > max_no_new_attempts:
                    logger.info("No new items found after multiple scrolls. Stopping.")
                    break

                # 2. Scroll Logic
                try:
                    # Method A: Scroll the specific container with WHEEL event (Simulate mouse wheel)
                    if scrollable_container:
                        try:
                            # 1. Dispatch Wheel Event (most modern apps listen to this)
                            self.driver.execute_script("""
                                var el = arguments[0];
                                var evt = new WheelEvent('wheel', {
                                    deltaY: 1000,
                                    bubbles: true,
                                    cancelable: true,
                                    view: window
                                });
                                el.dispatchEvent(evt);
                            """, scrollable_container)
                            
                            # 2. Also set scrollTop
                            self.driver.execute_script("arguments[0].scrollTop = arguments[0].scrollHeight;", scrollable_container)
                        except:
                            pass
                    
                    # Method B: Move to the last found link (triggers visibility events)
                    if links:
                        try:
                            last_link = links[-1]
                            ActionChains(self.driver).move_to_element(last_link).perform()
                            self.driver.execute_script("arguments[0].scrollIntoView({block: 'center', behavior: 'smooth'});", last_link)
                        except:
                            pass
                    
                    # Method C: Generic Page Down
                    try:
                        ActionChains(self.driver).send_keys(Keys.PAGE_DOWN).perform()
                    except:
                        pass

                    self.randomizer.random_sleep(2, 4) # Wait for dynamic load
                    
                except Exception as e:
                    pass
                
            logger.info(f"Found {len(unique_links)} unique companies for '{query}'.")
            return list(unique_links)[:max_items]

        except Exception as e:
            logger.error(f"Error collecting links: {e}")
            return []

    def parse_company_page(self, url: str, city: str = "") -> Optional[Dict]:
        """Navigates to a company page and extracts details."""
        try:
            self.driver.get(url)
            # Random delay to simulate human reading
            self.randomizer.random_sleep(2, 4)
            
            return self._extract_company_details(city=city.capitalize(), country="Россия")
        except Exception as e:
            logger.error(f"Error parsing page {url}: {e}")
            return None

    def _normalize_phone(self, phone: str) -> str:
        """Normalizes phone number to +7 format."""
        if not phone:
            return ""
        
        # Remove non-digit characters
        digits = re.sub(r'\D', '', phone)
        
        if not digits:
            return ""
            
        # Handle Russian codes
        if len(digits) == 11:
            if digits.startswith('7') or digits.startswith('8'):
                return f"+7{digits[1:]}"
        elif len(digits) == 10:
             return f"+7{digits}"
             
        # Return original if pattern doesn't match standard Russian mobile/landline
        return f"+{digits}" if not phone.startswith('+') else phone

    def _extract_company_details(self, city: str = "", country: str = "Россия") -> Dict:
        """Extracts details from the company detail page."""
        data = {
            "name": None,
            "city": city,
            "country": country,
            "address": None,
            "phones": [],
            "websites": [],
            "emails": []
        }
        
        # Name - usually h1
        try:
            data["name"] = self.driver.find_element(By.TAG_NAME, "h1").text
        except:
            pass
            
        # Address
        try:
            address_elem = self.driver.find_element(By.CSS_SELECTOR, "a[href*='/geo/']") 
            data["address"] = address_elem.text
        except:
            pass
            
        # Phones
        try:
            # Click all "Show phone" buttons
            buttons = self.driver.find_elements(By.XPATH, "//div[contains(text(), 'Show phone') or contains(text(), 'Показать телефон')]")
            for btn in buttons:
                try:
                    btn.click()
                    self.randomizer.random_sleep(0.5, 1)
                except:
                    pass
            
            # Now find phone links
            phones = self.driver.find_elements(By.CSS_SELECTOR, "a[href^='tel:']")
            raw_phones = [p.get_attribute("href").replace("tel:", "") for p in phones]
            # Normalize phones
            data["phones"] = list(set([self._normalize_phone(p) for p in raw_phones]))
        except:
            pass

        # Emails (Moved before Websites for fallback logic)
        try:
            emails = self.driver.find_elements(By.CSS_SELECTOR, "a[href^='mailto:']")
            data["emails"] = list(set([e.get_attribute("href").replace("mailto:", "") for e in emails]))
        except:
            pass
            
        # Websites
        try:
            ignored_domains = [
                "2gis.", "google.", "yandex.", "otello.ru", "vk.com", "t.me", 
                "instagram.com", "facebook.com", "twitter.com", "ok.ru", 
                "youtube.com", "whatsapp.com", "wa.me", "apple.com", 
                "play.google.com", "apps.apple.com", "booking.com", "tripadvisor.",
                "delivery-club.ru", "eda.yandex", "zoon.ru", "yell.ru", "onelink.me",
                "uber.com", "gettaxi", "city-mobil", "dostavista"
            ]
            
            # Keywords in text that indicate it's NOT a company website
            ignored_text_keywords = [
                "скачать", "download", "app", "приложение", "google play", "app store", 
                "заказать", "доставка", "меню", "такси", "маршрут", "поехать", "отзывы",
                "вход", "регистрация", "лицензионное", "соглашение", "политика", "конфиденциальности"
            ]
            
            sites = self.driver.find_elements(By.CSS_SELECTOR, "a[href^='http']")
            
            found_sites = []
            priority_sites = []
            
            for site in sites:
                href = site.get_attribute("href")
                if not href:
                    continue
                
                # Get text content (visible text)
                text = site.text.lower().strip()
                href_lower = href.lower()
                
                # 1. Check blacklist domains
                is_valid = True
                for domain in ignored_domains:
                    if domain in href_lower:
                        is_valid = False
                        break
                if not is_valid:
                    continue

                # 2. Check blacklist text keywords
                for kw in ignored_text_keywords:
                    if kw in text:
                        is_valid = False
                        break
                if not is_valid:
                    continue
                
                # 3. Heuristic: If text looks like a domain (has dot, no spaces) -> High probability
                if "." in text and " " not in text and len(text) > 3 and not text.endswith('...'):
                     priority_sites.append(href)
                     continue
                
                # 4. Heuristic: If text is "сайт" or "website"
                if text in ["сайт", "website", "веб-сайт"]:
                    priority_sites.append(href)
                    continue

                found_sites.append(href)
            
            # Combine priority sites first
            all_sites = priority_sites + found_sites
            
            # Deduplicate preserving order
            seen = set()
            final_sites = []
            for s in all_sites:
                if s not in seen:
                    final_sites.append(s)
                    seen.add(s)
            
            data["websites"] = final_sites
            
            # Fallback: Extract domain from email if no website found
            if not data["websites"] and data.get("emails"):
                generic_domains = [
                    "gmail.com", "yandex.ru", "yandex.com", "mail.ru", "bk.ru", 
                    "list.ru", "inbox.ru", "yahoo.com", "hotmail.com", "outlook.com", 
                    "icloud.com", "rambler.ru", "ya.ru"
                ]
                for email in data["emails"]:
                    try:
                        domain = email.split("@")[1]
                        if domain not in generic_domains:
                            fallback_site = f"http://{domain}"
                            # logger.info(f"Fallback: Added website from email domain: {fallback_site}")
                            data["websites"].append(fallback_site)
                            break # Only take the first non-generic one
                    except:
                        pass
                        
        except Exception as e:
            pass

        return data

def save_to_excel(data: List[Dict], filename: str = "companies.xlsx"):
    """Saves collected data to an Excel file."""
    if not data:
        logger.warning("No data to save.")
        return

    # Flatten lists (phones, emails, websites) into strings
    processed_data = []
    for item in data:
        processed_item = item.copy()
        processed_item['phones'] = ", ".join(item['phones']) if item['phones'] else ""
        processed_item['emails'] = ", ".join(item['emails']) if item['emails'] else ""
        processed_item['websites'] = ", ".join(item['websites']) if item['websites'] else ""
        processed_data.append(processed_item)
        
    df = pd.DataFrame(processed_data)
    
    # Rename columns for better readability
    df.rename(columns={
        "name": "Название",
        "city": "Город",
        "country": "Страна",
        "address": "Адрес",
        "phones": "Телефоны",
        "websites": "Сайты",
        "emails": "Email",
        "url": "Ссылка 2ГИС"
    }, inplace=True)
    
    # Ensure correct column order
    desired_order = ["Название", "Город", "Страна", "Адрес", "Телефоны", "Email", "Сайты", "Ссылка 2ГИС"]
    # Filter out columns that might not exist
    columns_to_export = [col for col in desired_order if col in df.columns]
    df = df[columns_to_export]
    
    try:
        df.to_excel(filename, index=False, engine='openpyxl')
        logger.info(f"Data successfully saved to {filename}")
    except Exception as e:
        logger.error(f"Error saving to Excel: {e}")

# --- Async / Parallel Execution Helpers ---

def process_links_chunk(links: List[str], city: str) -> List[Dict]:
    """Worker function to process a chunk of links using a single browser instance."""
    results = []
    # Create a new independent parser instance for this thread
    parser = SeleniumParser(headless=True)
    try:
        for url in links:
            data = parser.parse_company_page(url, city=city)
            if data:
                data['url'] = url
                results.append(data)
    finally:
        parser.close()
    return results

def run_concurrent_scraper(city: str, niches: List[str], max_items_per_niche: int, max_workers: int = 3, smart_expansion: bool = True):
    """Main orchestrator for concurrent scraping."""
    
    all_links = []
    
    logger.info("--- Phase 1: Collecting Links ---")
    
    # We use one parser for searching to save startup time
    search_parser = SeleniumParser(headless=True)
    try:
        for niche in niches:
            queries_to_process = [niche]
            
            # Smart Expansion: Get related categories from search suggestions
            if smart_expansion:
                suggestions = search_parser.get_smart_suggestions(city, niche)
                if suggestions:
                    logger.info(f"Smart Expansion: Expanding '{niche}' into {len(suggestions)} categories.")
                    # Add suggestions to the list. 
                    # If suggestions are found, we prioritize them.
                    # We can keep the original niche too, but usually suggestions cover it better.
                    queries_to_process = suggestions 
                    # Note: We are using suggestions as queries. 
            
            for query in queries_to_process:
                # print(f"Collecting links for: {query}")
                links = search_parser.get_search_links(city, query, max_items=max_items_per_niche)
                all_links.extend(links)
                
    finally:
        search_parser.close()
        
    unique_links = list(set(all_links))
    total_links = len(unique_links)
    logger.info(f"Total unique companies to scrape: {total_links}")
    
    if total_links == 0:
        return []

    # Step 2: Parallel Processing of Details
    logger.info(f"--- Phase 2: Scraping Details (Workers: {max_workers}) ---")
    
    chunk_size = max(1, total_links // max_workers) 
    if chunk_size < 5: chunk_size = 5
    
    chunks = [unique_links[i:i + chunk_size] for i in range(0, total_links, chunk_size)]
    
    results = []
    
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        # Submit tasks
        future_to_chunk = {executor.submit(process_links_chunk, chunk, city): chunk for chunk in chunks}
        
        # Progress bar
        with tqdm(total=total_links, desc="Scraping Companies", unit="company") as pbar:
            for future in concurrent.futures.as_completed(future_to_chunk):
                try:
                    chunk_results = future.result()
                    results.extend(chunk_results)
                    pbar.update(len(chunk_results))
                except Exception as e:
                    logger.error(f"Worker failed: {e}")
                    
    return results

def interactive_cli():
    print("\n=== 2GIS Selenium Parser (Async/Parallel) ===")
    
    # 1. City
    city = input("Enter city (default: moscow): ").strip() or "moscow"
    
    # 2. Niches
    print("\nEnter niches separated by comma (e.g., 'cafe, pharmacy').")
    print("Or type 'all' for a default list of popular niches.")
    niches_input = input("Niches: ").strip()
    
    if niches_input.lower() == 'all':
        niches = [
            "кафе", "ресторан", "аптека", "продукты", "салон красоты", 
            "автосервис", "цветы", "стоматология", "фитнес", "отель"
        ]
    elif niches_input:
        niches = [n.strip() for n in niches_input.split(",")]
    else:
        niches = ["кафе"] # Default
        
    # 3. Max Items
    try:
        max_items = int(input("Max items per niche (default: 30): ").strip() or "30")
    except:
        max_items = 30
        
    # 4. Workers
    try:
        max_workers = int(input("Number of parallel workers (default: 3): ").strip() or "3")
    except:
        max_workers = 3

    # 5. Smart Expansion
    smart_input = input("Use Smart Category Expansion? (Y/n): ").strip().lower()
    smart_expansion = smart_input != 'n'
        
    print(f"\nConfiguration: City={city}, Niches={niches}, Limit={max_items}, Workers={max_workers}, Smart={smart_expansion}")
    print("Starting process... (Press Ctrl+C to stop)")
    
    start_time = time.time()
    results = run_concurrent_scraper(city, niches, max_items, max_workers, smart_expansion)
    elapsed = time.time() - start_time
    
    if results:
        save_to_excel(results)
        print(f"\nDone! Processed {len(results)} items in {elapsed:.2f} seconds.")
    else:
        print("\nNo data collected.")

if __name__ == "__main__":
    interactive_cli()
