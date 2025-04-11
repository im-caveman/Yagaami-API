import requests
from bs4 import BeautifulSoup
import time
import random
import logging
from datetime import datetime
import json
import concurrent.futures
import re
from typing import List, Dict, Any, Optional
import feedparser
from urllib.parse import urljoin
import os
from dotenv import load_dotenv
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException
import pandas as pd
import redis
import hashlib

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Redis connection for caching and rate limiting
redis_client = redis.Redis(
    host=os.getenv("REDIS_HOST"),
    port=int(os.getenv("REDIS_PORT", 6379)),
    db=1,
    decode_responses=True
)

# Proxy rotation
class ProxyManager:
    def __init__(self):
        self.proxies = self._load_proxies()
        self.current_index = 0
    
    def _load_proxies(self) -> List[Dict[str, str]]:
        """Load proxies from environment or file"""
        proxy_list = os.getenv("PROXY_LIST")
        if proxy_list:
            return json.loads(proxy_list)
        
        # Load from file as fallback
        try:
            with open("proxies.json", "r") as f:
                return json.load(f)
        except:
            logger.warning("No proxies found, running without proxies")
            return [{}]  # Return empty dict as "no proxy"
    
    def get_proxy(self) -> Dict[str, str]:
        """Get next proxy from the rotation"""
        if not self.proxies:
            return {}
        
        proxy = self.proxies[self.current_index]
        self.current_index = (self.current_index + 1) % len(self.proxies)
        return proxy

# User agent rotation
class UserAgentManager:
    def __init__(self):
        self.user_agents = [
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.1.1 Safari/605.1.15",
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:89.0) Gecko/20100101 Firefox/89.0",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.114 Safari/537.36",
            "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.101 Safari/537.36"
        ]
    
    def get_random_user_agent(self) -> str:
        """Get a random user agent"""
        return random.choice(self.user_agents)

# Base scraper class
class BaseScraper:
    def __init__(self):
        self.proxy_manager = ProxyManager()
        self.ua_manager = UserAgentManager()
        self.session = requests.Session()
    
    def _make_request(self, url: str, method: str = "GET", params: Dict = None, 
                     data: Dict = None, headers: Dict = None, retry_count: int = 3) -> Optional[requests.Response]:
        """Make HTTP request with retry logic and proxy rotation"""
        if headers is None:
            headers = {}
        
        # Add user agent if not provided
        if "User-Agent" not in headers:
            headers["User-Agent"] = self.ua_manager.get_random_user_agent()
        
        # Try to get cached response
        cache_key = f"url:{hashlib.md5(url.encode()).hexdigest()}"
        cached = redis_client.get(cache_key)
        if cached:
            logger.debug(f"Cache hit for URL: {url}")
            return json.loads(cached)
        
        for attempt in range(retry_count):
            try:
                # Get proxy for this attempt
                proxy = self.proxy_manager.get_proxy()
                
                # Make request
                response = self.session.request(
                    method=method,
                    url=url,
                    params=params,
                    data=data,
                    headers=headers,
                    proxies=proxy,
                    timeout=30
                )
                
                # Implement rate limiting check
                if response.status_code == 429:
                    wait_time = int(response.headers.get('Retry-After', 60))
                    logger.warning(f"Rate limited by {url}, waiting {wait_time} seconds")
                    time.sleep(wait_time)
                    continue
                
                # Check for successful response
                if response.status_code == 200:
                    # Cache the response
                    redis_client.setex(cache_key, 3600, json.dumps({
                        "url": url,
                        "status_code": response.status_code,
                        "headers": dict(response.headers),
                        "content": response.text
                    }))
                    
                    return response
                else:
                    logger.warning(f"Request failed with status code {response.status_code} for URL: {url}")
            
            except Exception as e:
                logger.error(f"Request error on attempt {attempt+1}: {str(e)}")
            
            # Wait before retry with exponential backoff
            wait_time = 2 ** attempt + random.uniform(0, 1)
            logger.info(f"Retrying in {wait_time:.2f} seconds...")
            time.sleep(wait_time)
        
        logger.error(f"All {retry_count} attempts failed for URL: {url}")
        return None
    
    def _get_selenium_driver(self):
        """Initialize and return a Selenium WebDriver"""
        options = Options()
        options.add_argument("--headless")
        options.add_argument("--no-sandbox")
        options.add_argument("--disable-dev-shm-usage")
        options.add_argument(f"user-agent={self.ua_manager.get_random_user_agent()}")
        
        # Add proxy if available
        proxy = self.proxy_manager.get_proxy()
        if proxy:
            options.add_argument(f"--proxy-server={proxy.get('http', '')}")
        
        return webdriver.Chrome(options=options)
    
    def normalize_job_data(self, job_data: Dict[str, Any]) -> Dict[str, Any]:
        """Normalize job data to a standard format"""
        # Required fields
        required_fields = ["title", "company", "location", "description", "url", "posted_date", "source"]
        for field in required_fields:
            if field not in job_data:
                if field == "posted_date":
                    job_data[field] = datetime.now().isoformat()
                else:
                    job_data[field] = ""
        
        # Generate job_id if not provided
        if "job_id" not in job_data:
            job_data["job_id"] = hashlib.md5(f"{job_data['title']}:{job_data['company']}:{job_data['url']}".encode()).hexdigest()
        
        # Extract skills from description if not provided
        if "skills" not in job_data and job_data["description"]:
            # Simple skill extraction (can be improved with ML models)
            common_skills = ["python", "java", "javascript", "sql", "aws", "docker", "kubernetes", 
                            "react", "angular", "vue", "node.js", "mongodb", "postgresql", 
                            "machine learning", "data science", "ai", "product management"]
            
            skills = []
            for skill in common_skills:
                if re.search(r'\b' + re.escape(skill) + r'\b', job_data["description"].lower()):
                    skills.append(skill)
            
            job_data["skills"] = skills
        
        # Ensure qualifications and responsibilities are lists
        for field in ["qualifications", "responsibilities", "benefits"]:
            if field in job_data and isinstance(job_data[field], str):
                # Split string into list by bullets or newlines
                items = re.split(r'[\n•]+', job_data[field])
                job_data[field] = [item.strip() for item in items if item.strip()]
        
        return job_data

# Specific job board scrapers
class IndeedScraper(BaseScraper):
    def __init__(self):
        super().__init__()
        self.base_url = "https://www.indeed.com"
    
    def search_jobs(self, query: str, location: str, page: int = 1) -> List[Dict[str, Any]]:
        """Search jobs on Indeed"""
        logger.info(f"Searching Indeed for '{query}' in '{location}', page {page}")
        
        # Define search URL
        search_url = f"{self.base_url}/jobs"
        params = {
            "q": query,
            "l": location,
            "start": (page - 1) * 10  # Indeed uses 10 jobs per page
        }
        
        # Rate limit check
        rate_key = f"rate:indeed:{time.time() // 60}"
        rate_count = redis_client.incr(rate_key)
        redis_client.expire(rate_key, 60)
        
        if rate_count > 5:  # Max 5 requests per minute
            logger.warning("Rate limit reached for Indeed, waiting...")
            time.sleep(60)
        
        # Make request
        response = self._make_request(search_url, params=params)
        if not response:
            return []
        
        # Parse HTML
        soup = BeautifulSoup(response.text, "html.parser")
        job_cards = soup.select("div.job_seen_beacon")
        
        jobs = []
        for card in job_cards:
            try:
                # Extract job details
                title_elem = card.select_one("h2.jobTitle span")
                title = title_elem.get_text().strip() if title_elem else ""
                
                company_elem = card.select_one("span.companyName")
                company = company_elem.get_text().strip() if company_elem else ""
                
                location_elem = card.select_one("div.companyLocation")
                location = location_elem.get_text().strip() if location_elem else ""
                
                # Check for remote indicator
                remote = "remote" in location.lower()
                
                # Extract job link
                link_elem = card.select_one("h2.jobTitle a")
                job_url = urljoin(self.base_url, link_elem["href"]) if link_elem and "href" in link_elem.attrs else ""
                
                # Extract job ID from URL
                job_id_match = re.search(r'jk=([^&]+)', job_url)
                job_id = job_id_match.group(1) if job_id_match else ""
                
                # Extract snippet
                snippet_elem = card.select_one("div.job-snippet")
                snippet = snippet_elem.get_text().strip() if snippet_elem else ""
                
                # Extract posted date
                date_elem = card.select_one("span.date")
                posted_date = date_elem.get_text().strip() if date_elem else ""
                
                # Normalize date
                if "today" in posted_date.lower():
                    posted_date = datetime.now().strftime("%Y-%m-%d")
                elif "yesterday" in posted_date.lower():
                    posted_date = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
                elif "days ago" in posted_date.lower():
                    days = int(re.search(r'(\d+)', posted_date).group(1))
                    posted_date = (datetime.now() - timedelta(days=days)).strftime("%Y-%m-%d")
                
                # Create job object
                job = {
                    "job_id": job_id,
                    "title": title,
                    "company": company,
                    "location": location,
                    "remote": remote,
                    "summary": snippet,
                    "url": job_url,
                    "posted_date": posted_date,
                    "source": "indeed"
                }
                
                jobs.append(self.normalize_job_data(job))
            
            except Exception as e:
                logger.error(f"Error parsing Indeed job card: {str(e)}")
        
        return jobs
    
    def get_job_details(self, job_id: str) -> Dict[str, Any]:
        """Get detailed job information from Indeed"""
        logger.info(f"Getting Indeed job details for ID: {job_id}")
        
        # Define job details URL
        job_url = f"{self.base_url}/viewjob?jk={job_id}"
        
        # Check rate limit
        rate_key = f"rate:indeed_details:{time.time() // 60}"
        rate_count = redis_client.incr(rate_key)
        redis_client.expire(rate_key, 60)
        
        if rate_count > 3:  # Max 3 detail requests per minute
            logger.warning("Rate limit reached for Indeed job details, waiting...")
            time.sleep(60)
        
        # Use Selenium for JavaScript-rendered content
        driver = self._get_selenium_driver()
        
        try:
            driver.get(job_url)
            
            # Wait for job description to load
            WebDriverWait(driver, 10).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, "div#jobDescriptionText"))
            )
            
            # Extract job details
            title = driver.find_element(By.CSS_SELECTOR, "h1.jobsearch-JobInfoHeader-title").text.strip()
            company = driver.find_element(By.CSS_SELECTOR, "div.jobsearch-InlineCompanyRating").text.split('\n')[0]
            location = driver.find_element(By.CSS_SELECTOR, "div.jobsearch-JobInfoHeader-subtitle").text.strip()
            remote = "remote" in location.lower()
            
            # Extract job description
            description = driver.find_element(By.CSS_SELECTOR, "div#jobDescriptionText").text.strip()
            
            # Extract qualifications and responsibilities
            qualifications = []
            responsibilities = []
            
            # Simple extraction based on section headers
            sections = driver.find_elements(By.CSS_SELECTOR, "div#jobDescriptionText > *")
            current_section = None
            
            for section in sections:
                text = section.text.strip()
                
                if not text:
                    continue
                
                # Check for section headers
                if re.search(r'qualifications|requirements|what you need', text.lower()):
                    current_section = "qualifications"
                elif re.search(r'responsibilities|duties|what you\'ll do', text.lower()):
                    current_section = "responsibilities"
                elif re.search(r'benefits|perks|what we offer', text.lower()):
                    current_section = "benefits"
                elif text.lower().startswith("about"):
                    current_section = None
                elif current_section == "qualifications" and text.startswith("•"):
                    qualifications.append(text.strip("• "))
                elif current_section == "responsibilities" and text.startswith("•"):
                    responsibilities.append(text.strip("• "))
            
            # Extract salary if available
            salary_range = {}
            salary_elem = driver.find_elements(By.CSS_SELECTOR, "span.salary-snippet")
            if salary_elem:
                salary_text = salary_elem[0].text.strip()
                # Parse salary range
                salary_match = re.search(r'\$(\d+,?\d*)\s*-\s*\$(\d+,?\d*)', salary_text)
                if salary_match:
                    min_salary = float(salary_match.group(1).replace(',', ''))
                    max_salary = float(salary_match.group(2).replace(',', ''))
                    salary_range = {"min": min_salary, "max": max_salary}
            
            # Extract job type
            job_type = ""
            job_type_elem = driver.find_elements(By.XPATH, "//div[contains(text(), 'Job Type:')]")
