"""
REST API Client with Retry Logic
Demonstrates API integration skills valuable for Power Platform + Data Engineering
"""
import requests
import time
from functools import wraps

def retry(max_attempts=3, delay=2):
    """Decorator for retrying failed API calls"""
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            for attempt in range(max_attempts):
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    if attempt == max_attempts - 1:
                        raise
                    print(f"Attempt {attempt + 1} failed: {e}. Retrying...")
                    time.sleep(delay)
            return None
        return wrapper
    return decorator

class RESTClient:
    """Generic REST API client with error handling"""
    
    def __init__(self, base_url, api_key=None):
        self.base_url = base_url.rstrip('/')
        self.api_key = api_key
        self.session = requests.Session()
        if api_key:
            self.session.headers.update({'Authorization': f'Bearer {api_key}'})
    
    @retry(max_attempts=3)
    def get(self, endpoint, params=None):
        """GET request with retry logic"""
        url = f"{self.base_url}/{endpoint.lstrip('/')}"
        response = self.session.get(url, params=params, timeout=10)
        response.raise_for_status()
        return response.json()
    
    @retry(max_attempts=3)
    def post(self, endpoint, data=None, json=None):
        """POST request with retry logic"""
        url = f"{self.base_url}/{endpoint.lstrip('/')}"
        response = self.session.post(url, data=data, json=json, timeout=10)
        response.raise_for_status()
        return response.json()
    
    def paginated_get(self, endpoint, page_param='page', max_pages=None):
        """Handle paginated API responses"""
        all_results = []
        page = 1
        
        while True:
            params = {page_param: page}
            data = self.get(endpoint, params=params)
            
            # Assume API returns {'results': [...], 'next': url}
            results = data.get('results', data)
            if not results:
                break
                
            all_results.extend(results)
            
            if max_pages and page >= max_pages:
                break
                
            # Check if there's a next page
            if not data.get('next'):
                break
                
            page += 1
        
        return all_results

# Example usage
if __name__ == "__main__":
    # Public test API
    client = RESTClient('https://jsonplaceholder.typicode.com')
    
    # Get single resource
    post = client.get('posts/1')
    print(f"📝 Post 1 title: {post['title']}")
    
    # Get all posts (paginated example)
    all_posts = client.paginated_get('posts', max_pages=2)
    print(f"📚 Retrieved {len(all_posts)} posts")
