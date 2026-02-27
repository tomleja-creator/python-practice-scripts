"""
REST API Client with Retry Logic and Error Handling
Demonstrates professional API integration patterns
"""

import requests
import time
import json
from functools import wraps
from datetime import datetime
from typing import Optional, Dict, Any, List
import logging

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def retry(max_attempts: int = 3, delay: float = 1.0, backoff: float = 2.0):
    """
    Decorator for retrying failed API calls with exponential backoff
    
    Args:
        max_attempts: Maximum number of retry attempts
        delay: Initial delay between retries (seconds)
        backoff: Multiplier for exponential backoff
    """
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            current_delay = delay
            for attempt in range(max_attempts):
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    if attempt == max_attempts - 1:
                        logger.error(f"All {max_attempts} attempts failed: {e}")
                        raise
                    
                    logger.warning(f"Attempt {attempt + 1} failed: {e}. Retrying in {current_delay:.1f}s...")
                    time.sleep(current_delay)
                    current_delay *= backoff
            return None
        return wrapper
    return decorator

class RESTClient:
    """
    Professional REST API client with comprehensive features:
    - Authentication handling
    - Retry logic with exponential backoff
    - Rate limiting awareness
    - Pagination support
    - Request/response logging
    """
    
    def __init__(
        self,
        base_url: str,
        api_key: Optional[str] = None,
        timeout: int = 30,
        max_retries: int = 3,
        rate_limit: Optional[int] = None  # requests per minute
    ):
        """
        Initialize the REST client
        
        Args:
            base_url: Base URL for the API
            api_key: Optional API key for authentication
            timeout: Request timeout in seconds
            max_retries: Maximum retry attempts
            rate_limit: Optional rate limit (requests per minute)
        """
        self.base_url = base_url.rstrip('/')
        self.timeout = timeout
        self.max_retries = max_retries
        self.rate_limit = rate_limit
        self.request_timestamps = []
        
        # Setup session with headers
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'RESTClient/1.0',
            'Accept': 'application/json',
            'Content-Type': 'application/json'
        })
        
        if api_key:
            # Common authentication patterns
            self.session.headers.update({
                'Authorization': f'Bearer {api_key}',
                'X-API-Key': api_key
            })
        
        logger.info(f"RESTClient initialized for {base_url}")
    
    def _check_rate_limit(self):
        """Implement rate limiting if configured"""
        if not self.rate_limit:
            return
        
        now = time.time()
        # Remove timestamps older than 60 seconds
        self.request_timestamps = [t for t in self.request_timestamps if now - t < 60]
        
        if len(self.request_timestamps) >= self.rate_limit:
            sleep_time = 60 - (now - self.request_timestamps[0])
            if sleep_time > 0:
                logger.warning(f"Rate limit reached. Sleeping for {sleep_time:.1f}s")
                time.sleep(sleep_time)
        
        self.request_timestamps.append(now)
    
    def _handle_response(self, response: requests.Response) -> Dict[str, Any]:
        """
        Handle API response, including error cases
        
        Args:
            response: Requests response object
        
        Returns:
            Parsed JSON response
        
        Raises:
            Exception for various error conditions
        """
        # Log request details for debugging
        logger.debug(f"Request: {response.request.method} {response.request.url}")
        logger.debug(f"Response status: {response.status_code}")
        
        # Handle successful responses
        if response.status_code in [200, 201, 202]:
            try:
                return response.json()
            except json.JSONDecodeError:
                return {'text': response.text}
        
        # Handle error responses
        error_msg = f"API Error {response.status_code}"
        try:
            error_data = response.json()
            if 'message' in error_data:
                error_msg = error_data['message']
            elif 'error' in error_data:
                error_msg = error_data['error']
        except:
            error_msg = response.text or error_msg
        
        # Raise specific exceptions based on status code
        if response.status_code == 401:
            raise PermissionError(f"Authentication failed: {error_msg}")
        elif response.status_code == 403:
            raise PermissionError(f"Access forbidden: {error_msg}")
        elif response.status_code == 404:
            raise ValueError(f"Resource not found: {error_msg}")
        elif response.status_code == 429:
            raise Exception(f"Rate limit exceeded: {error_msg}")
        elif 500 <= response.status_code < 600:
            raise Exception(f"Server error: {error_msg}")
        else:
            raise Exception(f"Request failed: {error_msg}")
    
    @retry(max_attempts=3)
    def get(
        self,
        endpoint: str,
        params: Optional[Dict] = None,
        headers: Optional[Dict] = None
    ) -> Dict[str, Any]:
        """
        Make GET request with retry logic
        
        Args:
            endpoint: API endpoint (will be appended to base_url)
            params: Query parameters
            headers: Additional headers for this request
        
        Returns:
            Parsed JSON response
        """
        self._check_rate_limit()
        
        url = f"{self.base_url}/{endpoint.lstrip('/')}"
        
        # Merge headers
        request_headers = self.session.headers.copy()
        if headers:
            request_headers.update(headers)
        
        logger.info(f"GET {url}")
        
        response = self.session.get(
            url,
            params=params,
            headers=request_headers,
            timeout=self.timeout
        )
        
        return self._handle_response(response)
    
    @retry(max_attempts=3)
    def post(
        self,
        endpoint: str,
        data: Optional[Dict] = None,
        json_data: Optional[Dict] = None,
        headers: Optional[Dict] = None
    ) -> Dict[str, Any]:
        """
        Make POST request with retry logic
        
        Args:
            endpoint: API endpoint
            data: Form data
            json_data: JSON data
            headers: Additional headers
        
        Returns:
            Parsed JSON response
        """
        self._check_rate_limit()
        
        url = f"{self.base_url}/{endpoint.lstrip('/')}"
        
        request_headers = self.session.headers.copy()
        if headers:
            request_headers.update(headers)
        
        logger.info(f"POST {url}")
        
        response = self.session.post(
            url,
            data=data,
            json=json_data,
            headers=request_headers,
            timeout=self.timeout
        )
        
        return self._handle_response(response)
    
    @retry(max_attempts=3)
    def put(
        self,
        endpoint: str,
        data: Optional[Dict] = None,
        json_data: Optional[Dict] = None,
        headers: Optional[Dict] = None
    ) -> Dict[str, Any]:
        """Make PUT request (update resource)"""
        url = f"{self.base_url}/{endpoint.lstrip('/')}"
        
        request_headers = self.session.headers.copy()
        if headers:
            request_headers.update(headers)
        
        logger.info(f"PUT {url}")
        
        response = self.session.put(
            url,
            data=data,
            json=json_data,
            headers=request_headers,
            timeout=self.timeout
        )
        
        return self._handle_response(response)
    
    @retry(max_attempts=3)
    def delete(self, endpoint: str, headers: Optional[Dict] = None) -> bool:
        """Make DELETE request"""
        url = f"{self.base_url}/{endpoint.lstrip('/')}"
        
        request_headers = self.session.headers.copy()
        if headers:
            request_headers.update(headers)
        
        logger.info(f"DELETE {url}")
        
        response = self.session.delete(url, headers=request_headers, timeout=self.timeout)
        
        if response.status_code in [200, 204]:
            return True
        
        self._handle_response(response)  # This will raise an exception
        return False
    
    def get_paginated(
        self,
        endpoint: str,
        page_param: str = 'page',
        page_size_param: Optional[str] = None,
        page_size: int = 100,
        max_pages: Optional[int] = None
    ) -> List[Dict[str, Any]]:
        """
        Handle paginated API responses
        
        Supports:
        - Page number pagination (page=1, page=2)
        - Offset/limit pagination (offset=0, limit=100)
        - Next URL pagination (response contains 'next' URL)
        """
        all_results = []
        page = 1
        next_url = None
        
        while True:
            if next_url:
                # Handle APIs that return next URL
                response = self.get(next_url.replace(self.base_url, ''))
            else:
                # Handle page-based pagination
                params = {page_param: page}
                if page_size_param:
                    params[page_size_param] = page_size
                response = self.get(endpoint, params=params)
            
            # Extract results (handle different response structures)
            if 'results' in response:
                results = response['results']
            elif 'data' in response:
                results = response['data']
            elif 'items' in response:
                results = response['items']
            else:
                # Assume response itself is the list
                results = response if isinstance(response, list) else []
            
            if not results:
                break
            
            all_results.extend(results)
            logger.info(f"Fetched page {page}: {len(results)} items (total: {len(all_results)})")
            
            # Check for next page
            if 'next' in response and response['next']:
                next_url = response['next']
                page += 1
            elif len(results) < page_size:
                break  # Last page
            else:
                page += 1
            
            if max_pages and page > max_pages:
                break
        
        logger.info(f"Paginated fetch complete: {len(all_results)} total items")
        return all_results

class JSONPlaceholderClient(RESTClient):
    """Example client for JSONPlaceholder API (free test API)"""
    
    def __init__(self):
        super().__init__('https://jsonplaceholder.typicode.com')
    
    def get_posts(self, user_id: Optional[int] = None) -> List[Dict]:
        """Get posts, optionally filtered by user"""
        params = {'userId': user_id} if user_id else None
        return self.get('posts', params=params)
    
    def get_post(self, post_id: int) -> Dict:
        """Get single post"""
        return self.get(f'posts/{post_id}')
    
    def create_post(self, title: str, body: str, user_id: int) -> Dict:
        """Create a new post"""
        return self.post('posts', json_data={
            'title': title,
            'body': body,
            'userId': user_id
        })
    
    def get_comments(self, post_id: int) -> List[Dict]:
        """Get comments for a post"""
        return self.get(f'posts/{post_id}/comments')
    
    def get_users(self) -> List[Dict]:
        """Get all users"""
        return self.get('users')
    
    def get_todos(self, completed: Optional[bool] = None) -> List[Dict]:
        """Get todos, optionally filtered by completion status"""
        params = {}
        if completed is not None:
            params['completed'] = str(completed).lower()
        return self.get('todos', params=params)

if __name__ == "__main__":
    """
    Example usage demonstrating all features
    """
    print("\n" + "="*60)
    print("🚀 REST API Client Demonstration")
    print("="*60)
    
    # Create client for JSONPlaceholder (free test API)
    client = JSONPlaceholderClient()
    
    # Example 1: GET request
    print("\n📡 Example 1: GET posts for user 1")
    posts = client.get_posts(user_id=1)
    print(f"  Retrieved {len(posts)} posts")
    print(f"  First post title: {posts[0]['title'] if posts else 'N/A'}")
    
    # Example 2: GET single resource
    print("\n📡 Example 2: GET post #1")
    post = client.get_post(1)
    print(f"  Title: {post['title']}")
    print(f"  Body: {post['body'][:100]}...")
    
    # Example 3: GET with relationship
    print("\n📡 Example 3: GET comments for post #1")
    comments = client.get_comments(1)
    print(f"  Retrieved {len(comments)} comments")
    print(f"  First comment: {comments[0]['name'] if comments else 'N/A'}")
    
    # Example 4: POST request (create)
    print("\n📡 Example 4: POST - Create new post")
    new_post = client.create_post(
        title="Test Post from RESTClient",
        body="This is a test post created by the REST API client demonstration",
        user_id=1
    )
    print(f"  Created post with ID: {new_post.get('id', 'N/A')}")
    
    # Example 5: GET with filters
    print("\n📡 Example 5: GET - Filtered todos (completed)")
    todos = client.get_todos(completed=True)
    print(f"  Retrieved {len(todos)} completed todos")
    
    # Example 6: Error handling demonstration
    print("\n📡 Example 6: Error handling (404)")
    try:
        client.get_post(99999)  # This post doesn't exist
    except Exception as e:
        print(f"  ✅ Caught expected error: {e}")
    
    print("\n" + "="*60)
    print("✅ RESTClient demonstration complete")
    print("="*60)
