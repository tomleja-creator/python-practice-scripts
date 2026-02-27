"""
SharePoint REST API Connector
Demonstrates integration with SharePoint - directly relevant to your resume
"""

import requests
from requests.auth import HTTPBasicAuth
import json
from datetime import datetime
import os
from typing import Optional, Dict, Any, List
import base64

class SharePointConnector:
    """
    Connect to SharePoint REST API
    Demonstrates real-world integration with Microsoft 365
    """
    
    def __init__(self, site_url: str, username: str = None, password: str = None):
        """
        Initialize SharePoint connection
        
        Args:
            site_url: Full URL to SharePoint site (e.g., https://company.sharepoint.com/sites/MySite)
            username: SharePoint username (email)
            password: SharePoint password or app password
        """
        self.site_url = site_url.rstrip('/')
        self.username = username or os.getenv('SHAREPOINT_USER')
        self.password = password or os.getenv('SHAREPOINT_PASS')
        
        # SharePoint REST API base
        self.api_base = f"{self.site_url}/_api/web"
        
        # Setup session with authentication
        self.session = requests.Session()
        
        if self.username and self.password:
            # Basic authentication (for testing/demo - use app passwords in production)
            self.session.auth = HTTPBasicAuth(self.username, self.password)
        
        # Set required SharePoint headers
        self.session.headers.update({
            'Accept': 'application/json;odata=verbose',
            'Content-Type': 'application/json;odata=verbose',
            'User-Agent': 'SharePoint-Connector/1.0'
        })
        
        # Get request digest for POST operations
        self.request_digest = self._get_request_digest()
    
    def _get_request_digest(self) -> str:
        """Get request digest for SharePoint forms authentication"""
        try:
            response = self.session.post(
                f"{self.site_url}/_api/contextinfo",
                headers={'Accept': 'application/json;odata=verbose'}
            )
            response.raise_for_status()
            data = response.json()
            return data['d']['GetContextWebInformation']['FormDigestValue']
        except Exception as e:
            print(f"⚠️ Could not get request digest: {e}")
            return ""
    
    def _add_digest(self):
        """Add request digest to headers for POST operations"""
        if self.request_digest:
            self.session.headers.update({
                'X-RequestDigest': self.request_digest
            })
    
    def get_lists(self) -> List[Dict]:
        """
        Get all lists in the site
        """
        response = self.session.get(f"{self.api_base}/lists")
        response.raise_for_status()
        data = response.json()
        
        lists = []
        for lst in data['d']['results']:
            lists.append({
                'id': lst['Id'],
                'title': lst['Title'],
                'item_count': lst['ItemCount'],
                'created': lst['Created'],
                'url': lst['ParentWebUrl'] + '/Lists/' + lst['Title'].replace(' ', '')
            })
        
        return lists
    
    def get_list_items(self, list_name: str, top: int = 100) -> List[Dict]:
        """
        Get items from a SharePoint list
        
        Args:
            list_name: Title of the list
            top: Maximum number of items to retrieve
        """
        response = self.session.get(
            f"{self.api_base}/lists/getbytitle('{list_name}')/items",
            params={'$top': top}
        )
        response.raise_for_status()
        data = response.json()
        
        items = []
        for item in data['d'].get('results', []):
            # Clean up metadata
            clean_item = {}
            for key, value in item.items():
                if not key.startswith('__'):
                    clean_item[key] = value
            items.append(clean_item)
        
        return items
    
    def create_list_item(self, list_name: str, fields: Dict[str, Any]) -> Dict:
        """
        Create a new item in a SharePoint list
        
        Args:
            list_name: Title of the list
            fields: Dictionary of field names and values
        """
        self._add_digest()
        
        # Format fields for SharePoint
        item_data = {
            '__metadata': {'type': f'SP.Data.{list_name.replace(" ", "")}ListItem'}
        }
        item_data.update(fields)
        
        response = self.session.post(
            f"{self.api_base}/lists/getbytitle('{list_name}')/items",
            json=item_data
        )
        response.raise_for_status()
        return response.json()
    
    def update_list_item(self, list_name: str, item_id: int, fields: Dict[str, Any]) -> Dict:
        """
        Update an existing list item
        """
        self._add_digest()
        self.session.headers.update({
            'IF-MATCH': '*',
            'X-HTTP-Method': 'MERGE'
        })
        
        response = self.session.post(
            f"{self.api_base}/lists/getbytitle('{list_name}')/items({item_id})",
            json=fields
        )
        
        # Reset headers
        self.session.headers.pop('IF-MATCH', None)
        self.session.headers.pop('X-HTTP-Method', None)
        
        response.raise_for_status()
        return response.json() if response.content else {'status': 'updated'}
    
    def delete_list_item(self, list_name: str, item_id: int) -> bool:
        """
        Delete a list item
        """
        self._add_digest()
        self.session.headers.update({
            'IF-MATCH': '*',
            'X-HTTP-Method': 'DELETE'
        })
        
        response = self.session.post(
            f"{self.api_base}/lists/getbytitle('{list_name}')/items({item_id})"
        )
        
        # Reset headers
        self.session.headers.pop('IF-MATCH', None)
        self.session.headers.pop('X-HTTP-Method', None)
        
        return response.status_code == 204
    
    def get_files(self, library_name: str = 'Shared Documents') -> List[Dict]:
        """
        Get files from a document library
        """
        response = self.session.get(
            f"{self.api_base}/GetFolderByServerRelativeUrl('{library_name}')/Files"
        )
        response.raise_for_status()
        data = response.json()
        
        files = []
        for file in data['d'].get('results', []):
            files.append({
                'name': file['Name'],
                'size': file['Length'],
                'url': file['ServerRelativeUrl'],
                'modified': file['TimeLastModified']
            })
        
        return files
    
    def upload_file(self, library_name: str, file_path: str, file_name: str = None) -> Dict:
        """
        Upload a file to SharePoint document library
        
        Args:
            library_name: Document library name
            file_path: Local path to file
            file_name: Name to use in SharePoint (defaults to original filename)
        """
        if not file_name:
            file_name = os.path.basename(file_path)
        
        self._add_digest()
        
        # Read file content
        with open(file_path, 'rb') as f:
            file_content = f.read()
        
        # Add file
        headers = self.session.headers.copy()
        headers.update({
            'Content-Type': 'application/octet-stream'
        })
        
        response = self.session.post(
            f"{self.api_base}/GetFolderByServerRelativeUrl('{library_name}')/Files/add(url='{file_name}',overwrite=true)",
            data=file_content,
            headers=headers
        )
        
        response.raise_for_status()
        return response.json()
    
    def download_file(self, library_name: str, file_name: str, download_path: str = '.') -> str:
        """
        Download a file from SharePoint
        """
        # Get file URL
        response = self.session.get(
            f"{self.api_base}/GetFolderByServerRelativeUrl('{library_name}')/Files('{file_name}')"
        )
        response.raise_for_status()
        file_data = response.json()['d']
        
        # Download file
        file_url = self.site_url + file_data['ServerRelativeUrl']
        response = self.session.get(file_url)
        response.raise_for_status()
        
        # Save locally
        local_path = os.path.join(download_path, file_name)
        with open(local_path, 'wb') as f:
            f.write(response.content)
        
        return local_path
    
    def search(self, query: str) -> List[Dict]:
        """
        Search SharePoint content
        """
        response = self.session.get(
            f"{self.site_url}/_api/search/query",
            params={'querytext': f"'{query}'"}
        )
        response.raise_for_status()
        data = response.json()
        
        results = []
        for result in data['d']['query']['PrimaryQueryResult']['RelevantResults']['Table']['Rows']:
            item = {}
            for cell in result['Cells']:
                item[cell['Key']] = cell['Value']
            results.append(item)
        
        return results

class PowerAutomateSharePointBridge:
    """
    Bridge between SharePoint and Power Automate concepts
    Demonstrates how Python can extend Power Platform capabilities
    """
    
    def __init__(self, sp_connector: SharePointConnector):
        self.sp = sp_connector
        self.flow_log = []
    
    def log_flow_step(self, step: str, status: str = 'INFO'):
        """Simulate Power Automate flow logging"""
        timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        log_entry = f"[{timestamp}] {status}: {step}"
        self.flow_log.append(log_entry)
        print(log_entry)
    
    def export_list_to_csv(self, list_name: str, output_file: str = None):
        """
        Simulate Power Automate 'Export to CSV' action
        """
        self.log_flow_step(f"Starting export of list '{list_name}'")
        
        try:
            # Get list items
            items = self.sp.get_list_items(list_name, top=5000)
            self.log_flow_step(f"Retrieved {len(items)} items")
            
            if not items:
                self.log_flow_step("No items to export", 'WARNING')
                return None
            
            # Convert to CSV (simulated - would use pandas/csv in real implementation)
            import pandas as pd
            df = pd.DataFrame(items)
            
            # Save to file
            if not output_file:
                output_file = f"export_{list_name}_{datetime.now().strftime('%Y%m%d')}.csv"
            
            df.to_csv(output_file, index=False)
            self.log_flow_step(f"Exported to {output_file}")
            
            # Simulate upload to SharePoint
            self.log_flow_step(f"Uploading to SharePoint Document Library")
            
            return output_file
            
        except Exception as e:
            self.log_flow_step(f"Export failed: {e}", 'ERROR')
            raise
    
    def sync_with_external_system(self, list_name: str, external_data: List[Dict]):
        """
        Simulate Power Automate sync between SharePoint and external system
        """
        self.log_flow_step(f"Starting sync for list '{list_name}'")
        
        # Get existing items
        existing_items = self.sp.get_list_items(list_name)
        existing_ids = {item.get('Id') for item in existing_items if item.get('Id')}
        
        # Process external data
        created = 0
        updated = 0
        skipped = 0
        
        for ext_item in external_data:
            # Check if item exists (simplified - would use real ID mapping)
            if ext_item.get('id') in existing_ids:
                # Update existing
                self.sp.update_list_item(list_name, ext_item['id'], ext_item)
                updated += 1
                self.log_flow_step(f"Updated item {ext_item['id']}", 'DEBUG')
            else:
                # Create new
                self.sp.create_list_item(list_name, ext_item)
                created += 1
                self.log_flow_step(f"Created new item", 'DEBUG')
        
        self.log_flow_step(f"Sync complete: {created} created, {updated} updated, {skipped} skipped")
        
        return {
            'created': created,
            'updated': updated,
            'skipped': skipped
        }

if __name__ == "__main__":
    """
    Demonstration of SharePoint connector with simulated data
    (No actual SharePoint connection required for demo)
    """
    
    print("\n" + "="*60)
    print("🚀 SharePoint Connector Demonstration")
    print("="*60)
    
    # Create simulated connector (for demonstration purposes)
    print("\n📡 Initializing SharePoint connection (simulated)...")
    
    # For actual SharePoint, you would use:
    # sp = SharePointConnector(
    #     'https://yourcompany.sharepoint.com/sites/YourSite',
    #     'your.email@company.com',
    #     'your-password'
    # )
    
    # For demo, we'll create a mock version
    class MockSharePointConnector:
        def get_lists(self):
            return [
                {'title': 'Projects', 'item_count': 25},
                {'title': 'Tasks', 'item_count': 150},
                {'title': 'Risks', 'item_count': 18}
            ]
        
        def get_list_items(self, list_name, top=100):
            import random
            items = []
            for i in range(min(top, 10)):
                items.append({
                    'Id': i+1,
                    'Title': f"Item {i+1} from {list_name}",
                    'Created': '2024-01-15T12:00:00Z',
                    'Modified': '2024-02-20T15:30:00Z',
                    'Author': 'Tom Leja'
                })
            return items
        
        def create_list_item(self, list_name, fields):
            return {'Id': 999, 'Status': 'Created'}
        
        def update_list_item(self, list_name, item_id, fields):
            return {'Id': item_id, 'Status': 'Updated'}
    
    # Use mock for demo
    sp = MockSharePointConnector()
    bridge = PowerAutomateSharePointBridge(sp)
    
    # Demo 1: Get lists
    print("\n📋 SharePoint Lists:")
    lists = sp.get_lists()
    for lst in lists:
        print(f"  • {lst['title']} ({lst['item_count']} items)")
    
    # Demo 2: Get list items
    print("\n📄 List Items from 'Projects':")
    items = sp.get_list_items('Projects')
    for item in items[:3]:
        print(f"  • {item['Title']} (ID: {item['Id']})")
    
    # Demo 3: Export simulation
    print("\n📤 Simulating Export to CSV:")
    bridge.export_list_to_csv('Tasks')
    
    # Demo 4: Sync simulation
    print("\n🔄 Simulating External Sync:")
    external_data = [
        {'id': 1, 'Title': 'Updated Project', 'Status': 'Active'},
        {'Title': 'New Project', 'Status': 'Planning'}
    ]
    results = bridge.sync_with_external_system('Projects', external_data)
    
    print("\n" + "="*60)
    print("✅ SharePoint Connector demonstration complete")
    print("="*60)
    print("\n📝 Note: This is a simulated demo. For actual SharePoint:")
    print("  • Use app passwords or Azure AD for authentication")
    print("  • Handle rate limiting and pagination")
    print("  • Implement proper error handling and logging")
    print("  • Store credentials securely (environment variables)")
