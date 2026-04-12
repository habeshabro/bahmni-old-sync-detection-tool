import os
import re
import subprocess
import socket
from datetime import datetime

def test_atomfeed_credentials():
    """
    Find atomfeed credentials from Bahmni config files and test them.
    
    Returns:
        Dictionary with credential sources and test results
    """
    results = {
        "timestamp": datetime.now().isoformat(),
        "credentials_found": False,
        "working_credentials": [],
        "failed_credentials": [],
        "details": {}
    }
    
    # Look for credentials in various Bahmni config files
    credential_sources = []
    
    # 1. Check OpenMRS atomfeed properties
    openmrs_props = "/opt/openmrs/.OpenMRS/openmrs-runtime.properties"
    if os.path.exists(openmrs_props):
        credential_sources.append(("openmrs_properties", openmrs_props))
    
    # 2. Check Bahmni atomfeed client config
    atomfeed_configs = [
        "/etc/bahmni/atomfeed/atomfeed.properties",
        "/var/lib/bahmni/atomfeed/atomfeed.properties",
        "/opt/bahmni/atomfeed/conf/atomfeed.properties"
    ]
    
    for config in atomfeed_configs:
        if os.path.exists(config):
            credential_sources.append(("atomfeed_config", config))
    
    # 3. Check ERP connect config
    erp_configs = [
        "/etc/bahmni-erp-connect/bahmni-erp-connect.conf",
        "/opt/bahmni-erp-connect/etc/bahmni-erp-connect.conf"
    ]
    
    for config in erp_configs:
        if os.path.exists(config):
            credential_sources.append(("erp_connect", config))
    
    # 4. Check environment variables in service files
    service_files = [
        "/etc/systemd/system/atomfeed-client.service",
        "/etc/systemd/system/bahmni-erp-connect.service"
    ]
    
    for service_file in service_files:
        if os.path.exists(service_file):
            credential_sources.append(("systemd_service", service_file))
    
    # Extract and test credentials from each source
    for source_type, source_path in credential_sources:
        credentials = _extract_credentials(source_type, source_path)
        
        if credentials:
            results["credentials_found"] = True
            results["details"][source_path] = {
                "type": source_type,
                "credentials": {k: "***" for k in credentials.keys()}  # Hide actual values in output
            }
            
            # Test the credentials
            test_result = _test_credentials(credentials)
            if test_result["success"]:
                results["working_credentials"].append({
                    "source": source_path,
                    "test_result": test_result
                })
            else:
                results["failed_credentials"].append({
                    "source": source_path,
                    "error": test_result["error"]
                })
    
    return results


def _extract_credentials(source_type, source_path):
    """Extract credentials from different config file types"""
    credentials = {}
    
    try:
        if source_type in ["openmrs_properties", "atomfeed_config"]:
            # Format: connection.url, connection.user, connection.password
            with open(source_path, 'r') as f:
                content = f.read()
                
                # Look for OpenMRS connection properties
                url_match = re.search(r'connection\.url\s*=\s*([^\s]+)', content)
                user_match = re.search(r'connection\.username\s*=\s*([^\s]+)', content)
                pass_match = re.search(r'connection\.password\s*=\s*([^\s]+)', content)
                
                if url_match:
                    credentials["url"] = url_match.group(1)
                if user_match:
                    credentials["username"] = user_match.group(1)
                if pass_match:
                    credentials["password"] = pass_match.group(1)
                
                # Also check for atomfeed specific
                if not credentials:
                    feed_url_match = re.search(r'atomfeed\.url\s*=\s*([^\s]+)', content)
                    feed_user_match = re.search(r'atomfeed\.username\s*=\s*([^\s]+)', content)
                    feed_pass_match = re.search(r'atomfeed\.password\s*=\s*([^\s]+)', content)
                    
                    if feed_url_match:
                        credentials["url"] = feed_url_match.group(1)
                    if feed_user_match:
                        credentials["username"] = feed_user_match.group(1)
                    if feed_pass_match:
                        credentials["password"] = feed_pass_match.group(1)
        
        elif source_type == "erp_connect":
            # JSON or properties format
            with open(source_path, 'r') as f:
                content = f.read()
                
                # Try JSON format
                if '"openmrs"' in content:
                    openmrs_url_match = re.search(r'"openmrsUrl":\s*"([^"]+)"', content)
                    username_match = re.search(r'"username":\s*"([^"]+)"', content)
                    password_match = re.search(r'"password":\s*"([^"]+)"', content)
                    
                    if openmrs_url_match:
                        credentials["url"] = openmrs_url_match.group(1)
                    if username_match:
                        credentials["username"] = username_match.group(1)
                    if password_match:
                        credentials["password"] = password_match.group(1)
                
                # Try properties format
                else:
                    url_match = re.search(r'openmrs\.url\s*=\s*([^\s]+)', content)
                    user_match = re.search(r'openmrs\.username\s*=\s*([^\s]+)', content)
                    pass_match = re.search(r'openmrs\.password\s*=\s*([^\s]+)', content)
                    
                    if url_match:
                        credentials["url"] = url_match.group(1)
                    if user_match:
                        credentials["username"] = user_match.group(1)
                    if pass_match:
                        credentials["password"] = pass_match.group(1)
        
        elif source_type == "systemd_service":
            # Look for Environment variables
            with open(source_path, 'r') as f:
                content = f.read()
                
                env_vars = re.findall(r'Environment="?([^"\n]+)"?', content)
                for env in env_vars:
                    if 'OPENMRS_URL' in env:
                        credentials["url"] = env.split('=')[1].strip('"')
                    elif 'OPENMRS_USER' in env:
                        credentials["username"] = env.split('=')[1].strip('"')
                    elif 'OPENMRS_PASSWORD' in env:
                        credentials["password"] = env.split('=')[1].strip('"')
    
    except Exception as e:
        pass
    
    return credentials


def _test_credentials(credentials):
    """Test if credentials work by making a request to atomfeed endpoint"""
    result = {"success": False, "error": None, "response_code": None}
    
    if not credentials.get("url") or not credentials.get("username") or not credentials.get("password"):
        result["error"] = "Missing URL, username, or password"
        return result
    
    # Construct atomfeed URL if needed
    feed_url = credentials["url"]
    if not feed_url.endswith('/atomfeed'):
        if feed_url.endswith('/openmrs'):
            feed_url = f"{feed_url}/ws/atomfeed/patient/1"
        elif '/openmrs' in feed_url:
            feed_url = f"{feed_url}/ws/atomfeed/patient/1"
        else:
            feed_url = f"{feed_url}/openmrs/ws/atomfeed/patient/1"
    
    try:
        # Parse URL to get host and port
        from urllib.parse import urlparse
        parsed = urlparse(feed_url)
        host = parsed.hostname
        port = parsed.port or (443 if parsed.scheme == 'https' else 80)
        path = parsed.path
        
        # Create basic auth header
        import base64
        auth_string = f"{credentials['username']}:{credentials['password']}"
        auth_bytes = auth_string.encode('ascii')
        auth_b64 = base64.b64encode(auth_bytes).decode('ascii')
        
        # Create socket connection
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(10)
        sock.connect((host, port))
        
        # Send HTTP request with basic auth
        request = (
            f"GET {path} HTTP/1.1\r\n"
            f"Host: {host}\r\n"
            f"Authorization: Basic {auth_b64}\r\n"
            f"User-Agent: CredentialTester/1.0\r\n"
            f"Connection: close\r\n\r\n"
        )
        
        sock.send(request.encode())
        response = sock.recv(4096)
        sock.close()
        
        # Parse response
        response_str = response.decode('utf-8', errors='ignore')
        status_line = response_str.split('\r\n')[0]
        
        # Extract status code
        status_match = re.search(r'HTTP/\d\.\d\s+(\d+)', status_line)
        if status_match:
            result["response_code"] = int(status_match.group(1))
            
            # Check if authentication succeeded
            if result["response_code"] == 200:
                result["success"] = True
                result["error"] = None
            elif result["response_code"] == 401:
                result["error"] = "Authentication failed - invalid credentials"
            elif result["response_code"] == 403:
                result["error"] = "Access forbidden - check permissions"
            elif result["response_code"] == 404:
                result["error"] = "Feed endpoint not found - check URL"
            else:
                result["error"] = f"HTTP {result['response_code']}"
        else:
            result["error"] = "Invalid HTTP response"
            
    except socket.timeout:
        result["error"] = "Connection timeout - service may be down"
    except socket.error as e:
        result["error"] = f"Socket error: {str(e)}"
    except Exception as e:
        result["error"] = f"Unexpected error: {str(e)}"
    
    return result


# Alternative: Use subprocess with curl (more reliable on CentOS)
def test_credentials_with_curl(credentials):
    """Test credentials using curl command (more reliable)"""
    result = {"success": False, "error": None, "response_code": None}
    
    if not credentials.get("url") or not credentials.get("username") or not credentials.get("password"):
        result["error"] = "Missing credentials"
        return result
    
    feed_url = credentials["url"]
    if not feed_url.endswith('/atomfeed'):
        feed_url = f"{feed_url}/openmrs/ws/atomfeed/patient/1"
    
    try:
        # Use curl with basic auth
        cmd = [
            "curl", "-s", "-o", "/dev/null", "-w", "%{http_code}",
            "-u", f"{credentials['username']}:{credentials['password']}",
            "--connect-timeout", "10",
            feed_url
        ]
        
        proc = subprocess.run(cmd, capture_output=True, text=True, timeout=15)
        result["response_code"] = int(proc.stdout.strip()) if proc.stdout.strip().isdigit() else None
        
        if result["response_code"] == 200:
            result["success"] = True
        elif result["response_code"] == 401:
            result["error"] = "Authentication failed"
        elif result["response_code"]:
            result["error"] = f"HTTP {result['response_code']}"
        else:
            result["error"] = "Failed to connect"
            
    except subprocess.TimeoutExpired:
        result["error"] = "Request timeout"
    except Exception as e:
        result["error"] = str(e)
    
    return result


# Simple usage
if __name__ == "__main__":
    result = test_atomfeed_credentials()
    
    print("=" * 60)
    print("Atomfeed Credentials Test")
    print("=" * 60)
    
    if result["credentials_found"]:
        print(f"✓ Found {len(result['working_credentials'])} working credential set(s)")
        
        for cred in result["working_credentials"]:
            print(f"\n  ✓ Working: {cred['source']}")
            print(f"    → Response: HTTP {cred['test_result']['response_code']}")
        
        for cred in result["failed_credentials"]:
            print(f"\n  ✗ Failed: {cred['source']}")
            print(f"    → Error: {cred['error']}")
    else:
        print("✗ No credentials found in standard Bahmni config files")
    
    print("\nConfig files checked:")
    for source_path, details in result.get("details", {}).items():
        print(f"  - {source_path}")