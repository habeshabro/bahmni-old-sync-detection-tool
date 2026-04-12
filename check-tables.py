import subprocess
import re
import socket
import base64
from datetime import datetime, timedelta
from urllib.parse import urlparse

def check_atomfeed_tables(openmrs_url="http://localhost/openmrs", username="admin", password="admin"):
    """
    Check atomfeed tables and verify if events point to real entities.
    
    Args:
        openmrs_url: Base URL for OpenMRS
        username: Database username (usually 'openmrs' or 'root')
        password: Database password
    
    Returns:
        Dictionary with table status and entity verification results
    """
    results = {
        "timestamp": datetime.now().isoformat(),
        "database": "atomfeed",
        "tables": {},
        "issues": [],
        "total_invalid_events": 0
    }
    
    # Check each table
    results["tables"]["event_records"] = _check_event_records(username, password)
    results["tables"]["markers"] = _check_markers(username, password)
    results["tables"]["event_records_offset_marker"] = _check_offset_marker(username, password)
    results["tables"]["failed_events"] = _check_failed_events(username, password)
    
    # Verify events point to real entities
    results["entity_verification"] = _verify_entities(username, password, openmrs_url)
    
    # Compile issues
    if results["tables"]["event_records"]["issues"]:
        results["issues"].extend(results["tables"]["event_records"]["issues"])
    if results["tables"]["failed_events"]["count"] > 0:
        results["issues"].append(f"Found {results['tables']['failed_events']['count']} failed events")
    if results["entity_verification"]["invalid_count"] > 0:
        results["issues"].append(f"Found {results['entity_verification']['invalid_count']} events pointing to non-existent entities")
    
    results["total_invalid_events"] = results["entity_verification"]["invalid_count"]
    
    return results


def _check_event_records(db_user, db_pass):
    """Check event_records table for consistency"""
    result = {
        "count": 0,
        "unprocessed_count": 0,
        "oldest_unprocessed": None,
        "issues": []
    }
    
    try:
        # Get total count
        total_cmd = f'mysql -u{db_user} -p{db_pass} atomfeed -sN -e "SELECT COUNT(*) FROM event_records"'
        total_proc = subprocess.run(total_cmd, shell=True, capture_output=True, text=True, timeout=10)
        if total_proc.returncode == 0:
            result["count"] = int(total_proc.stdout.strip())
        
        # Get unprocessed events (events not yet consumed)
        unprocessed_cmd = f'mysql -u{db_user} -p{db_pass} atomfeed -sN -e "SELECT COUNT(*) FROM event_records WHERE event_status = \'PENDING\'"'
        unprocessed_proc = subprocess.run(unprocessed_cmd, shell=True, capture_output=True, text=True, timeout=10)
        if unprocessed_proc.returncode == 0:
            result["unprocessed_count"] = int(unprocessed_proc.stdout.strip())
        
        # Get oldest unprocessed event
        oldest_cmd = f'mysql -u{db_user} -p{db_pass} atomfeed -sN -e "SELECT MIN(time_created) FROM event_records WHERE event_status = \'PENDING\'"'
        oldest_proc = subprocess.run(oldest_cmd, shell=True, capture_output=True, text=True, timeout=10)
        if oldest_proc.returncode == 0 and oldest_proc.stdout.strip():
            result["oldest_unprocessed"] = oldest_proc.stdout.strip()
            
            # Check if there are very old unprocessed events (> 1 hour)
            oldest_time = datetime.fromisoformat(result["oldest_unprocessed"].replace(' ', 'T'))
            if datetime.now() - oldest_time > timedelta(hours=1):
                result["issues"].append(f"Unprocessed events from {result['oldest_unprocessed']} (> 1 hour old)")
        
        # Check for duplicate UUIDs
        dup_cmd = f'mysql -u{db_user} -p{db_pass} atomfeed -sN -e "SELECT uuid, COUNT(*) FROM event_records GROUP BY uuid HAVING COUNT(*) > 1 LIMIT 5"'
        dup_proc = subprocess.run(dup_cmd, shell=True, capture_output=True, text=True, timeout=10)
        if dup_proc.returncode == 0 and dup_proc.stdout.strip():
            duplicates = dup_proc.stdout.strip().split('\n')
            result["issues"].append(f"Found {len(duplicates)} duplicate UUIDs in event_records")
        
    except Exception as e:
        result["issues"].append(f"Error checking event_records: {str(e)}")
    
    return result


def _check_markers(db_user, db_pass):
    """Check markers table (tracks last processed event per feed)"""
    result = {
        "feed_markers": [],
        "issues": []
    }
    
    try:
        cmd = f'mysql -u{db_user} -p{db_pass} atomfeed -sN -e "SELECT feed_uri, last_read_entry_id, last_read_entry_time FROM markers"'
        proc = subprocess.run(cmd, shell=True, capture_output=True, text=True, timeout=10)
        
        if proc.returncode == 0 and proc.stdout.strip():
            for line in proc.stdout.strip().split('\n'):
                parts = line.split('\t')
                if len(parts) >= 3:
                    marker = {
                        "feed_uri": parts[0],
                        "last_read_entry_id": parts[1],
                        "last_read_entry_time": parts[2]
                    }
                    result["feed_markers"].append(marker)
                    
                    # Check if marker is very old
                    try:
                        last_time = datetime.fromisoformat(parts[2].replace(' ', 'T'))
                        if datetime.now() - last_time > timedelta(hours=24):
                            result["issues"].append(f"Feed {parts[0]} not updated since {parts[2]}")
                    except:
                        pass
        
        if not result["feed_markers"]:
            result["issues"].append("No markers found - atomfeed may not be consuming events")
            
    except Exception as e:
        result["issues"].append(f"Error checking markers: {str(e)}")
    
    return result


def _check_offset_marker(db_user, db_pass):
    """Check event_records_offset_marker table"""
    result = {
        "has_offset_marker": False,
        "offset_value": None,
        "issues": []
    }
    
    try:
        cmd = f'mysql -u{db_user} -p{db_pass} atomfeed -sN -e "SELECT * FROM event_records_offset_marker LIMIT 1"'
        proc = subprocess.run(cmd, shell=True, capture_output=True, text=True, timeout=10)
        
        if proc.returncode == 0 and proc.stdout.strip():
            result["has_offset_marker"] = True
            result["offset_value"] = proc.stdout.strip()
        else:
            result["issues"].append("No offset marker found - may cause duplicate processing")
            
    except Exception as e:
        result["issues"].append(f"Error checking offset_marker: {str(e)}")
    
    return result


def _check_failed_events(db_user, db_pass):
    """Check failed_events table for processing errors"""
    result = {
        "count": 0,
        "oldest_failure": None,
        "recent_failures": [],
        "issues": []
    }
    
    try:
        # Get total count
        count_cmd = f'mysql -u{db_user} -p{db_pass} atomfeed -sN -e "SELECT COUNT(*) FROM failed_events"'
        count_proc = subprocess.run(count_cmd, shell=True, capture_output=True, text=True, timeout=10)
        if count_proc.returncode == 0:
            result["count"] = int(count_proc.stdout.strip())
        
        if result["count"] > 0:
            # Get oldest failure
            oldest_cmd = f'mysql -u{db_user} -p{db_pass} atomfeed -sN -e "SELECT MIN(time_created) FROM failed_events"'
            oldest_proc = subprocess.run(oldest_cmd, shell=True, capture_output=True, text=True, timeout=10)
            if oldest_proc.returncode == 0 and oldest_proc.stdout.strip():
                result["oldest_failure"] = oldest_proc.stdout.strip()
            
            # Get recent failures (last 10)
            recent_cmd = f'mysql -u{db_user} -p{db_pass} atomfeed -sN -e "SELECT id, uuid, title, error_message, time_created FROM failed_events ORDER BY time_created DESC LIMIT 10"'
            recent_proc = subprocess.run(recent_cmd, shell=True, capture_output=True, text=True, timeout=10)
            if recent_proc.returncode == 0 and recent_proc.stdout.strip():
                for line in recent_proc.stdout.strip().split('\n'):
                    parts = line.split('\t')
                    if len(parts) >= 5:
                        result["recent_failures"].append({
                            "id": parts[0],
                            "uuid": parts[1],
                            "title": parts[2],
                            "error": parts[3][:100],  # Truncate long errors
                            "time": parts[4]
                        })
            
            result["issues"].append(f"Found {result['count']} failed events that need investigation")
            
    except Exception as e:
        result["issues"].append(f"Error checking failed_events: {str(e)}")
    
    return result


def _verify_entities(db_user, db_pass, openmrs_url):
    """Verify that events point to real entities by checking their endpoints"""
    result = {
        "total_checked": 0,
        "valid_count": 0,
        "invalid_count": 0,
        "invalid_events": [],
        "errors": []
    }
    
    try:
        # Get recent unprocessed events to check
        cmd = f'mysql -u{db_user} -p{db_pass} atomfeed -sN -e "SELECT id, uuid, title, url, content FROM event_records WHERE event_status = \'PENDING\' LIMIT 20"'
        proc = subprocess.run(cmd, shell=True, capture_output=True, text=True, timeout=10)
        
        if proc.returncode == 0 and proc.stdout.strip():
            for line in proc.stdout.strip().split('\n'):
                parts = line.split('\t')
                if len(parts) >= 5:
                    event_id = parts[0]
                    entity_uuid = parts[1]
                    title = parts[2]
                    url = parts[3]
                    content = parts[4]
                    
                    result["total_checked"] += 1
                    
                    # Verify the entity exists
                    is_valid, response_code, error = _check_entity_exists(openmrs_url, title, entity_uuid, content)
                    
                    if is_valid:
                        result["valid_count"] += 1
                    else:
                        result["invalid_count"] += 1
                        result["invalid_events"].append({
                            "event_id": event_id,
                            "uuid": entity_uuid,
                            "title": title,
                            "response_code": response_code,
                            "error": error
                        })
        
        # Also check failed events
        failed_cmd = f'mysql -u{db_user} -p{db_pass} atomfeed -sN -e "SELECT id, uuid, title, url, content FROM failed_events LIMIT 10"'
        failed_proc = subprocess.run(failed_cmd, shell=True, capture_output=True, text=True, timeout=10)
        
        if failed_proc.returncode == 0 and failed_proc.stdout.strip():
            for line in failed_proc.stdout.strip().split('\n'):
                parts = line.split('\t')
                if len(parts) >= 5:
                    event_id = parts[0]
                    entity_uuid = parts[1]
                    title = parts[2]
                    url = parts[3]
                    content = parts[4]
                    
                    is_valid, response_code, error = _check_entity_exists(openmrs_url, title, entity_uuid, content)
                    
                    if not is_valid:
                        result["invalid_events"].append({
                            "event_id": event_id,
                            "uuid": entity_uuid,
                            "title": title,
                            "response_code": response_code,
                            "error": error,
                            "from_failed_table": True
                        })
                        
    except Exception as e:
        result["errors"].append(f"Error verifying entities: {str(e)}")
    
    return result


def _check_entity_exists(base_url, entity_type, entity_uuid, content_url=None):
    """
    Check if an entity actually exists by making a request to its REST endpoint.
    Returns (is_valid, response_code, error_message)
    """
    # Map event titles to REST endpoints
    endpoint_mapping = {
        "patient": "patient",
        "encounter": "encounter",
        "order": "order",
        "drug": "drug",
        "sample": "sample",  # For OpenELIS
        "test": "test",
        "panel": "panel",
        "department": "department"
    }
    
    # Try to extract URL from content if provided
    if content_url and content_url.startswith('/'):
        full_url = f"{base_url}{content_url}"
    else:
        # Construct REST URL
        endpoint = endpoint_mapping.get(entity_type, entity_type)
        full_url = f"{base_url}/ws/rest/v1/{endpoint}/{entity_uuid}?v=full"
    
    try:
        # Parse URL
        parsed = urlparse(full_url)
        host = parsed.hostname
        port = parsed.port or 80
        path = parsed.path + ('?' + parsed.query if parsed.query else '')
        
        # Create socket connection
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(5)
        sock.connect((host, port))
        
        # Send request
        request = f"GET {path} HTTP/1.1\r\nHost: {host}\r\nConnection: close\r\n\r\n"
        sock.send(request.encode())
        
        # Read response
        response = sock.recv(4096)
        sock.close()
        
        # Parse status code
        response_str = response.decode('utf-8', errors='ignore')
        status_match = re.search(r'HTTP/\d\.\d\s+(\d+)', response_str)
        
        if status_match:
            status_code = int(status_match.group(1))
            
            if status_code == 200:
                return True, status_code, None
            elif status_code == 404:
                return False, status_code, "Entity not found (404)"
            elif status_code == 401:
                return False, status_code, "Authentication required"
            else:
                return False, status_code, f"HTTP {status_code}"
        else:
            return False, None, "Invalid HTTP response"
            
    except socket.timeout:
        return False, None, "Connection timeout"
    except socket.error as e:
        return False, None, f"Connection error: {str(e)}"
    except Exception as e:
        return False, None, f"Error: {str(e)}"


# Simple usage
if __name__ == "__main__":
    # You'll need to provide your MySQL credentials
    # Typically on Bahmni: username='openmrs' or 'root', password='password'
    
    result = check_atomfeed_tables(
        openmrs_url="http://localhost/openmrs",
        username="openmrs",
        password="password"
    )
    
    print("=" * 70)
    print("Atomfeed Tables Health Check")
    print("=" * 70)
    
    # Event Records
    print(f"\n📊 event_records:")
    er = result["tables"]["event_records"]
    print(f"   Total events: {er['count']}")
    print(f"   Unprocessed: {er['unprocessed_count']}")
    if er['oldest_unprocessed']:
        print(f"   Oldest unprocessed: {er['oldest_unprocessed']}")
    
    # Markers
    print(f"\n📍 markers:")
    markers = result["tables"]["markers"]
    for marker in markers["feed_markers"]:
        print(f"   Feed: {marker['feed_uri'][:50]}...")
        print(f"   Last read: {marker['last_read_entry_time']}")
    
    # Failed Events
    print(f"\n❌ failed_events:")
    fe = result["tables"]["failed_events"]
    print(f"   Count: {fe['count']}")
    if fe['recent_failures']:
        print(f"   Recent failures:")
        for failure in fe['recent_failures'][:3]:
            print(f"     - {failure['title']} ({failure['uuid']})")
            print(f"       Error: {failure['error'][:50]}...")
    
    # Entity Verification
    print(f"\n🔍 Entity Verification:")
    ev = result["entity_verification"]
    print(f"   Checked: {ev['total_checked']} events")
    print(f"   Valid: {ev['valid_count']}")
    print(f"   Invalid: {ev['invalid_count']}")
    
    if ev['invalid_events']:
        print(f"\n   ⚠️ Invalid events found:")
        for invalid in ev['invalid_events'][:5]:
            print(f"     - {invalid['title']}: {invalid['uuid']}")
            print(f"       Error: {invalid['error']}")
    
    # Summary
    print("\n" + "=" * 70)
    if result["issues"]:
        print(f"⚠️ ISSUES FOUND ({len(result['issues'])}):")
        for issue in result["issues"]:
            print(f"   • {issue}")
    else:
        print("✓ All tables look healthy!") 