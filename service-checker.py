# -*- coding: utf-8 -*-

import socket
import subprocess
from datetime import datetime
import time

def check_bahmni_sync_services():
    """
    Check if all required Bahmni services are running for sync to work.
    Uses only standard library modules.
    
    Returns:
        Dictionary with service status
    """
    results = {
        "timestamp": datetime.now().isoformat(),
        "all_services_running": True,
        "services": {},
        "failed_services": []
    }
    
    # List of services to check (CentOS service names)
    services_to_check = [
        "httpd",                     # Apache web server
        "bahmni-erp-connect",        # ERP sync service
        "mysqld",                    # MySQL database
        "odoo",
        "openmrs",                   # OpenMRS service
        "bahmni-lab"                   # OpenELIS service
    ]
    
    for service in services_to_check:
        status = _check_service(service)
        results["services"][service] = status
        
        if not status["running"]:
            results["all_services_running"] = False
            results["failed_services"].append(service)
    
    # Check critical sync endpoints
    results["sync_endpoints"] = _check_sync_endpoints()
    
    return results

# Source - https://stackoverflow.com/a/40590445
# Posted by Martijn Pieters, modified by community. See post 'Timeline' for change history
# Retrieved 2026-04-13, License - CC BY-SA 4.0

def run(*popenargs, **kwargs):
    input = kwargs.pop("input", None)
    check = kwargs.pop("handle", False)

    if input is not None:
        if 'stdin' in kwargs:
            raise ValueError('stdin and input arguments may not both be used.')
        kwargs['stdin'] = subprocess.PIPE

    process = subprocess.Popen(*popenargs, **kwargs)
    try:
        stdout, stderr = process.communicate(input)
    except:
        process.kill()
        process.wait()
        raise
    retcode = process.poll()
    if check and retcode:
        raise subprocess.CalledProcessError(
            retcode, process.args, output=stdout, stderr=stderr)
    return retcode, stdout, stderr



def _check_service(service_name):
    """Check if a systemd service is running on CentOS"""
    try:
        # Use systemctl to check service status
        
        stdout = subprocess.check_output(["systemctl", "is-active", service_name], stderr=subprocess.STDOUT).strip()


        is_running = stdout.strip() == "active"
        
        details = {}
        
        return {
            "running": is_running,
            "status": stdout.strip() if stdout else "inactive",
            "details": details
        }
    except subprocess.CalledProcessError as e:
        stdout = e.output
        is_running = stdout.strip() == "active"
        details = {}
        return {
            "running": is_running,
            "status": stdout.strip() if stdout else "inactive",
            "details": details
        }
    except Exception as e:
        print("error:", e.output)
        return {"running": False, "status": "not_found", "details": {}}


def _get_service_details(service_name):
    """Get additional details for critical services"""
    details = {}
    
    # Check if ports are listening
    ports_to_check = {
        "tomcat": [8080, 8443],      # OpenMRS/OpenELIS ports
        "bahmni-erp-connect": [8069]  # Odoo/ERP port
    }
    
    if service_name in ports_to_check:
        listening_ports = []
        for port in ports_to_check[service_name]:
            if _is_port_open(port):
                listening_ports.append(port)
        details["listening_ports"] = listening_ports
    
    # Check memory usage
    try:
        result = subprocess.run(
            ["ps", "aux"],
            capture_output=True,
            text=True,
            timeout=5
        )
        for line in result.stdout.split('\n'):
            if service_name in line.lower():
                parts = line.split()
                if len(parts) > 5:
                    details["memory_percent"] = parts[3]
                    details["cpu_percent"] = parts[2]
                break
    except:
        pass
    
    return details


def _is_port_open(port, host='localhost'):
    """Check if a TCP port is open"""
    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(2)
        result = sock.connect_ex((host, port))
        sock.close()
        return result == 0
    except:
        return False


def _check_sync_endpoints():
    """Check critical sync endpoints using basic socket connections"""
    endpoints = {
        "openmrs_atom_feed": {"host": "localhost", "port": 8080, "path": "/openmrs/ws/atomfeed"},
        "openelis_atom_feed": {"host": "localhost", "port": 8080, "path": "/openelis/ws/atomfeed"},
        "marker_tables": {"check": "mysql", "query": "SELECT 1 FROM atomfeed.failed_events LIMIT 1"}
    }
    
    results = {}
    
    # Check HTTP endpoints via socket
    for name, endpoint in endpoints.items():
        if "port" in endpoint:
            try:
                sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                sock.settimeout(3)
                sock.connect((endpoint["host"], endpoint["port"]))
                
                # Send simple HTTP request
                request = "GET {} HTTP/1.1\r\nHost: {}\r\n\r\n".format(endpoint['path'],endpoint['host'])
                sock.send(request.encode())
                response = sock.recv(1024)
                
                results[name] = response.startswith(b"HTTP/1.1 200") or response.startswith(b"HTTP/1.0 200")
                sock.close()
            except:
                results[name] = False
    
    # Check marker tables via MySQL
    try:
        # Use mysql command line to check failed events
        result = subprocess.run(
            ["mysql", "-u", "openmrs", "-e", "SELECT COUNT(*) FROM atomfeed.failed_events"],
            capture_output=True,
            text=True,
            timeout=5
        )
        if result.returncode == 0:
            count = result.stdout.strip().split('\n')[-1]
            results["failed_events_count"] = int(count) if count.isdigit() else 0
            results["has_failed_events"] = results["failed_events_count"] > 0
        else:
            results["has_failed_events"] = None
    except:
        results["has_failed_events"] = None
    
    return results


# Simple usage
if __name__ == "__main__":
    status = check_bahmni_sync_services()
    
    print("=" * 50)
    print("Bahmni Sync Health Check - {}".format(status['timestamp']))
    print("=" * 50)
    
    if status["all_services_running"]:
        print("✓ All services are running")
    else:
        print("✗ Failed services: {}".format(', '.join(status['failed_services'])))
    
    print("\nService Status:")
    for service, info in status["services"].items():
        status_icon = "✓" if info["running"] else "✗"
        print("  {} {}: {}".format(status_icon, service, info['status']))
    
    print("\nSync Endpoints:")
    for endpoint, is_ok in status["sync_endpoints"].items():
        if isinstance(is_ok, bool):
            status_icon = "✓" if is_ok else "✗"
            print("  {} {}".format(status_icon, endpoint))
        else:
            print("  → {}: {}".format(endpoint, is_ok))