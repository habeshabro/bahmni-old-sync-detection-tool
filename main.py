# check if all the services are running properly

# check the logs for specific errors
# check if config files are where they should be
# config files tick
# use the credentials and urls to fetch
# credentials and urls tick
# Just call the function
# import the function from the service-checker module
from service_checker import check_bahmni_sync_services
from atomfeed_credentials import test_atomfeed_credentials
from parser import parse_feed

result = check_bahmni_sync_services()

# Check if everything is ok
if result["all_services_running"]:
    print("All systems ready for sync")
else:
    print(f"Services down: {result['failed_services']}")


test_result = test_atomfeed_credentials()

# Check if we have working credentials
if test_result["working_credentials"]:
    print("✓ Atomfeed has valid credentials")
    for cred in test_result["working_credentials"]:
        print(f"  From: {cred['source']}")
        print(f"  Status: HTTP {cred['test_result']['response_code']}")
else:
    print("✗ No working credentials found")
    print("Check your Bahmni configuration")
# Go to each database
# check event_records as publisher "This tables holds the list of events which are to be published by Atom Feed for others to consume. The category column is used to indicate the event types (like patient, encounter, etc).Note: For the same patient updates there might be multiple rows. So to see unique rows: "select distinct object from event_records where category = 'patient';" "
# check markers as consumer "This table holds marker entries to indicate the records which have ALREADY been processed:
# check event_records_offset_marker as consumer "This table holds cached records for faster event process by the CONSUMER."
# check failed_events as CONSUMER This table holds the list of events which failed and could not be consumed. They are retried later by a different event handler.
