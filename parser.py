import re
from datetime import datetime
from typing import List, Dict

def parse_feed(feed_string: str) -> Dict:
    """
    Parse an Atom feed string into a structured object.
    
    Args:
        feed_string: The raw feed string from OpenMRS
    
    Returns:
        Dictionary with parsed feed entries
    """
    # Find all UUIDs
    uuids = re.findall(r'[a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12}', feed_string)
    
    # Find all timestamps (ISO format with Z)
    timestamps = re.findall(r'\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z', feed_string)
    
    # Find all feed entry IDs (tag:atomfeed.ict4h.org:UUID)
    entry_ids = re.findall(r'tag:atomfeed\.ict4h\.org:[a-f0-9\-]+', feed_string)
    
    # Build entries
    entries = []
    for i, entry_id in enumerate(entry_ids):
        entry = {
            'id': entry_id,
            'published': timestamps[i*2] if i*2 < len(timestamps) else None,
            'updated': timestamps[i*2 + 1] if i*2 + 1 < len(timestamps) else None,
            'patient_uuid': uuids[i] if i < len(uuids) else None
        }
        entries.append(entry)
    
    return {
        'total_entries': len(entries),
        'entries': entries
    }

# Example usage
feed_string = """OpenMRS bec795b1-3d17-451d-b43e-a094019f6984+2 OpenMRS Feed Publisher 2026-04-11T09:47:00Z tag:atomfeed.ict4h.org:34886755-bedc-4626-aeee-6c318ff7dc54 2026-04-11T07:07:58Z 2026-04-11T07:07:58Z tag:atomfeed.ict4h.org:d98bcd83-f6b7-4dea-8cda-8a19685f29d5 2026-04-11T07:08:10Z 2026-04-11T07:08:10Z tag:atomfeed.ict4h.org:2f42a878-8c09-40e6-a203-48e29e7ef38a 2026-04-11T07:31:26Z 2026-04-11T07:31:26Z tag:atomfeed.ict4h.org:38b16b3c-043b-4322-a92a-06f9c6bba335 2026-04-11T07:32:04Z 2026-04-11T07:32:04Z tag:atomfeed.ict4h.org:c36c3a68-83cc-4cfb-80b2-99a365dff40b 2026-04-11T09:47:00Z 2026-04-11T09:47:00Z"""

result = parse_feed(feed_string)
print(result)