
#!/usr/bin/env python
import json
import sys
import os

# Add parent directory to Python path to import NY_Traffic_Events_API_Client
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from extraction.api_client import NY_Traffic_Events_API_Client

def test_api():
    """Test ny_traffic_events API connection and data format"""
    print("Testing NYC Traffic Events API...")
    
    try:
        # Create API handler
        api_handler = NY_Traffic_Events_API_Client()
        
        # Get traffic events data
        traffic_events_data = api_handler.get_ny_traffic_events()
        
        # Analyze data
        if traffic_events_data:
            # Print sample data
            print("\nSample data (first record):")
            print(json.dumps(traffic_events_data[0], indent=2))
            
            # Print total record count
            print(f"\nSuccessfully retrieved data, total records: {len(traffic_events_data)}")
            
            # Data field analysis
            print("\nData field analysis:")
            fields = list(traffic_events_data[0].keys())
            print(f"Field list: {fields}")
            
            # Print data statistics
            regionNames = set(item.get('RegionName', 'Unknown') for item in traffic_events_data)
            countyNames = set(item.get('CountyName', 'Unknown') for item in traffic_events_data)
            
            print(f"\nTotal regions: {len(regionNames)}")
            print(f"Total counties: {len(countyNames)}")
            
            if len(regionNames) <= 12:  # If there aren't too many regions, print them all
                print(f"Region list: {sorted(regionNames)}")
            
            return len(traffic_events_data)
        else:
            print("No data received")
            return 0
            
    except Exception as e:
        print(f"API test failed: {str(e)}")
        return 0


if __name__ == "__main__":
    record_count = test_api()
    if record_count > 0:
        print("\nAPI test successful! ")
    else:
        print("\nAPI test failed! ")
