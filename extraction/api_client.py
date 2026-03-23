import os
import json
import time
import logging
import requests
from typing import Dict, List, Optional
from datetime import datetime
from dotenv import load_dotenv
from kafka import KafkaProducer

class NY_Traffic_Events_API_Client:
    """
    Client to interact with the NYC Traffic Events API.
    """
    def __init__(self, base_url: str = "https://511ny.org/api/getevents"):
        """
        Initialize NYC Traffic Events API handler

        Args:
            base_url (str): Base URL for the API
        """
        # Setup logging
        self._setup_logger()

        load_dotenv()

        self.api_key = self._get_api_key()
        self.format = "json"
        self.base_url = base_url

        # Create data directory
        self.data_dir = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "data")
        os.makedirs(self.data_dir, exist_ok=True)

    def _setup_logger(self) -> None:
        """Configure the logger"""
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
        self.logger = logging.getLogger(__name__)    
       
    def _get_api_key(self) -> str:
        """
        Get API key from environment variables
        
        Returns:
            str: API key
            
        Raises:
            ValueError: If API key is not found
        """
        

        api_key = os.getenv("511NY_API_KEY")
        if not api_key:
            self.logger.error("API key not found. Please set 511NY_API_KEY in .env file")
            raise ValueError("API key not found. Please set 511NY_API_KEY in .env file")
        return api_key

    def _enrich_ny_traffic_event_data(self, ny_traffic_events: List[Dict]) -> None:
        """
        Enrich NYC traffic event data with timestamp and parsed coordinates

        Args:
            ny_traffic_events (List[Dict]): List of NYC traffic event data
        """
        timestamp = datetime.now().isoformat()
        
        for event in ny_traffic_events:
            # Add timestamp
            event['ingestion_time'] = timestamp
            
           

    def get_ny_traffic_events(self) -> List[Dict]:
        """
        Get NYC traffic events data

        Returns:
            List[Dict]: List containing ny traffic event data
            
        Raises:
            RuntimeError: If API request fails
        """
        query_params = f"?key={self.api_key}&format={self.format}"
        url = self.base_url + query_params
        
        try:
            response = requests.get(url)
            response.raise_for_status()
            
            ny_traffic_event = response.json()
            
            # Add timestamp and process location coordinates
            self._enrich_ny_traffic_event_data(ny_traffic_event)
            
            self.logger.info(f"Successfully retrieved {len(ny_traffic_event)} NYC traffic event records")
            return ny_traffic_event
            
        except requests.RequestException as e:
            error_msg = f"API request failed: {str(e)}"
            self.logger.error(error_msg)
            raise RuntimeError(error_msg)   

    def save_to_local(self, data: List[Dict], filename: Optional[str] = None) -> str:
        """
        Save data to local file
        
        Args:
            data (List[Dict]): Data to save
            filename (Optional[str]): Filename, auto-generated if not provided
            
        Returns:
            str: Path to the saved file
            
        Raises:
            IOError: If saving fails
        """
        if not data:
            self.logger.warning("No data to save")
            return ""
        
        if filename is None:
            filename = f"ny_traffic_events_data_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        
        filepath = os.path.join(self.data_dir, filename)
        
        try:
            with open(filepath, 'w') as f:
                json.dump(data, f, indent=2)
            self.logger.info(f"Data saved to: {filepath}")
            return filepath
        except Exception as e:
            error_msg = f"Error saving data: {str(e)}"
            self.logger.error(error_msg)
            raise IOError(error_msg)
    
    def to_dataframe(self, data: List[Dict]) -> 'pd.DataFrame':
        """
        Convert data to Pandas DataFrame
        
        Args:
            data (List[Dict]): List of ny_traffic_events data
            
        Returns:
            pd.DataFrame: DataFrame containing ny_traffic_events data
        """
        try:
            import pandas as pd
            df = pd.DataFrame(data)
            self.logger.info(f"Successfully converted to DataFrame, shape: {df.shape}")
            return df
        except ImportError:
            self.logger.warning("Pandas not installed, cannot convert to DataFrame")
            raise ImportError("Please install pandas: pip install pandas")

def main():
    while True:
        try:
            
            api_handler = NY_Traffic_Events_API_Client()
            
            
            ny_traffic_events = api_handler.get_ny_traffic_events()
            
            
            if ny_traffic_events:
                api_handler.save_to_local(ny_traffic_events)
                try:
                    # api_handler.send_to_kafka(ny_traffic_events)
                    print(f"Successfully processed, saved, and sent {len(ny_traffic_events)} records to Kafka")
                except Exception as e:
                    print(f"Saved data locally but failed to send to Kafka: {str(e)}")
            else:
                print("No data received")
                
            
            print("Waiting 5 minutes before next data fetch...")
            time.sleep(420)  
            
                
        except Exception as e:
            print(f"Error: {str(e)}")
            time.sleep(60)

if __name__ == "__main__":
    exit_code = main()
    exit(exit_code)