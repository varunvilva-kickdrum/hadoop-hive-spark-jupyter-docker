#!/usr/bin/env python3
"""
Electric Vehicle Population Data ETL Script for Airflow
Fetches data from Washington State Open Data API and uploads to HDFS
"""

import requests
import logging
from hdfs import InsecureClient
import tempfile
import os
from urllib.parse import urlparse

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def fetch_and_upload_ev_data():
    """
    Fetch Electric Vehicle Population data from WA.gov API and upload to HDFS
    """
    # Configuration
    source_url = "https://data.wa.gov/api/views/f6w7-q2d2/rows.csv?accessType=DOWNLOAD"
    hdfs_namenode_url = "http://namenode:9000"
    hdfs_destination_path = "/data/Electric_Vehicle_Population_Data.csv"
    
    try:
        # Step 1: Fetch data from the API
        logger.info(f"Fetching data from: {source_url}")
        response = requests.get(source_url, stream=True, timeout=300)
        response.raise_for_status()
        
        # Step 2: Create HDFS client
        logger.info(f"Connecting to HDFS at: {hdfs_namenode_url}")
        hdfs_client = InsecureClient(hdfs_namenode_url)
        
        # Step 3: Create a temporary file to store the CSV data
        with tempfile.NamedTemporaryFile(mode='wb', delete=False, suffix='.csv') as temp_file:
            temp_file_path = temp_file.name
            
            # Write the response content to temporary file
            logger.info("Writing data to temporary file...")
            for chunk in response.iter_content(chunk_size=8192):
                if chunk:
                    temp_file.write(chunk)
        
        # Step 4: Get file size for logging
        file_size = os.path.getsize(temp_file_path)
        logger.info(f"Downloaded {file_size} bytes to temporary file: {temp_file_path}")
        
        # Step 5: Upload to HDFS (overwrite if exists)
        logger.info(f"Uploading to HDFS: {hdfs_destination_path}")
        
        # Create directory if it doesn't exist
        hdfs_dir = os.path.dirname(hdfs_destination_path)
        if hdfs_dir and hdfs_dir != '/':
            hdfs_client.makedirs(hdfs_dir, permission=755)
        
        # Upload file with overwrite=True
        with open(temp_file_path, 'rb') as local_file:
            hdfs_client.write(hdfs_destination_path, local_file, overwrite=True)
        
        # Step 6: Verify upload
        hdfs_status = hdfs_client.status(hdfs_destination_path)
        logger.info(f"Successfully uploaded to HDFS. File size: {hdfs_status['length']} bytes")
        
        # Step 7: Cleanup temporary file
        os.unlink(temp_file_path)
        logger.info("Temporary file cleaned up")
        
        return {
            'status': 'success',
            'source_url': source_url,
            'hdfs_path': hdfs_destination_path,
            'file_size': hdfs_status['length']
        }
        
    except requests.exceptions.RequestException as e:
        logger.error(f"Error fetching data from API: {e}")
        raise
    except Exception as e:
        logger.error(f"Error during HDFS operations: {e}")
        # Clean up temp file if it exists
        if 'temp_file_path' in locals() and os.path.exists(temp_file_path):
            os.unlink(temp_file_path)
        raise

def main():
    """
    Main function for standalone execution
    """
    try:
        result = fetch_and_upload_ev_data()
        logger.info(f"ETL process completed successfully: {result}")
        return result
    except Exception as e:
        logger.error(f"ETL process failed: {e}")
        raise

if __name__ == "__main__":
    main()