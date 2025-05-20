#!/usr/bin/env python
# HDFS Dataset Upload Script
# This script copies local datasets to HDFS for distributed processing

import os
import glob
import subprocess
import sys

# Check if running inside container or on host
in_container = os.path.exists('/home/jovyan/datasets')
if in_container:
    datasets_path = "/home/jovyan/datasets"
else:
    # Assuming running from project root directory
    datasets_path = "./datasets"

print(f"Checking datasets directory at {datasets_path}...")
files = glob.glob(f"{datasets_path}/*")

if not files:
    print("No files found in datasets directory!")
    exit(1)

print(f"Found {len(files)} files in datasets directory:")
for file in files:
    print(f"  - {os.path.basename(file)}")

print("\nCopying files to HDFS /data directory using namenode container...")
for file in files:
    filename = os.path.basename(file)
    print(f"Copying {filename} to HDFS...")
    
    # Copy to namenode container first and then use namenode to put into HDFS
    cmd1 = f"docker cp {file} namenode:/tmp/{filename}"
    cmd2 = f"docker exec namenode hdfs dfs -put -f /tmp/{filename} /data/{filename}"
    
    result1 = subprocess.run(cmd1, shell=True, capture_output=True, text=True)
    if result1.returncode == 0:
        print(f"  ✓ Successfully copied {filename} to namenode container")
        result2 = subprocess.run(cmd2, shell=True, capture_output=True, text=True)
        if result2.returncode == 0:
            print(f"  ✓ Successfully copied {filename} to HDFS")
        else:
            print(f"  ✗ Failed to copy {filename} to HDFS:\n{result2.stderr}")
    else:
        print(f"  ✗ Failed to copy {filename} to namenode container:\n{result1.stderr}")

print("\nVerifying files in HDFS...")
result = subprocess.run("docker exec namenode hdfs dfs -ls /data", shell=True, capture_output=True, text=True)
print(result.stdout)

print("\nDone! You can now access these files in your Spark application using:")
print('df = spark.read.csv("hdfs://namenode:9000/data/your_file.csv")') 