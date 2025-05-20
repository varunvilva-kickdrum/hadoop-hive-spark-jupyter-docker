#!/usr/bin/env python
# HDFS Dataset Upload Script
# This script copies local datasets to HDFS for distributed processing

import os
import glob
import subprocess

print("Checking datasets directory...")
datasets_path = "/home/jovyan/datasets"
files = glob.glob(f"{datasets_path}/*")

if not files:
    print("No files found in datasets directory!")
    exit(1)

print(f"Found {len(files)} files in datasets directory:")
for file in files:
    print(f"  - {os.path.basename(file)}")

print("\nCopying files to HDFS /data directory...")
for file in files:
    filename = os.path.basename(file)
    print(f"Copying {filename} to HDFS...")
    
    # Use subprocess to run HDFS commands
    cmd = f"hdfs dfs -put -f {file} /data/{filename}"
    result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
    
    if result.returncode == 0:
        print(f"  ✓ Successfully copied {filename} to HDFS")
    else:
        print(f"  ✗ Failed to copy {filename}:\n{result.stderr}")

print("\nVerifying files in HDFS...")
result = subprocess.run("hdfs dfs -ls /data", shell=True, capture_output=True, text=True)
print(result.stdout)

print("\nDone! You can now access these files in your Spark application using:")
print('df = spark.read.csv("hdfs://namenode:9000/data/your_file.csv")') 