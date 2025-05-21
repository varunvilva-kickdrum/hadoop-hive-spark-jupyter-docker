#!/bin/bash

# Wait for namenode to be ready
echo "Waiting for namenode to be ready..."
sleep 30

# Create /data directory in HDFS
echo "Creating /data directory in HDFS..."
docker exec namenode hdfs dfs -mkdir -p /data

# Function to get file hash
get_file_hash() {
    local file="$1"
    if [ -f "$file" ]; then
        md5sum "$file" | cut -d' ' -f1
    fi
}

# Function to get HDFS file hash
get_hdfs_hash() {
    local hdfs_path="$1"
    docker exec namenode hdfs dfs -cat "$hdfs_path" 2>/dev/null | md5sum | cut -d' ' -f1
}

# Function to check if file exists in HDFS
hdfs_file_exists() {
    local hdfs_path="$1"
    docker exec namenode hdfs dfs -test -e "$hdfs_path" 2>/dev/null
    return $?
}

# Function to copy files and directories recursively
copy_to_hdfs() {
    local source="$1"
    local target="$2"
    
    if [ -d "$source" ]; then
        # Create directory in HDFS
        echo "Creating directory $target in HDFS..."
        docker exec namenode hdfs dfs -mkdir -p "$target"
        
        # Copy all contents of the directory
        for item in "$source"/*; do
            if [ -e "$item" ]; then
                local item_name=$(basename "$item")
                copy_to_hdfs "$item" "$target/$item_name"
            fi
        done
    elif [ -f "$source" ]; then
        # Check if file exists in HDFS
        if ! hdfs_file_exists "$target"; then
            echo "Copying $(basename "$source") to HDFS (file was missing)..."
            docker cp "$source" namenode:/tmp/
            docker exec namenode hdfs dfs -put -f /tmp/"$(basename "$source")" "$target"
            docker exec namenode rm /tmp/"$(basename "$source")"
        else
            # Check if file needs to be updated
            local source_hash=$(get_file_hash "$source")
            local hdfs_hash=$(get_hdfs_hash "$target")
            
            if [ "$source_hash" != "$hdfs_hash" ]; then
                echo "Copying $(basename "$source") to HDFS (file was modified)..."
                docker cp "$source" namenode:/tmp/
                docker exec namenode hdfs dfs -put -f /tmp/"$(basename "$source")" "$target"
                docker exec namenode rm /tmp/"$(basename "$source")"
            else
                echo "Skipping $(basename "$source") - already up to date"
            fi
        fi
    fi
}

# Copy all contents from datasets directory to HDFS
echo "Copying datasets to HDFS..."
for item in datasets/*; do
    if [ -e "$item" ]; then
        item_name=$(basename "$item")
        copy_to_hdfs "$item" "/data/$item_name"
    fi
done

echo "HDFS initialization complete!" 