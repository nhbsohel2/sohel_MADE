#!/bin/bash

# Print the current directory
echo "Current directory: $(pwd)"

# List files in the current directory and the project directory for debugging
echo "Files in current directory:"
ls -l

echo "Files in project directory:"
ls -l project

# Activate the virtual environment
source venv/bin/activate

# Run the test script
python3 project/test.py
