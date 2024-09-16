import sys
import os

# Get the absolute path of the current script
current_dir = os.getcwd()

# Get the parent directory of 'tests'
project_root = os.path.join(current_dir,'code')

# Add the project root to the Python path
sys.path.insert(0, project_root)
