import sys
from pathlib import Path

# Add the project root to sys.path so 'src' can always be imported
root_dir = Path(__file__).resolve().parent
if str(root_dir) not in sys.path:
    sys.path.insert(0, str(root_dir))