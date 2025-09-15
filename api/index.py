#!/usr/bin/env python3
"""
Vercel deployment entry point for Market Analytics
"""

import os
import sys

# Add the project root to the path
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, project_root)

# Import the main app
from simple_app import app

# Export the FastAPI app for Vercel
app = app