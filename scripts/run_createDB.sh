#!/bin/bash

set -e

echo "Creating database and tables..."

python create_database.py

echo "✅ Database and tables created successfully!"