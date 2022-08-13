#!/bin/bash

python -m venv venv
source "./venv/bin/activate"

python -m pip install --upgrade pip
python -m pip install -r requirements.txt

echo "Environment setup complete"