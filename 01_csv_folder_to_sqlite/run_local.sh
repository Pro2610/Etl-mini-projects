#!/usr/bin/env bash
set -e
python -m pip install -r ../../00_common/requirements.txt
python flows/flow.py
