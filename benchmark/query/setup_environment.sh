#!/bin/bash

rm -r envs 2>/dev/null
mkdir envs

# Shared environment for all three libraries
python3.10 -m venv envs/query && \
    source envs/query/bin/activate && \
    pip install -U pip && \
    pip install -r requirements.txt && \
    deactivate
