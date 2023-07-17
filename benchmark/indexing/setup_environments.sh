#!/bin/bash

rm -r envs 2>/dev/null
mkdir envs

# Official pybids
python3.10 -m venv envs/pybids && \
    source envs/pybids/bin/activate && \
    pip install -U pip && \
    pip install pybids==0.16.0 && \
    deactivate

# ANCP-bids
python3.10 -m venv envs/ancpbids && \
    source envs/ancpbids/bin/activate && \
    pip install -U pip && \
    pip install ancpbids==0.2.2 && \
    deactivate

# bids2table
python3.10 -m venv envs/bids2table && \
    source envs/bids2table/bin/activate && \
    pip install -U pip && \
    pip install -U git+https://github.com/cmi-dair/elbow.git@3117427 && \
    pip install -U git+https://github.com/cmi-dair/bids2table.git@b7b1658 && \
    deactivate
