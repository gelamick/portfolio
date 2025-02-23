#!/bin/sh

# Set mandatory environment vars
export DS_API_PARAMS="./params/params_test.yml"
export DS_API_TEST_TO_RUN="all"
export DS_API_TEST_LOG="./logs/output.log"

# Run the script
python ./scripts/test_NYT.py
