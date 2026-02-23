#!/bin/bash
# Bearden Tax Intake — launcher script
source ~/.zprofile 2>/dev/null
cd "$(dirname "$0")"
PORT=5050 python3 app.py &
sleep 2
open "http://localhost:5050"
wait
