#!/bin/sh
$HOME/scripts/erfp_era_interim_process/execute_script.py 1> $HOME/logs/$(date +%y%m%d%H%M%S).log 2>&1
