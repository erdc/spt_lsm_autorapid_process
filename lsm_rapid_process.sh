#!/bin/sh
$HOME/scripts/spt_lsm_autorapid_process/execute_script.py 1> $HOME/logs/$(date +%y%m%d%H%M%S).log 2>&1
