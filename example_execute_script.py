# -*- coding: utf-8 -*-
##
##  example_execute_script.py
##  spt_lsm_autorapid_process
##
##  Created by Alan D. Snow and Scott D. Christensen.
##  Copyright © 2016 Alan D. Snow and Scott D. Christensen. All rights reserved.
##  License: BSD-3 Clause
"""
Instructions: Create a copy of this file named execute_script.py. Customize the paths to match the directories on your system.
"""
#------------------------------------------------------------------------------
#main process
#------------------------------------------------------------------------------
if __name__ == "__main__":
    from datetime import datetime
    from lsm_rapid_process import run_lsm_rapid_process
    run_lsm_rapid_process(
                          rapid_executable_location='/home/alan/autorapid/rapid/src/rapid',
                          rapid_io_files_location='/home/alan/autorapid/rapid-io',
                          lsm_data_location='/home/alan/autorapid/era_data',
                          simulation_start_datetime=datetime(1980, 1, 1),
                          simulation_end_datetime=datetime(2014, 12, 31),
                          generate_return_periods_file=False,
                          )