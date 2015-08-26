#!/usr/bin/env python
"""
Instructions: Create a copy of this file named execute_script.py. Customize the paths to match the directories on your system.
"""
from era_interim_rapid_process import run_era_interim_rapid_process

run_era_interim_rapid_process(
        rapid_executable_location='/home/nfieera/work/rapid/src/rapid',
        rapid_io_files_location='/home/nfieera/rapid-io',
        ecmwf_forecast_location ="/home/nfieera/ecmwf",
        era_interim_data_location="/home/nfieera/era_data",
        condor_log_directory='/home/nfieera/condor/',
        main_log_directory='/home/nfieera/logs/',
        data_store_url='http://ciwckan.chpc.utah.edu',
        data_store_api_key='8dcc1b34-0e09-4ddc-8356-df4a24e5be87',
        app_instance_id='53ab91374b7155b0a64f0efcd706854e',
        sync_rapid_input_with_ckan=False,
        download_era_interim=False,
        download_ecmwf=False,
        upload_output_to_ckan=False,
        generate_return_periods_file = True,
    )
