# spt_lsm_autorapid_process
Code to use to prepare input data for RAPID from Land Surface Models (LSM) such as ECMWF ERA Interim Data or NASA GLDAS/NLDAS/LIS data.

##Step 1: Install RAPID and RAPIDpy
See: https://github.com/erdc-cm/RAPIDpy

##Step 2: Install AutoRoute and AutoRoutePy
See: https://github.com/erdc-cm/AutoRoutePy

##Step 3: Install Python Libraries
```
$ sudo su
$ pip install requests_toolbelt tethys_dataset_services 
$ exit
```

##Step 4: Download the source code
```
$ cd /path/to/your/scripts/
$ git clone https://github.com/erdc-cm/spt_lsm_autorapid_process.git
$ cd spt_lsm_autorapid_process
$ git submodule init
$ git submodule update
```
##Step 5: Create folders for RAPID input and for downloading ECMWF
In this instance:
```
$ cd /mnt/sgeadmin/
$ mkdir rapid era_interim_data logs
$ mkdir rapid/input
```
##Step 6: Change the locations in the files
Create  *run.py* and add this code (note: you will need to change these variables for your instance):
```python
#------------------------------------------------------------------------------
#main process
#------------------------------------------------------------------------------
if __name__ == "__main__":
    from datetime import datetime
    from era_interim_rapid_process import run_era_interim_rapid_process
    run_era_interim_rapid_process(
        rapid_executable_location='/home/alan/work/rapid/src/rapid',
        rapid_io_files_location='/home/alan/work/rapid-io',
        era_interim_data_location="/home/alan/work/era_interim",
        main_log_directory='/home/alan/work/era_logs/',
        simulation_start_datetime=datetime(1980,1,1),
        download_era_interim=False,
        generate_return_periods_file=True,
    )
```
Go into *era_interim_rapid_process.sh* and change make sure the path locations and variables are correct for your instance.

##Step 7: Make sure permissions are correct for these files and any directories the script will use

Example:
```
$ chmod 554 era_interim_rapid_process.py
$ chmod 554 era_interim_rapid_process.sh
```
##Step 8: Add RAPID files to the work/rapid/input directory
Make sure the directory is in the format [watershed name]-[subbasin name]
with lowercase letters, numbers, and underscores only. No spaces!


Example:
```
$ ls /rapid/input
nfie_texas_gulf_region-huc_2_12
$ ls /rapid/input/nfie_texas_gulf_region-huc_2_12
comid_lat_lon_z.csv
k.csv
rapid_connect.csv
riv_bas_id.csv
weight_era_interim.csv
weight_high_res.csv
weight_low_res.csv
x.csv
```

#Troubleshooting
If you see this error:
ImportError: No module named packages.urllib3.poolmanager
```
$ pip install pip --upgrade
```
Restart your terminal
```
$ pip install requests --upgrade
```
