# spt_erai_autorapid_process
Code to use to prepare input data for RAPID from ECMWF forecast using HTCondor

Note: For steps 1-2, use the *install_rapid_htcondor.sh* at your own risk.

##Step 1: Install RAPID
**For Ubuntu:**
```
$ apt-get install gfortran g++
```
Follow the instructions on page 10-14: http://rapid-hub.org/docs/RAPID_Azure.pdf.

Here is a script to download prereqs: http://rapid-hub.org/data/rapid_install_prereqs.sh.gz
##Step 2: Install AutoRoute
For instructions, go to: https://github.com/erdc-cm/AutoRoute/tree/gdal

##Step 3: Install netCDF4-python
###Install on Ubuntu:
```
$ apt-get install python-dev zlib1g-dev libhdf5-serial-dev libnetcdf-dev
$ sudo su
$ pip install numpy netCDF4
$ exit
```
###Install on Redhat:
*Note: this tool was desgined and tested in Ubuntu*
```
$ yum install netcdf4-python
$ yum install hdf5-devel
$ yum install netcdf-devel
$ pip install numpy netCDF4
```
##Step 4: Install Other Python Libraries
```
$ sudo su
$ pip install requests_toolbelt tethys_dataset_services RAPIDpy
$ exit
```

##Step 5: Download the source code
```
$ cd /path/to/your/scripts/
$ git clone https://github.com/erdc-cm/spt_erai_autorapid_process.git
$ cd spt_erai_autorapid_process
$ git submodule init
$ git submodule update
```
##Step 6: Create folders for RAPID input and for downloading ECMWF
In this instance:
```
$ cd /mnt/sgeadmin/
$ mkdir rapid era_interim_data logs
$ mkdir rapid/input
```
##Step 7: Change the locations in the files
Go into *era_interim_rapid_process.py* and change these variables for your instance:
```python
#------------------------------------------------------------------------------
#main process
#------------------------------------------------------------------------------
if __name__ == "__main__":
    run_era_interim_rapid_process(
        rapid_executable_location='/home/alan/work/rapid/src/rapid',
        rapid_io_files_location='/home/alan/work/rapid-io',
        era_interim_data_location="/home/alan/work/era_interim",
        main_log_directory='/home/alan/work/era_logs/',
        download_era_interim=False,
        generate_return_periods_file=True,
    )
```
Go into *rapid_process.sh* and change make sure the path locations and variables are correct for your instance.

Go into *ftp_ecmwf_download.py* and add password and login information:
```python
    #init FTPClient
    ftp_client = PyFTPclient(host='ftp.ecmwf.int',
                             login='',
                             passwd='',
                             directory='tcyc')
```

##Step 8: Make sure permissions are correct for these files and any directories the script will use

Example:
```
$ chmod 554 era_interim_rapid_process.py
$ chmod 554 era_interim_rapid_process.sh
```
##Step 9: Add RAPID files to the work/rapid/input directory
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
