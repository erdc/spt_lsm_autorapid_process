#!/usr/bin/env python
from condorpy import Job as CJob
from condorpy import Templates as tmplt
import csv
import datetime
from glob import glob
import netCDF4 as NET
import numpy as np
import os
from shutil import rmtree

#local imports
from imports.ftp_ecmwf_download import download_all_ftp
from sfpt_dataset_manager.dataset_manager import (ECMWFRAPIDDatasetManager,
                                                  RAPIDInputDatasetManager)
from generate_return_periods import generate_return_periods

#----------------------------------------------------------------------------------------
# FUNCTIONS
#----------------------------------------------------------------------------------------
def clean_logs(condor_log_directory, main_log_directory):
    """
    This removed logs older than one week old
    """
    date_today = datetime.datetime.utcnow()
    week_timedelta = datetime.timedelta(7)
    #clean up condor logs
    condor_dirs = [d for d in os.listdir(condor_log_directory) if os.path.isdir(os.path.join(condor_log_directory, d))]
    for condor_dir in condor_dirs:
        dir_datetime = datetime.datetime.strptime(condor_dir, "%Y%m%d")
        if (date_today-dir_datetime > week_timedelta):
            rmtree(os.path.join(condor_log_directory, condor_dir))

    #clean up log files
    main_log_files = [f for f in os.listdir(main_log_directory) if not os.path.isdir(os.path.join(main_log_directory, f))]
    for main_log_file in main_log_files:
        try:
            log_datetime = datetime.datetime.strptime(main_log_file, "%y%m%d%H%M%S.log")
            if (date_today-log_datetime > week_timedelta):
                os.remove(os.path.join(main_log_directory, main_log_file))
        except Exception as ex:
            print ex
            pass

def find_current_rapid_output(forecast_directory, watershed, subbasin):
    """
    Finds the most current files output from RAPID
    """
    if os.path.exists(forecast_directory):
        basin_files = glob(os.path.join(forecast_directory,"Qout_%s_%s_*.nc" % (watershed, subbasin)))
        if len(basin_files) >0:
            return basin_files
    #there are none found
    return None

def csv_to_list(csv_file, delimiter=','):
    """
    Reads in a CSV file and returns the contents as list,
    where every row is stored as a sublist, and each element
    in the sublist represents 1 cell in the table.

    """
    with open(csv_file, 'rb') as csv_con:
        reader = csv.reader(csv_con, delimiter=delimiter)
        return list(reader)

def get_comids_in_netcdf_file(reach_id_list, prediction_file):
    """
    Gets the subset comid_index_list, reordered_comid_list from the netcdf file
    """
    data_nc = NET.Dataset(prediction_file, mode="r")
    com_ids = data_nc.variables['COMID'][:]
    data_nc.close()
    try:
        #get where comids are in netcdf file
        netcdf_reach_indices_list = np.where(np.in1d(com_ids, reach_id_list))[0]
    except Exception as ex:
        print ex

    return netcdf_reach_indices_list, com_ids[netcdf_reach_indices_list]

def compute_initial_rapid_flows(prediction_files, input_directory, forecast_date_timestep):
    """
    Gets mean of all 52 ensembles 12-hrs in future and prints to csv as initial flow
    Qinit_file (BS_opt_Qinit)
    The assumptions are that Qinit_file is ordered the same way as rapid_connect_file
    if subset of list, add zero where there is no flow
    """
    #remove old init files for this basin
    past_init_flow_files = glob(os.path.join(input_directory, 'Qinit_*.csv'))
    for past_init_flow_file in past_init_flow_files:
        try:
            os.remove(past_init_flow_file)
        except:
            pass
    current_forecast_date = datetime.datetime.strptime(forecast_date_timestep[:11],"%Y%m%d.%H").strftime("%Y%m%dt%H")
    init_file_location = os.path.join(input_directory,'Qinit_%s.csv' % current_forecast_date)
    #check to see if exists and only perform operation once
    if prediction_files:
        #get list of COMIDS
        connectivity_file = csv_to_list(os.path.join(input_directory,'rapid_connect.csv'))
        comid_list = np.array([int(row[0]) for row in connectivity_file])


        print "Finding COMID indices ..."
        comid_index_list, reordered_comid_list = get_comids_in_netcdf_file(comid_list, prediction_files[0])
        print "Extracting data ..."
        reach_prediciton_array = np.zeros((len(comid_list),len(prediction_files),1))
        #get information from datasets
        for file_index, prediction_file in enumerate(prediction_files):
            try:
                #Get hydrograph data from ECMWF Ensemble
                data_nc = NET.Dataset(prediction_file, mode="r")
                qout_dimensions = data_nc.variables['Qout'].dimensions
                if qout_dimensions[0].lower() == 'time' and qout_dimensions[1].lower() == 'comid':
                    data_values_2d_array = data_nc.variables['Qout'][2,comid_index_list].transpose()
                elif qout_dimensions[1].lower() == 'time' and qout_dimensions[0].lower() == 'comid':
                    data_values_2d_array = data_nc.variables['Qout'][comid_index_list,2]
                else:
                    print "Invalid ECMWF forecast file", prediction_file
                    data_nc.close()
                    continue
                data_nc.close()
                #organize the data
                for comid_index, comid in enumerate(reordered_comid_list):
                    reach_prediciton_array[comid_index][file_index] = data_values_2d_array[comid_index]
            except Exception, e:
                print e
                #pass

        print "Analyzing data ..."
        output_data = []
        for comid in comid_list:
            try:
                #get where comids are in netcdf file
                comid_index = np.where(reordered_comid_list==comid)[0][0]
            except Exception as ex:
                #comid not found in list. Adding zero init flow ...
                output_data.append([0])
                pass
                continue

            #get mean of series as init flow
            output_data.append([np.mean(reach_prediciton_array[comid_index])])

        print "Writing output ..."
        with open(init_file_location, 'wb') as outfile:
            writer = csv.writer(outfile)
            writer.writerows(output_data)
    else:
        print "No current forecasts found. Skipping ..."

def run_era_interim_rapid_process(rapid_executable_location, rapid_io_files_location, ecmwf_forecast_location,
                                  era_interim_data_location, condor_log_directory, main_log_directory, data_store_url,
                                  data_store_api_key, app_instance_id, sync_rapid_input_with_ckan, download_ecmwf,
                                  download_era_interim, upload_output_to_ckan, generate_return_periods_file):
    """
    This it the main process
    """
    time_begin_all = datetime.datetime.utcnow()
    date_string = time_begin_all.strftime('%Y%m%d')
    #date_string = datetime.datetime(2015,2,3).strftime('%Y%m%d')
    rapid_scripts_location = os.path.dirname(os.path.realpath(__file__))

    if sync_rapid_input_with_ckan and app_instance_id and data_store_url and data_store_api_key:
        #sync with data store
        ri_manager = RAPIDInputDatasetManager(data_store_url,
                                              data_store_api_key,
                                              'ecmwf',
                                              app_instance_id)
        ri_manager.sync_dataset(os.path.join(rapid_io_files_location,'input'))

    #clean up old log files
    clean_logs(condor_log_directory, main_log_directory)

    #initialize HTCondor Directory
    condor_init_dir = os.path.join(condor_log_directory, date_string)
    try:
        os.makedirs(condor_init_dir)
    except OSError:
        pass

    #get list of correclty formatted rapid input directories in rapid directory
    rapid_input_directories = []
    for directory in os.listdir(os.path.join(rapid_io_files_location,'input')):
        if os.path.isdir(os.path.join(rapid_io_files_location,'input', directory)) \
            and len(directory.split("-")) == 2:
            rapid_input_directories.append(directory)
        else:
            print directory, "incorrectly formatted. Skipping ..."

    era_interim_folder = era_interim_data_location
    if download_era_interim:
        #download historical ERA data
        era_interim_folders = download_all_ftp(era_interim_data_location,
           'erai_runoff_1980to20*.tar.gz.tar')
        era_interim_folder = era_interim_folders[0]

    if upload_output_to_ckan and data_store_url and data_store_api_key:
        #init data manager for CKAN
        data_manager = ECMWFRAPIDDatasetManager(data_store_url,
                                                data_store_api_key)

    #run ERA Interim processes
    iteration = 0
    job_list = []
    job_info_list = []
    for rapid_input_directory in rapid_input_directories:
        input_folder_split = rapid_input_directory.split("-")
        watershed = input_folder_split[0]
        subbasin = input_folder_split[1]
        master_watershed_input_directory = os.path.join(rapid_io_files_location, "input", rapid_input_directory)
        master_watershed_outflow_directory = os.path.join(rapid_io_files_location, 'output',
                                                          rapid_input_directory)
        try:
            os.makedirs(master_watershed_outflow_directory)
        except OSError:
            pass
        #get basin names
        interim_folder_basename = os.path.basename(era_interim_folder)
	print era_interim_folder, interim_folder_basename
        outflow_file_name = 'Qout_%s.nc' % interim_folder_basename
        node_rapid_outflow_file = outflow_file_name
        master_rapid_outflow_file = os.path.join(master_watershed_outflow_directory, outflow_file_name)

        #create job to downscale forecasts for watershed
        job = CJob('job_%s_%s_%s' % (interim_folder_basename, watershed, iteration), tmplt.vanilla_transfer_files)
        job.set('executable',os.path.join(rapid_scripts_location,'compute_ecmwf_rapid.py'))
        job.set('transfer_input_files', "%s, %s" % (master_watershed_input_directory, rapid_scripts_location))
        job.set('initialdir', condor_init_dir)
        job.set('arguments', '%s %s %s %s %s' % (watershed.lower(), subbasin.lower(), rapid_executable_location,
                                                    era_interim_folder, ecmwf_forecast_location))
        job.set('transfer_output_remaps', "\"%s = %s\"" % (node_rapid_outflow_file, master_rapid_outflow_file))
        job.submit()
        job_list.append(job)
        job_info_list.append({'watershed' : watershed,
                              'subbasin' : subbasin,
                              'outflow_file_name' : master_rapid_outflow_file,
                              'master_watershed_outflow_directory': master_watershed_outflow_directory,
                              })
        iteration += 1

    #wait for jobs to finish then upload files
    for index, job in enumerate(job_list):
        job.wait()

	#generate return periods
	if generate_return_periods_file:
	    job_info = job_info_list[index]
	    watershed_output_dir = job_info['master_watershed_outflow_directory']
	    erai_output_file = job_info['outflow_file_name']
	    return_periods_file = os.path.join(watershed_output_dir, 'return_periods.nc')
	    generate_return_periods(erai_output_file, return_periods_file)

        """
        #upload file when done
        if upload_output_to_ckan and data_store_url and data_store_api_key:
            job_info = job_info_list[index]
            print "Uploading", job_info['watershed'], job_info['subbasin'], \
                job_info['forecast_date_timestep'], job_info['ensemble_number']
            #Upload to CKAN
            data_manager.initialize_run_ecmwf(job_info['watershed'], job_info['subbasin'], job_info['forecast_date_timestep'])
            data_manager.update_resource_ensemble_number(job_info['ensemble_number'])
            #upload file
            try:
                #tar.gz file
                output_tar_file =  os.path.join(job_info['master_watershed_outflow_directory'], "%s.tar.gz" % data_manager.resource_name)
                if not os.path.exists(output_tar_file):
                    with tarfile.open(output_tar_file, "w:gz") as tar:
                        tar.add(job_info['outflow_file_name'], arcname=os.path.basename(job_info['outflow_file_name']))
                return_data = data_manager.upload_resource(output_tar_file)
                if not return_data['success']:
                    print return_data
                    print "Attempting to upload again"
                    return_data = data_manager.upload_resource(output_tar_file)
                    if not return_data['success']:
                        print return_data
                    else:
                        print "Upload success"
                else:
                    print "Upload success"
            except Exception, e:
                print e
                pass
            #remove tar.gz file
            os.remove(output_tar_file)

    #initialize flows for next run
    if initialize_flows:
        #create new init flow files
        for rapid_input_directory in rapid_input_directories:
            input_directory = os.path.join(rapid_io_files_location, 'input', rapid_input_directory)
            path_to_watershed_files = os.path.join(rapid_io_files_location, 'output', rapid_input_directory)
            forecast_date_timestep = None
            #finds the current output from downscaled ECMWF forecasts
            if os.path.exists(path_to_watershed_files):
                forecast_date_timestep = sorted([d for d in os.listdir(path_to_watershed_files) \
                                        if os.path.isdir(os.path.join(path_to_watershed_files, d))],
                                        reverse=True)[0]

            if forecast_date_timestep:
                #loop through all the rapid_namelist files in directory
                forecast_directory = os.path.join(path_to_watershed_files, forecast_date_timestep)
                input_folder_split = rapid_input_directory.split("-")
                watershed = input_folder_split[0]
                subbasin = input_folder_split[1]
                if initialize_flows:
                    print "Initializing flows for", watershed, subbasin, "from", forecast_date_timestep
                    basin_files = find_current_rapid_output(forecast_directory, watershed, subbasin)
                    try:
                        compute_initial_rapid_flows(basin_files, input_directory, forecast_date_timestep)
                    except Exception, ex:
                        print ex
                        pass
    if upload_output_to_ckan and data_store_url and data_store_api_key:
        #delete local datasets
        for job_info in job_info_list:
            try:
                rmtree(job_info['master_watershed_outflow_directory'])
            except OSError:
                pass
        #delete watershed folder if empty
        for item in os.listdir(os.path.join(rapid_io_files_location, 'output')):
            try:
                os.rmdir(os.path.join(rapid_io_files_location, 'output', item))
            except OSError:
                pass
    """

    #print info to user
    time_end = datetime.datetime.utcnow()
    print "Time Begin All: " + str(time_begin_all)
    print "Time Finish All: " + str(time_end)
    print "TOTAL TIME: "  + str(time_end-time_begin_all)

#------------------------------------------------------------------------------
#main process
#------------------------------------------------------------------------------
if __name__ == "__main__":
    run_era_interim_rapid_process(
        rapid_executable_location='/home/alan/work/rapid/src/rapid',
        rapid_io_files_location='/home/alan/work/rapid-io',
        ecmwf_forecast_location ="/home/alan/work/ecmwf",
        era_interim_data_location="/home/alan/work/era_interim",
        condor_log_directory='/home/alan/work/condor/',
        main_log_directory='/home/alan/work/era_logs/',
        data_store_url='http://ciwckan.chpc.utah.edu',
        data_store_api_key='8dcc1b34-0e09-4ddc-8356-df4a24e5be87',
        app_instance_id='53ab91374b7155b0a64f0efcd706854e',
        sync_rapid_input_with_ckan=False,
        download_era_interim=False,
        download_ecmwf=False,
        upload_output_to_ckan=False,
        generate_return_periods_file=True,
    )
