#!/usr/bin/env python
from condorpy import Job as CJob
from condorpy import Templates as tmplt
import datetime
import os
import re
from shutil import rmtree

#package imports
from RAPIDpy.rapid import RAPID

#local imports
from imports.CreateInflowFileFromERAInterimRunoff import CreateInflowFileFromERAInterimRunoff
from imports.ftp_ecmwf_download import download_all_ftp
from imports.generate_return_periods import generate_return_periods

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

def case_insensitive_file_search(directory, pattern):
    """
    Looks for file with patter with case insensitive search
    """
    try:
        return os.path.join(directory,
                            [filename for filename in os.listdir(directory) \
                             if re.search(pattern, filename, re.IGNORECASE)][0])
    except IndexError:
        print pattern, "not found"
        raise
        
        
#------------------------------------------------------------------------------
#MAIN PROCESS
#------------------------------------------------------------------------------
def run_era_interim_rapid_process(rapid_executable_location, 
                                  rapid_io_files_location, 
                                  era_interim_data_location, 
                                  condor_log_directory, 
                                  main_log_directory, 
                                  download_era_interim=False, 
                                  generate_return_periods_file=False):
    """
    This it the main process
    """
    time_begin_all = datetime.datetime.utcnow()
    date_string = time_begin_all.strftime('%Y%m%d')
    #date_string = datetime.datetime(2015,2,3).strftime('%Y%m%d')
    scripts_location = os.path.dirname(os.path.realpath(__file__))

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


    #get list of files
    era_interim_file_list = []
    for subdir, dirs, files in os.walk(era_interim_folder):
        for erai_file in files:
            if erai_file.endswith('.nc'):
                era_interim_file_list.append(os.path.join(subdir, erai_file))

    era_interim_file_list = sorted(era_interim_file_list)
    print len(era_interim_file_list)
    rapid_manager = RAPID(rapid_executable_location=rapid_executable_location,
                          use_all_processors=True,                          
                          ZS_TauR = 24*3600, #duration of routing procedure (time step of runoff data)
                          ZS_dtR = 15*60, #internal routing time step
                          ZS_TauM = len(era_interim_file_list)*24*3600, #total simulation time 
                          ZS_dtM = 24*3600 #input time step 
                         )

    #run ERA Interim processes
    iteration = 0
    for rapid_input_directory in rapid_input_directories:
        job_list = []
        job_info_list = []
        input_folder_split = rapid_input_directory.split("-")
        watershed = input_folder_split[0]
        subbasin = input_folder_split[1]
        master_watershed_input_directory = os.path.join(rapid_io_files_location, "input", rapid_input_directory)
        master_watershed_output_directory = os.path.join(rapid_io_files_location, 'output',
                                                          rapid_input_directory)
        try:
            os.makedirs(master_watershed_output_directory)
        except OSError:
            pass

        #create inflow to dump data into
        master_rapid_runoff_file = os.path.join(master_watershed_output_directory, 
                                                'm3_riv_bas_erai.nc')
                                                
        erai_weight_table_file = case_insensitive_file_search(master_watershed_input_directory,
                                                              r'weight_era_interim.csv')

        RAPIDinflowECMWF_tool = CreateInflowFileFromERAInterimRunoff()
        
        RAPIDinflowECMWF_tool.generateOutputInflowFile(out_nc=master_rapid_runoff_file,
                                                       in_weight_table=erai_weight_table_file,
                                                       size_time=len(era_interim_file_list)+1
                                                      )
        os.chmod(master_rapid_runoff_file, 0777)
        wait_index = 0
        for erai_file_index, erai_file in enumerate(era_interim_file_list):
            if erai_file_index > 4500:
                job_list[wait_index].wait()
                wait_index += 1
            #create job to downscale forecasts for watershed into inflow file
            job = CJob('job_%s_%s_%s' % (os.path.basename(era_interim_folder), watershed, iteration), 
                       tmplt.vanilla_transfer_files)
            job.set('executable',os.path.join(scripts_location,'htcondor_erai_process.py'))
            job.set('transfer_input_files', "%s" % (scripts_location))
            job.set('initialdir', condor_init_dir)
            job.set('arguments', '%s %s %s %s %s %s' % (watershed.lower(), subbasin.lower(),
                                                        erai_file, erai_file_index,
                                                        erai_weight_table_file,
                                                        master_rapid_runoff_file))
            job.submit()
            job_list.append(job)
            job_info_list.append({
                                  'master_watershed_input_directory' : master_watershed_input_directory, 
                                  'master_watershed_output_directory': master_watershed_output_directory,
                                  'master_rapid_runoff_file' : master_rapid_runoff_file,
                                  })
            iteration += 1
        
            

        #wait for jobs to finish then upload files
        for index, job in enumerate(job_list):
            job.wait()
            #run RAPID for the watershed
            rapid_input_directory = job_info_list[index]['master_watershed_input_directory']
            rapid_output_directory = job_info_list[index]['master_watershed_input_directory']
            master_rapid_outflow_file = os.path.join(rapid_output_directory,'Qout_erai.nc')
    
            rapid_manager.update_parameters(rapid_connect_file=case_insensitive_file_search(rapid_input_directory,
                                                                                         r'rapid_connect\.csv'),
                                            Vlat_file=job_info_list[index]['master_rapid_runoff_file'],
                                            riv_bas_id_file=case_insensitive_file_search(rapid_input_directory,
                                                                                         r'riv_bas_id\.csv'),
                                            k_file=case_insensitive_file_search(rapid_input_directory,
                                                                                r'k\.csv'),
                                            x_file=case_insensitive_file_search(rapid_input_directory,
                                                                                r'x\.csv'),
                                            Qout_file=master_rapid_outflow_file
                                            )
        
            comid_lat_lon_z_file = case_insensitive_file_search(rapid_input_directory,
                                                                r'comid_lat_lon_z.csv')

            rapid_manager.update_reach_number_data()
            rapid_manager.run()
            rapid_manager.make_output_CF_compliant(simulation_start_datetime=datetime.datetime(1980, 1, 1),
                                                   comid_lat_lon_z_file=comid_lat_lon_z_file,
                                                   project_name="ERA Interim Historical flows by US Army ERDC")

	#generate return periods
	if generate_return_periods_file:
	    job_info = job_info_list[index]
	    watershed_output_dir = job_info['master_watershed_outflow_directory']
	    erai_output_file = job_info['outflow_file_name']
	    return_periods_file = os.path.join(watershed_output_dir, 'return_periods.nc')
	    generate_return_periods(erai_output_file, return_periods_file)


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
        era_interim_data_location="/home/alan/work/era_interim",
        condor_log_directory='/home/alan/work/erai_condor/',
        main_log_directory='/home/alan/work/era_logs/',
        download_era_interim=False,
        generate_return_periods_file=True,
    )
