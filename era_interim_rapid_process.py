#!/usr/bin/env python
import datetime
import multiprocessing
from netCDF4 import Dataset
import os
from RAPIDpy.rapid import RAPID
import re

#local imports
from imports.CreateInflowFileFromERAInterimRunoff import CreateInflowFileFromERAInterimRunoff
from imports.ftp_ecmwf_download import download_all_ftp
from imports.generate_return_periods import generate_return_periods

#----------------------------------------------------------------------------------------
#HELPER FUNCTIONS
#----------------------------------------------------------------------------------------
def clean_logs(main_log_directory):
    """
    This removed logs older than one week old
    """
    date_today = datetime.datetime.utcnow()
    week_timedelta = datetime.timedelta(7)
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
#MAIN PROCESSES
#------------------------------------------------------------------------------
def downscale_erai(args):
    """
    prepare all ECMWF files for rapid
    """
    watershed = args[0]
    subbasin = args[1]
    era_interim_file = args[2]
    erai_file_index = args[3]
    erai_weight_table_file = args[4]
    grid_type = args[5]
    rapid_inflow_file = args[6]


    time_start_all = datetime.datetime.utcnow()

    #prepare ECMWF file for RAPID
    print "ERAI downscaling for:", watershed, subbasin, \
          erai_file_index, rapid_inflow_file

    print "Converting ERAI inflow"
    RAPIDinflowECMWF_tool = CreateInflowFileFromERAInterimRunoff()

    RAPIDinflowECMWF_tool.execute(nc_file=era_interim_file,
                                  index=erai_file_index,
                                  in_weight_table=erai_weight_table_file,
                                  out_nc=rapid_inflow_file,
                                  grid_type=grid_type,
                                  )

    time_finish_ecmwf = datetime.datetime.utcnow()
    print "Time to convert ECMWF: %s" % (time_finish_ecmwf-time_start_all)

def run_era_interim_rapid_process(rapid_executable_location, 
                                  rapid_io_files_location, 
                                  era_interim_data_location, 
                                  main_log_directory,
                                  simulation_start_datetime,
                                  simulation_end_datetime=datetime.datetime.utcnow(),
                                  download_era_interim=False,
                                  ensemble_list=[None],
                                  generate_return_periods_file=False,
                                  cygwin_bin_location=""
                                  ):
    """
    This it the main process
    """
    time_begin_all = datetime.datetime.utcnow()

    #clean up old log files
    clean_logs(main_log_directory)


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


    for ensemble in ensemble_list:
        ensemble_file_ending = ".nc"
        if ensemble != None:
            ensemble_file_ending = "_{}.nc".format(ensemble)
        finished_looking = False
        #get list of files
        era_interim_file_list = []
        for subdir, dirs, files in os.walk(era_interim_folder):
            for erai_file in files:
                if erai_file.endswith(ensemble_file_ending):
                    match = re.search(r'\d{8}', erai_file)
                    file_date = datetime.datetime.strptime(match.group(0), "%Y%m%d")
                    if file_date > simulation_end_datetime:
                        print file_date
                        finished_looking = True
                        break
                    if file_date >= simulation_start_datetime:
                        era_interim_file_list.append(os.path.join(subdir, erai_file))
                if finished_looking:
                    break
            if finished_looking:
                break
        print era_interim_file_list[0]
        print era_interim_file_list[-1]
        
        era_interim_file_list = sorted(era_interim_file_list)
        
        #check to see what kind of file we are dealing with
        era_example_file = Dataset(era_interim_file_list[0])
        
        var_list = era_example_file.variables.keys()
        lat_key = 'lat'
        if 'latitude' in var_list:
            lat_key = 'latitude'
        lat_dimension = len(era_example_file.variables[lat_key][:])
        lon_key = 'lon'
        if 'longitude' in var_list:
            lon_key = 'longitude'
        lon_dimension = len(era_example_file.variables[lon_key][:])
    
        #identify grid type 
        out_file_ending = ensemble_file_ending
            
        weight_file_name = ''
        grid_type = ''
        if lat_dimension == 361 and lon_dimension == 720:
            #A) ERA Interim Low Res (T255)
            #Downloaded as 0.5 degree grid
            # dimensions:
            #	 longitude = 720 ;
            #	 latitude = 361 ;
            weight_file_name = r'weight_era_t255\.csv'
            grid_type = 't255'

        elif lat_dimension == 512 and lon_dimension == 1024:
            #B) ERA Interim High Res (T511)
            # dimensions:
            #  lon = 1024 ;
            #  lat = 512 ;
            weight_file_name = r'weight_era_t511\.csv'
            grid_type = 't511'
        elif lat_dimension == 161 and lon_dimension == 320:
            #C) ERA 20CM (T159) - 3hr - 10 ensembles
            #Downloaded as 1.125 degree grid
            # dimensions:
            #  longitude = 320 ;
            #  latitude = 161 ;    
            weight_file_name = r'weight_era_t159\.csv'
            grid_type = 't159'
        else:
            era_example_file.close()
            raise Exception("Unsupported grid size.")
            
        file_size_time = len(era_example_file.variables['time'][:])
        time_step = 0
        if file_size_time == 1:
            time_step = 24*3600 #daily
        elif file_size_time == 8:
            time_step = 3*3600 #3 hourly
        else:
            era_example_file.close()
            raise Exception("Unsupported time step.")
        era_example_file.close()
    
        out_file_ending = "{0}_{1}{2}".format(grid_type, time_step/3600, out_file_ending)
        #set up RAPID manager
        rapid_manager = RAPID(rapid_executable_location=rapid_executable_location,
                              cygwin_bin_location=cygwin_bin_location,
                              #use_all_processors=True,                          
                              ZS_TauR=time_step, #duration of routing procedure (time step of runoff data)
                              ZS_dtR=15*60, #internal routing time step
                              ZS_TauM=len(era_interim_file_list)*24*3600, #total simulation time 
                              ZS_dtM=time_step #input time step 
                             )
    
        #run ERA Interim processes
        for rapid_input_directory in rapid_input_directories:
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
                                                    'm3_riv_bas_erai_{}'.format(out_file_ending))
                                                    
            erai_weight_table_file = case_insensitive_file_search(master_watershed_input_directory,
                                                                  weight_file_name)
                                                                  
    
            RAPIDinflowECMWF_tool = CreateInflowFileFromERAInterimRunoff()
            
            RAPIDinflowECMWF_tool.generateOutputInflowFile(out_nc=master_rapid_runoff_file,
                                                           in_weight_table=erai_weight_table_file,
                                                           tot_size_time=file_size_time*len(era_interim_file_list),
                                                          )
    
            job_combinations = []
            for erai_file_index, erai_file in enumerate(era_interim_file_list):
                job_combinations.append((watershed.lower(), 
                                         subbasin.lower(),
                                         erai_file, 
                                         erai_file_index,
                                         erai_weight_table_file,
                                         grid_type,
                                         master_rapid_runoff_file))
                """
                downscale_erai((watershed.lower(), 
                                         subbasin.lower(),
                                         erai_file, 
                                         erai_file_index,
                                         erai_weight_table_file,
                                         grid_type,
                                         master_rapid_runoff_file))
                """
                
            pool = multiprocessing.Pool()
            #chunksize=1 makes it so there is only one task per process
            pool.imap(downscale_erai, 
                      job_combinations,
                      chunksize=1)
            pool.close()
            pool.join()
    
            #run RAPID for the watershed
            era_rapid_output_file = os.path.join(master_watershed_output_directory,
                                                 'Qout_erai_{}'.format(out_file_ending))
                                                 
            rapid_manager.update_parameters(rapid_connect_file=case_insensitive_file_search(master_watershed_input_directory,
                                                                                         r'rapid_connect\.csv'),
                                            Vlat_file=master_rapid_runoff_file,
                                            riv_bas_id_file=case_insensitive_file_search(master_watershed_input_directory,
                                                                                         r'riv_bas_id\.csv'),
                                            k_file=case_insensitive_file_search(master_watershed_input_directory,
                                                                                r'k\.csv'),
                                            x_file=case_insensitive_file_search(master_watershed_input_directory,
                                                                                r'x\.csv'),
                                            Qout_file=era_rapid_output_file
                                            )
        
            comid_lat_lon_z_file = case_insensitive_file_search(master_watershed_input_directory,
                                                                r'comid_lat_lon_z\.csv')
    
            rapid_manager.update_reach_number_data()
            rapid_manager.run()
            rapid_manager.make_output_CF_compliant(simulation_start_datetime=simulation_start_datetime,
                                                   comid_lat_lon_z_file=comid_lat_lon_z_file,
                                                   project_name="ERA Interim Historical flows by US Army ERDC")
    
            #generate return periods
            if generate_return_periods_file:
                return_periods_file = os.path.join(master_watershed_output_directory, 'return_periods_{}'.format(out_file_ending))
                #assume storm has 3 day length, so step is file_size_time*3
                generate_return_periods(era_rapid_output_file, return_periods_file, int(len(era_interim_file_list)/365), file_size_time*3)
    
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
        rapid_executable_location='/Users/rdchlads/autorapid/rapid/src/rapid',
        rapid_io_files_location='/Users/rdchlads/autorapid/rapid-io',
        era_interim_data_location="/Users/rdchlads/autorapid/era_data/erai3_1980to2014",
        main_log_directory='/Users/rdchlads/autorapid/era_logs/',
        simulation_start_datetime=datetime.datetime(1980, 1, 1),
        #simulation_end_datetime=datetime.datetime(1980, 1, 3),
        #ensemble_list=range(10),
        download_era_interim=False,
        generate_return_periods_file=False,
    )
