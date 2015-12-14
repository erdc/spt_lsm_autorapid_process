#!/usr/bin/env python
import datetime
import multiprocessing
from netCDF4 import Dataset
import os
from RAPIDpy.rapid import RAPID
import re

#local imports
from imports.CreateInflowFileFromERAInterimRunoff import CreateInflowFileFromERAInterimRunoff
from imports.CreateInflowFileFromLDASRunoff import CreateInflowFileFromLDASRunoff
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

def partition(lst, n):
    """
    Divide list into n equal parts
    """
    q, r = divmod(len(lst), n)
    indices = [q*i + min(i,r) for i in xrange(n+1)]
    return [lst[indices[i]:indices[i+1]] for i in xrange(n)], [range(indices[i],indices[i+1]) for i in xrange(n)]
        
#------------------------------------------------------------------------------
#MAIN PROCESSES
#------------------------------------------------------------------------------
def generate_inflows_from_runoff(args):
    """
    prepare runoff inflow file for rapid
    """
    watershed = args[0]
    subbasin = args[1]
    runoff_file_list = args[2]
    file_index_list = args[3]
    erai_weight_table_file = args[4]
    grid_type = args[5]
    rapid_inflow_file = args[6]
    RAPIDinflowECMWF_tool = args[7]

    time_start_all = datetime.datetime.utcnow()

    #prepare ECMWF file for RAPID
    print "Runoff downscaling for:", watershed, subbasin
    print "Index:", file_index_list[0], "to", file_index_list[-1]
    print "File(s):", runoff_file_list[0], "to", runoff_file_list[-1]
          
    if not isinstance(runoff_file_list, list): 
        runoff_file_list = [runoff_file_list]
    else:
        runoff_file_list = runoff_file_list
       
    print "Converting inflow"
    RAPIDinflowECMWF_tool.execute(nc_file_list=runoff_file_list,
                                  index_list=file_index_list,
                                  in_weight_table=erai_weight_table_file,
                                  out_nc=rapid_inflow_file,
                                  grid_type=grid_type,
                                  )

    time_finish_ecmwf = datetime.datetime.utcnow()
    print "Time to convert inflows: %s" % (time_finish_ecmwf-time_start_all)

def run_era_interim_rapid_process(rapid_executable_location, 
                                  rapid_io_files_location, 
                                  era_interim_data_location, 
                                  main_log_directory,
                                  simulation_start_datetime,
                                  simulation_end_datetime=datetime.datetime.utcnow(),
                                  download_era_interim=False,
                                  ensemble_list=[None],
                                  generate_return_periods_file=False,
                                  ftp_host="",
                                  ftp_login="",
                                  ftp_passwd="",
                                  ftp_directory="",
                                  cygwin_bin_location=""
                                  ):
    """
    This it the main process
    """
    time_begin_all = datetime.datetime.utcnow()

    #clean up old log files
    clean_logs(main_log_directory)

    NUM_CPUS = multiprocessing.cpu_count()
    
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
                                               'erai_runoff_1980to20*.tar.gz.tar', 
                                               ftp_host, ftp_login, ftp_passwd, ftp_directory)
        era_interim_folder = era_interim_folders[0]


    for ensemble in ensemble_list:
        ensemble_file_ending = ".nc"
        if ensemble != None:
            ensemble_file_ending = "_{0}.nc".format(ensemble)
        #get list of files
        era_interim_file_list = []
        for subdir, dirs, files in os.walk(era_interim_folder):
            for erai_file in files:
                if erai_file.endswith(ensemble_file_ending):
                    era_interim_file_list.append(os.path.join(subdir, erai_file))
        
        era_interim_file_list_subset = []
        
        for erai_file in sorted(era_interim_file_list):
            match = re.search(r'\d{8}', erai_file)
            file_date = datetime.datetime.strptime(match.group(0), "%Y%m%d")
            if file_date > simulation_end_datetime:
                break
                print file_date
            if file_date >= simulation_start_datetime:
                era_interim_file_list_subset.append(os.path.join(subdir, erai_file))
        print era_interim_file_list_subset[0]
        actual_simulation_start_datetime = datetime.datetime.strptime(re.search(r'\d{8}', era_interim_file_list_subset[0]).group(0), "%Y%m%d")
        print era_interim_file_list_subset[-1]
        actual_simulation_end_datetime = datetime.datetime.strptime(re.search(r'\d{8}', era_interim_file_list_subset[-1]).group(0), "%Y%m%d")
        
        era_interim_file_list = sorted(era_interim_file_list_subset)
        
        #check to see what kind of file we are dealing with
        era_example_file = Dataset(era_interim_file_list[0])
        
    
        
        #INDENTIFY LAT/LON DIMENSIONS
        dim_list = era_example_file.dimensions.keys()

        latitude_dim = "lat"
        if 'latitude' in dim_list:
            latitude_dim = 'latitude'
        elif 'g0_lat_0' in dim_list:
            #GLDAS/NLDAS MOSAIC
            latitude_dim = 'g0_lat_0'
        elif 'lat_110' in dim_list:
            #NLDAS NOAH/VIC
            latitude_dim = 'lat_110'
        elif 'north_south' in dim_list:
            #LIS
            latitude_dim = 'north_south'
        
        longitude_dim = "lon"
        if 'longitude' in dim_list:
            longitude_dim = 'longitude'
        elif 'g0_lon_1' in dim_list:
            #GLDAS/NLDAS MOSAIC
            longitude_dim = 'g0_lon_1'
        elif 'lon_110' in dim_list:
            #NLDAS NOAH/VIC
            longitude_dim = 'lon_110'
        elif 'east_west' in dim_list:
            #LIS
            longitude_dim = 'east_west'
        
        lat_dim_size = len(era_example_file.dimensions[latitude_dim])
        lon_dim_size = len(era_example_file.dimensions[longitude_dim])

        #IDENTIFY VARIABLES
        var_list = era_example_file.variables.keys()
        
        latitude_var="lat"
        if 'latitude' in var_list:
            latitude_var = 'latitude'
        elif 'g0_lat_0' in var_list:
            latitude_var = 'g0_lat_0'
        elif 'lat_110' in var_list:
            latitude_var = 'lat_110'
        
        longitude_var="lon"
        if 'longitude' in var_list:
            longitude_var = 'longitude'
        elif 'g0_lon_1' in var_list:
            longitude_var = 'g0_lon_1'
        elif 'lon_110' in var_list:
            longitude_var = 'lon_110'
        
        surface_runoff_var=""
        subsurface_runoff_var=""
        for var in var_list:
            if var.startswith("SSRUN"):
                #NLDAS/GLDAS
                surface_runoff_var = var
            elif var.startswith("BGRUN"):
                #NLDAS/GLDAS
                subsurface_runoff_var = var
            elif var == "Qs_inst":
                #LIS
                surface_runoff_var = var
            elif var == "Qsb_inst":
                #LIS
                subsurface_runoff_var = var
            elif var.lower() == "ro":
                surface_runoff_var = var
            


        #IDENTIFY GRID TYPE & TIME STEP
        if 'time' in var_list:
            file_size_time = len(era_example_file.variables['time'][:])
        else:
            print "Assuming time dimension is 1"
            file_size_time = 1

        out_file_ending = "{0}to{1}{2}".format(actual_simulation_start_datetime.strftime("%Y%m%d"), 
                                               actual_simulation_end_datetime.strftime("%Y%m%d"), 
                                               ensemble_file_ending)
            
        weight_file_name = ''
        grid_type = ''
        model_name = ''
        time_step = 0
        description = ""
        RAPIDinflowECMWF_tool = None
        total_num_time_steps = 0
        institution = ""
        try:
            institution = era_example_file.getncattr("institution")
        except AttributeError:
            pass
        
        if institution == "European Centre for Medium-Range Weather Forecasts" \
            or surface_runoff_var.lower() == "ro":
            #these are the ECMWF models
            if lat_dim_size == 361 and lon_dim_size == 720:
                print "Runoff file identified as ERA Interim Low Res (T255) GRID"
                #A) ERA Interim Low Res (T255)
                #Downloaded as 0.5 degree grid
                # dimensions:
                #	 longitude = 720 ;
                #	 latitude = 361 ;
                description = "ERA Interim (T255 Grid)"
                model_name = "erai"
                weight_file_name = r'weight_era_t255\.csv'
                grid_type = 't255'

            elif lat_dim_size == 512 and lon_dim_size == 1024:
                print "Runoff file identified as ERA Interim High Res (T511) GRID"
                #B) ERA Interim High Res (T511)
                # dimensions:
                #  lon = 1024 ;
                #  lat = 512 ;
                description = "ERA Interim (T511 Grid)"
                weight_file_name = r'weight_era_t511\.csv'
                model_name = "erai"
                grid_type = 't511'
            elif lat_dim_size == 161 and lon_dim_size == 320:
                print "Runoff file identified as ERA 20CM (T159) GRID"
                #C) ERA 20CM (T159) - 3hr - 10 ensembles
                #Downloaded as 1.125 degree grid
                # dimensions:
                #  longitude = 320 ;
                #  latitude = 161 ;    
                description = "ERA 20CM (T159 Grid)"
                weight_file_name = r'weight_era_t159\.csv'
                model_name = "era_20cm"
                grid_type = 't159'
            else:
                era_example_file.close()
                raise Exception("Unsupported grid size.")

            #time units are in hours
            if file_size_time == 1:
                time_step = 24*3600 #daily
                description += " Daily Runoff"
            elif file_size_time == 8:
                time_step = 3*3600 #3 hourly
                description += " 3 Hourly Runoff"
            else:
                era_example_file.close()
                raise Exception("Unsupported ECMWF time step.")

            total_num_time_steps=file_size_time*len(era_interim_file_list)
            RAPIDinflowECMWF_tool = CreateInflowFileFromERAInterimRunoff()                 
        elif institution == "NASA GSFC":
            print "Runoff file identified as LIS GRID"
            #this is the LIS model
            weight_file_name = r'weight_lis\.csv'
            grid_type = 'lis'
            description = "NASA GFC LIS Hourly Runoff"
            model_name = "nasa"
            #time units are in minutes
            if file_size_time == 1:
                time_step = 1*3600 #hourly
            else:
                era_example_file.close()
                raise Exception("Unsupported LIS time step.")

            total_num_time_steps=file_size_time*len(era_interim_file_list)

            RAPIDinflowECMWF_tool = CreateInflowFileFromLDASRunoff(latitude_dim,
                                                                   longitude_dim,
                                                                   latitude_var,
                                                                   longitude_var,
                                                                   surface_runoff_var,
                                                                   subsurface_runoff_var,
                                                                   time_step)
               
        elif surface_runoff_var.startswith("SSRUN") \
            and subsurface_runoff_var.startswith("BGRUN"):

            model_name = "nasa"
            if lat_dim_size == 600 and lon_dim_size == 1440:
                print "Runoff file identified as GLDAS GRID"
                #GLDAS NC FILE
                #dimensions:
                #    g0_lat_0 = 600 ;
                #    g0_lon_1 = 1440 ;
                #variables
                #SSRUN_GDS0_SFC_ave1h (surface), BGRUN_GDS0_SFC_ave1h (subsurface)
                # or
                #SSRUNsfc_GDS0_SFC_ave1h (surface), BGRUNsfc_GDS0_SFC_ave1h (subsurface)
                description = "GLDAS 3 Hourly Runoff"
                weight_file_name = r'weight_gldas\.csv'
                grid_type = 'gldas'
 
                if file_size_time == 1:
                    time_step = 3*3600 #3 hourly
                else:
                    era_example_file.close()
                    raise Exception("Unsupported GLDAS time step.")
                
                total_num_time_steps=file_size_time*len(era_interim_file_list)

            elif lat_dim_size <= 224 and lon_dim_size <= 464:
                print "Runoff file identified as NLDAS GRID"
                #NLDAS MOSAIC FILE
                #dimensions:
                #    g0_lat_0 = 224 ;
                #    g0_lon_1 = 464 ;
                #NLDAS NOAH/VIC FILE
                #dimensions:
                #    lat_110 = 224 ;
                #    lon_110 = 464 ;

                description = "NLDAS Hourly Runoff"
                weight_file_name = r'weight_nldas\.csv'
                grid_type = 'nldas'

                if file_size_time == 1:
                    #time_step = 1*3600 #hourly
                    time_step = 3*3600 #3 hourly
                else:
                    era_example_file.close()
                    raise Exception("Unsupported NLDAS time step.")
                
                if file_size_time*len(era_interim_file_list) % 3 != 0:
                    era_example_file.close()
                    raise Exception("Number of files needs to be divisible by 3")
                    
                total_num_time_steps=file_size_time*len(era_interim_file_list)/3
            else:
                era_example_file.close()
                raise Exception("Unsupported runoff grid.")

            RAPIDinflowECMWF_tool = CreateInflowFileFromLDASRunoff(latitude_dim,
                                                                   longitude_dim,
                                                                   latitude_var,
                                                                   longitude_var,
                                                                   surface_runoff_var,
                                                                   subsurface_runoff_var,
                                                                   time_step)
        else:
            era_example_file.close()
            raise Exception("Unsupported runoff grid.")
        
        era_example_file.close()
    
        out_file_ending = "{0}_{1}_{2}hr_{3}".format(model_name, grid_type, time_step/3600, out_file_ending)
        #set up RAPID manager
        rapid_manager = RAPID(rapid_executable_location=rapid_executable_location,
                              cygwin_bin_location=cygwin_bin_location,
                              use_all_processors=True,                          
                              ZS_TauR=time_step, #duration of routing procedure (time step of runoff data)
                              ZS_dtR=15*60, #internal routing time step
                              ZS_TauM=total_num_time_steps*time_step, #total simulation time 
                              ZS_dtM=86400 #RAPID recommended internal time step (1 day)
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
                                                    'm3_riv_bas_{0}'.format(out_file_ending))
            
            erai_weight_table_file = case_insensitive_file_search(master_watershed_input_directory,
                                                                  weight_file_name)

            RAPIDinflowECMWF_tool.generateOutputInflowFile(out_nc=master_rapid_runoff_file,
                                                           in_weight_table=erai_weight_table_file,
                                                           tot_size_time=total_num_time_steps,
                                                           )
    
            job_combinations = []
            past_inflow_index_list_length = 0
            if grid_type == 'nldas':
                print "Grouping nldas in threes"
                #group files in three
                era_interim_file_list = [era_interim_file_list[nldas_index:nldas_index+3] for nldas_index in range(0, len(era_interim_file_list), 3)]

            partition_list, partition_index_list = partition(era_interim_file_list, NUM_CPUS)
            for loop_index, cpu_grouped_file_list in enumerate(partition_list):
                job_combinations.append((watershed.lower(), 
                                         subbasin.lower(),
                                         cpu_grouped_file_list, 
                                         partition_index_list[loop_index],
                                         erai_weight_table_file,
                                         grid_type,
                                         master_rapid_runoff_file,
                                         RAPIDinflowECMWF_tool))
                #generate_inflows_from_runoff((watershed.lower(), 
                #                              subbasin.lower(),
                #                              cpu_grouped_file_list, 
                #                              inflow_index_list,
                #                              erai_weight_table_file,
                #                              grid_type,
                #                              master_rapid_runoff_file,
                #                              RAPIDinflowECMWF_tool))
            pool = multiprocessing.Pool()
            #chunksize=1 makes it so there is only one task per cpu
            pool.imap(generate_inflows_from_runoff, 
                      job_combinations,
                      chunksize=1)
            pool.close()
            pool.join()
            
            #run RAPID for the watershed
            era_rapid_output_file = os.path.join(master_watershed_output_directory,
                                                 'Qout_{0}'.format(out_file_ending))
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
            #rapid_manager.generate_namelist_file("rapid_namelist_{}".format(out_file_ending[:-3]))
            rapid_manager.run()
            rapid_manager.make_output_CF_compliant(simulation_start_datetime=actual_simulation_start_datetime,
                                                   comid_lat_lon_z_file=comid_lat_lon_z_file,
                                                   project_name="{0} Based Historical flows by US Army ERDC".format(description))
            
            #generate return periods
            if generate_return_periods_file:
                return_periods_file = os.path.join(master_watershed_output_directory, 'return_periods_{0}'.format(out_file_ending))
                #assume storm has 3 day length
                storm_time_step_length = int(3*3600/time_step*24)
                generate_return_periods(era_rapid_output_file, return_periods_file, int(len(era_interim_file_list)/365), storm_time_step_length)
    #print info to user
    time_end = datetime.datetime.utcnow()
    print "Time Begin All: " + str(time_begin_all)
    print "Time Finish All: " + str(time_end)
    print "TOTAL TIME: "  + str(time_end-time_begin_all)
