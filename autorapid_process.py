#!/usr/bin/env python
from glob import glob
import os

#local imports
from imports.helper_functions import (case_insensitive_file_search, 
                                      get_valid_watershed_list,
                                      get_watershed_subbasin_from_folder)

#package imports
from AutoRoutePy.run_autoroute_multicore import run_autoroute_multicore 
from AutoRoutePy.post_process import rename_shapefiles
from spt_dataset_manager.dataset_manager import GeoServerDatasetManager

#----------------------------------------------------------------------------------------
# MAIN PROCESS
#----------------------------------------------------------------------------------------
def run_autorapid_process(autoroute_executable_location, #location of AutoRoute executable
                          autoroute_io_files_location, #path to AutoRoute input/outpuf directory
                          rapid_io_files_location, #path to AutoRoute input/outpuf directory
                          condor_log_directory,
                          return_period_list=['return_period_20', 'return_period_10', 'return_period_2'],
                          geoserver_url='',
                          geoserver_username='',
                          geoserver_password='',
                          app_instance_id=''     
                          ):
    """
    This it the main AutoRoute-RAPID process
    """
    valid_return_period_list = ['return_period_20', 'return_period_10', 'return_period_2']

    #validate return period list
    for return_period in return_period_list:
        if return_period not in valid_return_period_list:
            raise Exception("%s not a valid return period index ...")

    #loop through input watershed folders
    autoroute_input_folder = os.path.join(autoroute_io_files_location, "input")
    autoroute_output_folder = os.path.join(autoroute_io_files_location, "output")
    autoroute_input_directories = get_valid_watershed_list(autoroute_input_folder)

    for return_period in return_period_list:
        #initialize HTCondor Directory
        condor_init_dir = os.path.join(condor_log_directory, return_period)
        try:
            os.makedirs(condor_init_dir)
        except OSError:
            pass
    
        print "Running AutoRoute process for:", return_period
        #run autorapid for each watershed
        autoroute_watershed_jobs = {}
        for autoroute_input_directory in autoroute_input_directories:
            watershed, subbasin = get_watershed_subbasin_from_folder(autoroute_input_directory)
            
            #RAPID file paths
            master_watershed_rapid_input_directory = os.path.join(rapid_io_files_location, "input", autoroute_input_directory)
                                                                   
            if not os.path.exists(master_watershed_rapid_input_directory):
                print "AutoRoute watershed", autoroute_input_directory, "not in RAPID IO folder. Skipping ..."
                continue
            try:
                return_period_file=case_insensitive_file_search(master_watershed_rapid_input_directory, r'return_periods.nc')
            except Exception:
                print "AutoRoute watershed", autoroute_input_directory, "missing return period file. Skipping ..."
                continue
            
            #setup the output location
            master_watershed_autoroute_output_directory = os.path.join(autoroute_output_folder,
                                                                       autoroute_input_directory, 
                                                                       return_period)
            try:
                os.makedirs(master_watershed_autoroute_output_directory)
            except OSError:
                pass
            #loop through sub-directories
            autoroute_watershed_directory_path = os.path.join(autoroute_input_folder, autoroute_input_directory)        
            autoroute_watershed_jobs[autoroute_input_directory] = run_autoroute_multicore(autoroute_executable_location, #location of AutoRoute executable
                                                                                          autoroute_input_directory=autoroute_watershed_directory_path, #path to AutoRoute input directory
                                                                                          autoroute_output_directory=master_watershed_autoroute_output_directory, #path to AutoRoute output directory
                                                                                          return_period=return_period, # return period name in return period file
                                                                                          return_period_file=return_period_file, # return period file generated from RAPID historical run
                                                                                          mode="multiprocess", #multiprocess or htcondor
                                                                                          condor_log_directory=condor_init_dir,
                                                                                          #delete_flood_raster=True, #delete flood raster generated
                                                                                          #generate_floodmap_shapefile=True, #generate a flood map shapefile
                                                                                          #wait_for_all_processes_to_finish=True
                                                                                          )
        """
        #TODO                    
        autoroute_watershed_jobs[autoroute_input_directory]['jobs'].append(job)
            geoserver_manager = None
            if geoserver_url and geoserver_username and geoserver_password and app_instance_id:
                try:
                    geoserver_manager = GeoServerDatasetManager(geoserver_url, 
                                                                geoserver_username, 
                                                                geoserver_password, 
                                                                app_instance_id)
                except Exception as ex:
                    print ex
                    print "Skipping geoserver upload ..."
                    geoserver_manager = None
                    pass 
            #wait for jobs to finish by watershed
            for autoroute_input_directory, autoroute_watershed_job in autoroute_watershed_jobs.iteritems():
                #time stamped layer name
                geoserver_resource_name = "%s-floodmap-%s" % (autoroute_input_directory, return_period)
                #geoserver_resource_name = "%s-floodmap" % (autoroute_input_directory)
                upload_shapefile = os.path.join(master_watershed_autoroute_output_directory, "%s%s" % (geoserver_resource_name, ".shp"))
                for autoroute_job in autoroute_watershed_job['jobs']:
                    autoroute_job.wait()
                if len(autoroute_watershed_job['jobs'])> 1:
                    # merge files
                    merge_shapefiles(autoroute_watershed_job['output_folder'], 
                                     upload_shapefile, 
                                     reproject=True,
                                     remove_old=True)
                elif len(autoroute_watershed_job['jobs'])== 1:
                    #rename files
                    rename_shapefiles(master_watershed_autoroute_output_directory, 
                                      os.path.splitext(upload_shapefile)[0], 
                                      autoroute_input_directory)
    
                #upload to GeoServer
                if geoserver_manager:
                    print "Uploading", upload_shapefile, "to GeoServer as", geoserver_resource_name
                    shapefile_basename = os.path.splitext(upload_shapefile)[0]
                    #remove past layer if exists
                    geoserver_manager.purge_remove_geoserver_layer(geoserver_manager.get_layer_name(geoserver_resource_name))
                    #upload updated layer
                    shapefile_list = glob("%s*" % shapefile_basename)
                    geoserver_manager.upload_shapefile(geoserver_resource_name, 
                                                       shapefile_list)
                                                       
                    #remove local shapefile when done
                    for shapefile in shapefile_list:
                        try:
                            os.remove(shapefile)
                        except OSError:
                            pass
                    #remove local directories when done
                    try:
                        os.remove(os.path.join(master_watershed_autoroute_output_directory))
                    except OSError:
                        pass
                    #TODO: Upload to CKAN for historical floodmaps?
        """

if __name__ == "__main__":
    run_autorapid_process(autoroute_executable_location='/home/alan/work/scripts/AutoRouteGDAL/source_code/autoroute',
                          autoroute_io_files_location='/home/alan/work/autoroute-io',
                          rapid_io_files_location='/home/alan/work/rapid-io',
                          condor_log_directory='/home/alan/work/condor',
                          return_period_list=['return_period_20']
                          )