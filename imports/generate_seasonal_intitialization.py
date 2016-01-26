# -*- coding: utf-8 -*-
##
##  generate_seasonal_intitialization.py
##  spt_erai_autorapid_process
##
##  Created by Alan D. Snow 2016.
##  Copyright © 2016 Alan D Snow. All rights reserved.
##

import datetime
from netCDF4 import Dataset
import numpy as np
from pytz import utc
from RAPIDpy.helper_functions import csv_to_list

#------------------------------------------------------------------------------
# Functions
#------------------------------------------------------------------------------
def generate_seasonal_intitialization(rapid_historical_streamflow_file,
                                      rapid_connect_file,
                                      rapid_sreamflow_initialization_file,
                                      datetime_start_initialization):
    """
        This function loops through a CF compliant rapid streamflow
        file to produce estimates for current streamflow based on
        the seasonal average over the data within the historical streamflow
        file.
    """
    print "Generating seasonal average qinit file from qout file ..."
    #get information from datasets
    data_nc = Dataset(rapid_historical_streamflow_file, mode="r")
    
    dims = data_nc.dimensions
    id_dim_name = 'COMID'
    if 'rivid' in dims:
        id_dim_name = 'rivid'
    riv_bas_id_array = data_nc.variables[id_dim_name][:]

    if 'time' in dims:
        
        print "Determining dates with streamflows of interest ..."
        datetime_min = datetime_start_initialization - datetime.timedelta(3)
        datetime_max = datetime_start_initialization + datetime.timedelta(3)
        
        time_indices = []
        for idx, t in enumerate(data_nc.variables['time'][:]):
            var_time = datetime.datetime.fromtimestamp(t, tz=utc)
            #check if date within range of season
            if var_time.month >= datetime_min.month and var_time.month <= datetime_max.month:
                if var_time.month > datetime_min.month:
                    if var_time.day < datetime_max.day:
                        time_indices.append(idx)
                elif var_time.day >= datetime_min.day and var_time.day < datetime_max.day:
                    time_indices.append(idx)

        print "Extracting data ..."
        qout_dimensions = data_nc.variables['Qout'].dimensions
        if qout_dimensions[1].lower() == 'time' and qout_dimensions[0].lower() == id_dim_name.lower():
            #the data is CF compliant and has time=0 added to output
            data_values = data_nc.variables['Qout'][:,time_indices]
        else:
            data_nc.close()
            raise Exception( "Invalid RAPID historical streamflow file %s" % rapid_historical_streamflow_file)
        data_nc.close()

        print "Reordering data..."
        rapid_connect_array = csv_to_list(rapid_connect_file)
        stream_id_array = np.array([row[0] for row in rapid_connect_array], dtype=np.int)
        init_flows_array = np.zeros(len(rapid_connect_array))
        for riv_bas_index, riv_bas_id in enumerate(riv_bas_id_array):
            try:
                data_index = np.where(stream_id_array==riv_bas_id)[0][0]
                init_flows_array[data_index] = np.mean(data_values[riv_bas_index])
            except Exception:
                raise Exception ('riv bas id %s not found in connectivity list.' % riv_bas_id)

        print "Writing to file ..."
        with open(rapid_sreamflow_initialization_file, 'wb') as qinit_out:
            for init_flow in init_flows_array:
                qinit_out.write('{}\n'.format(init_flow))

        print "Initialization Complete!"

    else:
        raise Exception("ERROR: File must be CF 1.6 compliant with time dimension ...")

"""
if __name__ == "__main__":
    generate_seasonal_intitialization(rapid_historical_streamflow_file='/Users/rdchlads/autorapid/rapid-io/output/camp_lejeune-new_river/Qout_erai_t511_3hr_19800101to20141231.nc',
                                      rapid_connect_file='/Users/rdchlads/autorapid/rapid-io/input/camp_lejeune-new_river/rapid_connect.csv',
                                      rapid_sreamflow_initialization_file='/Users/rdchlads/autorapid/rapid-io/input/camp_lejeune-new_river/Qinit_seasonal_may.csv',
                                      datetime_start_initialization=datetime.datetime(2015,5,15))
"""