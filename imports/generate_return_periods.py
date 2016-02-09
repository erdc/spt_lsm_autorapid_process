# -*- coding: utf-8 -*-
##
##  generate_return_periods.py
##  spt_lsm_autorapid_process
##
##  Created by Alan D. Snow and Scott D. Christensen.
##  Copyright © 2015-2016 Alan D Snow and Scott D. Christensen. All rights reserved.
##  License: BSD-3 Clause

import netCDF4 as nc
import numpy as np
from RAPIDpy.dataset import RAPIDDataset

def generate_return_periods(era_interim_file, return_period_file, num_years, step=7):
    """
    Create warning points from era interim data and ECMWD prediction data

    """

    print "Extracting ERA Interim Data ..."
    #get ERA Interim Data Analyzed
    with RAPIDDataset(era_interim_file) as qout_nc_file:
        era_interim_comids = qout_nc_file.get_river_id_array()
        comid_list_length = qout_nc_file.size_river_id
        era_interim_lat_data = qout_nc_file.qout_nc.variables['lat'][:]
        era_interim_lon_data = qout_nc_file.qout_nc.variables['lon'][:]

        print "Setting up Return Periods File ..."

        return_period_nc = nc.Dataset(return_period_file, 'w', format='NETCDF3_CLASSIC')

        
        id_len = comid_list_length

        return_period_nc.createDimension('rivid', id_len)

        timeSeries_var = return_period_nc.createVariable('rivid', 'i4', ('rivid',))
        timeSeries_var.long_name = (
            'Unique NHDPlus COMID identifier for each river reach feature')

        max_flow_var = return_period_nc.createVariable('max_flow', 'f8', ('rivid',))
        return_period_20_var = return_period_nc.createVariable('return_period_20', 'f8', ('rivid',))
        return_period_10_var = return_period_nc.createVariable('return_period_10', 'f8', ('rivid',))
        return_period_2_var = return_period_nc.createVariable('return_period_2', 'f8', ('rivid',))

        lat_var = return_period_nc.createVariable('lat', 'f8', ('rivid',),
                                       fill_value=-9999.0)
        lat_var.long_name = 'latitude'
        lat_var.standard_name = 'latitude'
        lat_var.units = 'degrees_north'
        lat_var.axis = 'Y'

        lon_var = return_period_nc.createVariable('lon', 'f8', ('rivid',),
                                       fill_value=-9999.0)
        lon_var.long_name = 'longitude'
        lon_var.standard_name = 'longitude'
        lon_var.units = 'degrees_east'
        lon_var.axis = 'X'

        return_period_nc.variables['rivid'][:] = era_interim_comids
        return_period_nc.variables['lat'][:] = era_interim_lat_data
        return_period_nc.variables['lon'][:] = era_interim_lon_data

        print "Generating Return Periods ..."

        for comid_index, comid in enumerate(era_interim_comids):

            era_flow_data = qout_nc_file.get_qout_index(comid_index)
            filtered_era_flow_data = []
            for step_index in range(0,len(era_flow_data),step):
                flows_slice = era_flow_data[step_index:step_index + step]
                filtered_era_flow_data.append(max(flows_slice))
            sorted_era_flow_data = np.sort(filtered_era_flow_data)[:num_years:-1]

            rp_index_20 = round((num_years + 1)/20.0, 0)
            rp_index_10 = round((num_years + 1)/10.0, 0)
            rp_index_2 = round((num_years + 1)/2.0, 0)
            
            max_flow_var[comid_index] = sorted_era_flow_data[0]
            return_period_20_var[comid_index] = sorted_era_flow_data[rp_index_20]
            return_period_10_var[comid_index] = sorted_era_flow_data[rp_index_10]
            return_period_2_var[comid_index] = sorted_era_flow_data[rp_index_2]

        return_period_nc.close()