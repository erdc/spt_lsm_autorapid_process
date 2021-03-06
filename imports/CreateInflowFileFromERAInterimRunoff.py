# -*- coding: utf-8 -*-
##
##  CreateInflowFileFromERAInterimRunoff.py
##  spt_lsm_autorapid_process
##
##  Created by Alan D. Snow (adapted from CreateInflowFileFromECMWFRunoff.py).
##  Copyright © 2015-2016 Alan D Snow. All rights reserved.
##  License: BSD-3 Clause

import csv
import netCDF4 as NET
import numpy as NUM
import os

class CreateInflowFileFromERAInterimRunoff(object):
    def __init__(self):
        """Define the tool (tool name is the name of the class)."""
        self.label = "Create Inflow File From ERA Interim Runoff"
        self.description = ("Creates RAPID NetCDF input of water inflow "
                            "based on ERA Interim runoff results and "
                            "previously created weight table.")
        self.header_wt = ['StreamID', 'area_sqm', 'lon_index', 'lat_index', 'npoints']
        self.dims_oi = [['lon', 'lat', 'time'], ['longitude', 'latitude', 'time']]
        self.vars_oi = [["lon", "lat", "time", "RO"], ['longitude', 'latitude', 'time', 'ro']]
        self.length_time = {"Daily": 1, "3-Hourly": 8}
        self.errorMessages = ["Missing Variable 'time'",
                              "Incorrect dimensions in the input ERA Interim runoff file.",
                              "Incorrect variables in the input ERA Interim runoff file.",
                              "Incorrect time variable in the input ERA Interim runoff file",
                              "Incorrect number of columns in the weight table",
                              "No or incorrect header in the weight table",
                              "Incorrect sequence of rows in the weight table"]


    def dataValidation(self, in_nc):
        """Check the necessary dimensions and variables in the input netcdf data"""
        vars_oi_index = None

        data_nc = NET.Dataset(in_nc)
        
        dims = data_nc.dimensions.keys()
        if dims not in self.dims_oi:
            raise Exception(self.errorMessages[1])

        vars = data_nc.variables.keys()
        if vars == self.vars_oi[0]:
            vars_oi_index = 0
        elif vars == self.vars_oi[1]:
            vars_oi_index = 1
        else:    
            raise Exception(self.errorMessages[2])

        return vars_oi_index


    def dataIdentify(self, in_nc, vars_oi_index):
        """Check if the data is daily (one value) or 3 hourly"""
        data_nc = NET.Dataset(in_nc)
        name_time = self.vars_oi[vars_oi_index][2]
        time = data_nc.variables[name_time][:]
        if len(time) == self.length_time["Daily"]:
            return "Daily"
        
        diff = NUM.unique(NUM.diff(time))
        data_nc.close()
        time_interval_3hr = NUM.array([3.0],dtype=float)
        if (diff == time_interval_3hr).all():
            return "3-Hourly"
        else:
            return None

    def readInWeightTable(self, in_weight_table):
        """
        Read in weight table
        """
        
        print "Reading the weight table..."
        self.dict_list = {self.header_wt[0]:[], self.header_wt[1]:[], self.header_wt[2]:[],
                          self.header_wt[3]:[], self.header_wt[4]:[]}
                     
        with open(in_weight_table, "rb") as csvfile:
            reader = csv.reader(csvfile)
            self.count = 0
            for row in reader:
                if self.count == 0:
                    #check number of columns in the weight table
                    if len(row) < len(self.header_wt):
                        raise Exception(self.errorMessages[4])
                    #check header
                    if row[1:len(self.header_wt)] != self.header_wt[1:]:
                        raise Exception(self.errorMessages[5])
                    self.count += 1
                else:
                    for i in xrange(len(self.header_wt)):
                       self.dict_list[self.header_wt[i]].append(row[i])
                    self.count += 1

        self.size_streamID = len(set(self.dict_list[self.header_wt[0]]))

    def generateOutputInflowFile(self, out_nc, in_weight_table, tot_size_time):
        """
        Generate inflow file for RAPID
        """

        self.readInWeightTable(in_weight_table)
        # Create output inflow netcdf data
        print "Generating inflow file"
        # data_out_nc = NET.Dataset(out_nc, "w") # by default format = "NETCDF4"
        data_out_nc = NET.Dataset(out_nc, "w", format = "NETCDF3_CLASSIC")
        dim_Time = data_out_nc.createDimension('Time', tot_size_time)
        dim_RiverID = data_out_nc.createDimension('rivid', self.size_streamID)
        var_m3_riv = data_out_nc.createVariable('m3_riv', 'f4', 
                                                ('Time', 'rivid'),
                                                fill_value=0)
        data_out_nc.close()
        #empty list to be read in later
        self.dict_list = {}

    def execute(self, nc_file_list, index_list, in_weight_table, 
                out_nc, grid_type):
                
        """The source code of the tool."""
        if not os.path.exists(out_nc):
            print "ERROR: Outfile has not been created. You need to run: generateOutputInflowFile function ..."
            raise Exception("ERROR: Outfile has not been created. You need to run: generateOutputInflowFile function ...")
            
        if len(nc_file_list) != len(index_list):
            print "ERROR: Number of runoff files not equal to number of indices ..."
            raise Exception("ERROR: Number of runoff files not equal to number of indices ...")
        
        self.readInWeightTable(in_weight_table)
        
        lon_ind_all = [long(i) for i in self.dict_list[self.header_wt[2]]]
        lat_ind_all = [long(j) for j in self.dict_list[self.header_wt[3]]]

        # Obtain a subset of  runoff data based on the indices in the weight table
        min_lon_ind_all = min(lon_ind_all)
        max_lon_ind_all = max(lon_ind_all)
        min_lat_ind_all = min(lat_ind_all)
        max_lat_ind_all = max(lat_ind_all)
        
        index_new = []

        data_out_nc = NET.Dataset(out_nc, "a", format = "NETCDF3_CLASSIC")
        
        # Validate the netcdf dataset
        vars_oi_index = self.dataValidation(nc_file_list[0])

        id_data = self.dataIdentify(nc_file_list[0], vars_oi_index)
        if id_data is None:
            raise Exception(self.errorMessages[3])

        #combine inflow data
        for nc_file_array_index, nc_file in enumerate(nc_file_list):
            index = index_list[nc_file_array_index]
            
            '''Calculate water inflows'''
            print "Calculating water inflows for", os.path.basename(nc_file) , grid_type, "..."

            ''' Read the netcdf dataset'''
            data_in_nc = NET.Dataset(nc_file)
            time = data_in_nc.variables[self.vars_oi[vars_oi_index][2]][:]

            # Check the size of time variable in the netcdf data
            size_time = len(time)
            if size_time != self.length_time[id_data]:
                raise Exception(self.errorMessages[3])

            data_subset_all = data_in_nc.variables[self.vars_oi[vars_oi_index][3]][:, min_lat_ind_all:max_lat_ind_all+1, min_lon_ind_all:max_lon_ind_all+1]
            data_in_nc.close()
            
            len_time_subset_all = data_subset_all.shape[0]
            len_lat_subset_all = data_subset_all.shape[1]
            len_lon_subset_all = data_subset_all.shape[2]
            data_subset_all = data_subset_all.reshape(len_time_subset_all, (len_lat_subset_all * len_lon_subset_all))


            # compute new indices based on the data_subset_all
            if not index_new:
                for r in range(0,self.count-1):
                    ind_lat_orig = lat_ind_all[r]
                    ind_lon_orig = lon_ind_all[r]
                    index_new.append((ind_lat_orig - min_lat_ind_all)*len_lon_subset_all + (ind_lon_orig - min_lon_ind_all))

            # obtain a new subset of data
            data_subset_new = data_subset_all[:,index_new]

            # start compute inflow
            pointer = 0
            for stream_index in xrange(self.size_streamID):
                npoints = int(self.dict_list[self.header_wt[4]][pointer])
                # Check if all npoints points correspond to the same streamID
                if len(set(self.dict_list[self.header_wt[0]][pointer : (pointer + npoints)])) != 1:
                    print "ROW INDEX", pointer
                    print "COMID", self.dict_list[self.header_wt[0]][pointer]
                    raise Exception(self.errorMessages[2])

                area_sqm_npoints = [float(k) for k in self.dict_list[self.header_wt[1]][pointer : (pointer + npoints)]]
                area_sqm_npoints = NUM.array(area_sqm_npoints)
                area_sqm_npoints = area_sqm_npoints.reshape(1, npoints)
                data_goal = data_subset_new[:, pointer:(pointer + npoints)]

                if grid_type == 't255':
                    #A) ERA Interim Low Res (T255) - data is cumulative
                    data_goal = data_goal.astype(NUM.float32)
                    #from time 3/6/9/12 (time zero not included, so assumed to be zero)
                    ro_first_half = NUM.concatenate([data_goal[0:1,], NUM.subtract(data_goal[1:4,], data_goal[0:3,])])
                    #from time 15/18/21/24 (time restarts at time 12, assumed to be zero)
                    ro_second_half = NUM.concatenate([data_goal[4:5,], NUM.subtract(data_goal[5:,], data_goal[4:7,])])
                    ro_stream = NUM.concatenate([ro_first_half, ro_second_half]) * area_sqm_npoints
                else:
                    #A) ERA Interim High Res (T511) - data is incremental
                    #from time 3/6/9/12/15/18/21/24
                    ro_stream = data_goal * area_sqm_npoints

                data_out_nc.variables['m3_riv'][index*size_time:(index+1)*size_time,stream_index] = ro_stream.sum(axis = 1)
            
                pointer += npoints
        # close the input and output netcdf datasets
        data_out_nc.close()