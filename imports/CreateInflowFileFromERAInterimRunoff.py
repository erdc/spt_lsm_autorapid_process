'''-------------------------------------------------------------------------------
 Tool Name:   CreateInflowFileFromERAInterimRunoff
 Source Name: CreateInflowFileFromERAInterimRunoff.py
 Version:     ArcGIS 10.3
 Author:      Alan Snow (Adapted from CreateInflowFileFromECMWFRunoff.py)
 Description: Creates RAPID inflow file based on the ERA Interim land model output
              and the weight table previously created.
 History:     Initial coding - 6/20/2015, version 1.0
-------------------------------------------------------------------------------'''
import csv
import netCDF4 as NET
import numpy as NUM
import os
import re

class CreateInflowFileFromERAInterimRunoff(object):
    def __init__(self):
        """Define the tool (tool name is the name of the class)."""
        self.label = "Create Inflow File From ERA Interim Runoff"
        self.description = ("Creates RAPID NetCDF input of water inflow "
                            "based on ERA Interim runoff results and "
                            "previously created weight table.")
        self.header_wt = ['StreamID', 'area_sqm', 'lon_index', 'lat_index', 'npoints', 'weight', 'Lon', 'Lat']
        self.dims_oi = ['lon', 'lat', 'time']
        self.vars_oi = ["lon", "lat", "time", "RO"]
        self.length_time = 1
        self.errorMessages = ["Missing Variable 'time'",
                              "Incorrect dimensions in the input ERA Interim runoff file.",
                              "Incorrect variables in the input ERA Interim runoff file.",
                              "Incorrect time variable in the input ERA Interim runoff file",
                              "Incorrect number of columns in the weight table",
                              "No or incorrect header in the weight table",
                              "Incorrect sequence of rows in the weight table"]


    def dataValidation(self, in_nc):
        """Check the necessary dimensions and variables in the input netcdf data"""
        data_nc = NET.Dataset(in_nc)

        dims = data_nc.dimensions.keys()
        if dims != self.dims_oi:
            raise Exception(self.errorMessages[1])

        vars = data_nc.variables.keys()
        if vars != self.vars_oi:
            raise Exception(self.errorMessages[2])

        return


    def dataIdentify(self, in_nc):
        return

    def execute(self, in_sorted_nc_files, in_weight_table, out_nc):
        """The source code of the tool."""

        ''' Read the weight table '''
        print "Reading the weight table..."
        dict_list = {self.header_wt[0]:[], self.header_wt[1]:[], self.header_wt[2]:[],
                     self.header_wt[3]:[], self.header_wt[4]:[], self.header_wt[5]:[],
                     self.header_wt[6]:[], self.header_wt[7]:[]}
        streamID = ""
        with open(in_weight_table, "rb") as csvfile:
            reader = csv.reader(csvfile)
            count = 0
            for row in reader:
                if count == 0:
                    #check number of columns in the weight table
                    if len(row) != len(self.header_wt):
                        raise Exception(self.errorMessages[4])
                    #check header
                    if row[1:len(self.header_wt)] != self.header_wt[1:len(self.header_wt)]:
                        raise Exception(self.errorMessages[5])
                    streamID = row[0]
                    count += 1
                else:
                    for i in range(0,8):
                       dict_list[self.header_wt[i]].append(row[i])
                    count += 1

        size_streamID = len(set(dict_list[self.header_wt[0]]))

        size_time = len(in_sorted_nc_files)

        # Create output inflow netcdf data
        # data_out_nc = NET.Dataset(out_nc, "w") # by default format = "NETCDF4"
        data_out_nc = NET.Dataset(out_nc, "w", format = "NETCDF3_CLASSIC")
        dim_Time = data_out_nc.createDimension('Time', size_time)
        dim_RiverID = data_out_nc.createDimension(streamID, size_streamID)
        var_m3_riv = data_out_nc.createVariable('m3_riv', 'f4', ('Time', streamID))
        data_temp = NUM.empty(shape = [size_time, size_streamID])

        lon_ind_all = [long(i) for i in dict_list[self.header_wt[2]]]
        lat_ind_all = [long(j) for j in dict_list[self.header_wt[3]]]

        # Obtain a subset of  runoff data based on the indices in the weight table
        min_lon_ind_all = min(lon_ind_all)
        max_lon_ind_all = max(lon_ind_all)
        min_lat_ind_all = min(lat_ind_all)
        max_lat_ind_all = max(lat_ind_all)

        for index, nc_file in enumerate(in_sorted_nc_files):

            # Validate the netcdf dataset
            self.dataValidation(nc_file)

            ''' Read the netcdf dataset'''
            data_in_nc = NET.Dataset(nc_file)
            time = data_in_nc.variables[self.vars_oi[2]][:]

            # Check the size of time variable in the netcdf data
            if len(time) != self.length_time:
                raise Exception(self.errorMessages[3])

            '''Calculate water inflows'''
            print "Calculating water inflows for", os.path.basename(nc_file) , "..."

            data_subset_all = data_in_nc.variables[self.vars_oi[3]][:, min_lat_ind_all:max_lat_ind_all+1, min_lon_ind_all:max_lon_ind_all+1]
            data_in_nc.close()
            len_time_subset_all = data_subset_all.shape[0]
            len_lat_subset_all = data_subset_all.shape[1]
            len_lon_subset_all = data_subset_all.shape[2]
            data_subset_all = data_subset_all.reshape(len_time_subset_all, (len_lat_subset_all * len_lon_subset_all))


            # compute new indices based on the data_subset_all
            index_new = []
            for r in range(0,count-1):
                ind_lat_orig = lat_ind_all[r]
                ind_lon_orig = lon_ind_all[r]
                index_new.append((ind_lat_orig - min_lat_ind_all)*len_lon_subset_all + (ind_lon_orig - min_lon_ind_all))

            # obtain a new subset of data
            data_subset_new = data_subset_all[:,index_new]

            # start compute inflow
            pointer = 0
            for s in range(0, size_streamID):
                npoints = int(dict_list[self.header_wt[4]][pointer])
                # Check if all npoints points correspond to the same streamID
                if len(set(dict_list[self.header_wt[0]][pointer : (pointer + npoints)])) != 1:
                    print "ROW INDEX", pointer
                    print "COMID", dict_list[self.header_wt[0]][pointer]
                    raise Exception(self.errorMessages[2])

                area_sqm_npoints = [float(k) for k in dict_list[self.header_wt[1]][pointer : (pointer + npoints)]]
                area_sqm_npoints = NUM.array(area_sqm_npoints)
                area_sqm_npoints = area_sqm_npoints.reshape(1, npoints)
                ro_stream = data_subset_new[:, pointer:(pointer + npoints)] * area_sqm_npoints
                var_m3_riv[index,s] = ro_stream.sum(axis = 1)

                pointer += npoints

        # close the input and output netcdf datasets
        data_out_nc.close()

        return

if __name__ == "__main__":
    calc = CreateInflowFileFromERAInterimRunoff()
    in_nc_directory_path='/Users/Alan/Documents/RESEARCH/RAPID/ECMWF/erai_runoff_1980to2014/'
    nc_files = []
    for subdir, dirs, files in os.walk(in_nc_directory_path):
        for file in files:
            if file.endswith('.nc'):
                nc_files.append(os.path.join(subdir, file))

    calc.execute(in_sorted_nc_files=sorted(nc_files),
                 in_weight_table='/Users/Alan/Documents/RESEARCH/RAPID/input/nfie_great_basin_region/rapid_updated/weight_era_interim.csv',
                 out_nc='/Users/Alan/Documents/RESEARCH/RAPID/input/nfie_great_basin_region/rapid_updated/m3_era_1980_2014.nc')
