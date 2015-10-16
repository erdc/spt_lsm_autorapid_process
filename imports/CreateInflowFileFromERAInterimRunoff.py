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

class CreateInflowFileFromERAInterimRunoff(object):
    def __init__(self):
        """Define the tool (tool name is the name of the class)."""
        self.label = "Create Inflow File From ERA Interim Runoff"
        self.description = ("Creates RAPID NetCDF input of water inflow "
                            "based on ERA Interim runoff results and "
                            "previously created weight table.")
        self.header_wt = ['StreamID', 'area_sqm', 'lon_index', 'lat_index', 'npoints', 'weight', 'Lon', 'Lat']
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
        
        self.streamID = ""
        print "Reading the weight table..."
        self.dict_list = {self.header_wt[0]:[], self.header_wt[1]:[], self.header_wt[2]:[],
                          self.header_wt[3]:[], self.header_wt[4]:[], self.header_wt[5]:[],
                          self.header_wt[6]:[], self.header_wt[7]:[]}
                     
        with open(in_weight_table, "rb") as csvfile:
            reader = csv.reader(csvfile)
            self.count = 0
            for row in reader:
                if self.count == 0:
                    #check number of columns in the weight table
                    if len(row) != len(self.header_wt):
                        raise Exception(self.errorMessages[4])
                    #check header
                    if row[1:len(self.header_wt)] != self.header_wt[1:len(self.header_wt)]:
                        raise Exception(self.errorMessages[5])
                    self.streamID = row[0]
                    self.count += 1
                else:
                    for i in range(0,8):
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
        dim_RiverID = data_out_nc.createDimension(self.streamID, self.size_streamID)
        var_m3_riv = data_out_nc.createVariable('m3_riv', 'f4', 
                                                ('Time', self.streamID),
                                                fill_value=0)
        data_out_nc.close()

    def execute(self, nc_file, index, in_weight_table, out_nc, grid_type, num_files=0):
        """The source code of the tool."""

        
        # Validate the netcdf dataset
        vars_oi_index = self.dataValidation(nc_file)

        id_data = self.dataIdentify(nc_file, vars_oi_index)
        if id_data is None:
            raise Exception(self.errorMessages[3])

        ''' Read the netcdf dataset'''
        data_in_nc = NET.Dataset(nc_file)
        time = data_in_nc.variables[self.vars_oi[vars_oi_index][2]][:]

        # Check the size of time variable in the netcdf data
        size_time = len(time)
        if size_time != self.length_time[id_data]:
            raise Exception(self.errorMessages[3])

        if not os.path.exists(out_nc):
            self.generateOutputInflowFile(out_nc, in_weight_table, 
                                          tot_size_time=num_files*size_time)
        else:
            self.readInWeightTable(in_weight_table)
        
        data_out_nc = NET.Dataset(out_nc, "a", format = "NETCDF3_CLASSIC")
            
        lon_ind_all = [long(i) for i in self.dict_list[self.header_wt[2]]]
        lat_ind_all = [long(j) for j in self.dict_list[self.header_wt[3]]]

        # Obtain a subset of  runoff data based on the indices in the weight table
        min_lon_ind_all = min(lon_ind_all)
        max_lon_ind_all = max(lon_ind_all)
        min_lat_ind_all = min(lat_ind_all)
        max_lat_ind_all = max(lat_ind_all)


        '''Calculate water inflows'''
        print "Calculating water inflows for", os.path.basename(nc_file) , grid_type, "..."

        data_subset_all = data_in_nc.variables[self.vars_oi[vars_oi_index][3]][:, min_lat_ind_all:max_lat_ind_all+1, min_lon_ind_all:max_lon_ind_all+1]
        data_in_nc.close()
        len_time_subset_all = data_subset_all.shape[0]
        len_lat_subset_all = data_subset_all.shape[1]
        len_lon_subset_all = data_subset_all.shape[2]
        data_subset_all = data_subset_all.reshape(len_time_subset_all, (len_lat_subset_all * len_lon_subset_all))


        # compute new indices based on the data_subset_all
        index_new = []
        for r in range(0,self.count-1):
            ind_lat_orig = lat_ind_all[r]
            ind_lon_orig = lon_ind_all[r]
            index_new.append((ind_lat_orig - min_lat_ind_all)*len_lon_subset_all + (ind_lon_orig - min_lon_ind_all))

        # obtain a new subset of data
        data_subset_new = data_subset_all[:,index_new]
        # start compute inflow
        pointer = 0
        for stream_index in range(self.size_streamID):
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
            if id_data == "Daily":
                ro_stream = data_goal * area_sqm_npoints
                data_out_nc.variables['m3_riv'][index,stream_index] = ro_stream.sum(axis = 1)
            else: #id_data == "3-Hourly
                if grid_type == 't255':
                    #A) ERA Interim Low Res (T255) - data is cumulative
                    data_goal = data_goal.astype(NUM.float32)
                    #from time 3/6/9/12 (time zero not included, so assumed to be zero)
                    ro_first_half = NUM.concatenate([data_goal[0:1,], NUM.subtract(data_goal[1:4,], data_goal[0:3,])])
                    #from time 15/18/21/24 (time restarts at time 12, assumed to be zero)
                    ro_second_half = NUM.concatenate([data_goal[4:5,], NUM.subtract(data_goal[5:,], data_goal[4:7,])])
                    ro_stream = NUM.concatenate([ro_first_half, ro_second_half]) * area_sqm_npoints
                else:
                    #from time 3/6/9/12/15/18/21/24 (data is incremental)
                    ro_stream = data_goal * area_sqm_npoints

                data_out_nc.variables['m3_riv'][index*size_time:(index+1)*size_time,stream_index] = ro_stream.sum(axis = 1)
                                
            pointer += npoints

        # close the input and output netcdf datasets
        data_out_nc.close()