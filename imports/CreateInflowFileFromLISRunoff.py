'''-------------------------------------------------------------------------------
 Tool Name:   CreateInflowFileFromLISRunoff
 Source Name: CreateInflowFileFromLISRunoff.py
 Version:     ArcGIS 10.3
 Author:      Alan Snow (Adapted from CreateInflowFileFromECMWFRunoff.py)
 Description: Creates RAPID inflow file based on the LIS land model output
              and the weight table previously created.
 History:     Initial coding - 10/26/2015, version 1.0
-------------------------------------------------------------------------------'''
import csv
import netCDF4 as NET
import numpy as NUM
import os

class CreateInflowFileFromLISRunoff(object):
    def __init__(self, lat_dim="north_south", 
                       lon_dim="east_west", 
                       lat_var="lat", 
                       lon_var="lon", 
                       surface_runoff_var="Qs_inst",
                       subsurface_runoff_var="Qsb_inst"):
        """Define the attributes to look for"""
        self.header_wt = ['StreamID', 'area_sqm', 'lon_index', 'lat_index', 'npoints', 'weight', lon_var, lat_var]
        self.dims_oi = [lon_dim, lat_dim, 'time']
        self.vars_oi = [lon_var, lat_var, 'time', surface_runoff_var, subsurface_runoff_var]
        self.length_time = {"Hourly": 1}
        self.errorMessages = ["Missing Variable 'time'",
                              "Incorrect dimensions in the input LIS runoff file.",
                              "Incorrect variables in the input LIS runoff file.",
                              "Incorrect time variable in the input LIS runoff file",
                              "Incorrect number of columns in the weight table",
                              "No or incorrect header in the weight table",
                              "Incorrect sequence of rows in the weight table"]


    def dataValidation(self, in_nc):
        """Check the necessary dimensions and variables in the input netcdf data"""
        data_nc = NET.Dataset(in_nc)
        
        for dim in data_nc.dimensions.keys():
            if dim not in self.dims_oi:
                data_nc.close()
                raise Exception(self.errorMessages[1])

        for var in data_nc.variables.keys():
            if var not in self.vars_oi:
                data_nc.close()
                raise Exception(self.errorMessages[2])

        data_nc.close()
        return


    def dataIdentify(self, in_nc, vars_oi_index):
        """Check if the data is hourly (one value)"""
        data_nc = NET.Dataset(in_nc)
        name_time = self.vars_oi[vars_oi_index][2]
        time = data_nc.variables[name_time][:]
        if len(time) == self.length_time["Hourly"]:
            return "Hourly"
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

        data_subset_surface_runoff = data_in_nc.variables[self.vars_oi[3]][min_lat_ind_all:max_lat_ind_all+1, min_lon_ind_all:max_lon_ind_all+1]
        data_subset_subsurface_runoff = data_in_nc.variables[self.vars_oi[4]][min_lat_ind_all:max_lat_ind_all+1, min_lon_ind_all:max_lon_ind_all+1]
        data_in_nc.close()
        #check surface runoff dims
        len_time_subset_surface = data_subset_surface_runoff.shape[0]
        len_lat_subset_surface = data_subset_surface_runoff.shape[1]
        len_lon_subset_surface = data_subset_surface_runoff.shape[2]
        #check subsurface runoff dims
        len_time_subset_subsurface = data_subset_surface_runoff.shape[0]
        len_lat_subset_subsurface = data_subset_surface_runoff.shape[1]
        len_lon_subset_subsurface = data_subset_surface_runoff.shape[2]
        #make sure they are the same
        if len_time_subset_surface != len_time_subset_subsurface:
            raise Exception("Surface and subsurface time lengths do not agree ...")
        #make sure they are the same
        if len_lat_subset_surface != len_lat_subset_subsurface:
            raise Exception("Surface and subsurface lat lengths do not agree ...")
        #make sure they are the same
        if len_lon_subset_surface != len_lon_subset_subsurface:
            raise Exception("Surface and subsurface lon lengths do not agree ...")
        #reshape the runoff
        data_subset_surface_runoff = data_subset_surface_runoff.reshape(len_time_subset_surface, (len_lat_subset_surface * len_lon_subset_surface))
        data_subset_subsurface_runoff = data_subset_subsurface_runoff.reshape(len_time_subset_subsurface, (len_lat_subset_subsurface * len_lon_subset_subsurface))


        # compute new indices based on the data_subset_surface
        index_new = []
        for r in range(0,self.count-1):
            ind_lat_orig = lat_ind_all[r]
            ind_lon_orig = lon_ind_all[r]
            index_new.append((ind_lat_orig - min_lat_ind_all)*len_lat_subset_surface + (ind_lon_orig - min_lon_ind_all))

        # obtain a new subset of data
        data_subset_surface_new = data_subset_surface_runoff[:,index_new]
        data_subset_subsurface_new = data_subset_subsurface_runoff[:,index_new]
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
            data_goal_surface = data_subset_surface_new[:, pointer:(pointer + npoints)]
            data_goal_subsurface = data_subset_subsurface_new[:, pointer:(pointer + npoints)]

            #if id_data == "Hourly":
            #default units are kg/(s*m^s) or mm/s need m
            #multiply by 1hr * (3600s/hr) * (1m/1000mm)
            ro_stream = NUM.add(data_goal_surface, data_goal_subsurface) * area_sqm_npoints / 1000 * 3600
            #non zero
            out_data = ro_stream.sum(axis = 1)
            if out_data[NUM.nonzero(out_data)].any():
                print stream_index
                print out_data
                
            data_out_nc.variables['m3_riv'][index,stream_index] = ro_stream.sum(axis = 1) 
                                
            pointer += npoints

        # close the input and output netcdf datasets
        data_out_nc.close()