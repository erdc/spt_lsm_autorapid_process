#!/usr/bin/python
import datetime
import sys

from spt_erai_autorapid_process.imports.CreateInflowFileFromERAInterimRunoff import CreateInflowFileFromERAInterimRunoff


def process_upload_ECMWF_RAPID(watershed, subbasin, era_interim_file, 
                               erai_file_index, erai_weight_table_file,
                               rapid_inflow_file):
    """
    prepare all ECMWF files for rapid
    """

    time_start_all = datetime.datetime.utcnow()

    #prepare ECMWF file for RAPID
    print "ERAI downscaling for:", watershed, subbasin, \
          erai_file_index, rapid_inflow_file

    print "Converting ERAI inflow"
    RAPIDinflowECMWF_tool = CreateInflowFileFromERAInterimRunoff()

    RAPIDinflowECMWF_tool.execute(nc_file=era_interim_file,
                                  index=erai_file_index,
                                  in_weight_table=erai_weight_table_file,
                                  out_nc=rapid_inflow_file
                                  )

    time_finish_ecmwf = datetime.datetime.utcnow()
    print "Time to convert ECMWF: %s" % (time_finish_ecmwf-time_start_all)


if __name__ == "__main__":
    process_upload_ECMWF_RAPID(sys.argv[1],sys.argv[2], sys.argv[3], 
                               sys.argv[4], sys.argv[5], sys.argv[6])
