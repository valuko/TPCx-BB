#
# Copyright (C) 2016 Transaction Processing Performance Council (TPC) and/or
# its contributors.
#
# This file is part of a software package distributed by the TPC.
#
# The contents of this file have been developed by the TPC, and/or have been
# licensed to the TPC under one or more contributor license agreements.
#
#  This file is subject to the terms and conditions outlined in the End-User
#  License Agreement (EULA) which can be found in this distribution (EULA.txt)
#  and is available at the following URL:
#  http://www.tpc.org/TPC_Documents_Current_Versions/txt/EULA.txt
#
# Unless required by applicable law or agreed to in writing, this software
# is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF
# ANY KIND, either express or implied, and the user bears the entire risk as
# to quality and performance as well as the entire cost of service or repair
# in case of defect.  See the EULA for more details.

#
#Copyright 2015 Intel Corporation All Rights Reserved. 
#
#The source code contained or described herein and all documents related to the source code ("Material") are owned by Intel Corporation or its suppliers or licensors. Title to the Material remains with Intel Corporation or its suppliers and licensors. The Material contains trade secrets and proprietary and confidential information of Intel or its suppliers and licensors. The Material is protected by worldwide copyright and trade secret laws and treaty provisions. No part of the Material may be used, copied, reproduced, modified, published, uploaded, posted, transmitted, distributed, or disclosed in any way without Intel's prior express written permission.
#
#No license under any patent, copyright, trade secret or other intellectual property right is granted to or conferred upon you by disclosure or delivery of the Materials, either expressly, by implication, inducement, estoppel or otherwise. Any license under such intellectual property rights must be express and approved by Intel in writing.

import sys
import logging
import traceback
import os
import time
from time import strftime

timeout = long(sys.argv[1])

if __name__ == "__main__":

	# requires data to be in format <user_sk>\t<timestamp>\t<item_sk>, clustered by user_sk and sorted by <timestamp> ascending
	line = ''
	current_uid = ''
	last_click_time = ''
	perUser_sessionID_counter = 1
	sessionID = ''
	
	try:
		# algorithm expects input lines to be clustered by user_sk and sorted by <timestamp> ascending
		for line in sys.stdin:
			user_sk, tstamp_str, item_sk  = line.strip().split("\t")
			tstamp = long(tstamp_str)

			# reset if next partition beginns
			if current_uid != user_sk:
				current_uid = user_sk
				last_click_time = tstamp
				perUser_sessionID_counter = long(1)
				#sessionID = user_sk + "_" + tstamp_str + "_1"
				sessionID = user_sk + "_1"

			# time between clicks exceeds session timeout?
			# tstamp must be guaranteed to be >= last_click_time by hive (sorted ascending)
			if tstamp - last_click_time > timeout:
				perUser_sessionID_counter = perUser_sessionID_counter + 1
				
				# we should not require the session's start time as part of the sessionID. hives "distribute by x" is defined to pipe every line for the same X to the same reducer ( same instance of this script)
				#Hive uses the columns in Distribute By to distribute the rows among reducers. All rows with the same Distribute By columns will go to the same reducer. However, Distribute By does not guarantee clustering or sorting properties on the distributed keys.
				#sessionID = user_sk + "_" + tstamp_str + "_" + str(perUser_sessionID_counter)
				sessionID = user_sk + "_" + str(perUser_sessionID_counter)

			last_click_time =tstamp
			print "%s\t%s" % (item_sk, sessionID )
			#print item_sk +"\t"+ sessionID 

	except:
		## should only happen if input format is not correct, like 4 instead of 3 tab separated values
		logging.basicConfig(level=logging.DEBUG, filename=strftime("/tmp/bigbench_q30-sessionize.py_%Y%m%d-%H%M%S.log"))
		logging.info("sys.argv[1] timeout: " +str(timeout) + " line from hive: \"" + line + "\"")
		logging.exception("Oops:") 
		raise
		sys.exit(1)
