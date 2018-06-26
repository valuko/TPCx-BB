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
import traceback
import os
import string

days_param = long(sys.argv[1])
last_n_views = int(sys.argv[2])
purchasedItemFilter = sys.argv[3]

#Explanation:
#Reducer script logic: iterate through clicks of a user in descending order (most recent click first).if a purchase is found (wcs_sales_sk!=null) display the next 5 clicks if they are within the provided date range (max 10 days before)
#Reducer script selects only:
# * products viewed within 'q03_days_before_purchase' days before the purchase date
# * only the last 5 products that where purchased before a sale	
#
# Limitations of this implementation:
# a newly purchased item resets the "clicks before purchase"
#
# Future:
# This could be circumvented by iterating in ascending click_date order instead of descending and keeping a cyclic buffer of length 'last_n_views' storing the last n clicked_item_sk's and click_dates. Upon finding the next purchase, dump the buffer contents for each buffer item matching the date range. 

class RingBuffer:
	def __init__(self,size_max):
		self.max = size_max
		self.data = []
	def append(self,x):
		"""append an element at the end of the buffer"""
		self.data.append(x)
		if len(self.data) == self.max:
			self.cur=0
			self.__class__ = RingBufferFull
	def getAll(self):
  		""" return a list of elements from the oldest to the newest"""
		return self.data
		
	def get(self,pos):
	    return  self.data[pos]

	def size(self):
		return len(self.data)

class RingBufferFull:
	def __init__(self,n):
		raise "you should use RingBuffer"
	def append(self,x):		
		self.data[self.cur]=x
		self.cur=(self.cur+1) % self.max
		
	def get(self,pos):
		return  self.data[(self.cur+pos)% self.max]
		
	def getAll(self):
		return self.data[self.cur:]+self.data[:self.cur]
		
	def size(self):
		return self.max
		


if __name__ == "__main__":
	#init stuff
	last_user = ''
	vals = []
	last_n_ViewsBuffer=RingBuffer(last_n_views)

	for line in sys.stdin:
		
		#input from hive
		#values are clustered by user and pre-sorted by wcs_date which has to be done in hive
		user, wcs_date_str, item_key_str, sale_sk = line.strip().split("\t")
		
		wcs_date = long(wcs_date_str)

		
		#new user , reset everything
		if last_user != user :
			#print "reset"
			last_user = user;
			last_n_ViewsBuffer=RingBuffer(last_n_views)		
		
		#print "1)cur item " +item_key_str +" is purchased item? "+purchasedItemFilter+" " +str(item_key_str == purchasedItemFilter)+ " filterOn: "+ purchasedItemFilter +" salesk: "+ sale_sk +" isdecimal: "+  str(sale_sk.isdigit())
		#filter match, print last [0, n] viewed items in [0,days_param] range 
		if (item_key_str == purchasedItemFilter and sale_sk.isdigit() ):
			#print "2)hit! buf:" + str(last_n_ViewsBuffer.getAll())
			#print "2)buffsize: "+ str(last_n_ViewsBuffer.size())
			for i in xrange(0,last_n_ViewsBuffer.size()):  
				cur_item_sk, cur_date = last_n_ViewsBuffer.get(i)
				#print "3)cur buffer content["+str(i)+"]: item: " + cur_item_sk + " item_date: " + str(cur_date) + " line date: " + str(wcs_date)
				#is clicked before?
				if cur_date <= wcs_date  and cur_date >= ( wcs_date - days_param ) :
					print "%s\t%s" % (purchasedItemFilter, cur_item_sk)
					
	
		#append to buffer after check. This ensures that currently checked sold item itself is not in the buffer when writing buffer contents to hive. Avoids self paring: {x,x} e.g: {12,12}
		last_n_ViewsBuffer.append((item_key_str,wcs_date))

