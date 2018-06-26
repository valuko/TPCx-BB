#!/usr/bin/env bash

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

#
#Copyright 2015 Intel Corporation All Rights Reserved.
#
#The source code contained or described herein and all documents related to the source code ("Material") are owned by Intel Corporation or its suppliers or licensors. Title to the Material remains with Intel Corporation or its suppliers and licensors. The Material contains trade secrets and proprietary and confidential information of Intel or its suppliers and licensors. The Material is protected by worldwide copyright and trade secret laws and treaty provisions. No part of the Material may be used, copied, reproduced, modified, published, uploaded, posted, transmitted, distributed, or disclosed in any way without Intel's prior express written permission.
#
#No license under any patent, copyright, trade secret or other intellectual property right is granted to or conferred upon you by disclosure or delivery of the Materials, either expressly, by implication, inducement, estoppel or otherwise. Any license under such intellectual property rights must be express and approved by Intel in writing.

ENV_INFO_DIR_RELATIVE="envInfo-$HOSTNAME"
ENV_INFO_DIR="/tmp/$ENV_INFO_DIR_RELATIVE"
[ -d "$ENV_INFO_DIR" ] && rm -rf "$ENV_INFO_DIR"
mkdir "$ENV_INFO_DIR"
ENV_INFO_FILE="$ENV_INFO_DIR/envInfo.log"
> "$ENV_INFO_FILE"
if [ -w "$ENV_INFO_FILE" ]
then
  echo "##############################" >> "$ENV_INFO_FILE" 2>&1
  echo "#          Hardware          #" >> "$ENV_INFO_FILE" 2>&1
  echo "##############################" >> "$ENV_INFO_FILE" 2>&1

  if type dmidecode > /dev/null 2>&1
  then
    echo -e "\n##### dmidecode #####\n" >> "$ENV_INFO_FILE" 2>&1
    if [ "$UID" -eq 0 ]
    then
      dmidecode >> "$ENV_INFO_FILE" 2>&1
    else
      sudo dmidecode >> "$ENV_INFO_FILE" 2>&1
    fi
  fi
  echo -e "\n##### /proc/cpuinfo #####\n" >> "$ENV_INFO_FILE" 2>&1
  cat /proc/cpuinfo >> "$ENV_INFO_FILE" 2>&1
  echo -e "\n##### /proc/meminfo #####\n" >> "$ENV_INFO_FILE" 2>&1
  cat /proc/meminfo >> "$ENV_INFO_FILE" 2>&1
  if type lscpu > /dev/null 2>&1
  then
    echo -e "\n##### lscpu #####\n" >> "$ENV_INFO_FILE" 2>&1
    lscpu >> "$ENV_INFO_FILE" 2>&1
  fi
  if type lspci > /dev/null 2>&1
  then
    echo -e "\n##### lspci #####\n" >> "$ENV_INFO_FILE" 2>&1
    lspci >> "$ENV_INFO_FILE" 2>&1
  fi
  if type lsblk > /dev/null 2>&1
  then
    echo -e "\n##### lsblk #####\n" >> "$ENV_INFO_FILE" 2>&1
    lsblk >> "$ENV_INFO_FILE" 2>&1
  fi
  if type mount > /dev/null 2>&1
  then
    echo -e "\n##### mounted disks #####\n" >> "$ENV_INFO_FILE" 2>&1
    mount >> "$ENV_INFO_FILE" 2>&1
  fi
  if type ifconfig > /dev/null 2>&1
  then
    echo -e "\n##### ifconfig #####\n" >> "$ENV_INFO_FILE" 2>&1
    ifconfig >> "$ENV_INFO_FILE" 2>&1
  else
    if type ip > /dev/null 2>&1
    then
      echo -e "\n##### ip #####\n" >> "$ENV_INFO_FILE" 2>&1
      ip addr list >> "$ENV_INFO_FILE" 2>&1
    fi
  fi
  if type iptables-save > /dev/null 2>&1
  then
    echo -e "\n##### iptables #####\n" >> "$ENV_INFO_FILE" 2>&1
    if [ "$UID" -eq 0 ]
    then
      iptables-save >> "$ENV_INFO_FILE" 2>&1
    else
      sudo iptables-save >> "$ENV_INFO_FILE" 2>&1
    fi
  fi

  echo "##############################" >> "$ENV_INFO_FILE" 2>&1
  echo "#          Software          #" >> "$ENV_INFO_FILE" 2>&1
  echo "##############################" >> "$ENV_INFO_FILE" 2>&1

  echo -e "\n##### linux release #####\n" >> "$ENV_INFO_FILE" 2>&1
  cat /etc/*release >> "$ENV_INFO_FILE" 2>&1
  echo -e "\n##### kernel release #####\n" >> "$ENV_INFO_FILE" 2>&1
  uname -a >> "$ENV_INFO_FILE" 2>&1
  echo -e "\n##### date #####\n" >> "$ENV_INFO_FILE" 2>&1
  date >> "$ENV_INFO_FILE" 2>&1
  echo -e "\n##### hadoop version #####\n" >> "$ENV_INFO_FILE" 2>&1
  hadoop version >> "$ENV_INFO_FILE" 2>&1
  echo -e "\n##### hadoop classpath #####\n" >> "$ENV_INFO_FILE" 2>&1
  hadoop classpath >> "$ENV_INFO_FILE" 2>&1
  echo -e "\n##### java version #####\n" >> "$ENV_INFO_FILE" 2>&1
  java -version >> "$ENV_INFO_FILE" 2>&1
  echo -e "\n##### environment #####\n" >> "$ENV_INFO_FILE" 2>&1
  set >> "$ENV_INFO_FILE" 2>&1
  if type rpm > /dev/null 2>&1
  then
    echo -e "\n##### installed packages (from rpm) #####\n" >> "$ENV_INFO_FILE" 2>&1
    rpm -qa >> "$ENV_INFO_FILE" 2>&1
  fi
  if type dpkg > /dev/null 2>&1
  then
    echo -e "\n##### installed packages (from dpkg) #####\n" >> "$ENV_INFO_FILE" 2>&1
    dpkg -l >> "$ENV_INFO_FILE" 2>&1
  fi

  # copy files
  for DIR in /etc/hadoop /etc/hive /etc/spark
  do
    [ -d "$DIR" ] && cp -a "$DIR" "$ENV_INFO_DIR/"
  done

  cd /tmp
  zip -r "$ENV_INFO_DIR_RELATIVE.zip" "$ENV_INFO_DIR_RELATIVE"
else
  echo "environment information could not be written to $ENV_INFO_FILE"
  exit 1
fi
