####################################################################
# File Name: system_usage.sh
# Author: Justin
# mail: dhutsj@gmail.com
# Created Time: Sat 16 Jul 2016 12:53:43 AM PDT
# ================================================================
#!/bin/bash
clear
reset_terminal=$(tput sgr0)
if [[ $# -eq 0 ]]
then
#check memory usage
  system_memory_usgae=$(cat /proc/meminfo | awk '/MemTotal/{total=$2}/MemFree/{free=$2}END{print (total-free)/1024}' /proc/meminfo)
  echo -e "\e[1;35m" "System Memory used(MB): " ${reset_terminal} ${system_memory_usgae}
  app_memory_usage=$(cat /proc/meminfo | awk '/MemTotal/{total=$2}/MemFree/{free=$2}/^Cached/{cache=$2}/Buffers/{buffer=$2}END{print (total-free-cache-buffer)/1024}' /proc/meminfo)
  echo -e "\e[1;35m" "Application Memory used(MB): " ${reset_terminal} ${app_memory_usage}
#check load average
  load_average=$(uptime | awk '{print $8 $9 $10}')
  echo -e "\e[1;35m" "Load Average: " ${reset_terminal} ${load_average}
#check disk usage
disk_usage=$(df -h | grep -v "Filesystem" | awk '{print $1 " " $5}' | grep "^/dev")
echo -e "\e[1;35m" "Disk Usage: " ${reset_terminal} ${disk_usage}
fi
