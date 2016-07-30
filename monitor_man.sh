####################################################################
# File Name: monitor_man.sh
# Author: Justin
# mail: dhutsj@gmail.com
# Created Time: Fri 15 Jul 2016 09:45:47 PM PDT
# ================================================================
#!/bin/bash
resttem=$(tput sgr0)
declare -A ssharray
i=0
numbers=""
for script_file in `ls -I "monitor_man.sh" ./`
do 
  echo -e "\e[1;35m" "The script: " ${i} '==>' ${resttem} ${script_file}
  ssharray[$i]=${script_file}
  numbers="${numbers} | ${i}"
  i=$((i+1))
done

while true
do
  read -p "Please input a number [ ${numbers} ]:" execshell
  if [[ ! ${execshell} =~ ^[0-9]+ ]];then
     exit 0
  fi
  /bin/bash ./${ssharray[$execshell]}
done
