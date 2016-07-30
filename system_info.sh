####################################################################
# File Name: system_info.sh
# Author: Justin
# mail: dhutsj@gmail.com
# Created Time: Sat 16 Jul 2016 01:47:55 AM PDT
# ================================================================
#!/bin/bash
reset_terminal=$(tput sgr0)
if [[ $# -eq 0 ]]
then
#check OS Type
  os_type=$(uname -o)
  echo -e "\e[1;35m" "OS Type: " ${reset_terminal} ${os_type}
#check OS Release and version
  os_release=$(cat /etc/issue | sed 's/\\n//' | sed 's/\\l//')
  echo -e "\e[1;35m" "OS Release: " ${reset_terminal} ${os_release}
#check Architecture
  architecture=$(uname -m)
  echo -e "\e[1;35m" "Architecture: " ${reset_terminal} ${architecture}
#check Kernal
  kernal=$(uname -r)
  echo -e "\e[1;35m" "Kernal: " ${reset_terminal} ${kernal}
#check Hostname
  hostname=$(hostname)
  echo -e "\e[1;35m" "Hostname: " ${reset_terminal} ${hostname}
#check Internal IP
  internal_ip=$(hostname -I)
  echo -e "\e[1;35m" "Internal IP: " ${reset_terminal} ${internal_ip}
#check External IP
  external_ip=$(curl -s http://ipecho.net/plain)
  echo -e "\e[1;35m" "External IP: " ${reset_terminal} ${external_ip}
#check DNS
  dns=$(cat /etc/resolv.conf  | grep nameserver | cut -d " " -f 2)
  echo -e "\e[1;35m" "DNS Servers: " ${reset_terminal} ${dns}
#check if connected to Internet
  connected=$(ping -c 2 www.bing.com > /dev/null && echo "Internet Connected" || echo "Internet Disconnected")
  echo -e "\e[1;35m" "Connected To Internet or Not: " ${reset_terminal} ${connected}

fi
