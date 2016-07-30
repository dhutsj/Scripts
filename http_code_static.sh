####################################################################
# File Name: http_code.sh
# Author: Justin
# mail: dhutsj@gmail.com
# Created Time: Fri 29 Jul 2016 09:07:27 PM PDT
# ================================================================
#!/bin/bash
reset_terminal=`tput sgr0`
log_path="/var/log/apache2/access.log"
check_http_code()
{
    http_code=(`cat ${log_path} | grep -ioE "HTTP\/1\.1\"[[:blank:]][0-9]{3}" | awk -v total=0 -F "[ ]"+ '{
	    if($2!="" && $2>100 && $2<200)
	       {i++}
	    else if($2>=200 && $2<300)
	       {j++}
	    else if($2>=300 && $2<400)
	       {k++}
	    else if($2>=400 && $2<500)
	       {l++}
	    else if($2>=500)
	       {m++}
    }END{
     print i?i:0,j?j:0,k?k:0,l?l:0,m?m:0,i+j+k+l+m
    }'
`)
 echo -e "\e[1;35m" "The number of http code between 100 and 200: " ${reset_terminal} ${http_code[0]}
 echo -e "\e[1;35m" "The number of http code between 200 and 300: " ${reset_terminal} ${http_code[1]}
 echo -e "\e[1;35m" "The number of http code between 300 and 400: " ${reset_terminal} ${http_code[2]}
 echo -e "\e[1;35m" "The number of http code between 400 and 500: " ${reset_terminal} ${http_code[3]}
 echo -e "\e[1;35m" "The number of http code greater then 500: " ${reset_terminal} ${http_code[4]}
 echo -e "\e[1;35m" "The toatl number: " ${reset_terminal} ${http_code[5]}

}

check_http_code
