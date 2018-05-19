#! /bin/bash
log="restart_component.log"
if [ $# -ne 3 ];then
  ./fds-ctrl.sh list | grep -v Component | grep -v ==== | cut -d " " -f 1 > component_list.txt
  array=($(awk '{print $1}' component_list.txt)) 
  else
  array=($(awk '{print $1}' $3))
fi   
for((round=1;round<=$1;round++));
do
echo "----------------round $round----------------" | tee -a $log
  for((i=0;i<${#array[@]};i++));
    do
       for((j=1;j<=$2;j++));
          do
             echo "`date`: unload component ${array[$i]} for the $j time" | tee -a $log
             ./fds-ctrl.sh unload ${array[$i]} 
			 status=$(./fds-ctrl.sh list | grep ${array[$i]} | awk -F" " '{print $2}')
			   if [ "$status" = "UNLOADED" ];then
                     echo "unload component ${array[$i]} success" | tee -a $log
                fi					 
             sleep 60
             if [ -f /var/lib/systemd/coredump/core* ];then
               echo "`date`: there is core dump file" | tee -a $log
			 else
			   echo "no core dump during unload component ${array[$i]}"
             fi
             echo "`date`: start component ${array[$i]} for the $j time" | tee -a $log
             ./fds-ctrl.sh start ${array[$i]} 
			 status=$(./fds-ctrl.sh list | grep ${array[$i]} | awk -F" " '{print $2}')
			   if [ "$status" = "ACTIVE" ];then
			        echo "start component ${array[$i]} success"  | tee -a $log
				fi
             sleep 60
             if [ -f /var/lib/systemd/coredump/core* ];then
               echo "`date`: there is core dump file" | tee -a $log
			 else 
			   echo "no core dump during start component ${array[$i]}"
             fi
         done
    done 
done

