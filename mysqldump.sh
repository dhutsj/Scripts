#! /bin/bash
mysqluser=root
mysqlpasswd=linux
mysqldump()
{
echo " 1) Backup all the databases"
echo " 2) Backup specific database"
echo " 3) Backup specific tables"
read -p "Please choose your option in 1|2|3: " option
case $option in 
1) echo "You choose to backup all databases"
   echo "Backup job is running in backend, you can use jobs command to check it"
   mysqldump -u${mysqluser} -p${mysqlpasswd} --all-databases > all.sql &
;;
2) echo "You choose to backup specific database"
   read -p "Please input the database's name you want to backup " dbname
   echo "Backup job is running in backend, you can use jobs command to check it"
   mysqldump -u${mysqluser} -p${mysqlpasswd} --databases ${dbname} > ${dbname}.sql &
;;
3) echo "You choose to backup specific tables"
   read -p "Please input the database's and table's name you want to backup " dbname tbname
   echo "Backup job is running in backend, you can use jobs command to check it"
   mysqldump -u${mysqluser} -p${mysqlpasswd} ${dbname} ${tbname} > ${tbname}.sql &
;;
*) echo "Wrong input";;
esac
}
mysqldump
