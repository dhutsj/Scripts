#! /bin/bash

#set -xe
judge_args()
{
  if [[ $# == 2 ]]; then
     echo $1
     echo $2
     echo "pass"
  else
     echo "fail"
  fi
}

judge_args2()
{
  if [[ $# == 2 ]]; then
     for arg in $@;
         do
            echo ${arg}
         done
     echo "pass"
  else
     echo "fail"
  fi
}

judge_args $1 $2     
judge_args2 $3 $4
