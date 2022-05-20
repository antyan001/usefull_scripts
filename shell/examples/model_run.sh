#!/bin/bash

MODEL_PATH='/home/zhernokleev-ga_ca-sbrf-ru/ip_identification/'
LOCK_PATH='/tmp/privl_locks'
LOCK_FILE=' ip_lock.lock'
cd $MODEL_PATH

cat /home/$(whoami)/pass/userpswrd | kinit

auto=$(ps -fu $USER | grep "python" | grep "data_pipeline.py" | wc -c)
lock=$(ls $LOCK_PATH | grep $LOCK_FILE | wc -c)


if [ $auto -eq 0 ] && [ $lock -ne 0 ] ; 
    then
    /usr/bin/screen -L -D -m /home/zhernokleev-ga_ca-sbrf-ru/bin/python36 /home/zhernokleev-ga_ca-sbrf-ru/ip_identification/data_pipeline.py 
fi
# */1 * * * * /usr/bin/flock -w 0 /tmp/privl_locks/auto.lock /usr/bin/screen -L -D -m /home/zhernokleev-ga_ca-sbrf-ru/bin/python36 /home/zhernokleev-ga_ca-sbrf-ru/privlechenie/auto.py
# */1 * * * * /usr/bin/flock -w 0 /tmp/privl_locks/mmb.lock /usr/bin/screen -L -D -m /home/zhernokleev-ga_ca-sbrf-ru/bin/python36 /home/zhernokleev-ga_ca-sbrf-ru/gp3_mmb/gp3_final_mmb.py



