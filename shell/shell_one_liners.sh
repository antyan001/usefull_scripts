#!/bin/bash

# kill python script
ps -fu $USER|grep "python36"|grep "script_name.py"|awk '{print $2}'|xargs kill

# kill yarn application
yarn application --list|grep "context_name"|awk '{print $1}'|xargs yarn application --kill

# list all user's groups (one per line) and highlight grep pattern matches
for i in $(groups); do echo "- $i"; done | grep --color -e "card\|$" -e "cod\|$" -e "superapp\|$"

# cron+flock
### Flock util is used for managing cron job's 
### Flock util create *.lock file that will be removed only after job's completion
### EXAMPLE: when you're not sure, whether your previous job has been finished, you can enable
### -w 0 to prevent your next job to be started (in this case, new job will wait for 0 sec and will be killed after).
### In other case you will breed plenty of new jobs waiting for their turn to run.
### USAGE: 1 - cron schedule, 2 - flock util, 3 - lock-file name, 4 - run your job
### HINT: For a sake of robustness use full pathes for all your scripts and unix utils (screen, python parcells, etc.)
*/1 * * * * /usr/bin/flock -w 0 /tmp/privl_locks/mmb.lock /usr/bin/screen -L -D -m /home/zhernokleev-ga_ca-sbrf-ru/bin/python36 /home/zhernokleev-ga_ca-sbrf-ru/gp3_mmb/gp3_final_mmb.py

# remove already tracked filed but added to .gitignore
git rm -r --cached . && git add . && git commit -am "Remove ignored files"