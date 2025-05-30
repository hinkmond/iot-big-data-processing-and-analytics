#!/usr/bin/env bash

# Allow other scripts to complete first before locking log file
sleep 6

date
DATESTR=`date +%Y%m%d%H%M%S`

LOGSLIST="`cd ~/logs; ls *.log | xargs`"

for LOGFILE in $LOGSLIST; do
  echo "  Cleaning...:" $HOME/logs/$LOGFILE
  (tail -5000 $HOME/logs/$LOGFILE > $HOME/logs/$LOGFILE-$DATESTR; mv -f $HOME/logs/$LOGFILE-$DATESTR $HOME/logs/$LOGFILE) &
done
exit

