#/bin/bash

rm pgsql/data/durations.txt
touch pgsql/data/durations.txt
./script2conv.sh "-c enable_memo_recosting=off" -i
#./script10.sh 
#mv Durations Durations3
#cp -r results results4
#mv results_skew results_skew2

#mkdir Durations

#rm pgsql/data/durations.txt
#touch pgsql/data/durations.txt
rm pgsql/data/durations.txt
touch pgsql/data/durations.txt
#./script10conv.sh -i
./script10.sh "-c enable_memo_recosting=off" -i
