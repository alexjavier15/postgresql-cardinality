#!/bin/bash

dir=/home_local/rivas/pgsql
dbase=tpch
echo "dir is $dir"
pgdata="$dir/data"
home="/home_local/rivas"
explain=false
results=$home/results_skew3
results_skew=$home/results_skew
durationsAll=$home/Durations
results2=$home/results3
psql="psql"
posmaster="postgres"
start='-c auto_explain.log_memo=off -c auto_explain.log_analyze=on -c auto_explain.log_min_duration=0 -c enable_cost_check=off -c enable_memo=off -c enable_explain_memo=on -c enable_memo_propagation=off'
injectionOff='-c auto_explain.log_memo=on -c auto_explain.log_analyze=on -c auto_explain.log_min_duration=0 -c enable_cost_check=off -c enable_memo=off -c enable_explain_memo=on -c enable_memo_propagation=off'
injectionOnPrOff="-c auto_explain.log_memo=on -c auto_explain.log_analyze=on -c auto_explain.log_min_duration=0 -c enable_cost_check=off -c enable_memo=on -c enable_explain_memo=on -c enable_memo_propagation=off $1"
injectionOnPrOn="-c auto_explain.log_memo=on -c auto_explain.log_analyze=on -c auto_explain.log_min_duration=0 -c enable_cost_check=off -c enable_memo=on -c enable_explain_memo=on -c enable_memo_propagation=on  $1"
injectionOnNotPr='-c auto_explain.log_memo=off -c auto_explain.log_analyze=on -c enable_memo=on -c enable_cost_check=off -c auto_explain.log_min_duration=0 -c enable_explain_memo=on'
queryprefix1="-f $home/10"
queryprefix2="-f $home/10e"
query=""
inj=false;
force=false
debug=false
FILE=$pgdata/durations.txt



function start_database {
	if [ "$force" == false ]; then
		$dir/bin/postgres -D $pgdata $start  &
		sleep 2
		$dir/bin/psql $dbase -c "DELETE FROM pg_statistic"
		sed -i '$ d' $pgdata/durations.txt

		sync; echo 3 | sudo tee /proc/sys/vm/drop_caches
			if [ -f $pgdata/durations.txt ] ;	then
				echo "removing duration files"
			    	#rm $pgdata/durations.txt
				echo "$i	" >> $pgdata/durations.txt
				rm $pgdata/joins.txt

			fi
				if [ ! -f $pgdata/joins.txt ];	then
			 	touch $pgdata/joins.txt;
			fi
		$dir/bin/pg_ctl -D $pgdata stop
		sleep 2
	fi
}

function execute_query {
	if [ "$inj" == false ]; then
		$dir/bin/postgres -D $pgdata $5 &
		echo "executing query $queryprefix1/$3"
		sleep 3
		$dir/bin/psql $dbase -l  $queryprefix1/$3 > /dev/null
		sync; echo 3 | sudo tee /proc/sys/vm/drop_caches
		sleep 2
		$dir/bin/pg_ctl -D $pgdata stop
		sleep 2;
		cat $FILE
		
		mv  $pgdata/memoTxt.xml $4/$1/plans/plan_"$1"'_'"$2".xml
		mv  $pgdata/memoTxt_debug.xml $4/$1/plans/plan_"$1"'_'"$2"_debug.xml
		cp $pgdata/memoTxt.txt $pgdata/memoTxt.txt.old
		mv  $pgdata/memoTxt.txt $4/$1/memos/memo_"$1"'_'"$2".txt
		cp $pgdata/joins.txt $4/$1/joins_"$1"'_'"$2".txt
		
		echo "done reinitializing postmaster"
		
		sleep 4
	fi
}
function execute_injection {
	$dir/bin/postgres -D $pgdata $3  &
	sleep 4
	if [ "$debug" == false ]; then
		if [ "$inj" == true ]; then
		cp   $5/$1/memos/memo_"$1"'_'"$2".txt  $pgdata/memoTxt.txt.old 
		cp   $5/$1/joins_"$1"'_'"$2".txt  $pgdata/joins.txt 
		fi

		if [ -f $pgdata/memoTxt.txt.old ] ;  then
			cp $pgdata/memoTxt.txt.old $pgdata/memoTxt.txt
		fi
		$dir/bin/psql $dbase -l $queryprefix1/$4 > /dev/null	
		echo "injection done .. "
		now=$(date +"%s")
		mv  $pgdata/memoTxt.txt $5/$1/memos/memo_"$1"'_'"$2$now".txt
		sync; echo 3 | sudo tee /proc/sys/vm/drop_caches
		mv  $pgdata/memoTxt.xml $5/$1/plans/new_plan_"$1"'_'"$2$now".xml
		mv  $pgdata/memoTxt_debug.xml $5/$1/plans/new_plan_"$1"'_'"$2$now"_debug.xml
	
		echo "stoping server.."
		$dir/bin/pg_ctl -D $pgdata stop
		echo "*** File - $FILE contents ***"
		cat $FILE
		sleep 5
	else 
	$dir/bin/psql tpch
	fi
}




if [ ! -d $results ]  ; then

	mkdir $results
	fi

if [ ! -d $results_skew ]  ; then

	mkdir $results_skew

fi 
case "$2" in
        -f)
            force=true
		echo "force mode"
	;;
	-i)
            inj=true
		echo "INJECTION mode"

	;;
	-e)

            force=false
		echo "force mode"
	;;
	*)
	force=false;
            # unknown option
	;;
esac

 sync; echo 3 | sudo tee /proc/sys/vm/drop_caches;

#for k in 1 2
#	do #
#	if [ -f $pgdata/durations.txt ] ; then
	#	rm $pgata/durations.txt 
#	fi
# 2 4 5 6 7 8 9 10 11 12 13 14 16 17 
	for i in 22 4 5 6 7 8 9 10 11 19

#6 7 8 9 10 11 12 13 14 16 18 19 21 22
#  

		do


		if [ ! -d $results/$i ] ; 	then
			echo "creating folders for query $i"
			mkdir $results/$i
		fi
		if [ ! -d $results/$i/plans ] ; 	then
			mkdir $results/$i/plans
		fi
		if [ ! -d $results/$i/durations ] ; 	then
			mkdir $results/$i/durations
		fi
		if [ ! -d $results/"$i"/memos ] ; then
			mkdir $results/"$i"/memos
		fi


		for j in 1
			do
			query="$i"'_'"$j"'.sql'
			start_database
			execute_query $i $j "$query" "$results" "$injectionOff"
			execute_injection $i $j "$injectionOnPrOff" "$query" "$results"
	 		execute_injection $i $j "$injectionOnPrOn" "$query" "$results"
			execute_injection $i $j "$injectionOnPrOff -c enable_selectivity_injection=on" "$query" "$results"
	 		execute_injection $i $j "$injectionOnPrOn -c enable_selectivity_injection=on" "$query" "$results"
			execute_injection $i $j "$injectionOnPrOff -c enable_memo_recosting=on" "$query" "$results"
	 		execute_injection $i $j "$injectionOnPrOn -c enable_memo_recosting=on" "$query" "$results"
			execute_injection $i $j "$injectionOnPrOff -c enable_selectivity_injection=on -c enable_memo_recosting=on" "$query" "$results"
	 		execute_injection $i $j "$injectionOnPrOn -c enable_selectivity_injection=on -c enable_memo_recosting=on" "$query" "$results"

			cp $pgdata/durations.txt $results/$i/durations/"$query.txt"
			rm $pgdata/memoTxt.txt.old
			done
		echo "injection done for queries $i.. moving file "





	done
mv  $pgdata/durations.txt $durationsAll/tpch_skew3.txt



#done



