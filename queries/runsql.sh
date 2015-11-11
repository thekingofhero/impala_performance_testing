#
#   Impala Query Execution Script
#   The purpose of this script is to run impala queries and gather profile, logs and OS metrics into a predefined location
#   The script takes three arguments as input, i.e., 
#     impala server -> the ip or server name of the impala server;
#     database name -> one of the existing database name of Impala, the query is assumed to be executed on this database;
#     query name    -> the name of the file containing the actual query file; (queries should be stored in directory QUERY_DIRECTORY)
#
#    Impala Query Directory: the location of impala queries (specified in QUERY_DIRECTORY variable)
#    Impala Log Directory: the location of impala logs, it is assumed that impala folder location are same on all nodes (specified in IMPALA_LOG_DIRECTORY variable)
#    Execution Output Directory: all outputs, i.e., logs, NMONs and profile, are stored in the location specified by the value of EXECUTION_OUTPUT_DIRECTORY variable
#
#    NOTE: The above three directory locations have to be updated within the script.
#
#
if [ $# -lt 6 ]; then
	echo "Usage: runsql.sh [impala server] [query directory] [database name] [query name] [profile|all] [profile_logs_dir]"
	exit 1
fi
CLIENT=tracing017
IMPALA_NODES=(tracing024 tracing025 tracing026 tracing027)

CURRENT_TIMESTAMP=`date +%s`

IMPALA_SERVER=$1
QUERY_DIRECTORY=$2
DATABASE_NAME=$3
QUERY_NAME=$4
PROFILE_ONLY="all"
if [ $# -eq 6 ]; then
	PROFILE_ONLY=$5	
fi
echo "Gather ONLY PROFILE or ALL? -> [${PROFILE_ONLY}]"

#
# Update the following variables if different locations are required
#
IMPALAD_NMON_LOG_DIRECTORY="/home/wangdewei/nmon_log"
IMPALA_LOG_DIRECTORY="/home/wangdewei/impaladlog"
#EXECUTION_OUTPUT_DIRECTORY_PREFIX="/home/shihongl/queries/logs"
EXECUTION_OUTPUT_DIRECTORY_PREFIX=$6
NMON_LOCATION="~/mNMON"

if [ ! -d "$DIRECTORY" ]; then
	mkdir -p $QUERY_DIRECTORY
fi

EXECUTION_OUTPUT_DIRECTORY=${EXECUTION_OUTPUT_DIRECTORY_PREFIX}/${QUERY_NAME}_${CURRENT_TIMESTAMP}
if [ ! -d "$DIRECTORY" ]; then
	mkdir -p $EXECUTION_OUTPUT_DIRECTORY
fi

PROFILE_LOG_DIRECTORY=${EXECUTION_OUTPUT_DIRECTORY}/profiles
if [ ! -d "$DIRECTORY" ]; then
	mkdir -p $PROFILE_LOG_DIRECTORY
fi

NMON_LOG_DIRECTORY=${EXECUTION_OUTPUT_DIRECTORY}/nmon
if [ ! -d "$DIRECTORY" ]; then
	mkdir -p $NMON_LOG_DIRECTORY
fi
IMPALAD_LOG_DIRECTORY=${EXECUTION_OUTPUT_DIRECTORY}/impalad
if [ ! -d "$DIRECTORY" ]; then
	mkdir -p $IMPALAD_LOG_DIRECTORY
fi

echo "database name: ${DATABASE_NAME}"
echo "query name: ${QUERY_NAME}"
echo "query directory: ${QUERY_DIRECTORY}"
echo "query log directory: ${EXECUTION_OUTPUT_DIRECTORY}"
echo "profile log directory: ${PROFILE_LOG_DIRECTORY}"
echo "nmon log directory: ${NMON_LOG_DIRECTORY}"

#
# We need to clear OS cache on all nodes within the cluster
#
sync

echo "clear OS cache on tracing017"
free && sync && echo 3 >/proc/sys/vm/drop_caches && free

echo "clear OS cache on tracing024"
ssh tracing024 "free && sync && echo 3 >/proc/sys/vm/drop_caches && free"

echo "clear OS cache on tracing025"
ssh tracing025 "free && sync && echo 3 >/proc/sys/vm/drop_caches && free"

echo "clear OS cache on tracing026"
ssh tracing026 "free && sync && echo 3 >/proc/sys/vm/drop_caches && free"

echo "clear OS cache on tracing027"
ssh tracing027 "free && sync && echo 3 >/proc/sys/vm/drop_caches && free"

echo "start executing query"
if [ "$PROFILE_ONLY" = "all" ]; then
	# launch NMON
	for impalad in ${IMPALA_NODES[@]}
    do
        ssh $impalad "ntpdate 172.18.0.17 &> /etc/null;mkdir -p ${IMPALAD_NMON_LOG_DIRECTORY};rm -rf  ${IMPALAD_NMON_LOG_DIRECTORY}/*"
        ssh $impalad "/home/chenwenwen/nmon_linux_x86_64 -s 1 -c 36000 -f -m ${IMPALAD_NMON_LOG_DIRECTORY}"
        
        ssh $impalad "ntpdate 172.18.0.17 &> /etc/null;mkdir -p ${IMPALA_LOG_DIRECTORY};rm -rf  ${IMPALA_LOG_DIRECTORY}/*"
    done
fi

impala-shell -i "${IMPALA_SERVER}:21000" -d ${DATABASE_NAME} -f $QUERY_DIRECTORY/${QUERY_NAME} &> $PROFILE_LOG_DIRECTORY/${QUERY_NAME}.log
if [ "$PROFILE_ONLY" = "all" ]; then
    for impalad in ${IMPALA_NODES[@]}
    do
        ssh $impalad 'sh /home/chenwenwen/stop_nmon.sh &>/etc/null'
        ssh $impalad "python /home/wangdewei/extractImpaladLog.py ${CURRENT_TIMESTAMP} &>/etc/null"
        scp  ${impalad}:${IMPALA_LOG_DIRECTORY}/impaladlog ${CLIENT}:$IMPALAD_LOG_DIRECTORY/${QUERY_NAME}.${impalad}.impaladlog
        scp  ${impalad}:${IMPALAD_NMON_LOG_DIRECTORY}/* ${CLIENT}:$NMON_LOG_DIRECTORY
    done
fi
 
echo "output folder location: ${EXECUTION_OUTPUT_DIRECTORY}"

EXECUTION_TIME=`cat $PROFILE_LOG_DIRECTORY/${QUERY_NAME}.log | grep Fetched`
TIMELINE=`cat $PROFILE_LOG_DIRECTORY/${QUERY_NAME}.log | grep 'Query Timeline'`
RemoteFragmentStarted=`cat $PROFILE_LOG_DIRECTORY/${QUERY_NAME}.log | grep 'Remote fragments started'`
# echo "execution result -> $QUERY_NAME; $EXECUTION_TIME"
echo "CURRENT_TIMESTAMP:${CURRENT_TIMESTAMP}"
echo "execution result1 -> $QUERY_NAME;$TIMELINE"
echo "execution result2 -> $QUERY_NAME;$RemoteFragmentStarted"
echo $EXECUTION_TIME >> $PROFILE_LOG_DIRECTORY/time.log
