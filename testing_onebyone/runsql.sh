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

CURRENT_TIMESTAMP=`date +%s`

IMPALA_SERVER=$1
QUERY_DIRECTORY=$2
DATABASE_NAME=$3
QUERY_NAME=$4
PROFILE_ONLY="all"
EXECUTION_OUTPUT_DIRECTORY_PREFIX=$6
if [ $# -eq 6 ]; then
	PROFILE_ONLY=$5	
fi
echo "Gather ONLY PROFILE or ALL? -> [${PROFILE_ONLY}]"


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
echo "start executing query"
impala-shell -i "${IMPALA_SERVER}:21000" -d ${DATABASE_NAME} -f $QUERY_DIRECTORY/${QUERY_NAME} &> $PROFILE_LOG_DIRECTORY/${QUERY_NAME}.log
echo "output folder location: ${EXECUTION_OUTPUT_DIRECTORY}"
EXECUTION_TIME=`cat $PROFILE_LOG_DIRECTORY/${QUERY_NAME}.log | grep Fetched`
TIMELINE=`cat $PROFILE_LOG_DIRECTORY/${QUERY_NAME}.log | grep 'Query Timeline'`
RemoteFragmentStarted=`cat $PROFILE_LOG_DIRECTORY/${QUERY_NAME}.log | grep 'Remote fragments started'`
echo "CURRENT_TIMESTAMP:${CURRENT_TIMESTAMP}"
echo "execution result1 -> $QUERY_NAME;$TIMELINE"
echo "execution result2 -> $QUERY_NAME;$RemoteFragmentStarted"
echo $EXECUTION_TIME >> $PROFILE_LOG_DIRECTORY/time.log
