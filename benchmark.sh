FLINK_BIN="/opt/flink-1.6.0/bin/flink"
JAR="gradoop-examples-0.5.0-SNAPSHOT.jar"
CLASS="org.gradoop.benchmark.layouting.LayoutingBenchmark"
INPUT="hdfs:///ldbc/csv_gradoop_new/ldbc_1"
OUTPUT="hdfs:///db32geta/ldbc_1-out/"

#--------------------------------------------------

ALGORITHMS="FRLayouter"
PARALLELISM="96"
ARGS="25 4000;50 4000"

#--------------------------------------------------

IFS=';'
read -ra ALGORITHMS <<< "$ALGORITHMS"
read -ra PARALLELISM <<< "$PARALLELISM"
read -ra ARGS <<< "$ARGS"
unset IFS


for ALGO in "${ALGORITHMS[@]}"
do
  for P in "${PARALLELISM[@]}"
  do
    COMMAND="${FLINK_BIN} run -p $P -c ${CLASS} ${JAR} -i ${INPUT} -o ${OUTPUT} -f csv -d -x image -m -s cre -b benchmark.txt"

    for ARG in "${ARGS[@]}"
    do
      echo ${COMMAND} $ALGO ${ARG}
    done

  done
done
