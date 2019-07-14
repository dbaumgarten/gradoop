FLINK_BIN="docker run --rm -it --net=host -v /home/daniel/projects/gradoop/gradoop-examples/target:/data -w /data -v /home/daniel/projects/gradoop/out:/out flink:1.7.2-hadoop24-scala_2.11 flink"
JAR="/data/gradoop-examples-0.5.0-SNAPSHOT.jar"
CLASS="org.gradoop.benchmark.layouting.LayoutingBenchmark"
INPUT="/datasets/add32.mtx;/datasets/CA-GrQc.txt;/datasets/p2p-Gnutella04.mtx"
OUTPUT="/out/cluster/"

#FLINK_BIN="/opt/flink-1.6.0/bin/flink"
#JAR="gradoop-examples-0.5.0-SNAPSHOT.jar"
#CLASS="org.gradoop.benchmark.layouting.LayoutingBenchmark"
#INPUT="hdfs:///ldbc/csv_gradoop_new/ldbc_1"
#OUTPUT="hdfs:///db32geta/ldbc_1-out/"

#--------------------------------------------------

PARALLELISM="4"
ARGS="FRLayouter 50 k=50.0;CentroidFRLayouter 50 k=50.0;GiLaLayouter 50 3 optimumDistance=50.0"

#--------------------------------------------------

IFS=';'
read -ra INPUTS <<< "$INPUT"
read -ra PARALLELISM <<< "$PARALLELISM"
read -ra ARGS <<< "$ARGS"
unset IFS


for INP in "${INPUTS[@]}"
do
  for P in "${PARALLELISM[@]}"
  do
    COMMAND="${FLINK_BIN} run -p $P -c ${CLASS} ${JAR} -i ${INP} -o ${OUTPUT} -f mtx -d -x image -m -s cre -b /out/cluster/benchmark.txt"

    for ARG in "${ARGS[@]}"
    do
      ${COMMAND} ${ARG}
      if [ "$?" != "0" ]; then
        echo "ERROR WHEN EXECUTING BENCHMARK. ABORTING."
        exit 1
      fi
    done

  done
done
