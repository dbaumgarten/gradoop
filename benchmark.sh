#FLINK_BIN="docker run --rm -it --net=host -v /home/daniel/projects/gradoop/gradoop-examples/target:/data -w /data -v /home/daniel/projects/gradoop/out:/out flink:1.7.2-hadoop24-scala_2.11 flink"
#JAR="/data/gradoop-examples-0.5.0-SNAPSHOT.jar"
#CLASS="org.gradoop.benchmark.layouting.LayoutingBenchmark"
#INPUT="/datasets/add32.mtx;/datasets/CA-GrQc.txt;/datasets/p2p-Gnutella04.mtx"
#OUTPUT="/out/cluster/"

FLINK_BIN="/opt/flink-1.6.0/bin/flink"
JAR="gradoop-examples-0.5.0-SNAPSHOT.jar"
CLASS="org.gradoop.benchmark.layouting.LayoutingBenchmark"
#INPUT="hdfs:///db32geta/add32.mtx;hdfs:///db32geta/CA-GrQc.txt;hdfs:///db32geta/p2p-Gnutella04.mtx;hdfs:///db32geta/PGPgiantcompo.mtx;hdfs:///db32geta/ca-CondMat.mtx;hdfs:///db32geta/p2p-Gnutella31.mtx;hdfs:///db32geta/amazon0302.mtx;hdfs:///db32geta/om-Amazon.mtx;hdfs:///db32geta/com-DBLP.mtx;hdfs:///db32geta/roadNet-PA.mtx"
#INPUT="hdfs:///db32geta/p2p-Gnutella31.mtx;hdfs:///db32geta/amazon0302.mtx;hdfs:///db32geta/com-Amazon.mtx;hdfs:///db32geta/com-DBLP.mtx;hdfs:///db32geta/roadNet-PA.mtx"
INPUT="hdfs:///db32geta/p2p-Gnutella31.mtx"
OUTPUT="hdfs:///db32geta/mtx-out/"

#--------------------------------------------------

PARALLELISM="96"
ARGS="CentroidFRLayouter 50 k=300.0"

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
    COMMAND="${FLINK_BIN} run -p $P -c ${CLASS} ${JAR} -i ${INP} -o ${OUTPUT} -f mtx -d -x image -s lcre,eld -b benchmark.txt"

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
