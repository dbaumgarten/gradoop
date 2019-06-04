FLINK_BIN="docker run --rm -it --net=host -v /home/daniel/projects/gradoop/gradoop-examples/target:/data -w /data -v /home/daniel/projects/gradoop/out:/out flink:1.7.2-hadoop24-scala_2.11 flink"

JAR="/data/gradoop-examples-0.5.0-SNAPSHOT.jar"

CLASS="org.gradoop.benchmark.layouting.LayoutingBenchmark"

INPUT="/datasets/facebook_gradoop_csv"

OUTPUT="/out/facebook/"

#--------------------------------------------------

ALGORITHMS="0"
PARALLELISM="2"
ITERATIONS="1,10,20"
CELLSIZE="333"

#--------------------------------------------------

IFS=',' 
read -ra PARALLELISM <<< "$PARALLELISM"
read -ra ITERATIONS <<< "$ITERATIONS"
read -ra ALGORITHMS <<< "$ALGORITHMS"
read -ra CELLSIZE <<< "$CELLSIZE"
unset IFS


for A in "${ALGORITHMS[@]}"
do
  for P in "${PARALLELISM[@]}"
  do
    COMMAND="${FLINK_BIN} run -p $P -c ${CLASS} ${JAR} -i ${INPUT} -o ${OUTPUT} -f lgcsv"

    if [ "$A" == "0" ]; then
      echo A:$A P:$P
      ARGS="-a 0 10000 10000"
      ${COMMAND} ${ARGS}
    fi

    if [ "$A" == "1" ]; then
      echo A:$A P:$P I:$I
      ARGS="-a 1 150 1 10000 10000"
      ${COMMAND} ${ARGS}
    fi

    if [ "$A" == "2" ]; then
      for I in "${ITERATIONS[@]}"
      do
        for C in "${CELLSIZE[@]}"
        do
          echo A:$A P:$P I:$I C:$C
          ARGS="-a 2 150 $I 10000 10000 $C"
          ${COMMAND} ${ARGS}
        done
      done
    fi
  done
done
