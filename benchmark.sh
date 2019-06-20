FLINK_BIN="/opt/flink-1.6.0/bin/flink"

JAR="gradoop-examples-0.5.0-SNAPSHOT.jar"

CLASS="org.gradoop.benchmark.layouting.LayoutingBenchmark"

INPUT="hdfs:///ldbc/csv_gradoop_new/ldbc_1"

OUTPUT="hdfs:///db32geta/ldbc_1-out/"

#--------------------------------------------------

#ALGORITHMS="0,1,2"
#PARALLELISM="6,12,24,48,96"
#ITERATIONS="1,25,50,100"
#CELLSIZE="333"

ALGORITHMS="2"
PARALLELISM="96"
ITERATIONS="1,3,5,7,10,15"
CELLSIZE="15"

SIZE=0
K=0
VERTCOUNT=4000


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
    COMMAND="${FLINK_BIN} run -p $P -c ${CLASS} ${JAR} -i ${INPUT} -o ${OUTPUT} -f csv -d -x image -m -n -b benchmark.txt -a $A"

    if [ "$A" == "0" ]; then
      echo A:$A P:$P
      ${COMMAND} 0 $SIZE $SIZE
    fi

    if [ "$A" == "1" ]; then
      echo A:$A P:$P I:$I
      ${COMMAND} $VERTCOUNT $SIZE $SIZE 1 
    fi

    if [ "$A" == "2" ]; then
      for I in "${ITERATIONS[@]}"
      do
        for C in "${CELLSIZE[@]}"
        do
          echo A:$A P:$P I:$I C:$C
          ARGS="$VERTCOUNT $SIZE $SIZE $I $K $C"
          ${COMMAND} ${ARGS}
        done
      done
    fi
  done
done
