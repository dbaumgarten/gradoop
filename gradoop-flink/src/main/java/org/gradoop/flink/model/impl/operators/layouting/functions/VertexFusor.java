/*
 * Copyright © 2014 - 2019 Leipzig University (Database Research Group)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.gradoop.flink.model.impl.operators.layouting.functions;

import org.apache.flink.api.common.functions.FlatJoinFunction;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.util.Collector;
import org.gradoop.flink.model.impl.operators.layouting.util.LEdge;
import org.gradoop.flink.model.impl.operators.layouting.util.LVertex;
import org.gradoop.flink.model.impl.operators.layouting.util.Vector;

import java.util.Random;

public class VertexFusor {
  protected VertexCompareFunction compareFunction;
  protected double threshold;

  public VertexFusor(VertexCompareFunction compareFunction, double treschold) {
    this.compareFunction = compareFunction;
    this.threshold = treschold;
  }

  public Tuple2<DataSet<LVertex>,DataSet<LEdge>> execute(DataSet<LVertex> vertices,
    DataSet<LEdge> edges){

    final Random rng = new Random();
    DataSet<Tuple2<LVertex, Boolean>> classifiedVertices = vertices.map(v->new Tuple2<>(v,
      rng.nextBoolean())).returns(new TypeHint<Tuple2<LVertex, Boolean>>() {
    });


    DataSet<Tuple2<LVertex, LVertex>> fusions =
      edges.join(classifiedVertices).where(LEdge.SOURCE_ID).equalTo("0."+LVertex.ID)
      .join(classifiedVertices)
      .where("0."+LEdge.TARGET_ID).equalTo("0."+LVertex.ID)
      .with(new CandidateGenerator(compareFunction,threshold))
      .groupBy(0).reduce((a,b)->(a.f2 > b.f2)?a:b)
      .map(c->new Tuple2<>(c.f0,c.f1)).returns(new TypeHint<Tuple2<LVertex, LVertex>>() {
      });


    DataSet<LVertex> superVertices = fusions.groupBy(1).reduceGroup(new SuperVertexGenerator());

    DataSet<LVertex> remainingVertices =
      vertices.leftOuterJoin(superVertices).where(LVertex.ID).equalTo(LVertex.ID).with(new FlatJoinFunction<LVertex, LVertex, LVertex>() {
      @Override
      public void join(LVertex lVertex, LVertex lVertex2, Collector<LVertex> collector) {
        if (lVertex2 == null){
          collector.collect(lVertex);
        }
      }
    });

    remainingVertices =
      remainingVertices.leftOuterJoin(fusions).where(LVertex.ID).equalTo("0."+LVertex.ID).with(new FlatJoinFunction<LVertex, Tuple2<LVertex, LVertex>, LVertex>() {
        @Override
        public void join(LVertex lVertex,
          Tuple2<LVertex, LVertex> lVertexLVertexDoubleTuple3,
          Collector<LVertex> collector){
          if (lVertexLVertexDoubleTuple3 == null){
            collector.collect(lVertex);
          }
        }
      });

    vertices = remainingVertices.union(superVertices);


    edges =
      edges.leftOuterJoin(fusions).where(LEdge.SOURCE_ID).equalTo("0."+LVertex.ID).with(new JoinFunction<LEdge,
        Tuple2<LVertex, LVertex>, LEdge>() {
      @Override
      public LEdge join(LEdge lEdge,
        Tuple2<LVertex, LVertex> lVertexLVertexDoubleTuple3) {
        if (lVertexLVertexDoubleTuple3 != null) {
          lEdge.setSourceId(lVertexLVertexDoubleTuple3.f1.getId());
        }
        return lEdge;
      }
    });

    edges =
      edges.leftOuterJoin(fusions).where(LEdge.TARGET_ID).equalTo("0."+LVertex.ID).with(new JoinFunction<LEdge,
        Tuple2<LVertex, LVertex>, LEdge>() {
      @Override
      public LEdge join(LEdge lEdge,
        Tuple2<LVertex, LVertex> lVertexLVertexDoubleTuple3) {
        if (lVertexLVertexDoubleTuple3 != null) {
          lEdge.setTargetId(lVertexLVertexDoubleTuple3.f1.getId());
        }
        return lEdge;
      }
    });

    edges = edges.groupBy(LEdge.SOURCE_ID,LEdge.TARGET_ID).reduce((a,b)->{
      a.setCount(a.getCount()+b.getCount());
      return a;
    });

    return new Tuple2<>(vertices,edges);
  }

  protected static class CandidateGenerator implements FlatJoinFunction<Tuple2<LEdge,
    Tuple2<LVertex, Boolean>>, Tuple2<LVertex, Boolean>, Tuple3<LVertex,LVertex,Double>>{

    VertexCompareFunction cf;
    Double treshold;

    public CandidateGenerator(VertexCompareFunction cf, Double treshold) {
      this.cf = cf;
      this.treshold = treshold;
    }

    @Override
    public void join(Tuple2<LEdge, Tuple2<LVertex, Boolean>> source,
      Tuple2<LVertex, Boolean> target,
      Collector<Tuple3<LVertex, LVertex, Double>> collector) throws Exception {

      LVertex sourceVertex = source.f1.f0;
      boolean sourceType = source.f1.f1;

      LVertex targetVertex = target.f0;
      boolean targetType = target.f1;

      if ( sourceType == targetType){
        return;
      }

      Double similarity = cf.compare(sourceVertex,targetVertex);

      if (similarity < treshold){
        return;
      }

      if (targetType){
        collector.collect(new Tuple3<>(sourceVertex,targetVertex,similarity));
      }else{
        collector.collect(new Tuple3<>(targetVertex,sourceVertex,similarity));
      }

    }
  }

  protected static class SuperVertexGenerator implements GroupReduceFunction<Tuple2<LVertex,
    LVertex>, LVertex>{
    @Override
    public void reduce(Iterable<Tuple2<LVertex, LVertex>> iterable,
      Collector<LVertex> collector) throws Exception {
      int count = 0;
      Vector positionSum = new Vector();
      LVertex self = null;

      for (Tuple2<LVertex, LVertex> t : iterable) {
        if (count == 0){
          self = t.f1;
          count = t.f1.getCount();
          positionSum.mAdd(t.f1.getPosition());
          positionSum.mMul(t.f1.getCount());

        }
        count+=t.f0.getCount();
        positionSum.mAdd(t.f0.getPosition().mMul(t.f0.getCount()));
      }

      self.setPosition(positionSum.mDiv(count));
      self.setCount(count);

      collector.collect(self);
    }
  }

}
