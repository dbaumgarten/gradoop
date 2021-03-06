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
package org.gradoop.flink.model.impl.operators.matching.transactional.function;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.gradoop.common.model.api.entities.GraphHead;
import org.gradoop.common.model.api.entities.GraphHeadFactory;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.properties.Properties;

/**
 * Initializes a new graph head from a given GradoopId and its lineage information, e.g. the
 * source graph this one was created from.
 *
 * @param <G> graph head type
 */
public class InitGraphHeadWithLineage<G extends GraphHead>
  implements MapFunction<Tuple2<GradoopId, GradoopId>, G>, ResultTypeQueryable<G> {
  /**
   * GraphHeadFactory
   */
  private final GraphHeadFactory<G> graphHeadFactory;

  /**
   * Constructor
   *
   * @param epgmGraphHeadFactory graph head factory
   */
  public InitGraphHeadWithLineage(GraphHeadFactory<G> epgmGraphHeadFactory) {
    this.graphHeadFactory = epgmGraphHeadFactory;
  }

  @Override
  public G map(Tuple2<GradoopId, GradoopId> idTuple) {
    G head = graphHeadFactory.initGraphHead(idTuple.f0);
    Properties properties = Properties.createWithCapacity(1);
    properties.set("lineage", idTuple.f1);
    head.setProperties(properties);
    return head;
  }

  @Override
  public TypeInformation<G> getProducedType() {
    return TypeExtractor.createTypeInfo(graphHeadFactory.getType());
  }
}
