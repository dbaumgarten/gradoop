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
package org.gradoop.flink.model.impl.operators.sampling.functions;

import org.apache.flink.api.common.functions.CrossFunction;
import org.gradoop.common.model.impl.pojo.EPGMGraphHead;
import org.gradoop.common.model.impl.pojo.EPGMVertex;
import org.gradoop.flink.model.impl.operators.sampling.common.SamplingConstants;

/**
 * Writes the PageRank-scores stored in the graphHead to all vertices.
 */
public class AddPageRankScoresToVertexCrossFunction
  implements CrossFunction<EPGMVertex, EPGMGraphHead, EPGMVertex> {

  @Override
  public EPGMVertex cross(EPGMVertex vertex, EPGMGraphHead graphHead) {
    double min = graphHead.getPropertyValue(
      SamplingConstants.MIN_PAGE_RANK_SCORE_PROPERTY_KEY).getDouble();
    double max = graphHead.getPropertyValue(
      SamplingConstants.MAX_PAGE_RANK_SCORE_PROPERTY_KEY).getDouble();
    double sum = graphHead.getPropertyValue(
      SamplingConstants.SUM_PAGE_RANK_SCORE_PROPERTY_KEY).getDouble();

    vertex.setProperty(SamplingConstants.MIN_PAGE_RANK_SCORE_PROPERTY_KEY, min);
    vertex.setProperty(SamplingConstants.MAX_PAGE_RANK_SCORE_PROPERTY_KEY, max);
    vertex.setProperty(SamplingConstants.SUM_PAGE_RANK_SCORE_PROPERTY_KEY, sum);
    vertex.setProperty("vertexCount", graphHead.getPropertyValue("vertexCount"));

    if (min != max) {
      double score = vertex.getPropertyValue(
        SamplingConstants.PAGE_RANK_SCORE_PROPERTY_KEY).getDouble();
      vertex.setProperty(
        SamplingConstants.SCALED_PAGE_RANK_SCORE_PROPERTY_KEY, (score - min) / (max - min));
    }

    return vertex;
  }
}
