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
package org.gradoop.flink.model.impl.functions.graphcontainment;

import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.gradoop.common.model.impl.pojo.EPGMGraphElement;

/**
 * True, if an element is not contained in a given graph.
 *
 * @param <GE> element type
 */
@FunctionAnnotation.ReadFields("graphIds")
public class NotInGraphBroadcast<GE extends EPGMGraphElement>
  extends GraphContainmentFilterBroadcast<GE> {

  @Override
  public boolean filter(GE element) throws Exception {
    return !element.getGraphIds().contains(graphId);
  }
}
