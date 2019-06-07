package org.gradoop.flink.model.impl.operators.layouting.util;

import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.Edge;

public class LEdge {
  private GradoopId id;
  private GradoopId sourceId;
  private GradoopId targetId;
  private Vector sourcePosition;
  private Vector targetPosition;

  public LEdge(GradoopId id, GradoopId sourceId, GradoopId targetId, Vector sourcePosition,
    Vector targetPosition) {
    this.id = id;
    this.sourceId = sourceId;
    this.targetId = targetId;
    this.sourcePosition = sourcePosition;
    this.targetPosition = targetPosition;
  }

  public LEdge(Edge e){
    id = e.getId();
    sourceId = e.getSourceId();
    targetId = e.getTargetId();
  }

  public LEdge(){

  }

  /**
   * Gets id
   *
   * @return value of id
   */
  public GradoopId getId() {
    return id;
  }

  /**
   * Sets id
   *
   * @param id the new value
   */
  public void setId(GradoopId id) {
    this.id = id;
  }

  /**
   * Gets sourceId
   *
   * @return value of sourceId
   */
  public GradoopId getSourceId() {
    return sourceId;
  }

  /**
   * Sets sourceId
   *
   * @param sourceId the new value
   */
  public void setSourceId(GradoopId sourceId) {
    this.sourceId = sourceId;
  }

  /**
   * Gets targetId
   *
   * @return value of targetId
   */
  public GradoopId getTargetId() {
    return targetId;
  }

  /**
   * Sets targetId
   *
   * @param targetId the new value
   */
  public void setTargetId(GradoopId targetId) {
    this.targetId = targetId;
  }

  /**
   * Gets sourcePosition
   *
   * @return value of sourcePosition
   */
  public Vector getSourcePosition() {
    return sourcePosition;
  }

  /**
   * Sets sourcePosition
   *
   * @param sourcePosition the new value
   */
  public void setSourcePosition(Vector sourcePosition) {
    this.sourcePosition = sourcePosition;
  }

  /**
   * Gets targetPosition
   *
   * @return value of targetPosition
   */
  public Vector getTargetPosition() {
    return targetPosition;
  }

  /**
   * Sets targetPosition
   *
   * @param targetPosition the new value
   */
  public void setTargetPosition(Vector targetPosition) {
    this.targetPosition = targetPosition;
  }
}
