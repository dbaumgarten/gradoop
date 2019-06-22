package org.gradoop.flink.model.impl.operators.layouting.util;

import org.apache.flink.api.java.tuple.Tuple5;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.Edge;

public class LEdge extends Tuple5<GradoopId, GradoopId, GradoopId, Vector, Vector> {

  /**
   * Position of the ID-property in the tuple
   */
  public static final int ID = 0;

  /**
   * Position of the sourceId-property in the tuple
   */
  public static final int SOURCE_ID = 1;
  /**
   * Position of the targetId-property in the tuple
   */
  public static final int TARGET_ID = 2;

  public LEdge(GradoopId id, GradoopId sourceId, GradoopId targetId, Vector sourcePosition,
    Vector targetPosition) {
    this.f0 = id;
    this.f1 = sourceId;
    this.f2 = targetId;
    this.f3 = sourcePosition;
    this.f4 = targetPosition;
  }

  public LEdge(Edge e) {
    super(e.getId(), e.getSourceId(), e.getTargetId(), new Vector(0, 0), new Vector(0, 0));
  }

  public LEdge() {
    super();
  }

  /**
   * Gets id
   *
   * @return value of id
   */
  public GradoopId getId() {
    return f0;
  }

  /**
   * Sets id
   *
   * @param id the new value
   */
  public void setId(GradoopId id) {
    this.f0 = id;
  }

  /**
   * Gets sourceId
   *
   * @return value of sourceId
   */
  public GradoopId getSourceId() {
    return f1;
  }

  /**
   * Sets sourceId
   *
   * @param sourceId the new value
   */
  public void setSourceId(GradoopId sourceId) {
    this.f1 = sourceId;
  }

  /**
   * Gets targetId
   *
   * @return value of targetId
   */
  public GradoopId getTargetId() {
    return f2;
  }

  /**
   * Sets targetId
   *
   * @param targetId the new value
   */
  public void setTargetId(GradoopId targetId) {
    this.f2 = targetId;
  }

  /**
   * Gets sourcePosition
   *
   * @return value of sourcePosition
   */
  public Vector getSourcePosition() {
    return f3;
  }

  /**
   * Sets sourcePosition
   *
   * @param sourcePosition the new value
   */
  public void setSourcePosition(Vector sourcePosition) {
    this.f3 = sourcePosition;
  }

  /**
   * Gets targetPosition
   *
   * @return value of targetPosition
   */
  public Vector getTargetPosition() {
    return f4;
  }

  /**
   * Sets targetPosition
   *
   * @param targetPosition the new value
   */
  public void setTargetPosition(Vector targetPosition) {
    this.f4 = targetPosition;
  }
}
