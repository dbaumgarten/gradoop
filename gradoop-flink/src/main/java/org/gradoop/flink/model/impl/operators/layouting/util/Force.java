package org.gradoop.flink.model.impl.operators.layouting.util;


import org.gradoop.common.model.impl.id.GradoopId;

/** Represents a force that is applied to a vertex
 *
 */
public class Force {
  /** The id of the vertex that the force should be applied to */
  private GradoopId id;
  /** The force to apply */
  private Vector value;

  /** Create a new Force-Object
   *
   * @param id The id of the vertex that the force should be applied t
   * @param value The force to apply
   */
  public Force(GradoopId id, Vector value) {
    this.id = id;
    this.value = value;
  }

  /**
   * POJO-Constructor
   */
  public Force(){

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
   * Gets value
   *
   * @return value of value
   */
  public Vector getValue() {
    return value;
  }

  /**
   * Sets value
   *
   * @param value the new value
   */
  public void setValue(Vector value) {
    this.value = value;
  }
}
