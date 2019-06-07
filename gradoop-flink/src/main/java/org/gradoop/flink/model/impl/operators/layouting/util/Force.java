package org.gradoop.flink.model.impl.operators.layouting.util;


import org.apache.flink.api.java.tuple.Tuple2;
import org.gradoop.common.model.impl.id.GradoopId;

/** Represents a force that is applied to a vertex
 *
 */
public class Force extends Tuple2<GradoopId,Vector> {
  /**
   * Position of the ID-property in the tuple
   */
  public static final int ID = 0;


  /** Create a new Force-Object
   *
   * @param id The id of the vertex that the force should be applied t
   * @param value The force to apply
   */
  public Force(GradoopId id, Vector value) {
    super(id,value);
  }

  /**
   * POJO-Constructor
   */
  public Force(){
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
   * Gets value
   *
   * @return value of value
   */
  public Vector getValue() {
    return f1;
  }

  /**
   * Sets value
   *
   * @param value the new value
   */
  public void setValue(Vector value) {
    this.f1 = value;
  }

  /** Set id and value at once. Useful for functions that reuse objects
   *
   * @param id Id to set
   * @param value Value to set
   */
  public void set(GradoopId id, Vector value){
    this.f0 = id;
    this.f1 = value;
  }
}
