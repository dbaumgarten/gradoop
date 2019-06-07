package org.gradoop.flink.model.impl.operators.layouting.util;

import org.apache.flink.api.java.tuple.Tuple3;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.Vertex;

/** Leightweight/Layouting-Vertex. Has all properties of a Vertex that are important for
 * the layouting. This way we do not need to drag around a full Vertex through every operation.
 *
 */
public class LVertex extends Tuple3<GradoopId,Vector,Integer> {

  /**
   * Position of the ID-property in the tuple
   */
  public static final int ID = 0;

  /** Create new LVertex
   *
   * @param id Id of the original vertex
   * @param position Position of the original vertex
   */
  public LVertex(GradoopId id, Vector position) {
    super(id,position,-1);
  }

  /** Create new LVertex
   *
   * @param id Id of the original vertex
   * @param position Cellid of the original vertex
   */
  public LVertex(GradoopId id, Vector position, int cellid) {
    super(id,position,cellid);
  }

  /** Create new LVertex
   *
   * @param v The original vertex to copy all information from
   */
  public LVertex(Vertex v){
    super(v.getId(),Vector.fromVertexPosition(v),-1);
  }

  /** Default-Constructor to comply with Pojo-Rules
   *
   */
  public LVertex(){
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
   * Gets position
   *
   * @return value of position
   */
  public Vector getPosition() {
    return f1;
  }

  /**
   * Sets position
   *
   * @param position the new value
   */
  public void setPosition(Vector position) {
    this.f1 = position;
  }

  /**
   * Gets cellid
   *
   * @return value of cellid
   */
  public int getCellid() {
    return f2;
  }

  /**
   * Sets cellid
   *
   * @param cellid the new value
   */
  public void setCellid(int cellid) {
    this.f2 = cellid;
  }
}
