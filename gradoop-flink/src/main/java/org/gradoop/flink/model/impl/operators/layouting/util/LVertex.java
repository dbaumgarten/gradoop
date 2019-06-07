package org.gradoop.flink.model.impl.operators.layouting.util;

import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.Vertex;

/** Leightweight/Layouting-Vertex. Has all properties of a Vertex that are important for
 * the layouting. This way we do not need to drag around a full Vertex through every operation.
 *
 */
public class LVertex {
  private GradoopId id;
  private Vector position;
  private int cellid;

  /** Create new LVertex
   *
   * @param id Id of the original vertex
   * @param position Position of the original vertex
   */
  public LVertex(GradoopId id, Vector position) {
    this.id = id;
    this.position = position;
  }

  /** Create new LVertex
   *
   * @param id Id of the original vertex
   * @param position Cellid of the original vertex
   */
  public LVertex(GradoopId id, Vector position, int cellid) {
    this.id = id;
    this.position = position;
    this.cellid = cellid;
  }

  /** Create new LVertex
   *
   * @param v The original vertex to copy all information from
   */
  public LVertex(Vertex v){
    id = v.getId();
    position = Vector.fromVertexPosition(v);
  }

  /** Default-Constructor to comply with Pojo-Rules
   *
   */
  public LVertex(){
    this.id = GradoopId.get();
    this.position = new Vector(0,0);
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
   * Gets position
   *
   * @return value of position
   */
  public Vector getPosition() {
    return position;
  }

  /**
   * Sets position
   *
   * @param position the new value
   */
  public void setPosition(Vector position) {
    this.position = position;
  }

  /**
   * Gets cellid
   *
   * @return value of cellid
   */
  public int getCellid() {
    return cellid;
  }

  /**
   * Sets cellid
   *
   * @param cellid the new value
   */
  public void setCellid(int cellid) {
    this.cellid = cellid;
  }
}
