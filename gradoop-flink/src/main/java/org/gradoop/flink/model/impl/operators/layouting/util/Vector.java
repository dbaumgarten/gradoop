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
package org.gradoop.flink.model.impl.operators.layouting.util;

import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.flink.model.impl.operators.layouting.LayoutingAlgorithm;

import java.io.Serializable;


/**
 * Simple helper-class for some vector-math.
 * All math-operations will return a new Vector (instead of modifying the existing vector). This
 * prevents strange side-effects when performing complex computations.
 */
public class Vector implements Serializable {

  public double f0;
  public double f1;

  /**
   * Construct a vector from f0 and f1 coordinates
   *
   * @param x X-Coordinate of the new vector
   * @param y Y-Coordinate of the new vector
   */
  public Vector(double x, double y) {
    f0 = x;
    f1 = y;
  }

  /**
   * Construct new zero-Vector
   */
  public Vector() {
    f0 = 0d;
    f1 = 0d;
  }

  /**
   * Create a vector from the coordinate-properties of a Vertex
   *
   * @param v The vertex to extract position coordinates from properties (X,Y)
   * @return A matching vector
   */
  public static Vector fromVertexPosition(Vertex v) {
    double x = v.getPropertyValue(LayoutingAlgorithm.X_COORDINATE_PROPERTY).getInt();
    double y = v.getPropertyValue(LayoutingAlgorithm.Y_COORDINATE_PROPERTY).getInt();
    return new Vector(x, y);
  }

  /**
   * Set the coordinate-properties of a vertex to the values of this vector
   *
   * @param v The vertex that will receive the values of this vector as coordinates
   */
  public void setVertexPosition(Vertex v) {
    v.setProperty(LayoutingAlgorithm.X_COORDINATE_PROPERTY, (int) f0);
    v.setProperty(LayoutingAlgorithm.Y_COORDINATE_PROPERTY, (int) f1);
  }

  /**
   * Substract another vector from this vector and return the result
   *
   * @param other Vector to substract
   * @return this-other
   */
  public Vector sub(Vector other) {
    return new Vector(f0 - other.f0, f1 - other.f1);
  }

  /**
   * Add another vector to this vector and return the result
   *
   * @param other The vector to add
   * @return this+other
   */
  public Vector add(Vector other) {
    return new Vector(f0 + other.f0, f1 + other.f1);
  }

  /**
   * Multiply this vector by a factor and return the result
   *
   * @param factor The factor to multiply this vector with
   * @return this*factor
   */
  public Vector mul(double factor) {
    return new Vector(f0 * factor, f1 * factor);
  }

  /**
   * Divide this vector by a factor and return the result
   *
   * @param factor The factor to divide this vector by
   * @return this/factor
   */
  public Vector div(double factor) {
    return new Vector(f0 / factor, f1 / factor);
  }

  /**
   * Calculate the euclidean distance between this vector and another vector
   *
   * @param other The other vector
   * @return Math.sqrt(Math.pow ( f0 - other.f0, 2) + Math.pow(f1 - other.f1, 2))
   */
  public double distance(Vector other) {
    return Math.sqrt(Math.pow(f0 - other.f0, 2) + Math.pow(f1 - other.f1, 2));
  }

  /**
   * Calculate the scalar-product of this vector
   *
   * @param other The other vector
   * @return Skalar-product of this and other
   */
  public double scalar(Vector other) {
    return f0 * other.f0 + f1 * other.f1;
  }

  /**
   * Clamp this vector to a given length.
   * The returned vector will have the same orientation as this one, but will have at most a
   * length of maxLen.
   * If maxLen is smaller the the lenght of this Vector this vector (a copy of it) will be returned.
   *
   * @param maxLen maximum lenght of vector
   * @return This vector but constrained to the given length
   */
  public Vector clamped(double maxLen) {
    double len = magnitude();
    if (len == 0) {
      return new Vector(0, 0);
    }
    double newx = (f0 / len) * Math.min(len, maxLen);
    double newy = (f1 / len) * Math.min(len, maxLen);
    return new Vector(newx, newy);
  }

  /**
   * Normalize this vector.
   *
   * @return a vector with the same orientation as this one and a length of 1. If this vector is
   * (0,0) then (0,0) will be returned instead.
   */
  public Vector normalized() {
    double len = magnitude();
    if (len == 0) {
      return new Vector(0, 0);
    }
    double newx = f0 / len;
    double newy = f1 / len;
    return new Vector(newx, newy);
  }

  /**
   * Get the lenght of this vector
   *
   * @return euclidean length of this vector
   */
  public double magnitude() {
    return Math.sqrt(Math.pow(f0, 2) + Math.pow(f1, 2));
  }

  /**
   * Confine this point to the given bounding-box.
   *
   * @param minX Bounding-box
   * @param maxX Bounding-box
   * @param minY Bounding-box
   * @param maxY Bounding-box
   * @return A vector that does not violate the given bounding box.
   */
  public Vector confined(double minX, double maxX, double minY, double maxY) {
    double newx = Math.min(Math.max(f0, minX), maxX);
    double newy = Math.min(Math.max(f1, minY), maxY);
    return new Vector(newx, newy);
  }

  @Override
  public boolean equals(Object other) {
    if (other != null && other instanceof Vector) {
      Vector otherv = (Vector) other;
      return f0 == otherv.f0 && f1 == otherv.f1;
    }
    return false;
  }

  @Override
  public int hashCode() {
    return ((int) f0 << 16) + (int) f1;
  }

  @Override
  public String toString() {
    return "Vector{" + "x=" + f0 + ", y=" + f1 + '}';
  }

  /**
   * @return X coordinate of the vector
   */
  public double getX() {
    return f0;
  }

  /**
   * @param x Set X coordinate of the vector
   **/
  public void setX(double x) {
    this.f0 = x;
  }

  /**
   * @return Y coordinate of the vector
   */
  public double getY() {
    return f1;
  }

  /**
   * @param y Set Y coordinate of the vector
   **/
  public void setY(double y) {
    this.f1 = y;
  }

  /**
   * Set x and y at once
   *
   * @param x X to set
   * @param y y to set
   * @return This object for method-chaining
   */
  public Vector set(double x, double y) {
    f0 = x;
    f1 = y;
    return this;
  }

  /**
   * Copy the values of the other vector into this one
   *
   * @param other The other vector
   * @return this
   */
  public Vector set(Vector other) {
    f0 = other.f0;
    f1 = other.f1;
    return this;
  }

  /**
   * Reset this vector to 0
   *
   * @return This object for method-chaining
   */
  public Vector reset() {
    f0 = 0d;
    f1 = 0d;
    return this;
  }

  /**
   * Copy this object
   *
   * @return A copy of this object
   */
  public Vector copy() {
    return new Vector(f0, f1);
  }

  //-----------------------------------------------------------------------------------

  /**
   * Alternative MUTATING variant. Modifies this vector instead of creating a new one. BE CAREFUL!
   * Substract another vector from this vector and return the result
   *
   * @param other Vector to substract
   * @return this-other
   */
  public Vector mSub(Vector other) {
    f0 -= other.f0;
    f1 -= other.f1;
    return this;
  }

  /**
   * Alternative MUTATING variant. Modifies this vector instead of creating a new one. BE CAREFUL!
   * Add another vector to this vector and return the result
   *
   * @param other The vector to add
   * @return this+other
   */
  public Vector mAdd(Vector other) {
    f0 += other.f0;
    f1 += other.f1;
    return this;
  }

  /**
   * Alternative MUTATING variant. Modifies this vector instead of creating a new one. BE CAREFUL!
   * Multiply this vector by a factor and return the result
   *
   * @param factor The factor to multiply this vector with
   * @return this*factor
   */
  public Vector mMul(double factor) {
    f0 *= factor;
    f1 *= factor;
    return this;
  }

  /**
   * Alternative MUTATING variant. Modifies this vector instead of creating a new one. BE CAREFUL!
   * Divide this vector by a factor and return the result
   *
   * @param factor The factor to divide this vector by
   * @return this/factor
   */
  public Vector mDiv(double factor) {
    f0 /= factor;
    f1 /= factor;
    return this;
  }

  /**
   * Alternative MUTATING variant. Modifies this vector instead of creating a new one. BE CAREFUL!
   * Clamp this vector to a given length.
   * The returned vector will have the same orientation as this one, but will have at most a
   * length of maxLen.
   * If maxLen is smaller the the lenght of this Vector this vector (a copy of it) will be returned.
   *
   * @param maxLen maximum lenght of vector
   * @return This vector but constrained to the given length
   */
  public Vector mClamped(double maxLen) {
    double len = magnitude();
    if (len == 0) {
      return new Vector(0, 0);
    }
    f0 = (f0 / len) * Math.min(len, maxLen);
    f1 = (f1 / len) * Math.min(len, maxLen);
    return this;
  }

  /**
   * Alternative MUTATING variant. Modifies this vector instead of creating a new one. BE CAREFUL!
   * Normalize this vector.
   *
   * @return a vector with the same orientation as this one and a length of 1. If this vector is
   * (0,0) then (0,0) will be returned instead.
   */
  public Vector mNormalized() {
    double len = magnitude();
    if (len == 0) {
      f0 = 0.0;
      f1 = 0.0;
      return this;
    }
    f0 /= len;
    f1 /= len;
    return this;
  }

  /**
   * Alternative MUTATING variant. Modifies this vector instead of creating a new one. BE CAREFUL!
   * Confine this point to the given bounding-box.
   *
   * @param minX Bounding-box
   * @param maxX Bounding-box
   * @param minY Bounding-box
   * @param maxY Bounding-box
   * @return A vector that does not violate the given bounding box.
   */
  public Vector mConfined(double minX, double maxX, double minY, double maxY) {
    f0 = Math.min(Math.max(f0, minX), maxX);
    f1 = Math.min(Math.max(f1, minY), maxY);
    return this;
  }

}
