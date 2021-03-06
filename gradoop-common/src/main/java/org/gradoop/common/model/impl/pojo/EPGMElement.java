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
package org.gradoop.common.model.impl.pojo;

import com.google.common.base.Preconditions;
import org.gradoop.common.model.api.entities.Element;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.properties.Properties;
import org.gradoop.common.model.impl.properties.Property;
import org.gradoop.common.model.impl.properties.PropertyValue;

import javax.annotation.Nullable;
import java.util.Objects;

/**
 * Abstract base class for EPGM elements.
 */
public abstract class EPGMElement implements Element {
  /**
   * Entity identifier.
   */
  protected GradoopId id;

  /**
   * Label of that entity.
   */
  protected String label;

  /**
   * Internal property storage
   */
  protected Properties properties;

  /**
   * Default constructor.
   */
  protected EPGMElement() {
  }

  /**
   * Creates an object from the given parameters. Can only be called by
   * inheriting classes.
   *
   * @param id         entity identifier
   * @param label      entity label
   * @param properties key-value properties
   */
  protected EPGMElement(
    GradoopId id, String label, Properties properties) {
    this.id = id;
    this.label = label;
    this.properties = properties;
  }

  @Override
  public GradoopId getId() {
    return id;
  }

  @Override
  public void setId(GradoopId id) {
    this.id = id;
  }

  @Override
  public String getLabel() {
    return label;
  }

  @Override
  public void setLabel(String label) {
    this.label = label;
  }

  @Override
  @Nullable
  public Properties getProperties() {
    return properties;
  }

  @Override
  public Iterable<String> getPropertyKeys() {
    return (properties != null) ? properties.getKeys() : null;
  }

  @Override
  public PropertyValue getPropertyValue(String key) {
    // TODO: return PropertyValue.NULL_VALUE instead?
    return (properties != null) ? properties.get(key) : null;
  }

  @Override
  public void setProperties(Properties properties) {
    this.properties = properties;
  }

  @Override
  public void setProperty(Property property) {
    Preconditions.checkNotNull(property, "Property was null");
    initProperties();
    this.properties.set(property);
  }

  @Override
  public void setProperty(String key, Object value) {
    initProperties();
    this.properties.set(key, value);
  }

  @Override
  public void setProperty(String key, PropertyValue value) {
    initProperties();
    this.properties.set(key, value);
  }

  @Override
  public PropertyValue removeProperty(String key) {
    return this.properties != null ? properties.remove(key) : null;
  }

  @Override
  public int getPropertyCount() {
    return (this.properties != null) ? this.properties.size() : 0;
  }

  @Override
  public boolean hasProperty(String key) {
    return this.properties != null && this.properties.containsKey(key);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    EPGMElement that = (EPGMElement) o;

    return Objects.equals(id, that.id);
  }

  @Override
  public int hashCode() {
    int result = id.hashCode();
    result = 31 * result + id.hashCode();
    return result;
  }

  @Override
  public String toString() {
    return String.format("%s%s%s{%s}",
      id,
      label == null || label.equals("") ? "" : ":",
      label,
      properties == null ? "" : properties);
  }

  /**
   * Initializes the internal properties field if necessary.
   */
  private void initProperties() {
    if (this.properties == null) {
      this.properties = Properties.create();
    }
  }
}
