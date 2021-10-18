package edu.leipzig.model.graph;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Objects;

public class PropertiesModified implements Serializable {

  private HashMap<String, Object> properties;

  public PropertiesModified() {this.properties = new HashMap<>(10);}
  public PropertiesModified(int capacity) {this.properties = new HashMap<>(capacity);}
  public void addProperty(String key, Object o) {
    this.properties.put(key, o);
  }
  public void removePropertyByKey(String key) {
    this.properties.remove(key);
  }
  public void setProperties(HashMap<String, Object> hashMap) {
    this.properties = hashMap;
  }
  public HashMap<String, Object> getProperties() {
    return this.properties;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    PropertiesModified that = (PropertiesModified) o;
    return Objects.equals(properties, that.properties);
  }

  @Override
  public int hashCode() {
    return Objects.hash(properties);
  }
}
