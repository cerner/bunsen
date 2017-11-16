package com.cerner.bunsen.mappings;

import java.io.Serializable;
import java.util.Objects;

/**
 * A JavaBean to represent a ancestor row from one value to another. This class
 * is mutable for easy use as a Spark {@code Dataset<Ancestor>} instance.
 */
public class Ancestor implements Serializable {

  private String conceptMapUri;

  private String conceptMapVersion;

  private String descendantSystem;

  private String descendantValue;

  private String ancestorSystem;

  private String ancestorValue;

  /**
   * Nullary constructor so Spark can encode this as a bean.
   */
  public Ancestor() {
  }

  /**
   * Constructs an ancestor bean.
   *
   * @param conceptMapUri the URI of the concept map defining this relationship
   * @param conceptMapVersion the version of the concept map defining this relationship
   * @param descendantSystem the code system of the descendant
   * @param descendantValue the code value of the descendant
   * @param ancestorSystem the code system of the ancestor
   * @param ancestorValue the code value of the ancestor
   */
  public Ancestor(String conceptMapUri,
      String conceptMapVersion,
      String descendantSystem,
      String descendantValue,
      String ancestorSystem,
      String ancestorValue) {

    this.conceptMapUri = conceptMapUri;
    this.conceptMapVersion = conceptMapVersion;
    this.descendantSystem = descendantSystem;
    this.descendantValue = descendantValue;
    this.ancestorSystem = ancestorSystem;
    this.ancestorValue = ancestorValue;
  }

  /**
   * Returns the URI of the concept map that defines this relationship.
   *
   * @return the URI of the concept map
   */
  public String getConceptMapUri() {
    return conceptMapUri;
  }

  /**
   * Sets the URI of the concept map that defines this relationship.
   *
   * @param conceptMapUri the URI of the concept map
   */
  public void setConceptMapUri(String conceptMapUri) {
    this.conceptMapUri = conceptMapUri;
  }

  /**
   * Returns the version of the concept map that defines this relationship.
   *
   * @return the version of the concept map.
   */
  public String getConceptMapVersion() {
    return conceptMapVersion;
  }

  /**
   * Sets the version of the concept map that defines this relationship.
   *
   * @param conceptMapVersion the version of the concept map.
   */
  public void setConceptMapVersion(String conceptMapVersion) {
    this.conceptMapVersion = conceptMapVersion;
  }

  /**
   * Returns the system of the descendant code.
   *
   * @return the system of the descendant code
   */
  public String getDescendantSystem() {
    return descendantSystem;
  }

  /**
   * Sets the system of the descendant code.
   *
   * @param descendantSystem the system of the descendant code.
   */
  public void setDescendantSystem(String descendantSystem) {
    this.descendantSystem = descendantSystem;
  }

  /**
   * Returns the value of the descendant code.
   *
   * @return the value of the descendant code.
   */
  public String getDescendantValue() {
    return descendantValue;
  }

  /**
   * Sets the value of the descendant code.
   *
   * @param descendantValue the value of the descendant code.
   */
  public void setDescendantValue(String descendantValue) {
    this.descendantValue = descendantValue;
  }

  /**
   * Returns the system of the ancestor code.
   *
   * @return the system of the ancestor code
   */
  public String getAncestorSystem() {
    return ancestorSystem;
  }

  /**
   * Sets the system of the ancestor code.
   *
   * @param ancestorSystem the system of the ancestor code.
   */
  public void setAncestorSystem(String ancestorSystem) {
    this.ancestorSystem = ancestorSystem;
  }

  /**
   * Returns the value of the ancestor code.
   *
   * @return the value of the ancestor code.
   */
  public String getAncestorValue() {
    return ancestorValue;
  }

  /**
   * Sets the value of the ancestor code.
   *
   * @param ancestorValue the value of the ancestor code.
   */
  public void setAncestorValue(String ancestorValue) {
    this.ancestorValue = ancestorValue;
  }

  @Override
  public boolean equals(Object obj) {

    if (!(obj instanceof Ancestor)) {
      return false;
    }

    Ancestor that = (Ancestor) obj;

    return Objects.equals(this.conceptMapUri, that.conceptMapUri)
        && Objects.equals(this.conceptMapVersion, that.conceptMapVersion)
        && Objects.equals(this.descendantSystem, that.descendantSystem)
        && Objects.equals(this.descendantValue, that.descendantValue)
        && Objects.equals(this.ancestorSystem, that.ancestorSystem)
        && Objects.equals(this.ancestorValue, that.ancestorValue);
  }

  @Override
  public int hashCode() {
    return 37
        * Objects.hashCode(this.conceptMapUri)
        * Objects.hashCode(this.conceptMapVersion)
        * Objects.hashCode(this.descendantSystem)
        * Objects.hashCode(this.descendantValue)
        * Objects.hashCode(this.ancestorSystem)
        * Objects.hashCode(this.ancestorValue);
  }
}
