package com.cerner.bunsen.definitions;

/**
 * This is a container for a field in a FHIR composite type
 * that defines the field, its FHIR property, and a transformed result produced
 * by the visitor itself.
 *
 * @param <T> the type produced by the visitor.
 */
public class StructureField<T> {

  private final String propertyName;

  private final String fieldName;

  private final String extensionUrl;

  private final boolean isChoice;

  private final T visitorResult;

  /**
   * Constructor.
   *
   * @param propertyName the FHIR property name
   * @param fieldName the field name for which the property is encoded
   * @param extensionUrl the URL, if this is for an extension
   * @param isChoice flag indicating whether this is a choice type
   * @param visitorResult the result of the visitor.
   */
  public StructureField(String propertyName,
      String fieldName,
      String extensionUrl,
      boolean isChoice,
      T visitorResult) {

    this.propertyName = propertyName;
    this.fieldName = fieldName;
    this.extensionUrl = extensionUrl;
    this.isChoice = isChoice;
    this.visitorResult = visitorResult;
  }

  /**
   * The FHIR property name for the field.
   *
   * @return the FHIR property name.
   */
  public String propertyName() {

    return propertyName;
  }

  /**
   * The field name of the converted result. This generally be the same
   * as the FHIR property name, only changing for special cases like
   * fields generated to directly represent extensions.
   *
   * @return the field name
   */
  public String fieldName() {

    return fieldName;
  }

  /**
   * The URL of the extension represented by this field, or null if the given field is not an
   * extension.
   *
   * @return the URL for the extension, or null if it is not an extension.
   */
  public String extensionUrl() {

    return extensionUrl;
  }

  /**
   * An indicator whether the field is a choice type.
   *
   * @return true if it is a choice type, false otherwise.
   */
  public boolean isChoice() {

    return isChoice;
  }

  /**
   * The result produced by the visitor.
   *
   * @return the result produced by the visitor.
   */
  public T result() {
    return visitorResult;
  }

  /**
   * Creates a new StructureField for the given property.
   *
   * @param propertyName the name of the property.
   * @param visitorResult the visitor result.
   * @param <T> the return type of the visitor.
   * @return the StructField for the given property.
   */
  public static <T> StructureField<T> property(String propertyName, T visitorResult) {

    return new StructureField<>(propertyName, propertyName, null, false, visitorResult);
  }

  /**
   * Creates a new StructField for the given extension.
   *
   * @param fieldName the name of the field in which the extension is represented.
   * @param extensionUrl the URL for the extension.
   * @param visitorResult the visitor result.
   * @param <T> the return type of the visitor.
   * @return the StructField for the given extension.
   */
  public static <T> StructureField<T> extension(String fieldName,
      String extensionUrl,
      T visitorResult) {

    return new StructureField<>(null, fieldName, extensionUrl, false, visitorResult);
  }
}
