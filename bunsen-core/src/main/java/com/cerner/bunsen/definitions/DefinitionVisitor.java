package com.cerner.bunsen.definitions;

import java.util.List;
import java.util.Map;

/**
 * Visitor for each field in a FHIR StructureDefinition.
 *
 * @param <T> the type produced by the visitor.   
 */
public interface DefinitionVisitor<T> {

  /**
   * Visits a primitive type.
   *
   * @param elementName the element to visit.
   * @param primitiveType the FHIR type of the primitive.
   * @return the visitor result.
   */
  public T visitPrimitive(String elementName,
      String primitiveType);

  /**
   * Visits a composite type.
   *
   * @param elementName the element to visit.
   * @param elementType the type of the composite type.
   * @param children the composite type's children.
   * @return the visitor result.
   */
  public T visitComposite(String elementName,
      String elementType,
      List<StructureField<T>> children);

  /**
   * Visits a reference type.
   *
   * @param elementName the element to visit.
   * @param referenceTypes the types of resource that can be referenced
   * @param children the child fields of the reference
   * @return the visitor result.
   */
  public T visitReference(String elementName,
      List<String> referenceTypes,
      List<StructureField<T>> children);

  /**
   * Visits a non-leaf extension.
   *
   * @param elementName the element to visit.
   * @param extensionUrl the URL of the extension.
   * @param children the children of the extension
   * @return the visitor result.
   */
  public T visitParentExtension(String elementName,
      String extensionUrl,
      List<StructureField<T>> children);

  /**
   * Visits a leaf extension, which contains some value.
   *
   * @param elementName the element to visit.
   * @param extensionUrl the URL of the extension.
   * @param element the children of the extension.
   * @return the visitor result.
   */
  public T visitLeafExtension(String elementName,
      String extensionUrl,
      T element);

  /**
   * Visits a multi-valued element.
   *
   * @param elementName the element to visit.
   * @param arrayElement the visitor result for a single element of the array.
   * @return the visitor result.
   */
  public T visitMultiValued(String elementName,
      T arrayElement);

  /**
   * Visits a choice type.
   *
   * @param elementName the element to visit.
   * @param fhirToChoiceTypes a map of the choice type with the returned children.
   * @return the visitor result.
   */
  public T visitChoice(String elementName,
      Map<String,T> fhirToChoiceTypes);
}
