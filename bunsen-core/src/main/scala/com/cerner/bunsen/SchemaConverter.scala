package com.cerner.bunsen

import ca.uhn.fhir.context._
import com.cerner.bunsen.datatypes.DataTypeMappings
import org.apache.spark.sql.types.{BooleanType => _, DateType => _, IntegerType => _, StringType => _, _}
import org.hl7.fhir.instance.model.api.IBaseResource

import scala.collection.JavaConversions._
import scala.collection.immutable.Stream.Empty

/**
  * Extracts a Spark schema based on a FHIR data model.
  */
class SchemaConverter(fhirContext: FhirContext, dataTypeMappings: DataTypeMappings) {

  /**
    * Returns the Spark schema that represents the given FHIR resource
    *
    * @param resourceClass The class implementing the FHIR resource.
    * @return The schema as a Spark StructType
    */
  def resourceSchema[T <: IBaseResource](resourceClass: Class[T]): StructType = {

    val definition = fhirContext.getResourceDefinition(resourceClass)

    compositeToStructType(definition)
  }

  /**
    * Returns the fields used to represent the given child definition. In most cases this
    * will contain a single element, but in special cases like Choice elements, it will
    * contain a field for each possible FHIR choice.
    */
  private def childToFields(childDefinition: BaseRuntimeChildDefinition): Seq[StructField] = {

    // Contained resources and extensions not yet supported.
    if (childDefinition.isInstanceOf[RuntimeChildContainedResources] ||
      childDefinition.isInstanceOf[RuntimeChildExtension]) {

      Empty

    } else if (childDefinition.isInstanceOf[RuntimeChildChoiceDefinition]) {

      val choice = childDefinition.asInstanceOf[RuntimeChildChoiceDefinition]

      // Iterate by types and then lookup the field names so we get the preferred
      // field name for the given type.
      for (fhirChildType <- choice.getValidChildTypes.toList) yield {

        val childName = choice.getChildNameByDatatype(fhirChildType)

        val childType = choice.getChildByName(childName) match {

          // case reference if (reference.getImplementingClass == classOf[Reference]) => SchemaConverter.referenceSchema
          case composite: RuntimeCompositeDatatypeDefinition => compositeToStructType(composite)
          case primitive: RuntimePrimitiveDatatypeDefinition => dataTypeMappings.primitiveToDataType(primitive);
        }

        StructField(childName, childType)
      }

    } else {

      val definition = childDefinition.getChildByName(childDefinition.getElementName)

      val childType = definition match {
        case composite: BaseRuntimeElementCompositeDefinition[_] => compositeToStructType(composite)
        case primitive: RuntimePrimitiveDatatypeDefinition => dataTypeMappings.primitiveToDataType(primitive)
        case narrative: RuntimePrimitiveDatatypeNarrativeDefinition => DataTypes.StringType
        case hl7Org: RuntimePrimitiveDatatypeXhtmlHl7OrgDefinition => DataTypes.StringType
      }

      if (childDefinition.getMax != 1) {

        List(StructField(childDefinition.getElementName, ArrayType(childType)))

      } else {

        List(StructField(childDefinition.getElementName, childType))
      }
    }
  }

  /**
    * Returns the Spark struct type used to encode the given FHIR composite.
    *
    * @param definition The FHIR definition of a composite type.
    * @return The schema as a Spark StructType
    */
  private[bunsen] def compositeToStructType(definition: BaseRuntimeElementCompositeDefinition[_]): StructType = {

    // Map to (name, value, name, value) expressions for child elements.
    val fields: Seq[StructField] = definition
      .getChildren
      .filter(child => !dataTypeMappings.skipField(definition, child))
      .flatMap(childToFields)

    StructType(fields)
  }

}
