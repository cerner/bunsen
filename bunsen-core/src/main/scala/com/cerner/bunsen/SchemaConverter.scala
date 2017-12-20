package com.cerner.bunsen

import ca.uhn.fhir.context._
import org.apache.spark.sql.types.{BooleanType => _, DateType => _, IntegerType => _, StringType => _, _}
import org.hl7.fhir.dstu3.model.ValueSet
import org.hl7.fhir.dstu3.model._
import org.hl7.fhir.instance.model.api.{IBase, IBaseResource}

import scala.collection.JavaConversions._
import scala.collection.immutable.Stream.Empty

/**
  * Extracts a Spark schema based on a FHIR data model.
  */
object SchemaConverter {

  /**
    * Decimal type with reasonable precision.
    */
  private[bunsen] val decimalType = DataTypes.createDecimalType(12, 4)

  /**
    * Map associating FHIR primitive datatypes with the Spark types used to encode them.
    */
  private[bunsen] val fhirPrimitiveToSparkTypes: Map[Class[_ <: PrimitiveType[_]], DataType] =
    Map(classOf[org.hl7.fhir.dstu3.model.DecimalType] -> decimalType,
      classOf[MarkdownType] -> DataTypes.StringType,
      classOf[IdType] -> DataTypes.StringType,
      classOf[Enumeration[_]] -> DataTypes.StringType,
      classOf[DateTimeType] -> DataTypes.StringType,
      classOf[TimeType] -> DataTypes.StringType,
      classOf[DateType] -> DataTypes.StringType,
      classOf[CodeType] -> DataTypes.StringType,
      classOf[StringType] -> DataTypes.StringType,
      classOf[UriType] -> DataTypes.StringType,
      classOf[IntegerType] -> DataTypes.IntegerType,
      classOf[UnsignedIntType] -> DataTypes.IntegerType,
      classOf[PositiveIntType] -> DataTypes.IntegerType,
      classOf[BooleanType] -> DataTypes.BooleanType,
      classOf[InstantType] -> DataTypes.TimestampType,
      classOf[Base64BinaryType] -> DataTypes.BinaryType)

  private[bunsen] val referenceSchema = StructType(List(
    StructField("reference", DataTypes.StringType),
    StructField("display", DataTypes.StringType)))

  private[bunsen] val containsSchema = StructType(List(
    StructField("system", DataTypes.StringType),
    StructField("abstract", DataTypes.BooleanType),
    StructField("inactive", DataTypes.BooleanType),
    StructField("version", DataTypes.StringType),
    StructField("code", DataTypes.StringType),
    StructField("display", DataTypes.StringType)))
}

/**
  * Extracts a Spark schema based on a FHIR data model.
  */
class SchemaConverter(fhirContext: FhirContext) {

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

          case reference if (reference.getImplementingClass == classOf[Reference]) => SchemaConverter.referenceSchema
          case composite: RuntimeCompositeDatatypeDefinition => compositeToStructType(composite)
          case primitive: RuntimePrimitiveDatatypeDefinition => primitiveToDataType(primitive);
        }

        StructField(childName, childType)
      }

    } else if (childDefinition.getChildByName(childDefinition.getElementName)
      .getImplementingClass == classOf[Reference]) {

      // Return a field with the child definition name
      // and a struct containing the reference and display.
      // The identifier sub-field is omitted here since it recursively contains
      // a reference.
      if (childDefinition.getMax != 1) {

        List(StructField(childDefinition.getElementName, ArrayType(SchemaConverter.referenceSchema)))

      } else {

        List(StructField(childDefinition.getElementName, SchemaConverter.referenceSchema))
      }

    } else if (childDefinition.getChildByName(childDefinition.getElementName)
      .getImplementingClass == classOf[ValueSet.ValueSetExpansionContainsComponent]) {

      if (childDefinition.getMax != 1) {

        List(StructField(childDefinition.getElementName, ArrayType(SchemaConverter.containsSchema)))

      } else {
        List(StructField(childDefinition.getElementName, SchemaConverter.containsSchema))
      }

    } else {

      val definition = childDefinition.getChildByName(childDefinition.getElementName)

      val childType = definition match {
        case composite: BaseRuntimeElementCompositeDefinition[_] => compositeToStructType(composite)
        case primitive: RuntimePrimitiveDatatypeDefinition => primitiveToDataType(primitive)
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
    * Returns the Spark DataType used to encode the given FHIR primitive.
    */
  private[bunsen] def primitiveToDataType(definition: RuntimePrimitiveDatatypeDefinition): DataType = {

    val dataType = SchemaConverter.fhirPrimitiveToSparkTypes.get(definition.getImplementingClass)

    if (dataType == null)
      throw new IllegalArgumentException("Unknown primitive type: " + definition.getImplementingClass.getName)

    dataType
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
      .flatMap(childToFields)

    StructType(fields)
  }

}
