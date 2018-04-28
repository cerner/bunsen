package com.cerner.bunsen.datatypes

import ca.uhn.fhir.context.{BaseRuntimeChildDefinition,
  BaseRuntimeElementCompositeDefinition, RuntimePrimitiveDatatypeDefinition}
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.expressions.objects.{Invoke, StaticInvoke}
import org.apache.spark.sql.types.{DataType, DataTypes, ObjectType}
import org.apache.spark.unsafe.types.UTF8String
import org.hl7.fhir.instance.model.api.{IBaseBundle, IBaseDatatype, IBaseResource, IPrimitiveType}

/**
  * Interface for mapping FHIR datatypes to Spark datatypes.
  */
trait DataTypeMappings {

  /**
    * Converts the given FHIR primitive to the Spark DataType used to encode it.
    *
    * @param definition the FHIR datatype definition.
    * @return the corresponding Spark datatype
    */
  def primitiveToDataType(definition: RuntimePrimitiveDatatypeDefinition): DataType

  /**
    * Returns the base class of FHIR types in the version used.
    *
    * @return the base class for FHIR types in the version used
    */
  def baseType() : Class[_ <: IBaseDatatype]

  /**
    * Helper method to extract resources from a bundle. The FHIR bundle API isn't used
    * directly here since this supports code where the FHIR type is not known at compile time.
    *
    * @param bundle a FHIR bundle
    * @param resourceName the resoure to extract
    * @return a list of resources extracted from the bundle
    */
  def extractEntryFromBundle(bundle: IBaseBundle, resourceName : String) : java.util.List[IBaseResource]

  /**
    * Returns a Spark expression that translate a string-based FHIR type to a Spark UTF8 string.
    * This pattern occurs frequently, so this method is here as a convenience.
    *
    * @param inputObject the expression referring to a string-based FHIR type
    * @return an expression that produces a Spark UTF8 string
    */
  def dataTypeToUtf8Expr(inputObject: Expression): Expression = {

    StaticInvoke(
      classOf[UTF8String],
      DataTypes.StringType,
      "fromString",
      List(Invoke(inputObject,
        "getValueAsString",
        ObjectType(classOf[String]))))
  }

  /**
    * Allows custom expressions to be used when encoding composite objects. This supports
    * special cases where FHIR objects don't follow conventions expected by reusable
    * encoding logic, allowing custom expressions to be used just for that case.
    *
    * For most expressions this method should simply return None, indicate that no override
    * is necessary.
    *
    * @param inputObject an expression referring to the composite object to encode
    * @param definition the composite definition to encode
    * @return an optional expression sequence if the composit is overridden.
    */
  def overrideCompositeExpression(inputObject: Expression,
                                  definition: BaseRuntimeElementCompositeDefinition[_]) : Option[Seq[Expression]]

  /**
    * Returns true if the given field should be skipped during encoding and decoding, false otherwise.
    * This allows the data type to explicitily short circuit a handful of recursive data model
    * definitions that cannot be encoded in Spark.
    */
  def skipField(compositeDefinition: BaseRuntimeElementCompositeDefinition[_],
                child: BaseRuntimeChildDefinition ) : Boolean

  /**
    * Returns an expression to serialize a primitive type.
    */
  def primitiveEncoderExpression(inputObject: Expression,
                                 primitive: RuntimePrimitiveDatatypeDefinition): Expression

  /**
    * Returns an expression to deserialize a primitive type.
    */
  def primitiveDecoderExpression(primitiveClass: Class[_ <: IPrimitiveType[_]],
                                 path: Option[Expression]): Expression
}
