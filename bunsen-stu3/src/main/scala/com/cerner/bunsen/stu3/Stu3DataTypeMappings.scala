package com.cerner.bunsen.stu3

import java.util.TimeZone

import ca.uhn.fhir.context.{BaseRuntimeChildDefinition, BaseRuntimeElementCompositeDefinition, RuntimePrimitiveDatatypeDefinition}
import ca.uhn.fhir.model.api.TemporalPrecisionEnum
import com.cerner.bunsen.backports.StaticField
import com.cerner.bunsen.datatypes.DataTypeMappings
import org.apache.spark.sql.catalyst.analysis.GetColumnByOrdinal
import org.apache.spark.sql.catalyst.expressions.{Cast, Expression, Literal}
import org.apache.spark.sql.catalyst.expressions.objects.{InitializeJavaBean, Invoke, NewInstance, StaticInvoke}
import org.apache.spark.sql.types.{DataType, DataTypes, Decimal, ObjectType}
import org.hl7.fhir.dstu3.model._
import org.hl7.fhir.instance.model.api.{IBaseBundle, IBaseDatatype, IBaseResource, IPrimitiveType}

import scala.collection.JavaConversions._

/**
  * Data type mappings for FHIR STU3.
  */
class Stu3DataTypeMappings extends DataTypeMappings {

  /**
    * Decimal type with reasonable precision.
    */
  private val decimalType = DataTypes.createDecimalType(12, 4)

  /**
    * Map associating FHIR primitive datatypes with the Spark types used to encode them.
    */
  private val fhirPrimitiveToSparkTypes: Map[Class[_ <: IPrimitiveType[_]], DataType] =
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

  override def primitiveToDataType(definition: RuntimePrimitiveDatatypeDefinition): DataType = {

    fhirPrimitiveToSparkTypes.get(definition.getImplementingClass) match {

      case Some(dataType) => dataType
      case None => throw new IllegalArgumentException("Unknown primitive type: " + definition.getImplementingClass.getName)
    }
  }

  override def baseType() : Class[_ <: IBaseDatatype] = classOf[org.hl7.fhir.dstu3.model.Type]

  override def extractEntryFromBundle(bundle: IBaseBundle, resourceName : String): java.util.List[IBaseResource] = {

    val stu3Bundle : Bundle = bundle.asInstanceOf[Bundle]

    val items = new java.util.ArrayList[IBaseResource]

    for (component <- stu3Bundle.getEntry()) {

      val resource = component.getResource()

      if (resource != null && resourceName == resource.getResourceType().name())
        items.add(resource)
    }

    items
  }

  override def overrideCompositeExpression(inputObject: Expression,
                                           definition: BaseRuntimeElementCompositeDefinition[_]) : Option[Seq[Expression]] = {

    if (definition.getImplementingClass == classOf[Reference]) {

      // Reference type, so return only supported fields.
      // We also explicitly use the IIDType for the reference element,
      // since that differs from the conventions used to infer
      // other types.
      val reference = dataTypeToUtf8Expr(
        Invoke(inputObject,
          "getReferenceElement",
          ObjectType(classOf[IdType])))


      val display = dataTypeToUtf8Expr(
        Invoke(inputObject,
          "getDisplayElement",
          ObjectType(classOf[org.hl7.fhir.dstu3.model.StringType])))

      Some(List(Literal("reference"), reference,
        Literal("display"), display))

    }  else {

      None
    }
  }

  override def skipField(definition: BaseRuntimeElementCompositeDefinition[_],
                child: BaseRuntimeChildDefinition ) : Boolean = {

    // References may be recursive, so include only the reference adn display name.
    val skipRecursiveReference = definition.getImplementingClass == classOf[Reference] &&
      !(child.getElementName == "reference" ||
        child.getElementName == "display")

    // Contains elements are currently not encoded in our Spark dataset.
    val skipContains = definition.getImplementingClass == classOf[ValueSet.ValueSetExpansionContainsComponent] &&
      child.getElementName == "contains"

     skipRecursiveReference || skipContains
   }

   override def primitiveEncoderExpression(inputObject: Expression,
                                           primitive: RuntimePrimitiveDatatypeDefinition): Expression = {

    primitive.getImplementingClass match {

      // If the FHIR primitive is serialized as a string, convert it to UTF8.
      case cls if fhirPrimitiveToSparkTypes.get(cls).contains(DataTypes.StringType) =>
        dataTypeToUtf8Expr(inputObject)

      case boolClass if boolClass == classOf[org.hl7.fhir.dstu3.model.BooleanType] =>
        Invoke(inputObject, "getValue", DataTypes.BooleanType)

      case tsClass if tsClass == classOf[org.hl7.fhir.dstu3.model.InstantType] =>

        Cast(dataTypeToUtf8Expr(inputObject), DataTypes.TimestampType).withTimeZone("UTC")

      case base64Class if base64Class == classOf[org.hl7.fhir.dstu3.model.Base64BinaryType] =>

        Invoke(inputObject, "getValue", DataTypes.BinaryType)

      case intClass if intClass == classOf[org.hl7.fhir.dstu3.model.IntegerType] =>

        Invoke(inputObject, "getValue", DataTypes.IntegerType)

      case unsignedIntClass if unsignedIntClass == classOf[org.hl7.fhir.dstu3.model.UnsignedIntType] =>

        Invoke(inputObject, "getValue", DataTypes.IntegerType)

      case unsignedIntClass if unsignedIntClass == classOf[org.hl7.fhir.dstu3.model.PositiveIntType] =>

        Invoke(inputObject, "getValue", DataTypes.IntegerType)

      case decimalClass if decimalClass == classOf[org.hl7.fhir.dstu3.model.DecimalType] =>

        StaticInvoke(classOf[Decimal],
          decimalType,
          "apply",
          Invoke(inputObject, "getValue", ObjectType(classOf[java.math.BigDecimal])) :: Nil)

      case unknown =>
        throw new IllegalArgumentException("Cannot serialize unknown primitive type: " + unknown.getName)
    }
  }


  override def primitiveDecoderExpression(primitiveClass: Class[_ <: IPrimitiveType[_]],
                                          path: Option[Expression]): Expression = {

    def getPath: Expression = path.getOrElse(GetColumnByOrdinal(0, ObjectType(primitiveClass)))

    primitiveClass match {

      // If the FHIR primitive is represented as a string type, read it from UTF8 and
      // set the value.
      case cls if fhirPrimitiveToSparkTypes.get(primitiveClass).contains(DataTypes.StringType) => {

        val newInstance = NewInstance(primitiveClass,
          Nil,
          ObjectType(primitiveClass))

        // Convert UTF8String to a regular string.
        InitializeJavaBean(newInstance, Map("setValueAsString" ->
          Invoke(getPath, "toString", ObjectType(classOf[String]), Nil)))
      }

      // Classes that can be directly encoded as their primitive type.
      case cls if cls == classOf[org.hl7.fhir.dstu3.model.BooleanType] ||
        cls == classOf[org.hl7.fhir.dstu3.model.Base64BinaryType] ||
        cls == classOf[org.hl7.fhir.dstu3.model.IntegerType] ||
        cls == classOf[org.hl7.fhir.dstu3.model.UnsignedIntType] ||
        cls == classOf[org.hl7.fhir.dstu3.model.PositiveIntType] =>
        NewInstance(primitiveClass,
          List(getPath),
          ObjectType(primitiveClass))

      case decimalClass if decimalClass == classOf[org.hl7.fhir.dstu3.model.DecimalType] =>

        NewInstance(primitiveClass,
          List(Invoke(getPath, "toJavaBigDecimal", ObjectType(classOf[java.math.BigDecimal]))),
          ObjectType(primitiveClass))

      case instantClass if instantClass == classOf[org.hl7.fhir.dstu3.model.InstantType] => {

        val millis = StaticField(classOf[TemporalPrecisionEnum],
          ObjectType(classOf[TemporalPrecisionEnum]),
          "MILLI")

        val UTCZone = StaticInvoke(classOf[TimeZone],
          ObjectType(classOf[TimeZone]),
          "getTimeZone",
          Literal("UTC", ObjectType(classOf[String])) :: Nil)

        NewInstance(primitiveClass,
          List(NewInstance(classOf[java.sql.Timestamp],
            getPath :: Nil,
            ObjectType(classOf[java.sql.Timestamp])),
            millis,
            UTCZone),
          ObjectType(primitiveClass))
      }

      case unknown => throw new IllegalArgumentException("Cannot deserialize unknown primitive type: " + unknown.getName)
    }
  }

}
