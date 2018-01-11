package com.cerner.bunsen

import java.util.TimeZone

import ca.uhn.fhir.context.BaseRuntimeElementDefinition.ChildTypeEnum
import ca.uhn.fhir.context._
import ca.uhn.fhir.model.api.IValueSetEnumBinder
import EncoderBuilder.{StaticField, StaticInvokeCatchException}
import org.apache.spark.sql.catalyst.analysis.{GetColumnByOrdinal, UnresolvedAttribute, UnresolvedExtractValue}
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, ExprCode}
import org.apache.spark.sql.catalyst.expressions.objects._
import org.apache.spark.sql.catalyst.{InternalRow, expressions}
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String
import org.hl7.fhir.dstu3.model._
import org.hl7.fhir.instance.model.api.{IBase, IBaseDatatype, IIdType}
import org.hl7.fhir.utilities.xhtml.XhtmlNode

import scala.collection.JavaConversions._
import scala.collection.immutable.Stream.Empty
import scala.reflect.ClassTag

/**
  * Spark Encoder for FHIR data models.
  */
private[bunsen] object EncoderBuilder {

  /**
    * Returns an encoder for the FHIR resource implemented by the given class
    *
    * @param definition The FHIR resource definition
    * @return An ExpressionEncoder for the resource
    */

  def of(definition: BaseRuntimeElementCompositeDefinition[_],
         context: FhirContext,
         converter: SchemaConverter): ExpressionEncoder[_] = {

    val fhirClass = definition.getImplementingClass()

    val schema = converter.compositeToStructType(definition)

    val inputObject = BoundReference(0, ObjectType(fhirClass), nullable = true)

    val encoderBuilder = new EncoderBuilder(context, converter)

    val serializers = encoderBuilder.serializer(inputObject, definition)

    assert(schema.fields.size == serializers.size,
      "Must have a serializer for each field.")

    val deserializer = encoderBuilder.compositeToDeserializer(definition, None)

    new ExpressionEncoder(
      schema,
      flat = false,
      serializers,
      deserializer = deserializer,
      ClassTag(fhirClass))
  }

  /**
    * This is a customized version of the StaticInvoke expression that catches and re-throws
    * an exception.
    *
    * Invokes a static function, returning the result.  By default, any of the arguments being null
    * will result in returning null instead of calling the function.
    *
    * @param staticObject  The target of the static call.  This can either be the object itself
    *                      (methods defined on scala objects), or the class object
    *                      (static methods defined in java).
    * @param dataType      The expected return type of the function call
    * @param functionName  The name of the method to call.
    * @param arguments     An optional list of expressions to pass as arguments to the function.
    * @param propagateNull When true, and any of the arguments is null, null will be returned instead
    *                      of calling the function.
    */
  case class StaticInvokeCatchException(
                                         staticObject: Class[_],
                                         dataType: DataType,
                                         functionName: String,
                                         arguments: Seq[Expression] = Nil,
                                         propagateNull: Boolean = true) extends Expression with NonSQLExpression {

    val objectName = staticObject.getName.stripSuffix("$")

    override def nullable: Boolean = true

    override def children: Seq[Expression] = arguments

    override def eval(input: InternalRow): Any =
      throw new UnsupportedOperationException("Only code-generated evaluation is supported.")

    override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
      val javaType = ctx.javaType(dataType)
      val argGen = arguments.map(_.genCode(ctx))
      val argString = argGen.map(_.value).mkString(", ")

      val callFunc = s"$objectName.$functionName($argString)"

      val setIsNull = if (propagateNull && arguments.nonEmpty) {
        s"boolean ${ev.isNull} = ${argGen.map(_.isNull).mkString(" || ")};"
      } else {
        s"boolean ${ev.isNull} = false;"
      }

      // If the function can return null, we do an extra check to make sure our null bit is still set
      // correctly.
      val postNullCheck = if (ctx.defaultValue(dataType) == "null") {
        s"${ev.isNull} = ${ev.value} == null;"
      } else {
        ""
      }

      ctx.addMutableState(javaType, ev.value, s"${ev.value} = ${ctx.defaultValue(dataType)};")

      val code =
        s"""
      ${argGen.map(_.code).mkString("\n")}
      $setIsNull
      try {
      ${ev.value} = ${ev.isNull} ? ${ctx.defaultValue(dataType)} : $callFunc;
      } catch (Exception e) {
         org.apache.spark.unsafe.Platform.throwException(e);
      }
      $postNullCheck
     """
      ev.copy(code = code)
    }
  }

  /**
    * Invokes a static function, returning the result.  By default, any of the arguments being null
    * will result in returning null instead of calling the function.
    *
    * @param staticObject The target of the static call.  This can either be the object itself
    *                     (methods defined on scala objects), or the class object
    *                     (static methods defined in java).
    * @param dataType     The expected type of the static field
    * @param fieldName    The name of the field to retrieve
    */
  private[bunsen] case class StaticField(staticObject: Class[_],
                                       dataType: DataType,
                                       fieldName: String) extends Expression with NonSQLExpression {

    val objectName = staticObject.getName.stripSuffix("$")

    override def nullable: Boolean = false

    override def children: Seq[Expression] = Nil

    override def eval(input: InternalRow): Any =
      throw new UnsupportedOperationException("Only code-generated evaluation is supported.")

    override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
      val javaType = ctx.javaType(dataType)

      val code =
        s"""
      final $javaType ${ev.value} = $objectName.$fieldName;
        """
      ev.copy(code = code, isNull = "false")
    }
  }

  /**
    * Returns the value if it is of the specified type, or null otherwise
    *
    * @param value       The value to returned
    * @param checkedType The type to check against the value via instanceOf
    * @param dataType    The type returned by the expression
    */
  private[bunsen] case class ValueIfType(value: Expression,
                                       checkedType: Class[_],
                                       dataType: DataType) extends Expression with NonSQLExpression {

    override def nullable: Boolean = true

    override def eval(input: InternalRow): Any =
      throw new UnsupportedOperationException("Only code-generated evaluation is supported.")

    override def children: Seq[Expression] = value :: Nil

    override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {

      val javaType = ctx.javaType(dataType)
      val obj = value.genCode(ctx)

      val code =
        s"""
        ${obj.code}
      final $javaType ${ev.value} = ${obj.value} instanceof ${checkedType.getName} ? (${checkedType.getName}) ${obj.value} : null;

         """

      ev.copy(code = code, isNull = s"(${obj.isNull} || !(${obj.value} instanceof ${checkedType.getName}))")
    }
  }

  /**
    * Casts an expression to another object.
    *
    * @param value      The value to cast
    * @param resultType The type the value should be cast to.
    */
  private[bunsen] case class ObjectCast(value: Expression,
                                      resultType: DataType) extends Expression with NonSQLExpression {

    override def nullable: Boolean = value.nullable

    override def eval(input: InternalRow): Any =
      throw new UnsupportedOperationException("Only code-generated evaluation is supported.")

    override def dataType: DataType = resultType

    override def children: Seq[Expression] = value :: Nil

    override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {

      value.genCode(ctx)
    }
  }

}

/**
  * Spark encoder for FHIR resources.
  */
private[bunsen] class EncoderBuilder(fhirContext: FhirContext, schemaConverter: SchemaConverter) {

  def enumerationToDeserializer(enumeration: RuntimeChildPrimitiveEnumerationDatatypeDefinition,
                                path: Option[Expression]): Expression = {

    def getPath: Expression = path.getOrElse(GetColumnByOrdinal(0, ObjectType(classOf[String])))

    // Get the value and initialize the instance.
    val utfToString = Invoke(getPath, "toString", ObjectType(classOf[String]), Nil)

    StaticInvokeCatchException(enumeration.getBoundEnumType,
      ObjectType(enumeration.getBoundEnumType),
      "fromCode",
      List(utfToString))
  }

  /**
    * Converts a FHIR DataType to a UTF8 string usable by Spark.
    */
  private def dataTypeToUtf8Expr(inputObject: Expression): Expression = {

    StaticInvoke(
      classOf[UTF8String],
      DataTypes.StringType,
      "fromString",
      List(Invoke(inputObject,
        "getValueAsString",
        ObjectType(classOf[String]))))
  }

  /**
    * Converts primitive-derived types that inherit from IPrimitiveType
    */
  private def primitiveToExpr(inputObject: Expression,
                              primitive: RuntimePrimitiveDatatypeDefinition): Expression = {

    primitive.getImplementingClass match {

      // If the FHIR primitive is serialized as a string, convert it to UTF8.
      case cls if SchemaConverter.fhirPrimitiveToSparkTypes.get(cls) == DataTypes.StringType =>
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
          SchemaConverter.decimalType,
          "apply",
          Invoke(inputObject, "getValue", ObjectType(classOf[java.math.BigDecimal])) :: Nil)

      case unknown =>
        throw new IllegalArgumentException("Cannot serialize unknown primitive type: " + unknown.getName)
    }
  }

  /**
    * Returns the accessor method for the given child field.
    */
  private def accessorFor(field: BaseRuntimeChildDefinition): String = {

    // Primitive single-value types typically use the Element suffix in their
    // accessors, with the exception of the "div" field for reasons that are not clear.
    if (field.isInstanceOf[RuntimeChildPrimitiveDatatypeDefinition] &&
      field.getMax == 1 &&
      field.getElementName != "div")
      "get" + field.getElementName.capitalize + "Element"
    else {
      if (field.getElementName.equals("class")) {
        "get" + field.getElementName.capitalize + "_"
      } else {
        "get" + field.getElementName.capitalize
      }
    }
  }

  /**
    * Returns the setter for the given field name.s
    */
  private def setterFor(field: BaseRuntimeChildDefinition): String = {

    // Primitive single-value types typically use the Element suffix in their
    // setters, with the exception of the "div" field for reasons that are not clear.
    if (field.isInstanceOf[RuntimeChildPrimitiveDatatypeDefinition] &&

      // Enumerations are set directly rather than via elements.
      !field.isInstanceOf[RuntimeChildPrimitiveEnumerationDatatypeDefinition] &&
      field.getMax == 1 && field.getElementName != "div")
      "set" + field.getElementName.capitalize + "Element"
    else
      "set" + field.getElementName.capitalize
  }

  /**
    * Returns the object type of the given child
    */
  private def objectTypeFor(field: BaseRuntimeChildDefinition): ObjectType = {

    val cls = field match {

      case resource: RuntimeChildResourceDefinition =>
        resource.getChildByName(resource.getElementName).getImplementingClass

      case block: RuntimeChildResourceBlockDefinition =>
        block.getSingleChildOrThrow.getImplementingClass

      case composite: RuntimeChildCompositeDatatypeDefinition =>
        composite.getDatatype

      case primitive: RuntimeChildPrimitiveDatatypeDefinition =>

        primitive.getSingleChildOrThrow.getChildType match {

          case ChildTypeEnum.PRIMITIVE_DATATYPE =>
            primitive.getSingleChildOrThrow.getImplementingClass

          case ChildTypeEnum.PRIMITIVE_XHTML_HL7ORG =>
            classOf[XhtmlNode]

          case ChildTypeEnum.ID_DATATYPE =>
            primitive.getSingleChildOrThrow.getImplementingClass

          case unsupported =>
            throw new IllegalArgumentException("Unsupported child primitive type: " + unsupported)
        }
    }

    ObjectType(cls)
  }

  /**
    * Returns a sequence of name, value expressions
    */
  private def childToExpr(parentObject: Expression,
                          childDefinition: BaseRuntimeChildDefinition): Seq[Expression] = {

    // Contained resources and extensions not yet supported.
    if (childDefinition.isInstanceOf[RuntimeChildContainedResources] ||
      childDefinition.isInstanceOf[RuntimeChildExtension]) {

      Empty

    } else if (childDefinition.isInstanceOf[RuntimeChildChoiceDefinition]) {

      val getterFunc = if (childDefinition.getElementName.equals("class")) {
        "get" + childDefinition.getElementName.capitalize + "_"
      } else {
        "get" + childDefinition.getElementName.capitalize
      }

      // At this point we don't the actual type of the child, so get it as the general IBaseDatatype
      val choiceObject = Invoke(parentObject,
        getterFunc,
        ObjectType(classOf[IBaseDatatype]))

      val choice = childDefinition.asInstanceOf[RuntimeChildChoiceDefinition]

      // Flatmap to create a list of (name, expression) items.
      // Note that we iterate by types and then look up the name, since this
      // ensures we get the preferred field name for the given type.
      val namedExpressions = choice.getValidChildTypes.toList.flatMap(fhirChildType => {

        val childName = choice.getChildNameByDatatype(fhirChildType)

        val choiceChildDefinition = choice.getChildByName(childName)

        val childObject = EncoderBuilder.ValueIfType(choiceObject,
          choiceChildDefinition.getImplementingClass,
          ObjectType(choiceChildDefinition.getImplementingClass))

        val childExpr = choiceChildDefinition match {

          case composite: BaseRuntimeElementCompositeDefinition[_] => compositeToExpr(childObject, composite)
          case primitive: RuntimePrimitiveDatatypeDefinition => primitiveToExpr(childObject, primitive);
        }

        List(Literal(childName), childExpr)
      })

      namedExpressions

    } else {

      val definition = childDefinition.getChildByName(childDefinition.getElementName)

      if (childDefinition.getMax != 1) {

        val childList = Invoke(parentObject,
          accessorFor(childDefinition),
          ObjectType(classOf[java.util.List[_]]))

        val elementExpr = definition match {
          case composite: BaseRuntimeElementCompositeDefinition[_] => (elem: Expression) => compositeToExpr(elem, composite)
          case primitive: RuntimePrimitiveDatatypeDefinition => (elem: Expression) => primitiveToExpr(elem, primitive)
        }

        val childExpr = MapObjects(elementExpr,
          childList,
          objectTypeFor(childDefinition))

        List(Literal(childDefinition.getElementName), childExpr)

      } else {

        // Get the field accessor
        val childObject = Invoke(parentObject,
          accessorFor(childDefinition),
          objectTypeFor(childDefinition))

        val childExpr = definition match {
          case composite: BaseRuntimeElementCompositeDefinition[_] => compositeToExpr(childObject, composite)
          case primitive: RuntimePrimitiveDatatypeDefinition => primitiveToExpr(childObject, primitive);
          case narrative: RuntimePrimitiveDatatypeNarrativeDefinition => dataTypeToUtf8Expr(childObject);
          case htmlHl7Org: RuntimePrimitiveDatatypeXhtmlHl7OrgDefinition => dataTypeToUtf8Expr(childObject)
        }

        List(Literal(childDefinition.getElementName), childExpr)
      }
    }
  }

  private def compositeToExpr(inputObject: Expression,
                              definition: BaseRuntimeElementCompositeDefinition[_]): Expression = {

    // Handle references as special cases, since they include a recursive structure
    // that can't be mapped onto a dataframe
    val fields: Seq[Expression] =
    if (definition.getImplementingClass == classOf[Reference]) {

      // Reference type, so return only supported fields.
      // We also explicitly use the IIDType for the reference element,
      // since that differs from the conventions used to infer
      // other types.
      val reference = dataTypeToUtf8Expr(
        Invoke(inputObject,
          "getReferenceElement",
          ObjectType(classOf[IIdType])))

      val display = dataTypeToUtf8Expr(
        Invoke(inputObject,
          "getDisplayElement",
          ObjectType(classOf[org.hl7.fhir.dstu3.model.StringType])))

      List(Literal("reference"), reference,
        Literal("display"), display)

    } else if (definition.getImplementingClass == classOf[ValueSet.ValueSetExpansionContainsComponent]) {

      val system = dataTypeToUtf8Expr(
        Invoke(inputObject,
          "getSystemElement",
          ObjectType(classOf[UriType])))

      val abstract_ = Invoke(inputObject,
        "getAbstract",
        DataTypes.BooleanType)

      val inactive = Invoke(inputObject,
        "getInactive",
        DataTypes.BooleanType)

      val version = dataTypeToUtf8Expr(
        Invoke(inputObject,
          "getVersionElement",
          ObjectType(classOf[org.hl7.fhir.dstu3.model.StringType])))

      val code = dataTypeToUtf8Expr(
        Invoke(inputObject,
          "getCodeElement",
          ObjectType(classOf[org.hl7.fhir.dstu3.model.CodeType])))

      val display = dataTypeToUtf8Expr(
        Invoke(inputObject,
          "getDisplayElement",
          ObjectType(classOf[org.hl7.fhir.dstu3.model.StringType])))

      List(Literal("system"), system,
        Literal("abstract"), abstract_,
        Literal("inactive"), inactive,
        Literal("version"), version,
        Literal("code"), code,
        Literal("display"), display)

    } else {
      // Map to (name, value, name, value) expressions for child elements.
      definition.getChildren
        .flatMap(child => childToExpr(inputObject, child))
    }

    val createStruct = CreateNamedStruct(fields)

    expressions.If(IsNull(inputObject),
      Literal.create(null, createStruct.dataType),
      createStruct)
  }

  private def serializer(inputObject: Expression,
                         definition: BaseRuntimeElementCompositeDefinition[_]): Seq[Expression] = {

    // Map to (name, value, name, value) expressions for child elements.
    val childFields: Seq[Expression] =
      definition.getChildren
        .flatMap(child => childToExpr(inputObject, child))

    // The fields are (name, expr) tuples, so just get the expressions for the top level.
    childFields.grouped(2)
      .map(group => group.get(1))
      .toList
  }

  private def listToDeserializer(definition: BaseRuntimeElementDefinition[_ <: IBase],
                                 path: Expression): Expression = {

    val array = definition match {

      case composite: BaseRuntimeElementCompositeDefinition[_] => {

        val elementType = schemaConverter.compositeToStructType(composite)

        Invoke(
          MapObjects(element =>
            compositeToDeserializer(composite, Some(element)),
            path,
            elementType),
          "array",
          ObjectType(classOf[Array[Any]]))
      }

      case primitive: RuntimePrimitiveDatatypeDefinition => {
        val elementType = schemaConverter.primitiveToDataType(primitive)

        Invoke(
          MapObjects(element =>
            primitiveToDeserializer(primitive.getImplementingClass, Some(element)),
            path,
            elementType),
          "array",
          ObjectType(classOf[Array[Any]]))
      }
    }

    StaticInvoke(
      classOf[java.util.Arrays],
      ObjectType(classOf[java.util.List[_]]),
      "asList",
      array :: Nil)
  }

  /**
    * Returns a deserializer for the choice, which will return the first
    * non-null field included in the choice definition. Note this method
    * is based on choice types rather than names, since this will ensure
    * the underlying FHIR API uses the preferred name for the given type.
    *
    * @param fhirChildTypes the choice's types to deserialize
    * @param choiceChildDefinition the choice definition
    * @param path path to the field
    * @return a deserializer expression.
    */
  private def choiceToDeserializer(fhirChildTypes: Seq[Class[_ <: IBase]],
                                   choiceChildDefinition: RuntimeChildChoiceDefinition,
                                   path: Option[Expression]): Expression = {

    if (fhirChildTypes.isEmpty) {

      // No remaining choices, so return null.
      Literal.create(null, ObjectType(classOf[org.hl7.fhir.dstu3.model.Type]))

    } else {

      def getPath: Expression = path.getOrElse(GetColumnByOrdinal(0, ObjectType(classOf[String])))

      def addToPath(part: String): Expression = path
        .map(p => UnresolvedExtractValue(p, expressions.Literal(part)))
        .getOrElse(UnresolvedAttribute(part))

      val fhirChildType = fhirChildTypes.head
      val childName = choiceChildDefinition.getChildNameByDatatype(fhirChildType)

      val choiceField = choiceChildDefinition.getChildByName(childName)
      val childPath = addToPath(childName)

      val deserializer = choiceField match {

        case composite: BaseRuntimeElementCompositeDefinition[_] => compositeToDeserializer(composite, Some(childPath))
        case primitive: RuntimePrimitiveDatatypeDefinition => primitiveToDeserializer(primitive.getImplementingClass, Some(childPath))
      }

      val child = EncoderBuilder.ObjectCast(deserializer, ObjectType(classOf[org.hl7.fhir.dstu3.model.Type]))

      // If this item is not null, deserialize it. Otherwise attempt other choices.
      expressions.If(IsNotNull(childPath),
        child,
        choiceToDeserializer(fhirChildTypes.tail, choiceChildDefinition, path))
    }
  }

  private def childToDeserializer(childDefinition: BaseRuntimeChildDefinition,
                                  path: Option[Expression]): Map[String, Expression] = {


    def getPath: Expression = path.getOrElse(GetColumnByOrdinal(0, ObjectType(classOf[String])))

    def addToPath(part: String): Expression = path
      .map(p => UnresolvedExtractValue(p, expressions.Literal(part)))
      .getOrElse(UnresolvedAttribute(part))

    // Contained resources and extensions not yet supported.
    if (childDefinition.isInstanceOf[RuntimeChildContainedResources] ||
      childDefinition.isInstanceOf[RuntimeChildExtension]) {

      Map()

    } else if (childDefinition.isInstanceOf[RuntimeChildChoiceDefinition]) {

      val choiceChildDefinition = childDefinition.asInstanceOf[RuntimeChildChoiceDefinition]

      Map(childDefinition.getElementName ->
        choiceToDeserializer(choiceChildDefinition.getValidChildTypes.toList,
          choiceChildDefinition,
          path))

    } else if (childDefinition.getMax != 1) {

      val definition = childDefinition.getChildByName(childDefinition.getElementName)
        .asInstanceOf[BaseRuntimeElementDefinition[_ <: IBase]]

      // Handle lists
      Map(childDefinition.getElementName -> listToDeserializer(definition, addToPath(childDefinition.getElementName)))

    } else {

      val childPath = Some(addToPath(childDefinition.getElementName))

      // These must match on the RuntimeChild* structures rather than the definitions,
      // since only the RuntimeChild* structures include default values to be passed
      // to constructors when deserializing some bound objects.
      val result = childDefinition match {

        case boundCode: RuntimeChildPrimitiveBoundCodeDatatypeDefinition =>
          boundCodeToDeserializer(boundCode, childPath)

        case enumeration: RuntimeChildPrimitiveEnumerationDatatypeDefinition =>
          enumerationToDeserializer(enumeration, childPath)

        // Handle bound codeable concepts
        case boundComposite: RuntimeChildCompositeBoundDatatypeDefinition =>
          boundCodeableConceptToDeserializer(boundComposite, childPath)

        case resource: RuntimeChildResourceDefinition =>
          compositeToDeserializer(
            resource.getChildByName(resource.getElementName)
              .asInstanceOf[BaseRuntimeElementCompositeDefinition[_]],
            childPath)

        case block: RuntimeChildResourceBlockDefinition =>
          compositeToDeserializer(block.getSingleChildOrThrow.asInstanceOf[BaseRuntimeElementCompositeDefinition[_]], childPath)

        case composite: RuntimeChildCompositeDatatypeDefinition =>
          compositeToDeserializer(composite.getSingleChildOrThrow.asInstanceOf[BaseRuntimeElementCompositeDefinition[_]], childPath)

        case primitive: RuntimeChildPrimitiveDatatypeDefinition => {

          val definition = childDefinition.getChildByName(childDefinition.getElementName)

          definition match {
            case primitive: RuntimePrimitiveDatatypeDefinition => primitiveToDeserializer(primitive.getImplementingClass, childPath)
            case htmlHl7: RuntimePrimitiveDatatypeXhtmlHl7OrgDefinition => xhtmlHl7ToDeserializer(htmlHl7, childPath)
          }
        }
      }

      // Item is not a list,
      // nonListChildToDeserializer(childDefinition, path)

      Map(childDefinition.getElementName -> result)
    }

  }

  private def boundCodeToDeserializer(boundCode: RuntimeChildPrimitiveBoundCodeDatatypeDefinition,
                                      path: Option[Expression]): Expression = {

    // Construct a bound code instance with the value set based on the enumeration.
    val boundCodeInstance = NewInstance(boundCode.getDatatype,
      EncoderBuilder.StaticField(boundCode.getBoundEnumType,
        ObjectType(classOf[IValueSetEnumBinder[_]]),
        "VALUESET_BINDER") :: Nil,
      ObjectType(boundCode.getDatatype))

    def getPath: Expression = path.getOrElse(GetColumnByOrdinal(0, ObjectType(classOf[String])))

    // Get the value and initialize the instance.
    val utfToString = Invoke(getPath, "toString", ObjectType(classOf[String]), Nil)

    InitializeJavaBean(boundCodeInstance, Map("setValueAsString" -> utfToString))
  }

  private def boundCodeableConceptToDeserializer(boundCodeable: RuntimeChildCompositeBoundDatatypeDefinition,
                                                 path: Option[Expression]): Expression = {

    val boundCodeInstance = NewInstance(boundCodeable.getDatatype,
      EncoderBuilder.StaticField(boundCodeable.getBoundEnumType,
        ObjectType(classOf[IValueSetEnumBinder[_]]),
        "VALUESET_BINDER") :: Nil,
      ObjectType(boundCodeable.getDatatype))

    def getPath: Expression = path.getOrElse(GetColumnByOrdinal(0, ObjectType(classOf[String])))

    def addToPath(part: String): Expression = path
      .map(p => UnresolvedExtractValue(p, expressions.Literal(part)))
      .getOrElse(UnresolvedAttribute(part))


    // Temporarily treat as a composite.
    val definition = boundCodeable.getChildByName(boundCodeable.getElementName)

    compositeToDeserializer(definition.asInstanceOf[BaseRuntimeElementCompositeDefinition[_]], path)
  }

  private def xhtmlHl7ToDeserializer(xhtmlHl7: RuntimePrimitiveDatatypeXhtmlHl7OrgDefinition, path: Option[Expression]): Expression = {

    def getPath: Expression = path.getOrElse(GetColumnByOrdinal(0, ObjectType(xhtmlHl7.getImplementingClass)))

    val newInstance = NewInstance(xhtmlHl7.getImplementingClass,
      Nil,
      ObjectType(xhtmlHl7.getImplementingClass))

    val stringValue = Invoke(getPath, "toString", ObjectType(classOf[String]), Nil)

    // If there is a non-null value, create and set the xhtml node.
    expressions.If(IsNull(stringValue),
      expressions.Literal.create(null, newInstance.dataType),
      InitializeJavaBean(newInstance, Map("setValueAsString" ->
        stringValue)))
  }


  /**
    * Returns an expression that deserializes the given FHIR primitive type.
    */
  private def primitiveToDeserializer(primitiveClass: Class[_],
                                      path: Option[Expression]): Expression = {

    def getPath: Expression = path.getOrElse(GetColumnByOrdinal(0, ObjectType(primitiveClass)))

    primitiveClass match {

      // If the FHIR primitive is represented as a string type, read it from UTF8 and
      // set the value.
      case cls if SchemaConverter.fhirPrimitiveToSparkTypes.get(cls) == DataTypes.StringType => {

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

  /**
    * Returns an expression for deserializing a composite structure at the given path.
    */
  private def compositeToDeserializer(definition: BaseRuntimeElementCompositeDefinition[_],
                                      path: Option[Expression]): Expression = {

    def addToPath(part: String): Expression = path
      .map(p => UnresolvedExtractValue(p, expressions.Literal(part)))
      .getOrElse(UnresolvedAttribute(part))

    def getPath: Expression = path.getOrElse(GetColumnByOrdinal(0,
      schemaConverter.compositeToStructType(definition)))

    // Map to (name, value, name, value) expressions for child elements.
    val childExpressions: Map[String, Expression] = definition.getChildren
      .filter(child => definition.getImplementingClass != classOf[Reference] ||
        child.getElementName == "reference" ||
        child.getElementName == "display")
      .filter(child => definition.getImplementingClass != classOf[ValueSet.ValueSetExpansionContainsComponent] ||
        child.getElementName != "contains")
      .flatMap(child => childToDeserializer(child, path)).toMap

    val compositeInstance = NewInstance(definition.getImplementingClass,
      Nil,
      ObjectType(definition.getImplementingClass))

    val setters = childExpressions.map { case (name, expression) =>

      // Option types are not visible in the getChildByName, so we fall back
      // to looking for them in the child list.
      val childDefinition = if (definition.getChildByName(name) != null)
        definition.getChildByName(name)
      else
        definition.getChildren.find(childDef => childDef.getElementName == name).get

      (setterFor(childDefinition), expression)
    }

    val result = InitializeJavaBean(compositeInstance, setters)

    if (path.nonEmpty) {
      expressions.If(
        IsNull(getPath),
        expressions.Literal.create(null, ObjectType(definition.getImplementingClass)),
        result)
    } else {
      result
    }
  }
}


