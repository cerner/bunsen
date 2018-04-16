/*
 * This is a backport of encoder functionality targeted for Spark 2.4.
 *
 * See https://issues.apache.org/jira/browse/SPARK-22739 for details.
 */

package com.cerner.bunsen.backports

import scala.language.existentials

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, ExprCode}
import org.apache.spark.sql.types._

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
case class StaticField(staticObject: Class[_],
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
  * Determines if the given value is an instanceof a given class
  *
  * @param value the value to check
  * @param checkedType the class to check the value against
  */
case class InstanceOf(value: Expression,
                      checkedType: Class[_]) extends Expression with NonSQLExpression {

  override def nullable: Boolean = false
  override def children: Seq[Expression] = value :: Nil
  override def dataType: DataType = BooleanType

  override def eval(input: InternalRow): Any =
    throw new UnsupportedOperationException("Only code-generated evaluation is supported.")

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {

    val obj = value.genCode(ctx)

    val code =
      s"""
         ${obj.code}
         final boolean ${ev.value} = ${obj.value} instanceof ${checkedType.getName};
       """

    ev.copy(code = code, isNull = "false")
  }
}

/**
  * Casts the result of an expression to another type.
  *
  * @param value The value to cast
  * @param resultType The type to which the value should be cast
  */
case class ObjectCast(value: Expression, resultType: DataType)
  extends Expression with NonSQLExpression {

  override def nullable: Boolean = value.nullable
  override def dataType: DataType = resultType
  override def children: Seq[Expression] = value :: Nil

  override def eval(input: InternalRow): Any =
    throw new UnsupportedOperationException("Only code-generated evaluation is supported.")

  override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {

    val javaType = ctx.javaType(resultType)
    val obj = value.genCode(ctx)

    val code =
      s"""
         ${obj.code}
         final $javaType ${ev.value} = ($javaType) ${obj.value};
       """

    ev.copy(code = code, isNull = obj.isNull)
  }
}