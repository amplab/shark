/*
 * Copyright (C) 2012 The Regents of The University California.
 * All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package shark.execution.cg.row

/**
 * Root class node of source code generating tree 
 */
abstract class ExecuteOrderedExprNode(val nested: ExecuteOrderedExprNode = null)
    extends ExprNode[ExecuteOrderedExprNode] {
  self: Product =>

  def children = (nested :: Nil).filter(_ != null)

  def code(ctx: CGExprContext): String = currCode(ctx) + 
    (if (nested != null) nested.code(ctx) else "")

  protected def currCode(ctx: CGExprContext): String = ""

  def essential: TypedExprNode = if (nested == null) null else nested.essential

  def initialEssential(ctx: CGExprContext) {}

  def initialAll(ctx: CGExprContext) {
    initialEssential(ctx)

    children.foreach(_.initialAll(ctx))
  }
}

/**
 * Node of declare the expression variable in generated source code
 */
case class EENDeclare(ten: TypedExprNode, expr: ExecuteOrderedExprNode)
    extends ExecuteOrderedExprNode(expr) {
  private def define(ctx: CGExprContext, clazz: String, variable: String): String = clazz match {
    case "boolean" => "%s %s = %s;".format(clazz, variable, ctx.exprDefaultValue(ten, "false"))
    case "byte" | "short" | "int" | "float" | "long" | "double" => {
      "%s %s = %s;".format(clazz, variable, ctx.exprDefaultValue(ten, "0"))
    }
    case _ => "%s %s = %s;".format(clazz, variable, ctx.exprDefaultValue(ten, "null"))
  }

  override def currCode(ctx: CGExprContext): String = {
    val variableType = ctx.exprType(ten)
    val variableName = ctx.exprName(ten)

    val nullIndicatorName = ctx.indicatorName(ten)
    val code = new StringBuffer()

    if (variableType != null) {
      code.append(define(ctx, variableType, variableName))
    }
    if (nullIndicatorName != null) {
      code.append("boolean %s = %s;".format(nullIndicatorName, ctx.indicatorDefaultValue(ten)))
    }

    code.toString()
  }

  override def essential: TypedExprNode = ten
}

/**
 * Node of assignment for the expression value and its variable, this is only used in 
 * the conditional expression currently.
 */
case class EENAssignment(ten: TypedExprNode, expr: ExecuteOrderedExprNode)
    extends ExecuteOrderedExprNode(expr) {
  override def code(ctx: CGExprContext): String = {
    val e = expr.essential

    val code = "%s = %s; %s".format(ctx.exprName(ten), ctx.exprName(e), ctx.codeValidate(ten))

    code
  }

  override def essential: TypedExprNode = ten
  override def children = (expr :: Nil).filter(_ != null)
}

/**
 * Node of joint 2 statements
 */
case class EENSequence(expr: ExecuteOrderedExprNode, next: ExecuteOrderedExprNode) 
    extends ExecuteOrderedExprNode(expr) {
  override def code(ctx: CGExprContext): String =
    (if (expr != null)
      expr.code(ctx)
    else
      "") + (
      if (next != null)
        next.code(ctx)
      else
        "")

  override def children = (expr :: next :: Nil).filter(_ != null)
}

/**
 * Node of conditional checking, but only test the expression validity
 */
case class EENGuardNull(ten: TypedExprNode, inner: ExecuteOrderedExprNode) 
    extends ExecuteOrderedExprNode(inner) {
  override def code(ctx: CGExprContext): String = {
    if ((ten.isInstanceOf[TENLiteral] && ten.asInstanceOf[TENLiteral].obj == null) || 
      inner == null) {
      ""
    } else {
      val code = new StringBuffer()
      val cond = ctx.codeIsValid(ten)
      val innerCode = inner.code(ctx)
      if (innerCode == null || innerCode.isEmpty()) {
        code.append("")
      } else {
        if (cond != null) {
          code.append("if(%s) {\n%s\n}".format(cond, innerCode))
        } else {
          code.append(innerCode)
        }
      }

      code.toString
    }
  }

  override def children = (inner :: Nil).filter(_ != null)
}
