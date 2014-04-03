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

import scala.collection.mutable.Map

/**
 * In interpret mode, expressions is represented as pre-traversal tree, however, it's the 
 * post-traversal order in executing. 
 * 
 * Class TEN2EEN transforms (rotate) the expression tree into and executing order tree, by 
 * adding 'statement'.
 * 
 * For example:
 * 
 * Expression (a+b)*((a+b)+c)
 * In interpret mode the expression tree is known as:
 *                 * 
 *               /   \
 *              +     +
 *             / \   / \
 *            a   b +   c
 *                 / \
 *                a   b
 * The executing order tree may looks like (by rotate the interpret tree):
 *               a(declare)
 *           /       \
 *        isnull(a)   b (declare)
 *         /       /     \
 *        *   isnull(b)   + (a+b) (declare)
 *               /         \
 *              *           a (declare)
 *                      /       \
 *                   isnull(a)   b (declare)
 *                    /       /     \
 *             null(a+b)  isnull(b)   + (a+b) (declare)
 *                 /         /         \
 *        null((a+b)+c) null(a+b)        c (declare)
 *               /         /          /     \
 *              *  null((a+b)+c) isnull(c)   + ((a+b)+c) (declare)
 *                       /          /          \
 *                      *   null((a+b)+c)       * (a+b)*((a+b)+c) (result)
 *                              /
 *                             *
 *                             
 * The leaf node represents the expression value, by default the expression value is null, only the
 * right most leaf node may change that. 
 * 
 * However, there are still lots of duplicated computing in checking/declaring the node value. We
 * need to eliminate them in the executing path tree.  
 * As result we will see a new executing order tree as: (a+b) is not shown up again.
 *               a(declare)
 *           /       \
 *        isnull(a)   b (declare)
 *         /       /     \
 *        *   isnull(b)   + (a+b) (declare)
 *               /         \
 *              *           c (declare)
 *                       /     \
 *               isnull(c)      + ((a+b)+c) (declare)
 *                    /          /      \
 *                   *   null((a+b)+c)   * (a+b)*((a+b)+c) (result)
 *                              /
 *                             *
 * The above demonstrates the basic idea on tree node "rotate" and "eos" (Executing Order Simplify).
 * Besides the algebraical expression, we also need to support logical and conditional expression, 
 * which may presents as UDF / GenericUDF and partial evaluating. Finding more details in the code.
 */

/**
 * Store the expression in the executing order path.
 */
class PathNodeContext(table: Map[TypedExprNode, 
  TypedExprNode] = Map[TypedExprNode, TypedExprNode]()) {

  def update(ten: TypedExprNode): PathNodeContext = {
    if (ten != null) {
      table.getOrElseUpdate(ten, ten)
    }

    this
  }

  def update(tens: Seq[TypedExprNode]): PathNodeContext = {
    tens.foreach(this update _)

    this
  }

  def get(ten: TypedExprNode): TypedExprNode = if (ten == null) null else table.getOrElse(ten, null)

  override def clone = new PathNodeContext(table.clone)
}

class TEN2EEN {
  /**
   * Mapping the expression from TEN(Typed Expression Node) => EEN (Executing order Expression Node) 
   */
  private def create(ten: TypedExprNode, sibling: ExecuteOrderedExprNode = null)
  : EENExpr = ten match {
    case x: TENAttribute => EENAttribute(x, sibling)
    case x: TENBuiltin => EENBuiltin(x, sibling)
    case x: TENGUDF => EENGUDF(x, sibling)
    case x: TENUDF => EENUDF(x, sibling)
    case x: TENConvertR2R => EENConvertR2R(x, sibling)
    case x: TENConvertR2W => EENConvertR2W(x, sibling)
    case x: TENConvertW2R => EENConvertW2R(x, sibling)
    case x: TENConvertW2D => EENConvertW2D(x, sibling)
    case x: TENLiteral => EENLiteral(x, sibling)
    case x: TENInputRow => EENInputRow(x, sibling)
    case x: TENOutputExpr => EENOutputExpr(x, sibling)
    case _ => EENAlias(ten, sibling)
  }

  /**
   * Stateful UDFs can not be considered as simple common sub expression, cause it can not be 
   * ignored in partial evaluating. It has to be computed and only once for each row feeding, hence
   * we pick up the stateful udfs and computing it before everything else, those expressions
   * who child the stateful udfs directly, will hold a reference to the udf(by EENAlias).
   */
  private val statefulUDFs = new java.util.LinkedHashMap[TypedExprNode, ExecuteOrderedExprNode]()

  def rotate(ten: TypedExprNode, sibling: ExecuteOrderedExprNode, nullCheck: Boolean)
  : ExecuteOrderedExprNode = {
    if (ten.isStateful) {
      // put the stateful udf into very beginning.
      if (statefulUDFs.get(ten) == null) {
        statefulUDFs.put(ten, transform(ten, null, false))
      }
      if (nullCheck) {
        EENGuardNull(ten, sibling)
      } else {
        EENAlias(ten, sibling)
      }
    } else {
      transform(ten, sibling, nullCheck)
    }
  }

  // transform the tree (from the desc(pre-order travesal) to executing order (post-order travesal)
  private def transform(ten: TypedExprNode, sibling: ExecuteOrderedExprNode, nullCheck: Boolean)
  : ExecuteOrderedExprNode = {
    ten match {
      case x: TENInputRow => {
        if (nullCheck) {
          EENDeclare(x, create(x, EENGuardNull(x, sibling)))
        } else {
          EENDeclare(x, create(x, sibling))
        }
      }
      case x @ TENAttribute(attr, outter) => {
        if (nullCheck) {
          rotate(outter, EENDeclare(x, create(x, EENGuardNull(x, sibling))), true)
        } else {
          EENDeclare(x, rotate(outter, EENGuardNull(outter, create(x, sibling)), false))
        }
      }
      case x @ TENBranch(branchIf, branchThen, branchElse) => {
        if (nullCheck) {
          rotate(
            branchIf,
            EENDeclare(
              x,
              EENSequence(
                EENGuardNull(branchIf,
                  EENCondition(
                    EENAlias(branchIf),
                    rotate(branchThen, 
                      EENGuardNull(branchThen, EENAssignment(x, create(branchThen))), true),
                    rotate(branchElse, 
                      EENGuardNull(branchElse, EENAssignment(x, create(branchElse))), true),
                    x, null)),
                sibling)),
            true)
        } else {
          EENSequence(EENDeclare(x,
            rotate(
              branchIf,
              EENGuardNull(branchIf,
                EENCondition(
                  EENAlias(branchIf),
                  rotate(branchThen, 
                    EENGuardNull(branchThen, EENAssignment(x, create(branchThen))), true),
                  rotate(branchElse, 
                    EENGuardNull(branchElse, EENAssignment(x, create(branchElse))), true),
                  x, null)),
              true)),
            sibling)
        }
      }
      case x @ TENBuiltin(op, children, dt, true) => {
        if (nullCheck) {
          val core = EENDeclare(x, create(x, EENGuardNull(x, sibling)))
          children.foldRight[ExecuteOrderedExprNode](core)((e1, e2) => {
            rotate(e1, e2, true)
          })
        } else {
          val core = create(x)
          EENDeclare(x, EENSequence(children.foldRight[ExecuteOrderedExprNode](core)((e1, e2) => {
            rotate(e1, e2, true)
          }), EENGuardNull(x, sibling)))
        }
      }
      case x @ TENBuiltin(op, children, dt, false) => {
        val core = create(x)
        EENDeclare(x, EENSequence(children.foldRight[ExecuteOrderedExprNode](core)((e1, e2) => {
          EENSequence(rotate(e1, null, false), e2)
        }), sibling))
      }
      case x @ TENConvertR2R(expr, _) => {
        if (nullCheck) {
          rotate(expr, EENDeclare(x, create(x, EENGuardNull(x, sibling))), true)
        } else {
          EENDeclare(x, rotate(expr, EENGuardNull(expr, create(x, sibling)), false))
        }
      }
      case x @ TENConvertR2W(expr) => {
        if (nullCheck) {
          rotate(expr, EENDeclare(x, create(x, EENGuardNull(x, sibling))), true)
        } else {
          EENDeclare(x, rotate(expr, EENGuardNull(expr, create(x, sibling)), false))
        }
      }
      case x @ TENConvertW2D(expr) => {
        EENDeclare(x, EENSequence(rotate(expr, null, false), create(x, sibling)))
      }
      case x @ TENConvertW2R(expr) => {
        if (nullCheck) {
          rotate(expr, EENDeclare(x, create(x, EENGuardNull(x, sibling))), true)
        } else {
          EENDeclare(x, rotate(expr, EENGuardNull(expr, create(x, sibling)), false))
        }
      }
      case x @ TENGUDF(_, children) => {
        if (nullCheck) {
          val core = EENDeclare(x, create(x, EENGuardNull(x, sibling)))
          children.foldRight[ExecuteOrderedExprNode](core)((e1, e2) => {
            EENSequence(rotate(e1, null, false), e2)
          })
        } else {
          val core = create(x)
          EENDeclare(x, EENSequence(children.foldRight[ExecuteOrderedExprNode](core)((e1, e2) => {
            EENSequence(rotate(e1, null, false), e2)
          }), sibling))
        }
      }
      case x @ TENUDF(bridge, children) => {
        if (nullCheck) {
          val core = EENDeclare(x, create(x, EENGuardNull(x, sibling)))
          children.foldRight[ExecuteOrderedExprNode](core)((e1, e2) => {
            EENSequence(rotate(e1, null, false), e2)
          })
        } else {
          val core = create(x)
          EENDeclare(x, EENSequence(children.foldRight[ExecuteOrderedExprNode](core)((e1, e2) => {
            EENSequence(rotate(e1, null, false), e2)
          }), sibling))
        }
      }
      case x @ TENOutputField(name, expr, dt) => {
        rotate(expr, EENGuardNull(expr, EENOutputField(x)), true)
      }
      case x @ TENOutputWritableField(name, expr, dt) => {
        rotate(expr, EENGuardNull(expr, EENOutputWritableField(x)), true)
      }
      case x @ TENOutputExpr(expr) => {
        // rotate the fields first, and in the mean time, will collect the stateful UDF node
        val value = rotate(expr, EENGuardNull(expr, create(x)), true)
        mergeSeq(value :: Nil)
      }
      case x @ TENOutputRow(fields, dt) => {
        // rotate the fields first, and in the mean time, will collect the stateful UDF node
        val fieldSeq = fields.map(rotate(_, null, false))
        EENOutputRow(x, mergeSeq(fieldSeq))
      }
      case x: TENLiteral => if (nullCheck) {
        create(x, EENGuardNull(x, sibling))
      } else {
        create(x, sibling)
      }
    }
  }

  private def mergeSeq(next: Seq[ExecuteOrderedExprNode]) = {
    import scala.collection.JavaConversions._
    val udfSeq = statefulUDFs.values().toSeq

    (udfSeq ++ next).foldRight(EENSequence(null, null))((a, b) => EENSequence(a, b))
  }

  case class TENGuardNullHolder(delegate: TypedExprNode) extends TypedExprNode {
    override def children = (delegate :: Nil).filter(_ != null)
  }

  case class TENDeclareHolder(delegate: TypedExprNode) extends TypedExprNode {
    override def children = (delegate :: Nil).filter(_ != null)
  }
  
  // TODO incomplete version of Eliminate the common sub expression (expr / declare / null guard) 
  // cause we don't get the common sub expression from branches / sequences
  def eos(tree: ExecuteOrderedExprNode, ctx: PathNodeContext, previous: ExecuteOrderedExprNode)
  : ExecuteOrderedExprNode = {
    tree match {
      case null => previous
      case x @ EENDeclare(ten, een) => {
        val placeholder = TENDeclareHolder(ten)

        val output = if (ten.isInstanceOf[TENLiteral] || 
            ctx.get(ten) != null || 
            ctx.get(placeholder) != null) {
          // the node exists in the previous list
          eos(een, ctx, null)
        } else {
          ctx.update(placeholder)
          EENDeclare(ten, eos(een, ctx, null))
        }
        ctx.update(ten)

        output
      }
      case x @ EENGuardNull(ten, inner) => {
        val value = ctx.get(ten)
        val placeholder = TENGuardNullHolder(ten)
        val output = if (ctx.get(placeholder) != null) {
          // we don't need the guard any more
          eos(inner, ctx, EENAlias(value))
        } else {
          val nested = eos(inner, ctx.clone.update(placeholder), EENAlias(value))
          EENGuardNull(ten, nested)
        }

        output
      }
      case x @ EENSequence(expr, next) => {
        val ten = if (expr != null) expr.essential else null
        val p = if (ten != null) {
          eos(expr, ctx, previous)
        } else {
          null
        }

        val n = if (next != null) {
          val prev = ctx.get(ten)

          val t = eos(next, ctx.update(ten), if (prev == null) null else EENAlias(prev))
          ctx.update(next.essential)

          t
        } else {
          null
        }
        EENSequence(p, n)
      }
      case x @ EENAssignment(ten, een) => {
        if (ctx.get(ten) != null) {
          eos(een, ctx, null)
        } else {
          val output = EENAssignment(ten, eos(een, ctx, null))
          ctx.update(ten)

          output
        }
      }
      case x @ EENCondition(predict, branchThen, branchElse, branch, sibling) => {
        val bvalue = ctx.get(branch)
        if (bvalue != null) {
          eos(sibling, ctx, EENAlias(bvalue))
        } else {
          val pvalue = ctx.get(predict.essential)
          val eenPredict = if (pvalue != null) {
            EENAlias(pvalue)
          } else {
            eos(predict, ctx, null)
          }
          ctx.update(predict.essential)

          EENCondition(eenPredict,
            eos(branchThen, ctx.clone, null),
            eos(branchElse, ctx.clone, null),
            branch, eos(sibling, ctx.update(branch), EENAlias(ctx.get(branch))))
        }
      }
      case x @ EENAlias(ten, sibling) => {
        var first = ctx.get(ten)
        if (first == null) {
          first = ten
          ctx.update(ten)
        }

        EENAlias(first, eos(sibling, ctx, EENAlias(first)))
      }
      case x @ EENInputRow(expr, sibling) => {
        val value = ctx.get(expr)
        if (value != null) {
          eos(sibling, ctx, EENAlias(value))
        } else {
          EENInputRow(expr, eos(sibling, ctx.update(expr), EENAlias(expr)))
        }
      }
      case x @ EENOutputField(field, _) =>
        val value = ctx.get(field)
        if (value != null) {
          EENAlias(value)
        } else {
          ctx.update(field)
          EENOutputField(field, previous)
        }
      case x @ EENOutputWritableField(field, _) =>
        val value = ctx.get(field)
        if (value != null) {
          EENAlias(value)
        } else {
          ctx.update(field)
          EENOutputWritableField(field, previous)
        }
      case x @ EENOutputExpr(ten, _) => EENOutputExpr(ten, previous)
      case x @ EENOutputRow(row, seq) => {
        EENOutputRow(row, eos(seq, ctx, null))
      }
      case x @ EENLiteral(expr, sibling) => {
        val value = ctx.get(expr)
        if (value != null) {
          eos(sibling, ctx, EENAlias(value))
        } else {
          EENLiteral(expr, eos(sibling, ctx.update(expr), EENAlias(expr)))
        }
      }
      case x @ EENAttribute(expr, sibling) =>
        val value = ctx.get(expr)
        if (value != null) {
          eos(sibling, ctx, EENAlias(value))
        } else {
          EENAttribute(expr, eos(sibling, ctx.update(expr), EENAlias(expr)))
        }
      case x @ EENBuiltin(expr, sibling) =>
        val value = ctx.get(expr)
        if (value != null) {
          // not need to re-compute the builtin
          eos(sibling, ctx, EENAlias(value))
        } else {
          EENBuiltin(expr, eos(sibling, ctx.update(expr), EENAlias(expr)))
        }
      case x @ EENConvertR2R(expr, sibling) =>
        val value = ctx.get(expr)
        if (value != null) {
          eos(sibling, ctx, EENAlias(value))
        } else {
          EENConvertR2R(expr, eos(sibling, ctx.update(expr), EENAlias(expr)))
        }
      case x @ EENConvertR2W(expr, sibling) =>
        val value = ctx.get(expr)
        if (value != null) {
          eos(sibling, ctx, EENAlias(value))
        } else {
          EENConvertR2W(expr, eos(sibling, ctx.update(expr), EENAlias(expr)))
        }
      case x @ EENConvertW2R(expr, sibling) =>
        val value = ctx.get(expr)
        if (value != null) {
          eos(sibling, ctx, EENAlias(value))
        } else {
          EENConvertW2R(expr, eos(sibling, ctx.update(expr), EENAlias(expr)))
        }
      case x @ EENConvertW2D(expr, sibling) =>
        val value = ctx.get(expr)
        if (value != null) {
          eos(sibling, ctx, EENAlias(value))
        } else {
          EENConvertW2D(expr, eos(sibling, ctx.update(expr), EENAlias(expr)))
        }
      case x @ EENGUDF(expr, sibling) =>
        val value = ctx.get(expr)
        if (value != null) {
          eos(sibling, ctx, EENAlias(value))
        } else {
          EENGUDF(expr, eos(sibling, ctx.update(expr), EENAlias(expr)))
        }
      case x @ EENUDF(expr, sibling) =>
        val value = ctx.get(expr)
        if (value != null) {
          eos(sibling, ctx, EENAlias(value))
        } else {
          EENUDF(expr, eos(sibling, ctx.update(expr), EENAlias(expr)))
        }
    }
  }
}
