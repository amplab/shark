package shark.execution.cg.row

abstract class EENExpr(val ten: TypedExprNode, nested: ExecuteOrderedExprNode = null) extends ExecuteOrderedExprNode(nested) {
  self: Product =>

  override def currCode(ctx: CGExprContext): String = {
  	val codeCompute = exprCode(ctx)
  	
  	val code = new StringBuffer()
  	if(codeCompute != null) {
  		code.append("%s = %s;".format(ctx.exprName(ten), codeCompute))
  	}
  	
    val validate = ctx.codeValidate(ten)
    if(validate != null) {
    	code.append(validate)
    }
  		
  	code.toString()
  }
  
  override def initialEssential(ctx: CGExprContext) {
	  register(ctx, essential)
	  
	  initial(ctx)
  }
  
  protected def register(ctx: CGExprContext, ten: TypedExprNode) {
		if (ten != null) {
	  	  ctx.register(ten)
	      ctx.register(ten,
	      	ten.outputDT.primitive,
		    ctx.indicatorName(ten),
		    ctx.exprName(ten),
			"%s = false;".format(ctx.indicatorName(ten)),
			"%s = true;".format(ctx.indicatorName(ten)))
	      ctx.register(ten, ctx.EXPR_NULL_INDICATOR_DEFAULT_VALUE, "false")
		}
	}
  
  protected def initial(ctx: CGExprContext) {}
  
  protected def exprCode(ctx: CGExprContext): String = ""
  
  override def essential: TypedExprNode = ten
}

case class EENAlias(expr: TypedExprNode, sibling: ExecuteOrderedExprNode = null) extends EENExpr(expr, sibling) {
  override def initialEssential(ctx: CGExprContext) {
  	// do nothing rather than its default initialization.
  }
  
  override def currCode(ctx: CGExprContext): String = ctx.exprName(expr)
}

case class EENInputRow(expr: TENInputRow, sibling: ExecuteOrderedExprNode) extends EENExpr(expr, sibling) {
	override def initial(ctx: CGExprContext) {
	    ctx.register(expr, ctx.EXPR_VARIABLE_TYPE, expr.outputDT.primitive)
		ctx.register(expr, ctx.EXPR_NULL_INDICATOR_DEFAULT_VALUE, 
		    "%s.mask.get(%s.%s)".format(Constant.CG_EXPR_NAME_INPUT, 
		        expr.struct.clazz, expr.maskBitName))
//		ctx.register(expr, ctx.EXPR_VARIABLE_NAME, exprName)
//		ctx.register(expr, ctx.CODE_IS_VALID, )
//		ctx.register(expr, ctx.CODE_VALUE_REPL, exprName)
		ctx.register(expr, ctx.CODE_INVALIDATE, null)
		ctx.register(expr, ctx.CODE_VALIDATE, null)
	}

	override def exprCode(ctx: CGExprContext): String = "%s.%s".format(Constant.CG_EXPR_NAME_INPUT, expr.escapedName)
}

case class EENCondition(predict: ExecuteOrderedExprNode, t: ExecuteOrderedExprNode, f: ExecuteOrderedExprNode, branch: TENBranch, sibling: ExecuteOrderedExprNode) extends EENExpr(branch, sibling) {
  override def currCode(ctx: CGExprContext): String = {
  	val pc = predict.code(ctx)
  	val tc = t.code(ctx)
  	val fc = f.code(ctx)
  	"if(%s) \n{%s\n} else {%s\n}".format(pc, tc, fc)
  }
  
  override def children = (predict :: t :: f :: sibling :: Nil).filter(_ != null)
}

case class EENOutputField(output: TENOutputField, outter: ExecuteOrderedExprNode = null) extends EENExpr(output)  {
	self: Product =>

  override def initial(ctx: CGExprContext) {
	  ctx.register(output.expr)
	  ctx.register(output, ctx.CODE_VALIDATE, "%s.mask.set(%s.%s, true);".format(Constant.CG_EXPR_NAME_OUTPUT, ctx.row.output.clazz, output.maskBitName))
	  ctx.register(output, ctx.EXPR_VARIABLE_NAME, "%s.%s".format(Constant.CG_EXPR_NAME_OUTPUT, output.escapedName))
	  ctx.register(output, ctx.CODE_VALUE_REPL, "%s.%s".format(Constant.CG_EXPR_NAME_OUTPUT, output.escapedName))
  }
  
  override def exprCode(ctx: CGExprContext): String = ctx.exprName(outter.essential)
}

case class EENOutputExpr(output: TENOutputExpr, een: ExecuteOrderedExprNode) extends EENExpr(output) {
  override def initial(ctx: CGExprContext) {
    ctx.register(output, ctx.EXPR_VARIABLE_NAME, Constant.CG_EXPR_NAME_OUTPUT)
    ctx.register(output, ctx.EXPR_NULL_INDICATOR_NAME, null)
    ctx.register(output, ctx.CODE_IS_VALID, null)
    ctx.register(output, ctx.CODE_INVALIDATE, null)
    ctx.register(output, ctx.EXPR_VARIABLE_TYPE, null)
    ctx.register(output, ctx.CODE_VALIDATE, null)
  }
  
  override def exprCode(ctx: CGExprContext): String = ctx.exprName(een.essential)
}

case class EENOutputRow(expr: TENOutputRow, een: ExecuteOrderedExprNode) extends ExecuteOrderedExprNode(een) {
  override def initialEssential(ctx: CGExprContext) {
	ctx.register(expr, ctx.CODE_INVALIDATE, null)
	ctx.row = expr
  }

  override def code(ctx: CGExprContext): String = een.code(ctx)
  
  override def essential: TypedExprNode = expr
}

/**
 * exprs is Seq of (isUDF, Node)
 */
//case class EENSource(expr: EENOutput) extends ExecuteOrderedExprNode {
//  override def code(ctx: CGExprContext): String = {
//	CGTE.layout("shark/execution/cg/operator/cg_op_select.ssp", 
//	scala.collection.immutable.Map("ctx"-> ctx, "cs" -> this))
//  }
//}

