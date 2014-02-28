package shark.execution.cg.row

abstract class EENExpr(val ten: TypedExprNode, nested: ExecuteOrderedExprNode = null) extends ExecuteOrderedExprNode(nested) {
  self: Product =>

  override def currCode(ctx: CGExprContext): String = {
  	val codeCompute = exprCode(ctx)
  	
  	val code = new StringBuffer()
  	if(codeCompute != null && !codeCompute.isEmpty()) {
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
	private var oiName: String = _
    private var sfName: String = _
    
	override def initial(ctx: CGExprContext) {
	  import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector
	  import org.apache.hadoop.hive.serde2.objectinspector.StructField
	  
	  ctx.defineImport(classOf[StructObjectInspector])
	  val oiType = TypeUtil.dtToTypeOIString(expr.outputDT)
	  
	  oiName = ctx.property(oiType, false, false, null, false)
	  sfName = ctx.property(classOf[StructField].getCanonicalName(), false, false, null, false)
	  
	  ctx.addInitials("%s = %s.getStructFieldRef(\"%s\");".format(sfName, 
	    Constant.CG_EXPR_NAME_INPUT_SOI, expr.attr))
	  ctx.addInitials("%s = (%s)(%s.getFieldObjectInspector());".format(oiName, oiType, sfName))
	  
	  ctx.register(expr, ctx.EXPR_VARIABLE_TYPE, expr.outputDT.writable)
	  ctx.register(expr, ctx.EXPR_NULL_INDICATOR_NAME, null)
      ctx.register(expr, ctx.CODE_IS_VALID, "%s != null".format(ctx.exprName(expr)))
	  ctx.register(expr, ctx.CODE_VALUE_REPL, ctx.exprName(expr))
	  ctx.register(expr, ctx.CODE_INVALIDATE, null)
	  ctx.register(expr, ctx.CODE_VALIDATE, null)
	  // TODO need to thing about the union / map / array / structure type 
	  ctx.register(expr, ctx.EXPR_DEFAULT_VALUE, 
	  "(%s)%s.getPrimitiveWritableObject(%s.getStructFieldData(%s, %s))".format(
	      expr.outputDT.writable,
	      oiName,
	      Constant.CG_EXPR_NAME_INPUT_SOI, 
	      Constant.CG_EXPR_NAME_INPUT, 
	      sfName))
	}
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

