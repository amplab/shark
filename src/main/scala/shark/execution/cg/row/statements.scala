package shark.execution.cg.row

trait InitTEN {
	def imports(ctx: CGExprContext, dt: DataType) {
		dt match {
			case TypeUtil.TimestampType => ctx.defineImport(dt.clazz)
			case _ => // do nothing
		}
	}
}

abstract class ExecuteOrderedExprNode(val nested: ExecuteOrderedExprNode = null) extends ExprNode[ExecuteOrderedExprNode] with InitTEN {
  self: Product =>
  def children = (nested :: Nil).filter(_ != null)	
  
  def code(ctx: CGExprContext): String = currCode(ctx) + (if(nested != null) nested.code(ctx) else "")

  protected def currCode(ctx: CGExprContext): String = ""
  
  def essential: TypedExprNode = if(nested == null) null else nested.essential
  
  def initialEssential(ctx: CGExprContext) {}
  
  def initialAll(ctx: CGExprContext) {
	initialEssential(ctx)

	children.foreach(_.initialAll(ctx))
  }
}

case class EENDeclare(ten: TypedExprNode, expr: ExecuteOrderedExprNode = null) extends ExecuteOrderedExprNode(expr) {
  private def define(clazz: String, variable: String): String = {
    val template = clazz match {
      case "boolean" | "byte" | "short" | "int" | "float" | "long" | "double" => {
        "%s %s;".format(clazz, variable)
      }
      case _ => "%s %s = null;"
      }
    template.format(clazz, variable)
  }
  
  override def currCode(ctx: CGExprContext): String = {
  	val variableType = ctx.exprType(ten)
  	val variableName = ctx.exprName(ten)
  	
  	val nullIndicatorName = ctx.indicatorName(ten)
  	val code = new StringBuffer()
  	
  	if(variableType != null) {
  	  code.append(define(variableType, variableName))
  	}
  	if(nullIndicatorName != null) code.append("boolean %s = false;".format(nullIndicatorName))
  	
  	code.toString()
  }
  
  override def essential: TypedExprNode = ten
}

case class EENAssignment(ten: TypedExprNode, expr: ExecuteOrderedExprNode) extends ExecuteOrderedExprNode(expr) {
  override def code(ctx: CGExprContext): String = {
  	val e = expr.essential
  	
  	val code = "%s = %s; %s".format(ctx.exprName(ten), ctx.exprName(e), ctx.codeValidate(ten))
  	
  	code
  }
  
  override def essential: TypedExprNode = ten
  override def children = (expr :: Nil).filter(_ != null)
}

case class EENSequence(expr: ExecuteOrderedExprNode, next: ExecuteOrderedExprNode) extends ExecuteOrderedExprNode(expr) {
  override def code(ctx: CGExprContext): String = 
    (if(expr != null) 
      expr.code(ctx)
    else 
      "") + (
  	if(next != null) 
  	  next.code(ctx) 
  	else 
  	  "") 
  
  override def children = (expr :: next :: Nil).filter(_ != null)	
}

case class EENGuardNull(een: EENExpr, inner: ExecuteOrderedExprNode) extends ExecuteOrderedExprNode(inner) {
  override def code(ctx: CGExprContext): String = {
  	if(een.ten == constantNull) {
  		"" 
  	} else {
  		val code = new StringBuffer()
  	    if(!een.isInstanceOf[EENAlias] && !een.isInstanceOf[EENInputRow]) {
  	    	code.append(een.code(ctx))
  	    }
  		val cond = ctx.codeIsValid(een.ten)
  		if (cond != null) {
  			code.append("if(%s) {\n%s\n}".format(cond, inner.code(ctx))) 
  		} else {
  			code.append(inner.code(ctx))
  		}
  		
  		code.toString
  	}
  }
  
  override def children = (een :: inner :: Nil).filter(_ != null)
}
