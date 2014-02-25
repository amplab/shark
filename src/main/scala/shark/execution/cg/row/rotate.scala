package shark.execution.cg.row

class PathNodeContext(table: scala.collection.mutable.Map[TypedExprNode, Int] = scala.collection.mutable.Map[TypedExprNode, Int]()) {
		def + (ten: TypedExprNode) = {
		  if(ten != null) { 
		    val c = count(ten)
			table.update(ten, c + 1)
		  }
		  
		  this
		}
		
	    def ++ (tens: Seq[TypedExprNode]) = {
	    	tens.foreach(this + _)
	    	
	    	this
		}
	    
		def count(ten: TypedExprNode): Int = if(ten == null) 0 else table.getOrElse(ten, 0)
		
		override def clone = new PathNodeContext(table.clone)
}

class RuleValueGuard {
	

//	class NodeCountTable {
//		private val table = scala.collection.mutable.Map[TypedExprNode, Int]()
//		def +(ten: TypedExprNode): NodeCountTable = {
//			val refs = table.getOrElseUpdate(ten, 0)
//			table += (ten -> (refs + 1))
//			
//			this
//		}
//		
//	    def + (tens: Seq[TypedExprNode]): NodeCountTable = {
//			tens.foreach(this + _)
//			
//	    	this
//		}
//		
//		def count(ten: TypedExprNode): Int = table.getOrElse(ten, 0)
//	}
//	
//  val nct = new NodeCountTable()
  
  def create(ten: TypedExprNode, sibling: ExecuteOrderedExprNode = null): EENExpr = ten match {
  	case x: TENAttribute => EENAttribute(x, sibling)
	case x: TENBuiltin => EENBuiltin(x, sibling)
	case x: TENGUDF => EENGUDF(x, sibling)
	case x: TENUDF => EENUDF(x, sibling)
	case x: TENConvertR2R => EENConvertR2R(x, sibling)
	case x: TENConvertR2W => EENConvertR2W(x, sibling)
	case x: TENConvertW2R => EENConvertW2R(x, sibling)
	case x: TENConvertW2D => EENConvertW2D(x, sibling)
	case x: TENLiteral => EENLiteral(x, sibling)
	case x: TENInputRow => EENInputRow(x)
	case x: TENOutputExpr => EENOutputExpr(x, sibling)
	case _ => EENAlias(ten, sibling)
  }
  
  private val statefulUDFs = new java.util.LinkedHashMap[TypedExprNode, ExecuteOrderedExprNode]()
  
  def rotate(ten: TypedExprNode, sibling: ExecuteOrderedExprNode, nullCheck: Boolean): ExecuteOrderedExprNode = {
  	if(ten.isStateful) {
  		// put the stateful udf into very beginning.
  		if(statefulUDFs.get(ten) == null) {
  			statefulUDFs.put(ten, transform(ten, null, false))
  		}
  		if(nullCheck) {
  		  EENGuardNull(EENAlias(ten), sibling)	
  		} else {
  		  EENAlias(ten, sibling)	
  		}
  	} else {
  		transform(ten, sibling, nullCheck)
  	}
  }
  
  // transform the tree (from the desc(pre-order travesal) to executing order (post-order travesal)
  private def transform(ten: TypedExprNode, sibling: ExecuteOrderedExprNode, nullCheck: Boolean): ExecuteOrderedExprNode = {
  	  ten match {
  		  case x : TENInputRow => {
  		  	sibling
  		  }
	      case x @ TENAttribute(attr, outter) => {
	      	if(nullCheck) {
     	        // rotate(outter, EENDeclare(x, EENGuardNull(create(outter), EENGuardNull(EENAttribute(x, null), sibling))), true)
	      		rotate(outter, EENDeclare(x, EENGuardNull(create(outter), EENGuardNull(create(x), sibling))), true)
	      	} else {
	      		EENDeclare(x, rotate(outter, EENSequence(EENGuardNull(create(outter), create(x)), sibling), true))
	      	}
	      }
	      case x @ TENBranch(branchIf, branchThen, branchElse) => {
	      	if(nullCheck) {
	      		rotate(
	      			branchIf, 
		        	  	EENDeclare(
		        	  	  x, 
		        	  	  EENCondition(
		        	  	  	EENAlias(branchIf), 
		        			rotate(branchThen, EENAssignment(x, create(branchThen)), true), 
		        			rotate(branchElse, EENAssignment(x, create(branchElse)), true), 
		        			x, sibling)), 
		        	true)
	      	} else {
		        EENDeclare(x, 
		          	rotate(
		          	  branchIf, 
		              EENCondition(
		              	  EENAlias(branchIf), 
		        		  rotate(branchThen, EENAssignment(x, create(branchThen)), true), 
		        		  rotate(branchElse, EENAssignment(x, create(branchElse)), true), 
		        		  x, sibling), 
		        	  true)
		        )
	      	}
	      }
	      case x @ TENBuiltin(op, children, dt, true) => {
            if(nullCheck) {
	           children.foldRight[ExecuteOrderedExprNode](EENDeclare(x, EENSequence(create(x), sibling)))((e1, e2) => {
	             rotate(e1, e2, true)
	           })
	      	} else {
	      	  EENDeclare(x, EENSequence(children.foldRight[ExecuteOrderedExprNode](create(x))((e1, e2) => {
	            rotate(e1, e2, true)
	          }), sibling))
	      	}
	      }
	      case x @ TENBuiltin(op, children, dt, false) => {
	         children.foldRight[ExecuteOrderedExprNode](EENDeclare(x, EENSequence(create(x), sibling)))((e1, e2) => {
	           rotate(e1, e2, false)
	         })
	      }
	      case x @ TENConvertR2R(expr, _) => {
	      	if(nullCheck) {
	      	  rotate(expr, EENDeclare(x, EENGuardNull(create(x), sibling)), true)
	      	} else {
	      	  EENDeclare(x, rotate(expr, EENSequence(create(x), sibling), true))
	      	}
	      }
	      case x @ TENConvertR2W(expr) => {
	      	if(nullCheck) {
	      	  rotate(expr, EENDeclare(x, EENGuardNull(create(x), sibling)), true)
	      	} else {
	      	  EENDeclare(x, rotate(expr, EENSequence(create(x), sibling), true))
	      	}
	      }
	      case x @ TENConvertW2D(expr) => {
	      	  EENDeclare(x, EENSequence(rotate(expr, null, false), create(x, sibling)))
	      }
	      case x @ TENConvertW2R(expr) => {
	      	if(nullCheck) {
	      	  rotate(expr, EENDeclare(x, EENGuardNull(create(x), sibling)), true)
	      	} else {
	      	  EENDeclare(x, rotate(expr, EENSequence(create(x), sibling), true))
	      	}
	      }
	      case x @ TENGUDF(clazz, children) => {
	      	if(nullCheck) {
	          children.foldRight[ExecuteOrderedExprNode](EENDeclare(x, EENGuardNull(create(x), sibling)))((e1, e2) => {
	            EENSequence(rotate(e1, null, false), e2)
	          })
	      	} else {
	      	  EENDeclare(x, EENSequence(children.foldRight[ExecuteOrderedExprNode](create(x))((e1, e2) => {
	            EENSequence(rotate(e1, null, false), e2)
	          }), sibling))
	      	}
	      }
	      case x @ TENUDF(bridge, children) => {
	      	if(nullCheck) {
	          children.foldRight[ExecuteOrderedExprNode](EENDeclare(x, EENGuardNull(create(x), sibling)))((e1, e2) => {
	            EENSequence(rotate(e1, null, false), e2)
	          })
	      	} else {
	      	  EENDeclare(x, EENSequence(children.foldRight[ExecuteOrderedExprNode](create(x))((e1, e2) => {
	            EENSequence(rotate(e1, null, false), e2)
	          }), sibling))
	      	}
	      }
	      case x @ TENOutputField(name, expr, dt) => {
	      	rotate(expr, EENOutputField(x), true)
	      }
	      case x @ TENOutputExpr(expr) => {
	        // rotate the fields first, and in the mean time, will collect the stateful UDF node
	      	val value = rotate(expr, create(x), true)
	      	mergeSeq(value :: Nil)
	      }
	      case x @ TENOutputRow(fields, dt) => {
	      	// rotate the fields first, and in the mean time, will collect the stateful UDF node
	      	val fieldSeq = fields.map(rotate(_, null, false))
	      	EENOutputRow(x, mergeSeq(fieldSeq))
	      }
	      case x : TENLiteral => if(nullCheck) {
	      	EENGuardNull(create(x), sibling)
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
	def cse(tree: ExecuteOrderedExprNode, ctx: PathNodeContext, previous: ExecuteOrderedExprNode): ExecuteOrderedExprNode = {
		tree match {
		case null => null
		case x @ EENDeclare(ten, een) => {
	    	val placeholder = TENDeclareHolder(ten)
	    	ctx + placeholder
	    	val output = if(ten.isInstanceOf[TENLiteral] || ctx.count(ten) > 0 || ctx.count(placeholder) > 1) {
	    		// the node exists in the previous list
	    		cse(een, ctx, null)
	    	} else {
	    		EENDeclare(ten, cse(een, ctx, null))
	    	}
	    	ctx + ten
	    	
	    	output
	    }
		case x @ EENGuardNull(een, inner) => {
		  val ten = een.essential
		  val placeholder = TENGuardNullHolder(ten)
			
		  if(ctx.count(placeholder) > 0) {
				// we don't need the guard any more
				cse(inner, ctx, een)
			} else {
				EENGuardNull(een, cse(inner, ctx.clone + placeholder, een))
			}
	    }
		case x @ EENSequence(expr, next) => {
		  val ten = if(expr != null) expr.essential else null
		  val output = EENSequence(cse(expr, ctx, previous), cse(next, ctx + ten, EENAlias(ten)))
		  if(next != null) ctx + next.essential
		  output
		} 
	    case x @ EENAssignment(ten, een) => {
	      if(ctx.count(ten) > 0) {
	        cse(een, ctx + ten, null)
	      } else {
	    	EENAssignment(ten, cse(een, ctx + ten, null))
	      }
	    }
	    case x @ EENCondition(predict, branchThen, branchElse, branch, sibling) => {
	      if(ctx.count(branch) > 0) {
	        cse(sibling, ctx, EENAlias(branch))
	      } else {
		    ctx + predict.ten
		    
            val eenPredict = if(ctx.count(predict.ten) > 1) {
	    	  EENAlias(predict.ten)
	        } else {
	    	  create(predict.ten)
	        }

		    EENCondition(eenPredict, 
	    		cse(branchThen, ctx.clone, null), 
	    		cse(branchElse, ctx.clone, null), 
	    		branch, cse(sibling, ctx + branch, EENAlias(branch)))
	      }
	    }
	    case x @ EENAlias(ten, sibling) => if(sibling == null) {
	      x
	    } else {
	      EENAlias(ten, cse(sibling, ctx + ten, null))
	    }
		case x @ EENInputRow(expr) => if(ctx.count(expr) > 0) EENAlias(expr) else x
		case x @ EENOutputField(field, _) => if(ctx.count(field) > 0) {
		  EENAlias(field)
		} else {
		  EENOutputField(field, previous)
		}
		case x @ EENOutputExpr(ten, _) => EENOutputExpr(ten, previous)
		case x @ EENOutputRow(row, seq) => {
			EENOutputRow(row, cse(seq, ctx, null))
		}
		case x @ EENLiteral(expr, sibling) => {
		  EENLiteral(expr, cse(sibling, ctx + TENGuardNullHolder(expr), EENAlias(expr)))
		}
		case x @ EENAttribute(expr, sibling) => 
		ctx + expr
		if(ctx.count(expr) > 1) {
			cse(sibling, ctx, EENAlias(expr))
		} else {
			EENAttribute(expr, cse(sibling, ctx + TENGuardNullHolder(expr), EENAlias(expr)))
		}
		case x @ EENBuiltin(expr, sibling) => 
		ctx + expr
		if(ctx.count(expr) > 1) {
			// not need to re-compute the builtin
			cse(sibling, ctx, EENAlias(expr))
		} else {
			EENBuiltin(expr, cse(sibling, ctx + TENGuardNullHolder(expr), EENAlias(expr)))
		}
		case x @ EENConvertR2R(expr, sibling) => 
		ctx + expr
		if(ctx.count(expr) > 1) {
			cse(sibling, ctx, EENAlias(expr))
		} else {
			EENConvertR2R(expr, cse(sibling, ctx, EENAlias(expr)))
		}
		case x @ EENConvertR2W(expr, sibling) => 
		ctx + expr
		if(ctx.count(expr) > 1) {
			cse(sibling, ctx, EENAlias(expr))
		} else {
			EENConvertR2W(expr, cse(sibling, ctx, EENAlias(expr)))
		}
		case x @ EENConvertW2R(expr, sibling) => 
		ctx + expr
		if(ctx.count(expr) > 1) {
			cse(sibling, ctx, EENAlias(expr))
		} else {
			EENConvertW2R(expr, cse(sibling, ctx, EENAlias(expr)))
		}
		case x @ EENConvertW2D(expr, sibling) => 
		ctx + expr
		if(ctx.count(expr) > 1) {
			cse(sibling, ctx, EENAlias(expr))
		} else {
			EENConvertW2D(expr, cse(sibling, ctx, EENAlias(expr)))
		}
		case x @ EENGUDF(expr, sibling) => 
		ctx + expr
		if(ctx.count(expr) > 1) {
			cse(sibling, ctx, EENAlias(expr))
		} else {
			EENGUDF(expr, cse(sibling, ctx, EENAlias(expr)))
		}
		case x @ EENUDF(expr, sibling) => 
		ctx + expr
		if(ctx.count(expr) > 1) {
			cse(sibling, ctx, EENAlias(expr))
		} else {
			EENUDF(expr, cse(sibling, ctx, EENAlias(expr)))
		}
	}
	}
}