package shark.execution.cg.row

class PathNodeContext(table: scala.collection.mutable.Map[TypedExprNode, TypedExprNode] = 
  scala.collection.mutable.Map[TypedExprNode, TypedExprNode]()) {
  
  def update(ten: TypedExprNode): PathNodeContext = {
    if(ten != null) { 
      table.getOrElseUpdate(ten, ten)
    }

    this
  }

  def update(tens: Seq[TypedExprNode]): PathNodeContext = {
    tens.foreach(this update _)

    this
  }

  def get(ten: TypedExprNode): TypedExprNode = if(ten == null) null else table.getOrElse(ten, null)

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
	case x: TENInputRow => EENInputRow(x, sibling)
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
  		  EENGuardNull(ten, sibling)	
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
  		    if(nullCheck) {
  		  	  EENDeclare(x, create(x, EENGuardNull(x, sibling)))
  		    } else {
  		      create(x, EENGuardNull(x, sibling))
  		    }
  		  }
	      case x @ TENAttribute(attr, outter) => {
	      	if(nullCheck) {
	      	  EENDeclare(x, rotate(outter, create(x, EENGuardNull(x, sibling)), true))
	      	} else {
	      	  EENDeclare(x, rotate(outter, create(x, sibling), false))
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
	      	  rotate(expr, EENDeclare(x, create(x, EENGuardNull(x, sibling))), true)
	      	} else {
	      	  EENDeclare(x, rotate(expr, create(x, sibling), true))
	      	}
	      }
	      case x @ TENConvertR2W(expr) => {
	      	if(nullCheck) {
	      	  rotate(expr, EENDeclare(x, create(x, EENGuardNull(x, sibling))), true)
	      	} else {
	      	  EENDeclare(x, rotate(expr, create(x, sibling), true))
	      	}
	      }
	      case x @ TENConvertW2D(expr) => {
	      	  EENDeclare(x, EENSequence(rotate(expr, null, false), create(x, sibling)))
	      }
	      case x @ TENConvertW2R(expr) => {
	      	if(nullCheck) {
	      	  rotate(expr, EENDeclare(x, create(x, EENGuardNull(x, sibling))), true)
	      	} else {
	      	  EENDeclare(x, rotate(expr, create(x, sibling), true))
	      	}
	      }
	      case x @ TENGUDF(clazz, children) => {
	      	if(nullCheck) {
	          children.foldRight[ExecuteOrderedExprNode](EENDeclare(x, create(x, EENGuardNull(x, sibling))))((e1, e2) => {
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
	          children.foldRight[ExecuteOrderedExprNode](EENDeclare(x, create(x, EENGuardNull(x, sibling))))((e1, e2) => {
	            EENSequence(rotate(e1, null, false), e2)
	          })
	      	} else {
	      	  EENDeclare(x, EENSequence(children.foldRight[ExecuteOrderedExprNode](create(x))((e1, e2) => {
	            EENSequence(rotate(e1, null, false), e2)
	          }), sibling))
	      	}
	      }
	      case x @ TENOutputField(name, expr, dt) => {
	      	rotate(expr, EENOutputField(x), false)
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
	def cse(tree: ExecuteOrderedExprNode, ctx: PathNodeContext, previous: ExecuteOrderedExprNode): ExecuteOrderedExprNode = {
		tree match {
		case null => null
		case x @ EENDeclare(ten, een) => {
	    	val placeholder = TENDeclareHolder(ten)
	    	
	    	val output = if(ten.isInstanceOf[TENLiteral] || ctx.get(ten) != null || ctx.get(placeholder) != null) {
	    		// the node exists in the previous list
	    		cse(een, ctx, null)
	    	} else {
	    	  ctx.update(placeholder)
	    	  val nested = cse(een, ctx, null)
	    	  if(nested != null) EENDeclare(ten, nested) else null
	    	}
	    	ctx.update(ten)
	    	
	    	output
	    }
		case x @ EENGuardNull(ten, inner) => {
		  val value = ctx.get(ten)

		  val placeholder = TENGuardNullHolder(ten)
		  val output = if(ctx.get(placeholder) != null) {
		    // we don't need the guard any more
		    cse(inner, ctx, if(value == null) null else EENAlias(value))
		  } else {
		    val nested = cse(inner, ctx.clone.update(placeholder), if(value == null) null else EENAlias(value))
		    if(nested != null) EENGuardNull(ten, nested) else null
		  }
		  
		  output
	    }
		case x @ EENSequence(expr, next) => {
		  val ten = if(expr != null) expr.essential else null
		  val p = if(ten != null) {
		    cse(expr, ctx, previous)
		  } else {
		    null
		  }
		  
		  val n = if(next != null) {
		    val prev = ctx.get(ten)
		    
		    val t = cse(next, ctx.update(ten), if(prev == null) null else EENAlias(prev))
		    ctx.update(next.essential)
		    
		    t
		  } else {
		    null
		  }
		  if(p != null || n != null) EENSequence(p, n) else null
		} 
	    case x @ EENAssignment(ten, een) => {
	      if(ctx.get(ten) != null) {
	        cse(een, ctx, null)
	      } else {
	    	val output = EENAssignment(ten, cse(een, ctx, null))
	    	ctx.update(ten)
	    	
	    	output
	      }
	    }
	    case x @ EENCondition(predict, branchThen, branchElse, branch, sibling) => {
	      val bvalue = ctx.get(branch)
	      if(bvalue != null) {
	        cse(sibling, ctx, EENAlias(bvalue))
	      } else {
	        val pvalue = ctx.get(predict.essential)
            val eenPredict = if(pvalue != null) {
	    	  EENAlias(pvalue)
	        } else {
	    	  cse(predict, ctx, previous)
	        }
	        ctx.update(predict.essential)

          EENCondition(eenPredict, 
	    		cse(branchThen, ctx.clone, null), 
	    		cse(branchElse, ctx.clone, null), 
	    		branch, cse(sibling, ctx.update(branch), EENAlias(ctx.get(branch))))
	      }
	    }
	    case x @ EENAlias(ten, sibling) => EENAlias(ctx.get(ten), cse(sibling, ctx, null))
		case x @ EENInputRow(expr, sibling) => {
		  val value = ctx.get(expr)
		  if(value != null) {
		    cse(sibling, ctx, EENAlias(value)) 
		  } else {
		    val output = EENInputRow(expr, cse(sibling, ctx, null))
		    ctx.update(expr)
		    
		    output
		  }
		}
		case x @ EENOutputField(field, _) => 
		val value = ctx.get(field)
		if(value != null) {
		  EENAlias(value)
		} else {
		  ctx.update(field)
		  EENOutputField(field, previous)
		}
		case x @ EENOutputExpr(ten, _) => EENOutputExpr(ten, previous)
		case x @ EENOutputRow(row, seq) => {
			EENOutputRow(row, cse(seq, ctx, null))
		}
		case x @ EENLiteral(expr, sibling) => {
		  val value = ctx.get(expr)
		  if(value != null) {
		    cse(sibling, ctx, EENAlias(value))
		  } else {
		    EENLiteral(expr, cse(sibling, ctx.update(expr).update(TENGuardNullHolder(expr)), EENAlias(expr)))
		  }
		}
		case x @ EENAttribute(expr, sibling) => 
		val value = ctx.get(expr)
		if(value != null) {
			cse(sibling, ctx, EENAlias(value))
		} else {
		  EENAttribute(expr, cse(sibling, ctx.update(expr).update(TENGuardNullHolder(expr)), EENAlias(expr)))
		}
		case x @ EENBuiltin(expr, sibling) => 
		val value = ctx.get(expr)
		if(value != null) {
			// not need to re-compute the builtin
			cse(sibling, ctx, EENAlias(value))
		} else {
		  EENBuiltin(expr, cse(sibling, ctx.update(expr).update(TENGuardNullHolder(expr)), EENAlias(expr)))
		}
		case x @ EENConvertR2R(expr, sibling) => 
		val value = ctx.get(expr)
		if(value != null) {
			cse(sibling, ctx, EENAlias(value))
		} else {
			EENConvertR2R(expr, cse(sibling, ctx.update(expr), EENAlias(expr)))
		}
		case x @ EENConvertR2W(expr, sibling) => 
		val value = ctx.get(expr)
		if(value != null) {
			cse(sibling, ctx, EENAlias(value))
		} else {
			EENConvertR2W(expr, cse(sibling, ctx.update(expr), EENAlias(expr)))
		}
		case x @ EENConvertW2R(expr, sibling) => 
		val value = ctx.get(expr)
		if(value != null) {
			cse(sibling, ctx, EENAlias(value))
		} else {
			EENConvertW2R(expr, cse(sibling, ctx.update(expr), EENAlias(expr)))
		}
		case x @ EENConvertW2D(expr, sibling) => 
		val value = ctx.get(expr)
		if(value != null) {
			cse(sibling, ctx, EENAlias(value))
		} else {
			EENConvertW2D(expr, cse(sibling, ctx.update(expr), EENAlias(expr)))
		}
		case x @ EENGUDF(expr, sibling) => 
		val value = ctx.get(expr)
		if(value != null) {
			cse(sibling, ctx, EENAlias(value))
		} else {
			EENGUDF(expr, cse(sibling, ctx.update(expr), EENAlias(expr)))
		}
		case x @ EENUDF(expr, sibling) => 
		val value = ctx.get(expr)
		if(value != null) {
			cse(sibling, ctx, EENAlias(value))
		} else {
			EENUDF(expr, cse(sibling, ctx.update(expr), EENAlias(expr)))
		}
	}
	}
}