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

package shark.parse

import java.util.{List => JavaList}

import scala.collection.mutable.{ArrayBuffer, Queue}
import scala.collection.JavaConversions._

import org.apache.hadoop.hive.ql.lib.Node
import org.apache.hadoop.hive.ql.parse.{ASTNode, HiveParser}
import org.apache.hadoop.util.StringUtils

import shark.LogHelper
import shark.parse.ASTNodeFactory._


object ASTRewriteUtil extends LogHelper {

  val DISTINCT_SUBQUERY_ALIAS = "subqueryAliasForCountDistinctRewrite_"

  def getDistinctSubqueryAlias(id: Int) = {
    DISTINCT_SUBQUERY_ALIAS + id
  }

  /** Prints the tree starting at the given `node` root */
  def printTree(node: Node, builder: StringBuilder = new StringBuilder, indent: Int = 0)
  : StringBuilder = {
    node match {
      case a: ASTNode => builder.append(("  " * indent) + a.getText + "\n")
      case other => sys.error("Non ASTNode encountered: " + other)
    }

    Option(node.getChildren).map(_.toList).getOrElse(Nil).foreach(printTree(_, builder, indent + 1))
    builder
  }

  /**
   * Returns the children of `node`. Nil is returned if there are no children, as opposed the
   * the NULL list that `node.getChildren` returns.
   */
  def getChildren(node: ASTNode): Seq[ASTNode] = {
    Option(node.getChildren).map(_.toSeq).getOrElse(Nil).asInstanceOf[Seq[ASTNode]]
  }

  /**
   * Returns a indicies to expressions in `selExprNodes` that contain nodes corresponding to
   * `nodeTokenType`.
   */
  private def findIndicesForNodeTokenType(
      selExprNodes: JavaList[ASTNode],
      nodeTokenType: Int): Seq[Int] = {
    val indices = new ArrayBuffer[Int]()
    for ((selExprNode, index) <- selExprNodes.zipWithIndex) {
      if (getNodeForTokenType(selExprNode, nodeTokenType).isDefined) {
        indices += index
      }
    }
    indices
  }

  /**
   * Returns the node corresponding to `tokenType` reachable from `node`.
   * Only the first child from a node is traversed, so this should only be used to find
   * TOK_FUNCTIONDI or TOK_FUNCTION nodes.
   *
   * Note:
   * TOK_FUNCTIONDI can be nested if there are multiple expressions selected. For example,
   *   SELECT COUNT(DISTINCT key) * 10 + 5 ...
   * will have two nodes, one each for "+" and "*", between TOK_SELEXPR and TOK_FUNCTIONDI nodes.
   */
  private def getNodeForTokenType(node: ASTNode, tokenType: Int): Option[ASTNode] = {
    if (node.getToken.getType == tokenType) {
      Some(node)
    } else {
      val children = getChildren(node)
      if (children.isEmpty) {
        None
      } else {
        getNodeForTokenType(children.head.asInstanceOf[ASTNode], tokenType)
      }
    }
  }

  /**
   * Returns true if `hiveTokenId` exists in the list of `nodes` provided. See HiveParser for the
   * map of ID to token type.
   */
  private def hasInChildren(nodes: JavaList[ASTNode], hiveTokenId: Int): Boolean = {
    nodes.exists(_.getToken.getType == hiveTokenId)
  }

  /** Returns all TOK_QUERY nodes found using breadth-first traversal, including `rootAstNode`. */
  private def findQueryNodes(rootAstNode: ASTNode): Seq[ASTNode] = {
    def isQueryNode(node: ASTNode): Boolean = node.getToken.getType == HiveParser.TOK_QUERY

    val foundQueryNodes = new ArrayBuffer[ASTNode]()
    val queue = new Queue[ASTNode]()
    queue.enqueue(rootAstNode)
    while (!queue.isEmpty) {
      val currentNode = queue.dequeue
      if (isQueryNode(currentNode)) {
        foundQueryNodes += currentNode
      }
      for (child <- getChildren(currentNode)) {
        queue.enqueue(child)
      }
    }
    foundQueryNodes.toSeq
  }

  /**
   * Main entry point for DISTINCT aggregate rewrites. After the function returns, for each
   * (sub)query, a DISTINCT aggregate expression without a partitoning key will be reordered
   * into an aggregation over a DISTINCT subquery.
   * This function finds all TOK_QUERY nodes and delegates to countDistinctQueryToGroupBy() for
   * rewriting any query with a DISTINCT aggregate.
   */
  def countDistinctToGroupBy(rootAstNode: ASTNode): ASTNode = {
    // Find all TOK_QUERY nodes and transform any count-distincts subtree into a one with a
    // distinct/hash partition.
    try {
      val queryNodes = findQueryNodes(rootAstNode)
      for ((queryNode, queryId) <- queryNodes.zipWithIndex) {
        countDistinctQueryToGroupBy(queryNode, queryId)
      }
    } catch {
      case e: Exception => {
        logError("Attempt to rewrite query failed.\n" + StringUtils.stringifyException(e))
      } 
    }
    rootAstNode
  }

  /**
   * Starting at the TOK_QUERY node, detects whether there is a DISTINCT aggregate without a
   * partitioning key. If so, calls reorderCountDistinctToGroupBy() to do an AST transformation into
   * an aggregation over a DISTINCT subquery.
   *
   * @param rootAstNode Root node for the AST to transform.
   * @param queryId Unique ID for the query starting at `rootAstNode`. For a command with multiple
   *                subqueries, countDistinctQueryToGroupBy() will be called multiple times, each
   *                with a different `queryId` argument.
   */
  private def countDistinctQueryToGroupBy(rootAstNode: ASTNode, queryId: Int): ASTNode = {
    if (rootAstNode.getToken.getType == HiveParser.TOK_QUERY) {
      val rootQueryChildren = getChildren(rootAstNode)
      if (rootQueryChildren.size == 2) {
        // TOK_QUERY always has two children, TOK_FROM and TOK_INSERT, in order.
        val (fromClause, insertStmt) = (rootQueryChildren.get(0), rootQueryChildren.get(1))
        val insertStmtChildren = getChildren(insertStmt)
        val containsLimit = hasInChildren(insertStmtChildren, HiveParser.TOK_LIMIT)
        if (containsLimit) {
          logWarning("Query contains a LIMIT. Skipping applicable COUNT DISTINCT rewrites." +
            "A LIMIT shouldn't be paired with an aggregation that only returns one line ...")
        }
        val continueRewrite = insertStmtChildren.size >= 2 &&
          !containsLimit &&
          !hasInChildren(insertStmtChildren, HiveParser.TOK_GROUPBY) &&
          !hasInChildren(insertStmtChildren, HiveParser.TOK_ROLLUP_GROUPBY) &&
          !hasInChildren(insertStmtChildren, HiveParser.TOK_CUBE_GROUPBY)

        if (continueRewrite) {
          // The subtree starting at TOK_INSERT has this structure (parenthesis indicate children):
          // TOK_INSERT (TOK_DESTINATION ... ) (TOK_SELECT (TOK_SELEXPR (TOK_FUNCTIONDI ... )))
          // Note that at this point, the insert statement can have more than 2 children if there
          // is a WHERE filter and/or an ORDER BY or SORT BY.
          val (destinationAndSelectStmt, stmtClauses) = insertStmtChildren.splitAt(2)
          val destination = destinationAndSelectStmt.get(0)
          val selectStmt = destinationAndSelectStmt.get(1)
          val selectExprs = getChildren(selectStmt)
          // With respect to the select node's children list, find the index to the TOK_SELEXPR root
          // for the subtree that contains the TOK_FUNCTIONDI parent of the distinct aggregate node.
          val distinctFunctionIndices = findIndicesForNodeTokenType(selectExprs,
            HiveParser.TOK_FUNCTIONDI)
          val functionIndices = findIndicesForNodeTokenType(selectExprs,
            HiveParser.TOK_FUNCTION)
          if (distinctFunctionIndices.size == 1 && functionIndices.isEmpty) {
            setChildren(insertStmt, destinationAndSelectStmt)
            // We've found a distinct aggregate - rewrite.
            val distinctFunctionIndex = distinctFunctionIndices.get(0)
            val selectExpr = selectExprs.get(distinctFunctionIndex)
            // TODO(harvey): Might be nice (though verbose) to print the before/after trees.
            logInfo("Rewriting a detected DISTINCT aggregate.")
            reorderCountDistinctToGroupBy(
              rootAstNode,
              fromClause,
              destination,
              selectExpr,
              stmtClauses,
              queryId)
          }
        }
      }
    }
    rootAstNode
  }

  /**
   * Rewrites a query with a distinct aggregate to one with an aggregation over a distinct subquey.
   * For example, this AST:
   *   TOK_QUERY
   *     TOK_FROM
   *       TOK_TABREF
   *         TOK_TABNAME
   *           src
   *     TOK_INSERT
   *       TOK_DESTINATION
   *         TOK_DIR
   *           TOK_TMP_FILE
   *       TOK_SELECT
   *         TOK_SELEXPR
   *           TOK_FUNCTIONDI
   *             count
   *             TOK_TABLE_OR_COL
   *               key
   * corresponding to the query:
   *   SELECT COUNT(DISTINCT key) FROM src
   *
   * is transformed into:
   *   TOK_QUERY
   *     TOK_FROM
   *       TOK_SUBQUERY
   *         TOK_QUERY
   *           TOK_FROM
   *             TOK_TABREF
   *               TOK_TABNAME
   *                 src
   *           TOK_INSERT
   *             TOK_DESTINATION
   *               TOK_DIR
   *                 TOK_TMP_FILE
   *             TOK_SELECTDI
   *               TOK_SELEXPR
   *                 TOK_TABLE_OR_COL
   *                   key
   *         subqueryAliasForCountDistinctRewrite_0
   *     TOK_INSERT
   *       TOK_DESTINATION
   *         TOK_DIR
   *           TOK_TMP_FILE
   *       TOK_SELECT
   *         TOK_SELEXPR
   *           TOK_FUNCTIONSTAR
   *             count
   * corresponding to the query:
   *   SELECT COUNT(*) FROM
   *     (SELECT DISTINCT key FROM src) subqueryAliasForCountDistinctRewrite_0
   */
  private def reorderCountDistinctToGroupBy(
      rootQuery: ASTNode,
      fromClause: ASTNode,
      destination: ASTNode,
      selectExpr: ASTNode,
      stmtClauses: Seq[ASTNode],
      queryId: Int): ASTNode = {
    // Construct the subtree starting at the TOK_INSERT child of `rootQuery`.

    // Separate the text node containing the distinct function name from the function arguments.
    val distinctFunction = getNodeForTokenType(selectExpr, HiveParser.TOK_FUNCTIONDI).get
    val distinctFunctionChildren = distinctFunction.asInstanceOf[ASTNode].getChildren
    val distinctFunctionName = distinctFunctionChildren.get(0).asInstanceOf[ASTNode]
    val distinctFunctionArgs = distinctFunctionChildren.subList(
      1, distinctFunctionChildren.size)

    // Transform the TOK_FUNCTIONDI node from the original AST into a TOK_FUNCTIONSTAR and attach
    // the text node containing the distinct function name as the only child.
    // This subtree starting at TOK_FUNCTIONDI is the only component that is modified.
    distinctFunction.token = new org.antlr.runtime.CommonToken(HiveParser.TOK_FUNCTIONSTAR)
    setText(distinctFunction, "TOK_FUNCTIONSTAR")
    val functionStar = distinctFunction
    setChildren(functionStar, Seq(distinctFunctionName))

    // Construct the subtree starting at the TOK_FROM root child of `rootQuery`, from the bottom-up.

    // Create a subtree, starting at a TOK_DESTINATION node, that represents a temporary directory.
    val tmpDestination = destinationNode(Seq(dirNode(Seq(tmpFileNode(Nil)))))
    // Create a subtree, starting at a TOK_SELECTDI node.
    // Assign a TOK_SELEXPR parent for each expression argument to the distinct aggregate function.
    // These will be the children of the TOK_SELECTDI node.
    val selectDistinctExprs =
      for (arg <- distinctFunctionArgs.asInstanceOf[JavaList[ASTNode]]) yield {
        selectExprNode(Seq(arg))
      }
    val selectDistinctStmt = selectDINode(selectDistinctExprs)

    // Add the TOK_DESTINATION and TOK_SELECTDI subtrees as the children of the TOK_INSERT node.
    val insertStmt = insertNode(Seq(tmpDestination, selectDistinctStmt) ++ stmtClauses)
    // Piece together the subtree starting at a TOK_SUBQUERY node.
    val subqueryAlias = textNode(getDistinctSubqueryAlias(queryId))
    val subquery = subqueryNode(Seq(queryNode(Seq(fromClause, insertStmt)), subqueryAlias))
    // Create and set TOK_FROM as the first child of the root TOK_QUERY.
    val outerFromClause = fromNode(Seq(subquery))
    rootQuery.setChild(0, outerFromClause)

    rootQuery
  }
}
