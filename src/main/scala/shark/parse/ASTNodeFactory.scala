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

import scala.collection.JavaConversions._

import org.antlr.runtime.CommonToken
import org.apache.hadoop.hive.ql.parse.{ASTNode, HiveParser}


object ASTNodeFactory {

  def setText(node: ASTNode, newText: String): ASTNode = {
    node.token.asInstanceOf[org.antlr.runtime.CommonToken].setText(newText)
    node
  }

  def setChildren(node: ASTNode, newChildren: Seq[ASTNode]): ASTNode = {
    (1 to node.getChildCount).foreach(_ => node.deleteChild(0))
    node.addChildren(newChildren)
    node
  }

  def queryNode(children: Seq[ASTNode] = Nil): ASTNode = {
    val newNode = new ASTNode(new org.antlr.runtime.CommonToken(HiveParser.TOK_QUERY))
    setText(newNode, "TOK_QUERY")
    setChildren(newNode, children)
  }

  def fromNode(children: Seq[ASTNode] = Nil): ASTNode = {
    val newNode = new ASTNode(new org.antlr.runtime.CommonToken(HiveParser.TOK_FROM))
    setText(newNode, "TOK_FROM")
    setChildren(newNode, children)
  }

  def subqueryNode(children: Seq[ASTNode] = Nil): ASTNode = {
    val newNode = new ASTNode(new org.antlr.runtime.CommonToken(HiveParser.TOK_SUBQUERY))
    setText(newNode, "TOK_SUBQUERY")
    setChildren(newNode, children)
  }

  def insertNode(children: Seq[ASTNode] = Nil): ASTNode = {
    val newNode = new ASTNode(new org.antlr.runtime.CommonToken(HiveParser.TOK_INSERT))
    setText(newNode, "TOK_INSERT")
    setChildren(newNode, children)    
  }

  def selectNode(children: Seq[ASTNode] = Nil): ASTNode = {
    val newNode = new ASTNode(new org.antlr.runtime.CommonToken(HiveParser.TOK_SELECT))
    setText(newNode, "TOK_SELECT")
    setChildren(newNode, children)    
  }

  def selectDINode(children: Seq[ASTNode] = Nil): ASTNode = {
    val newNode = new ASTNode(new org.antlr.runtime.CommonToken(HiveParser.TOK_SELECTDI))
    setText(newNode, "TOK_SELECTDI")
    setChildren(newNode, children)    
  }

  def selectExprNode(children: Seq[ASTNode] = Nil): ASTNode = {
    val newNode = new ASTNode(new org.antlr.runtime.CommonToken(HiveParser.TOK_SELEXPR))
    setText(newNode, "TOK_SELEXPR")
    setChildren(newNode, children)    
  }

  def functionStarNode(children: Seq[ASTNode] = Nil): ASTNode = {
    val newNode = new ASTNode(new org.antlr.runtime.CommonToken(HiveParser.TOK_FUNCTIONSTAR))
    setText(newNode, "TOK_FUNCTIONSTAR")
    setChildren(newNode, children)    
  }

  def tmpFileNode(children: Seq[ASTNode] = Nil): ASTNode = {
    val newNode = new ASTNode(new org.antlr.runtime.CommonToken(HiveParser.TOK_TMP_FILE))
    setText(newNode, "TOK_TMP_FILE")
    setChildren(newNode, children)    
  }

  def dirNode(children: Seq[ASTNode] = Nil): ASTNode = {
    val newNode = new ASTNode(new org.antlr.runtime.CommonToken(HiveParser.TOK_DIR))
    setText(newNode, "TOK_DIR")
    setChildren(newNode, children)    
  }

  def destinationNode(children: Seq[ASTNode] = Nil): ASTNode = {
    val newNode = new ASTNode(new org.antlr.runtime.CommonToken(HiveParser.TOK_DESTINATION))
    setText(newNode, "TOK_DESTINATION")
    setChildren(newNode, children)    
  }

  def textNode(newText: String, children: Seq[ASTNode] = Nil): ASTNode = {
    val newNode = new ASTNode(new org.antlr.runtime.CommonToken(HiveParser.Identifier))
    setText(newNode, newText)
    setChildren(newNode, children)
  }
}
