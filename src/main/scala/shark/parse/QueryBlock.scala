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

import org.apache.hadoop.hive.ql.parse.{QB => HiveQueryBlock}
import org.apache.hadoop.hive.ql.plan.CreateTableDesc
import org.apache.hadoop.hive.ql.plan.TableDesc

import org.apache.spark.storage.StorageLevel

import shark.memstore2.CacheType
import shark.memstore2.CacheType._


/**
 * A container for flags and table metadata. Used in SharkSemanticAnalyzer while parsing
 * and analyzing ASTs (e.g. in SharkSemanticAnalyzer#analyzeCreateTable()).
 */
class QueryBlock(outerID: String, alias: String, isSubQuery: Boolean)
    extends HiveQueryBlock(outerID, alias, isSubQuery) {

  // The CacheType for the table that will be created from CREATE TABLE/CTAS.
  private var _cacheModeForCreateTable = CacheType.NONE

  private var _preferredStorageLevel: StorageLevel = StorageLevel.NONE

  // Whether the created to be created or the table specified by CACHED should be backed by disk.
  private var _unifyView = false

  // Descriptor for the table being updated by an INSERT.
  private var _targetTableDesc: TableDesc = _

  def cacheModeForCreateTable_= (mode: CacheType) = _cacheModeForCreateTable = mode

  def cacheModeForCreateTable: CacheType = _cacheModeForCreateTable

  def preferredStorageLevel_= (storageLevel: StorageLevel) = _preferredStorageLevel = storageLevel

  def preferredStorageLevel: StorageLevel = _preferredStorageLevel

  def unifyView_= (shouldUnify: Boolean) = _unifyView = shouldUnify

  def unifyView: Boolean = _unifyView

  // Hive's QB uses `tableDesc` to refer to the CreateTableDesc. A direct `createTableDesc`
  // makes it easier to differentiate from `_targetTableDesc`.
  def createTableDesc: CreateTableDesc = super.getTableDesc

  def createTableDesc_= (desc: CreateTableDesc) = super.setTableDesc(desc)

  def targetTableDesc: TableDesc = _targetTableDesc

  def targetTableDesc_= (desc: TableDesc) = _targetTableDesc = desc
}
