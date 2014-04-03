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

package shark.execution.cg.row.helper;

import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;

/**
 * This is root class for the generated StructField 
 * @param <StructT>
 * @param <CacheT>
 */
public abstract class CGStructField<StructT, CacheT> implements StructField {
  public CacheT cache;
  public transient int idx = -1;
  protected transient String fieldName = null;
  protected transient ObjectInspector fieldObjectInspector = null;
  protected transient String fieldComment = null;
  
  public CGStructField(String fieldName, ObjectInspector fieldObjectInspector, String fieldComment, int idx) {
    this.fieldName = fieldName;
    this.fieldObjectInspector = fieldObjectInspector;
    this.fieldComment = fieldComment;
    this.idx = idx;
  }
  
  @Override
  public String getFieldName() {
    return fieldName;
  }

  @Override
  public ObjectInspector getFieldObjectInspector() {
    return fieldObjectInspector;
  }

  @Override
  public String getFieldComment() {
    return fieldComment;
  }
  
  public abstract CacheT get(StructT data);
}