package shark.execution.cg.row.helper;

import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;

public class CGStructField implements StructField {
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
}