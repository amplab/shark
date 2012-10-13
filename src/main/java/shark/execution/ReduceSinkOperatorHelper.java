package shark.execution;

import org.apache.hadoop.hive.ql.exec.ReduceSinkOperator;
import org.apache.hadoop.hive.ql.exec.ExprNodeEvaluator;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.ql.metadata.HiveException;

import java.util.List;


/**
 * This class is used because we cannot call protected static methods from
 * Scala.
 */
@SuppressWarnings("serial")
public class ReduceSinkOperatorHelper extends ReduceSinkOperator {

  public static StructObjectInspector initEvaluatorsAndReturnStruct(
      ExprNodeEvaluator[] evals, List<List<Integer>> distinctColIndices,
      List<String> outputColNames, int length, ObjectInspector rowInspector) 
    throws HiveException {

    return ReduceSinkOperator.initEvaluatorsAndReturnStruct(
        evals,
        distinctColIndices,
        outputColNames,
        length,
        rowInspector);
  }
}
