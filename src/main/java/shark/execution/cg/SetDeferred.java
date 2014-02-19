package shark.execution.cg;

import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF.DeferredJavaObject;

public class SetDeferred extends DeferredJavaObject {
	public SetDeferred() {
		super(null);
	}

	private Object value = null;
	
	public final SetDeferred build(Object v) {
		value = v;
		return this;
	}
	
    public Object get() throws HiveException {
      return value;
    }
}

