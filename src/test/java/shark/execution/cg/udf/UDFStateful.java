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

package shark.execution.cg.udf;

import org.apache.hadoop.hive.ql.udf.UDFType;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;


@Description(name = "udf_stateful", value = "_FUNC_() -")
@UDFType(deterministic = true, stateful = true)
public class UDFStateful extends org.apache.hadoop.hive.ql.exec.UDF {
  private LongWritable result = new LongWritable(0);

  public UDFStateful() {
  }

  public Text evaluate(Text a) {
    result.set(result.get() + 1);
    if (a != null) {
      return new Text(result.toString() + "," + a.toString());
    } else {
      return new Text(result.toString());
    }
  }
}
