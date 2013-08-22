/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package shark.execution;

import java.io.PrintStream;
import java.io.Serializable;
import java.lang.annotation.Annotation;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;

import org.apache.hadoop.hive.ql.exec.ExplainTask.MethodComparator;
import org.apache.hadoop.hive.ql.plan.Explain;

/**
 * A helper method for explaining Shark query plan. Based on
 * org.apache.hadoop.hive.ql.exec.ExplainTask. The main difference is we
 * recursively output parent operators rather than child operators.
 * 
 * @author rxin
 */
public class ExplainTaskHelper {
	
  @SuppressWarnings("unchecked")
  public static void outputPlan(Serializable work, PrintStream out, boolean extended,
      int indent) throws Exception {
    // Check if work has an explain annotation
    Annotation note = work.getClass().getAnnotation(Explain.class);

    if (note instanceof Explain) {
      Explain xpl_note = (Explain) note;
      if (extended || xpl_note.normalExplain()) {
        out.print(indentString(indent));
        out.println(xpl_note.displayName());
      }
    }

    // If this is an operator then we need to call the plan generation on the
    // conf and then
    // the children
    if (work instanceof shark.execution.Operator) {
      shark.execution.Operator<? extends Serializable> operator =
          (shark.execution.Operator<? extends Serializable>) work;
      out.println(indentString(indent) + "**" + operator.getClass().getName());
      if (operator.desc() != null) {
        outputPlan(operator.desc(), out, extended, indent);
      }
      if (operator.parentOperators() != null) {
        for (shark.execution.Operator op : operator.parentOperatorsAsJavaList()) {
          outputPlan(op, out, extended, indent + 2);
        }
      }
      return;
    }
    
    if (work instanceof org.apache.hadoop.hive.ql.exec.Operator) {
      org.apache.hadoop.hive.ql.exec.Operator<? extends Serializable> operator =
          (org.apache.hadoop.hive.ql.exec.Operator<? extends Serializable>) work;
      if (operator.getConf() != null) {
        outputPlan(operator.getConf(), out, extended, indent);
      }
      if (operator.getChildOperators() != null) {
        for (org.apache.hadoop.hive.ql.exec.Operator<? extends Serializable> op
            : operator.getChildOperators()) {
          outputPlan(op, out, extended, indent + 2);
        }
      }
      return;
    }
    
    outputWork(work, out, extended, indent);
  }
    
  public static void outputWork(Serializable work, PrintStream out, boolean extended,
        int indent) throws Exception {
    // Check if work has an explain annotation
    Annotation note = work.getClass().getAnnotation(Explain.class);

    // We look at all methods that generate values for explain
    Method[] methods = work.getClass().getMethods();
    Arrays.sort(methods, new MethodComparator());

    for (Method m : methods) {
      int prop_indents = indent + 2;
      note = m.getAnnotation(Explain.class);

      if (note instanceof Explain) {
        Explain xpl_note = (Explain) note;

        if (extended || xpl_note.normalExplain()) {

          Object val = m.invoke(work);

          if (val == null) {
            continue;
          }

          String header = null;
          if (!xpl_note.displayName().equals("")) {
            header = indentString(prop_indents) + xpl_note.displayName() + ":";
          } else {
            prop_indents = indent;
            header = indentString(prop_indents);
          }

          if (isPrintable(val)) {

            out.printf("%s ", header);
            out.println(val);
            continue;
          }
          // Try this as a map
          try {
            // Go through the map and print out the stuff
            Map<?, ?> mp = (Map<?, ?>) val;
            outputMap(mp, header, out, extended, prop_indents + 2);
            continue;
          } catch (ClassCastException ce) {
            // Ignore - all this means is that this is not a map
          }

          // Try this as a list
          try {
            List<?> l = (List<?>) val;
            outputList(l, header, out, extended, prop_indents + 2);

            continue;
          } catch (ClassCastException ce) {
            // Ignore
          }

          // Finally check if it is serializable
          try {
            Serializable s = (Serializable) val;
            out.println(header);
            outputPlan(s, out, extended, prop_indents + 2);

            continue;
          } catch (ClassCastException ce) {
            // Ignore
          }
        }
      }
    }
  }

  private static boolean isPrintable(Object val) {
    if (val instanceof Boolean || val instanceof String
        || val instanceof Integer || val instanceof Byte
        || val instanceof Float || val instanceof Double) {
      return true;
    }

    if (val != null && val.getClass().isPrimitive()) {
      return true;
    }

    return false;
  }

  private static void outputMap(Map<?, ?> mp, String header, PrintStream out,
      boolean extended, int indent) throws Exception {

    boolean first_el = true;
    TreeMap<Object, Object> tree = new TreeMap<Object, Object>();
    tree.putAll(mp);
    for (Entry<?, ?> ent : tree.entrySet()) {
      if (first_el) {
        out.println(header);
      }
      first_el = false;

      // Print the key
      out.print(indentString(indent));
      out.printf("%s ", ent.getKey().toString());
      // Print the value
      if (isPrintable(ent.getValue())) {
        out.print(ent.getValue());
        out.println();
      } else if (ent.getValue() instanceof List
          || ent.getValue() instanceof Map) {
        out.print(ent.getValue().toString());
        out.println();
      } else if (ent.getValue() instanceof Serializable) {
        out.println();
        outputPlan((Serializable) ent.getValue(), out, extended, indent + 2);
      } else {
        out.println();
      }
    }
  }

  private static void outputList(List<?> l, String header, PrintStream out,
      boolean extended, int indent) throws Exception {

    boolean first_el = true;
    boolean nl = false;
    for (Object o : l) {
      if (first_el) {
        out.print(header);
      }

      if (isPrintable(o)) {
        if (!first_el) {
          out.print(", ");
        } else {
          out.print(" ");
        }

        out.print(o);
        nl = true;
      } else if (o instanceof Serializable) {
        if (first_el) {
          out.println();
        }
        outputPlan((Serializable) o, out, extended, indent + 2);
      }

      first_el = false;
    }

    if (nl) {
      out.println();
    }
  }
  
  private static String indentString(int indent) {
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < indent; ++i) {
      sb.append(" ");
    }

    return sb.toString();
  }

}

