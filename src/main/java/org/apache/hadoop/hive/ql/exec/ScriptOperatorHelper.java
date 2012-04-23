package org.apache.hadoop.hive.ql.exec;

import java.util.Map;

import org.apache.hadoop.conf.Configuration;


/**
 * A helper class that gets us PathFinder. This is needed since PathFinder
 * inner class is not declared as static/public (which it really should be
 * since it doesn't reference the outer class at all).
 */
public class ScriptOperatorHelper extends ScriptOperator {

  private static final long serialVersionUID = 1L;
  
  private ScriptOperator op;
  
  public ScriptOperatorHelper(ScriptOperator op) {
    this.op = op;
  }

  public String getAlias() {
    return op.alias;
  }
  
  public static String safeEnvVarName(String var) {
    return ScriptOperator.safeEnvVarName(var);
  }
  
  public static void addJobConfToEnvironment(Configuration conf, Map<String, String> env) {
    ScriptOperator.addJobConfToEnvironment(conf, env);
  }

  public PathFinder newPathFinderInstance(String envpath) {
    return op.new PathFinder(envpath);
  }
}

