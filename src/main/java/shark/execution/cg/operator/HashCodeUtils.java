package shark.execution.cg.operator;

import java.sql.Date;
import java.sql.Timestamp;

public class HashCodeUtils {
  public static int hashCodeNull() {
    return 0;
  }
  public static int hashCode(boolean a) {
    return a ? 1 : 0;
  }
  
  public static int hashCode(byte a) {
    return a;
  }
  public static int hashCode(short a) {
    return a;
  }
  public static int hashCode(int a) {
    return a;
  }
  public static int hashCode(long a) {
    return (int) ((a >>> 32) ^ a);
  }
  public static int hashCode(float a) {
    return Float.floatToIntBits(a);
  }
  public static int hashCode(double a) {
    long b = Double.doubleToLongBits(a);
    return (int) ((b >>> 32) ^ b);
  }
  public static int hashCode(String a) {
    return hashCode(a.getBytes());
  }
  public static int hashCode(byte[] a) {
    int r = 0;
    for (int i = 0; i < a.length; i++) {
      r = r * 31 + a[i];
    }
    
    return r;
  }
  
  public static int hashCode(Date a) {
    return hashCode(a.getTime());
  }
  public static int hashCode(Timestamp a) {
    return hashCode(a.getTime());
  }
  
  public static int hashCode(Object o) {
    // TODO List / Map / Struct / Union
    throw new RuntimeException(
        "Hash code on complex types not supported yet.");
  }
}
