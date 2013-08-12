package shark.api;

import java.io.Serializable;


public class DataType implements Serializable {

  public final String name;
  public final String hiveName;
  public final boolean isPrimitive;

  DataType(String name, String hiveName, boolean isPrimitive) {
    this.name = name;
    this.hiveName = hiveName;
    this.isPrimitive = isPrimitive;
  }

  @Override
  public String toString() {
    return name;
  }
}
