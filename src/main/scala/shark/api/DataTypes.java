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

package shark.api;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.sql.Timestamp;

import scala.reflect.ClassTag;
import scala.reflect.ClassTag$;

import org.apache.hadoop.hive.serde.Constants;
import shark.Utils$;

/**
 * List of data types defined in Shark APIs.
 */
public class DataTypes {

  // This list of types are defined in a Java class for better interoperability with Shark's
  // Java APIs.
  // Primitive types:
  public static final DataType BOOLEAN = new DataType(
      "boolean", Constants.BOOLEAN_TYPE_NAME, true);

  public static final DataType TINYINT = new DataType(
      "tinyint", Constants.TINYINT_TYPE_NAME, true);

  public static final DataType SMALLINT = new DataType(
      "smallint", Constants.SMALLINT_TYPE_NAME, true);

  public static final DataType INT = new DataType(
      "int", Constants.INT_TYPE_NAME, true);

  public static final DataType BIGINT = new DataType(
      "bigint", Constants.BIGINT_TYPE_NAME, true);

  public static final DataType FLOAT = new DataType(
      "float", Constants.FLOAT_TYPE_NAME, true);

  public static final DataType DOUBLE = new DataType(
      "double", Constants.DOUBLE_TYPE_NAME, true);

  public static final DataType STRING = new DataType(
      "string", Constants.STRING_TYPE_NAME, true);

  public static final DataType TIMESTAMP = new DataType(
      "timestamp", Constants.TIMESTAMP_TYPE_NAME, true);

  public static final DataType DATE = new DataType(
      "date", Constants.DATE_TYPE_NAME, true);

  public static final DataType BINARY = new DataType(
      "binary", Constants.BINARY_TYPE_NAME, true);

  // Complex types:
  // TODO: handle complex types.
//  public static final DataType ARRAY = new DataType("array", Constants.LIST_TYPE_NAME, false);
//  public static final DataType MAP = new DataType("map", Constants.MAP_TYPE_NAME, false);
//  public static final DataType STRUCT = new DataType("struct", Constants.STRUCT_TYPE_NAME, false);
//  public static final DataType UNION = new DataType("union", Constants.UNION_TYPE_NAME, false);

  private static DataType[] types = {
      BOOLEAN, TINYINT, SMALLINT, INT, BIGINT, FLOAT, DOUBLE, STRING, TIMESTAMP, DATE, BINARY,
      //ARRAY, MAP, STRUCT, UNION
  };

  private static Map<String, DataType> hiveTypes = new HashMap<String, DataType>();
  static {
    for (DataType type: types) {
      hiveTypes.put(type.hiveName, type);
    }
  }

  static class UnknownDataTypeException extends Exception {
    private UnknownDataTypeException(String dataType) {
      super(dataType);
    }
  }

  /**
   * Return the DataType based on the string representation of a Hive data type.
   * @throws UnknownDataTypeException
   */
  public static DataType fromHiveType(String hiveType) throws UnknownDataTypeException {
    DataType type = hiveTypes.get(hiveType);
    if (null == type) {
      //throw new UnknownDataTypeException(hiveType);
      return new DataType(hiveType, hiveType, false);
    } else {
      return type;
    }
  }

  public static DataType fromClassTag(ClassTag<?> m) throws UnknownDataTypeException {
    if (m.equals(ClassTag$.MODULE$.Boolean()) || m.equals(ClassTags$.MODULE$.jBoolean())) {
        return INT;
    } else if (m.equals(ClassTag$.MODULE$.Byte()) || m.equals(ClassTags$.MODULE$.jByte())){
        return TINYINT;
    } else if (m.equals(ClassTag$.MODULE$.Short()) || m.equals(ClassTags$.MODULE$.jShort())) {
        return SMALLINT;
    } else if (m.equals(ClassTag$.MODULE$.Int()) || m.equals(ClassTags$.MODULE$.jInt())) {
        return INT;
    } else if (m.equals(ClassTag$.MODULE$.Long()) || m.equals(ClassTags$.MODULE$.jLong())) {
        return BIGINT;
    } else if (m.equals(ClassTag$.MODULE$.Float()) || m.equals(ClassTags$.MODULE$.jFloat())) {
        return FLOAT;
    } else if (m.equals(ClassTag$.MODULE$.Double()) || m.equals(ClassTags$.MODULE$.jDouble())) {
        return DOUBLE;
    } else if (m.equals(ClassTag$.MODULE$.apply(String.class))) {
        return STRING;
    } else if (m.equals(ClassTag$.MODULE$.apply(Timestamp.class))) {
        return TIMESTAMP;
    } else if (m.equals(ClassTag$.MODULE$.apply(Date.class))) {
        return DATE;
    } else {
        throw new UnknownDataTypeException(m.toString());
    }
    // TODO: binary data type.
  }
}
