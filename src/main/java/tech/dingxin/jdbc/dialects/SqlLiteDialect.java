/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package tech.dingxin.jdbc.dialects;

import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.ArrowType;
import tech.dingxin.jdbc.JdbcDialect;

import java.sql.ResultSetMetaData;

/**
 * @author dingxin (zhangdingxin.zdx@alibaba-inc.com)
 */
public class SqlLiteDialect implements JdbcDialect {
    public static final JdbcDialect INSTANCE = new SqlLiteDialect();

    @Override
    public ArrowType getArrowType(int sqlType, String typeName, int size, ResultSetMetaData md) {
        if ("BOOLEAN".equals(typeName)) {
            return new ArrowType.Bool();
        }
        if ("TINYINT".equals(typeName)) {
            return new ArrowType.Int(8, true);
        }
        if ("SMALLINT".equals(typeName) || "INT2".equals(typeName)) {
            return new ArrowType.Int(16, true);
        }
        if ("BIGINT".equals(typeName) || "INT8".equals(typeName) || "UNSIGNED BIG INT".equals(typeName)) {
            return new ArrowType.Int(64, true);
        }
        if ("INT".equals(typeName) || "INTEGER".equals(typeName) || "MEDIUMINT".equals(typeName)) {
            return new ArrowType.Int(32, true);
        }
        if ("DATE".equals(typeName)) {
            return Types.MinorType.DATEDAY.getType();
        }
        if ("DATETIME".equals(typeName)) {
            return Types.MinorType.TIMESTAMPMILLI.getType();
        }
        if ("TIMESTAMP".equals(typeName)) {
            return Types.MinorType.TIMESTAMPMILLI.getType();
        }
        if ("DECIMAL".equals(typeName)) {
            return Types.MinorType.DECIMAL.getType();
        }
        if ("DOUBLE".equals(typeName) || "DOUBLE PRECISION".equals(typeName)) {
            return new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE);
        }
        if ("FLOAT".equals(typeName) || "REAL".equals(typeName)) {
            return new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE);
        }
        if ("CHARACTER".equals(typeName) || "NCHAR".equals(typeName) || "NATIVE CHARACTER".equals(typeName) ||
                "CHAR".equals(typeName)) {
            return new ArrowType.Utf8();
        }
        if ("VARCHAR".equals(typeName) || "VARYING CHARACTER".equals(typeName) ||
                "NVARCHAR".equals(typeName) || "TEXT".equals(typeName) || "STRING".equals(typeName)) {
            return new ArrowType.Utf8();
        }

        if ("BINARY".equals(typeName) || "BLOB".equals(typeName)) {
            return Types.MinorType.VARBINARY.getType();
        }
        throw new UnsupportedOperationException("Unsupported type: " + typeName);
    }
}
