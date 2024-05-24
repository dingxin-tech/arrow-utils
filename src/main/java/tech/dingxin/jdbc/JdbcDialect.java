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

package tech.dingxin.jdbc;

import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;

import java.sql.ResultSetMetaData;

/**
 * @author dingxin (zhangdingxin.zdx@alibaba-inc.com)
 */
public interface JdbcDialect {
    /**
     * Get the custom datatype mapping for the given jdbc meta information.
     *
     * @param sqlType  Refers to [[java.sql.Types]] constants, or other constants defined by the
     *                 target database, e.g. `-101` is Oracle's TIMESTAMP WITH TIME ZONE type.
     *                 This value is returned by [[java.sql.ResultSetMetaData#getColumnType]].
     * @param typeName The column type name used by the database (e.g. "BIGINT UNSIGNED"). This is
     *                 sometimes used to determine the target data type when `sqlType` is not
     *                 sufficient if multiple database types are conflated into a single id.
     *                 This value is returned by [[java.sql.ResultSetMetaData#getColumnTypeName]].
     * @param size     The size of the type, e.g. the maximum precision for numeric types, length for
     *                 character string, etc.
     *                 This value is returned by [[java.sql.ResultSetMetaData#getPrecision]].
     * @param md       Result metadata associated with this type. This contains additional information
     *                 from [[java.sql.ResultSetMetaData]] or user specified options.
     * @return An option the actual DataType (subclasses of [[org.apache.arrow.vector.types.pojo.ArrowType]])
     * or None if the default type mapping should be used.
     */
    ArrowType getArrowType(
            int sqlType, String typeName, int size, ResultSetMetaData md);

    default Field getComplexArrowField(int sqlType, String typeName, int size, ResultSetMetaData md) {
        throw new UnsupportedOperationException("Should implemented getComplexArrowField() in JdbcDialect when encounter complex data type: " + typeName);
    }
}
