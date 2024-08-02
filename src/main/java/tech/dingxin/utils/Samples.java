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

package tech.dingxin.utils;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import tech.dingxin.PojoToArrowConverter;
import tech.dingxin.writers.ArrowWriter;
import tech.dingxin.writers.ArrowWriterFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * @author dingxin (zhangdingxin.zdx@alibaba-inc.com)
 */
public class Samples {
    private static BufferAllocator allocator = new RootAllocator();

    public static VectorSchemaRoot utf8Vector() {
        return createSampleVector("sample", String.class, "hello", "world");
    }

    public static VectorSchemaRoot doubleVector() {
        return createSampleVector("sample", Double.class, 1.23, 2.34);
    }

    public static VectorSchemaRoot intVector() {
        return createSampleVector("sample", Integer.class, 1, 2);
    }

    public static VectorSchemaRoot longVector() {
        return createSampleVector("sample", Long.class, 1L, 2L);
    }

    public static VectorSchemaRoot booleanVector() {
        return createSampleVector("sample", Boolean.class, true, false);
    }

    public static VectorSchemaRoot floatVector() {
        return createSampleVector("sample", Float.class, 1.23f, 2.34f);
    }

    public static VectorSchemaRoot shortVector() {
        return createSampleVector("sample", Short.class, (short) 1, (short) 2);
    }

    public static VectorSchemaRoot byteVector() {
        return createSampleVector("sample", Byte.class, (byte) 1, (byte) 2);
    }

    public static VectorSchemaRoot dateVector() {
        return createSampleVector("sample", java.sql.Date.class, java.sql.Date.valueOf("2023-01-01"), java.sql.Date.valueOf("2023-01-02"));
    }

    public static VectorSchemaRoot datetimeVector() {
        return createSampleVector("sample", java.util.Date.class, java.util.Date.from(java.time.Instant.parse("2023-01-02T12:00:00.00Z")), java.util.Date.from(java.time.Instant.parse("2023-01-02T13:00:00.00Z")));
    }


    public static VectorSchemaRoot timestampVector() {
        return createSampleVector("sample", java.sql.Timestamp.class, java.sql.Timestamp.valueOf("2023-01-01 12:00:00"), java.sql.Timestamp.valueOf("2023-01-01 13:00:00"));
    }

    public static VectorSchemaRoot decimalVector() {
        return createSampleVector("sample", java.math.BigDecimal.class, java.math.BigDecimal.valueOf(1.23), java.math.BigDecimal.valueOf(2.34));
    }

    public static VectorSchemaRoot jsonVector() {
        return createSampleVector("sample", String.class, "{\"a\":1}", "{\"b\":2}");
    }

    public static VectorSchemaRoot binaryVector() {
        return createSampleVector("sample", byte[].class, new byte[]{1, 2}, new byte[]{3, 4});
    }


    private static VectorSchemaRoot createSampleVector(String fieldName, Class clazz, Object... rows) {
        Field field = PojoToArrowConverter.toArrowField(fieldName, clazz, null, false);
        List<Field> fields = new ArrayList<>();
        fields.add(field);
        Schema schema = new Schema(fields);
        VectorSchemaRoot result = VectorSchemaRoot.create(schema, allocator);

        ArrowWriter<Object> arrowWriter = ArrowWriterFactory.createArrowWriter(result);
        for (Object row : rows) {
            arrowWriter.write(row, fieldName);
        }
        arrowWriter.finish();
        result.setRowCount(rows.length);
        return result;
    }
}
