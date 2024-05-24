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

package tech.dingxin;

import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Schema;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDate;
import java.util.List;
import java.util.Map;

/**
 * @author dingxin (zhangdingxin.zdx@alibaba-inc.com)
 */
public class ArrowRowData {

    private final Object[] data;

    private final Schema schema;

    public ArrowRowData(Schema schema) {
        this.data = new Object[schema.getFields().size()];
        this.schema = schema;
    }

    public void set(int index, Object value) {
        validateType(index, value);
        this.data[index] = value;
    }

    public Object get(int index) {
        return this.data[index];
    }

    public Schema getSchema() {
        return schema;
    }

    public ArrowType getType(int index) {
        return schema.getFields().get(index).getType();
    }

    public String getName(int index) {
        return schema.getFields().get(index).getName();
    }

    public int getLength() {
        return data.length;
    }

    private void validateType(int index, Object value) {
        if (value == null) {
            return;
        }
        ArrowType.ArrowTypeID typeId = schema.getFields().get(index).getType().getTypeID();
        switch (typeId) {
            case Int:
                if (!(value instanceof Integer) && !(value instanceof Long) && !(value instanceof Short) && !(value instanceof Byte)) {
                    throw new IllegalArgumentException("Expected Integer/Long/Short/Byte at index " + index);
                }
                break;
            case FloatingPoint:
                if (!(value instanceof Float) && !(value instanceof Double)) {
                    throw new IllegalArgumentException("Expected Float/Double at index " + index);
                }
                break;
            case Bool:
                if (!(value instanceof Boolean)) {
                    throw new IllegalArgumentException("Expected Boolean at index " + index);
                }
                break;
            case Utf8:
                if (!(value instanceof String)) {
                    throw new IllegalArgumentException("Expected String at index " + index);
                }
                break;
            case Decimal:
                if (!(value instanceof BigDecimal)) {
                    throw new IllegalArgumentException("Expected BigDecimal at index " + index);
                }
                break;
            case Date:
                if (!(value instanceof LocalDate) && !(value instanceof Integer)) {
                    throw new IllegalArgumentException("Expected LocalDate/Integer at index " + index);
                }
                break;
            case Timestamp:
                if (!(value instanceof Instant) && !(value instanceof Long)) {
                    throw new IllegalArgumentException("Expected Instant/Long at index " + index);
                }
                break;
            case Binary:
                if (!(value instanceof byte[])) {
                    throw new IllegalArgumentException("Expected byte[] at index " + index);
                }
                break;
            case List:
                if (!(value instanceof List)) {
                    throw new IllegalArgumentException("Expected List at index " + index);
                }
                break;
            case Map:
                if (!(value instanceof Map)) {
                    throw new IllegalArgumentException("Expected Map at index " + index);
                }
                break;
            case Struct:
                break;
            default:
                throw new IllegalArgumentException("Unsupported type " + typeId);
        }
    }

}
