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

package tech.dingxin.writers;

import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.Float4Vector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.SmallIntVector;
import org.apache.arrow.vector.TinyIntVector;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.MapVector;
import org.apache.arrow.vector.complex.StructVector;
import tech.dingxin.writers.type.BooleanWriter;
import tech.dingxin.writers.type.ByteWriter;
import tech.dingxin.writers.type.DoubleWriter;
import tech.dingxin.writers.type.FloatWriter;
import tech.dingxin.writers.type.IntegerWriter;
import tech.dingxin.writers.type.ListWriter;
import tech.dingxin.writers.type.LongWriter;
import tech.dingxin.writers.type.MapWriter;
import tech.dingxin.writers.type.ShortWriter;
import tech.dingxin.writers.type.StringWriter;
import tech.dingxin.writers.type.StructWriter;

import java.util.List;

/**
 * @author dingxin (zhangdingxin.zdx@alibaba-inc.com)
 */
public class ArrowWriterFactory {
    private ArrowWriterFactory() {
    }

    public static ArrowFieldWriter createFieldWriter(ValueVector vector) {
        if (vector instanceof TinyIntVector) {
            return new ByteWriter(vector);
        } else if (vector instanceof SmallIntVector) {
            return new ShortWriter(vector);
        } else if (vector instanceof BigIntVector) {
            return new LongWriter(vector);
        } else if (vector instanceof BitVector) {
            return new BooleanWriter(vector);
        } else if (vector instanceof Float4Vector) {
            return new FloatWriter(vector);
        } else if (vector instanceof Float8Vector) {
            return new DoubleWriter(vector);
        } else if (vector instanceof VarCharVector) {
            return new StringWriter(vector);
        } else if (vector instanceof IntVector) {
            return new IntegerWriter(vector);
        } else if (vector instanceof MapVector) {
            MapVector mapVector = (MapVector) vector;
            StructVector structVector = (StructVector) mapVector.getDataVector();
            return new MapWriter(mapVector, createFieldWriter(structVector.getChild(MapVector.KEY_NAME)),
                    createFieldWriter(structVector.getChild(MapVector.VALUE_NAME)));
        } else if (vector instanceof ListVector) {
            ListVector listVector = (ListVector) vector;
            FieldVector elementVector = listVector.getDataVector();
            return new ListWriter(listVector, createFieldWriter(elementVector));
        } else if (vector instanceof StructVector) {
            StructVector structVector = (StructVector) vector;
            List<FieldVector> childrenVectors = structVector.getChildrenFromFields();
            ArrowFieldWriter[] fieldsWriters = new ArrowFieldWriter[childrenVectors.size()];
            for (int i = 0; i < fieldsWriters.length; i++) {
                fieldsWriters[i] = createFieldWriter(((StructVector) vector).getVectorById(i));
            }
            return new StructWriter(structVector, fieldsWriters);
        } else {
            throw new UnsupportedOperationException(String.format("Unsupported type %s.", vector.getName()));
        }
    }

    public static ArrowWriter<Object> createArrowWriter(VectorSchemaRoot root) {
        ArrowFieldWriter<Object>[] fieldWriters = new ArrowFieldWriter[root.getFieldVectors().size()];
        List<FieldVector> vectors = root.getFieldVectors();
        for (int i = 0; i < vectors.size(); i++) {
            FieldVector vector = vectors.get(i);
            vector.allocateNew();
            fieldWriters[i] = createFieldWriter(vector);
        }
        return new ArrowWriter<>(root, fieldWriters);
    }
}
