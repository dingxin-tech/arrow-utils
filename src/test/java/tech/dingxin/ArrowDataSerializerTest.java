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

import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

/**
 * @author dingxin (zhangdingxin.zdx@alibaba-inc.com)
 */
class ArrowDataSerializerTest {

    private static Schema testSchema;

    @BeforeAll
    public static void init() {
        List<Field> fields = new ArrayList<>();
        fields.add(Field.nullable("name", ArrowType.Utf8.INSTANCE));
        fields.add(Field.nullable("age", new ArrowType.Int(32, true)));
        testSchema = new Schema(fields);
    }

    @Test
    void testE2E() {
        ArrowDataSerializer arrowDataSerializer = new ArrowDataSerializer(testSchema, new RootAllocator());

        ArrowRowData rowData = new ArrowRowData(testSchema);
        rowData.set(0, "Jack");
        rowData.set(1, 18);

        for (int i = 0; i < 10; i++) {
            arrowDataSerializer.add(rowData);
        }
        Assertions.assertEquals(10, arrowDataSerializer.getVectorSchemaRoot().getRowCount());

        rowData.set(0, "Tom");
        rowData.set(1, 20);
        for (int i = 0; i < 5; i++) {
            arrowDataSerializer.add(rowData);
        }
        Assertions.assertEquals(15, arrowDataSerializer.getVectorSchemaRoot().getRowCount());


        arrowDataSerializer.reset();
        Assertions.assertEquals(0, arrowDataSerializer.getVectorSchemaRoot().getRowCount());
    }
}
