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

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Schema;
import tech.dingxin.utils.Preconditions;
import tech.dingxin.writers.ArrowWriter;
import tech.dingxin.writers.ArrowWriterFactory;

import java.util.List;

/**
 * @author dingxin (zhangdingxin.zdx@alibaba-inc.com)
 */
public class ArrowDataSerializer implements AutoCloseable {
    private final VectorSchemaRoot vectorSchemaRoot;
    private final ArrowWriter<Object> arrowWriter;
    private int rowCount;

    public ArrowDataSerializer(Schema schema, BufferAllocator allocator) {
        Preconditions.checkNotNull(schema);
        Preconditions.checkNotNull(allocator);
        rowCount = 0;
        this.vectorSchemaRoot = VectorSchemaRoot.create(schema, allocator);
        this.arrowWriter = ArrowWriterFactory.createArrowWriter(vectorSchemaRoot);
    }

    public void addAll(List<ArrowRowData> rowDataList) {
        for (ArrowRowData rowData : rowDataList) {
            add(rowData);
        }
    }

    public void add(ArrowRowData rowData) {
        rowCount++;
        for (int i = 0; i < rowData.getLength(); i++) {
            arrowWriter.write(rowData.get(i), rowData.getName(i));
        }
    }

    public VectorSchemaRoot getVectorSchemaRoot() {
        arrowWriter.finish();
        vectorSchemaRoot.setRowCount(rowCount);
        return vectorSchemaRoot;
    }

    public void reset() {
        rowCount = 0;
        arrowWriter.reset();
    }

    @Override
    public void close() throws Exception {
        vectorSchemaRoot.close();
    }
}
