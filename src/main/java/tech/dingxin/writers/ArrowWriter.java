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

import org.apache.arrow.vector.VectorSchemaRoot;
import tech.dingxin.utils.Preconditions;

import java.util.Arrays;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Writer which serializes the Flink rows to Arrow format.
 *
 * @param <IN> Type of the row to write.
 */
public class ArrowWriter<IN> {

    /**
     * Container that holds a set of vectors for the rows to be sent to the Python worker.
     */
    private final VectorSchemaRoot root;

    /**
     * An array of writers which are responsible for the serialization of each column of the rows.
     */
    private final Map<String, ArrowFieldWriter<IN>> fieldWriters;

    private boolean isClosed;


    public ArrowWriter(VectorSchemaRoot root, ArrowFieldWriter<IN>[] fieldWriters) {
        this.root = Preconditions.checkNotNull(root);
        Preconditions.checkNotNull(fieldWriters);
        this.fieldWriters = Arrays.stream(fieldWriters).collect(
                Collectors.toMap(o -> o.getValueVector().getName(), o -> o));
        this.isClosed = false;
    }

    /**
     * Writes the specified row which is serialized into Arrow format.
     */
    public void write(IN row, String fieldName) {
        ArrowFieldWriter<IN> fieldWriter = fieldWriters.get(fieldName);
        fieldWriter.write(row);
    }

    /**
     * Finishes the writing of the current row batch.
     */
    public void finish() {
        for (ArrowFieldWriter<IN> fieldWriter : fieldWriters.values()) {
            fieldWriter.finish();
        }
    }

    /**
     * Resets the state of the writer to write the next batch of rows.
     */
    public void reset() {
        root.setRowCount(0);
        for (ArrowFieldWriter fieldWriter : fieldWriters.values()) {
            fieldWriter.reset();
        }
    }


    /**
     * do not close the writer unless you don't need the batch data.
     */
    public void close() {
        if (!isClosed) {
            root.close();
            isClosed = true;
        }
    }

    public VectorSchemaRoot getRecordBatch() {
        return root;
    }
}
