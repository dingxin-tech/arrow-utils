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

package tech.dingxin.writers.type;

import org.apache.arrow.vector.complex.MapVector;
import org.apache.arrow.vector.complex.StructVector;
import tech.dingxin.writers.ArrowFieldWriter;
import tech.dingxin.utils.Preconditions;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * @author dingxin (zhangdingxin.zdx@alibaba-inc.com)
 */
public class MapWriter extends ArrowFieldWriter<Map> {
    private final ArrowFieldWriter<Object> keyWriter;
    private final ArrowFieldWriter<Object> valueWriter;
    protected StructVector structVector;

    public MapWriter(MapVector mapVector, ArrowFieldWriter keyWriter, ArrowFieldWriter valueWriter) {
        super(mapVector);
        this.structVector = (StructVector) mapVector.getDataVector();
        this.keyWriter = Preconditions.checkNotNull(keyWriter);
        this.valueWriter = Preconditions.checkNotNull(valueWriter);
    }

    @Override
    public void doWrite(Map row) {
        if (row != null) {
            ((MapVector) getValueVector()).startNewValue(getCount());
            List<Map.Entry> list = new ArrayList<>(row.entrySet());
            for (Map.Entry entry : list) {
                structVector.setIndexDefined(keyWriter.getCount());
                keyWriter.write(entry.getKey());
                valueWriter.write(entry.getValue());
            }
            ((MapVector) getValueVector()).endValue(getCount(), list.size());
        }
    }

    @Override
    public void finish() {
        super.finish();
        keyWriter.finish();
        valueWriter.finish();
    }

    @Override
    public void reset() {
        super.reset();
        keyWriter.reset();
        valueWriter.reset();
    }
}
