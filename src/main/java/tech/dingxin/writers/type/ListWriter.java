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

import org.apache.arrow.vector.complex.ListVector;
import tech.dingxin.writers.ArrowFieldWriter;
import tech.dingxin.utils.Preconditions;

import java.util.List;

/**
 * @author dingxin (zhangdingxin.zdx@alibaba-inc.com)
 */
public class ListWriter extends ArrowFieldWriter<List> {
    private final ArrowFieldWriter<Object> elementWriter;

    public ListWriter(ListVector mapVector, ArrowFieldWriter valueWriter) {
        super(mapVector);
        this.elementWriter = Preconditions.checkNotNull(valueWriter);
    }

    @Override
    public void doWrite(List row) {
        if (row != null) {
            ((ListVector) getValueVector()).startNewValue(getCount());
            for (Object o : row) {
                elementWriter.write(o);
            }
            ((ListVector) getValueVector()).endValue(getCount(), row.size());
        }
    }

    @Override
    public void finish() {
        super.finish();
        elementWriter.finish();
    }

    @Override
    public void reset() {
        super.reset();
        elementWriter.reset();
    }
}
