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

import org.apache.arrow.vector.DateMilliVector;
import org.apache.arrow.vector.TimeStampMicroVector;
import org.apache.arrow.vector.TimeStampMilliVector;
import org.apache.arrow.vector.TimeStampNanoVector;
import org.apache.arrow.vector.TimeStampSecVector;
import org.apache.arrow.vector.TimeStampVector;
import org.apache.arrow.vector.ValueVector;
import tech.dingxin.writers.ArrowFieldWriter;

import java.time.Instant;

/**
 * @author dingxin (zhangdingxin.zdx@alibaba-inc.com)
 */
public class TimestampWriter extends ArrowFieldWriter<Instant> {
    public TimestampWriter(ValueVector valueVector) {
        super(valueVector);
    }

    @Override
    public void doWrite(Instant row) {
        if (row == null) {
            ((DateMilliVector) getValueVector()).setNull(getCount());
        } else {
            TimeStampVector valueVector = (TimeStampVector) getValueVector();
            if (valueVector instanceof TimeStampSecVector) {
                ((TimeStampSecVector) valueVector)
                        .setSafe(getCount(), row.getEpochSecond());
            } else if (valueVector instanceof TimeStampMilliVector) {
                ((TimeStampMilliVector) valueVector)
                        .setSafe(getCount(), row.toEpochMilli());
            } else if (valueVector instanceof TimeStampMicroVector) {
                ((TimeStampMicroVector) valueVector)
                        .setSafe(
                                getCount(),
                                row.getEpochSecond() * 1000
                                        + row.getNano() / 1000);
            } else {
                ((TimeStampNanoVector) valueVector)
                        .setSafe(
                                getCount(),
                                row.getEpochSecond() * 1_000_000
                                        + row.getNano());
            }
        }
    }
}
