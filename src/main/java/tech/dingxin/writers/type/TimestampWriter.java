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
import org.apache.arrow.vector.TimeStampMicroTZVector;
import org.apache.arrow.vector.TimeStampMicroVector;
import org.apache.arrow.vector.TimeStampMilliTZVector;
import org.apache.arrow.vector.TimeStampMilliVector;
import org.apache.arrow.vector.TimeStampSecTZVector;
import org.apache.arrow.vector.TimeStampSecVector;
import org.apache.arrow.vector.TimeStampVector;
import org.apache.arrow.vector.ValueVector;
import tech.dingxin.writers.ArrowFieldWriter;

import java.sql.Timestamp;

/**
 * @author dingxin (zhangdingxin.zdx@alibaba-inc.com)
 */
public class TimestampWriter extends ArrowFieldWriter<Timestamp> {
    public TimestampWriter(ValueVector valueVector) {
        super(valueVector);
    }

    @Override
    public void doWrite(Timestamp row) {
        if (row == null) {
            ((DateMilliVector) getValueVector()).setNull(getCount());
        } else {
            TimeStampVector valueVector = (TimeStampVector) getValueVector();
            if (valueVector instanceof TimeStampSecVector || valueVector instanceof TimeStampSecTZVector) {
                ((TimeStampSecVector) valueVector)
                        .setSafe(getCount(), row.getTime() / 1000);
            } else if (valueVector instanceof TimeStampMilliVector || valueVector instanceof TimeStampMilliTZVector) {
                valueVector
                        .setSafe(getCount(), row.getTime());
            } else if (valueVector instanceof TimeStampMicroVector || valueVector instanceof TimeStampMicroTZVector) {
                valueVector
                        .setSafe(
                                getCount(),
                                row.getTime() * 1_000 +
                                        row.getNanos() / 1_000);
            } else {
                valueVector.setSafe(
                        getCount(),
                        row.getTime() * 1_000_000
                                + row.getNanos());
            }
        }
    }
}
