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

import org.apache.arrow.vector.DateDayVector;
import org.apache.arrow.vector.ValueVector;
import tech.dingxin.writers.ArrowFieldWriter;

import java.time.LocalDate;

/**
 * @author dingxin (zhangdingxin.zdx@alibaba-inc.com)
 */
public class DateWriter extends ArrowFieldWriter<LocalDate> {
    public DateWriter(ValueVector valueVector) {
        super(valueVector);
    }

    @Override
    public void doWrite(LocalDate row) {
        if (row == null) {
            ((DateDayVector) getValueVector()).setNull(getCount());
        } else {
            ((DateDayVector) getValueVector()).setSafe(getCount(), (int) row.toEpochDay());
        }
    }
}
