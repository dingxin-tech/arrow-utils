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

import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.complex.StructVector;
import tech.dingxin.writers.ArrowFieldWriter;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;

/**
 * @author dingxin (zhangdingxin.zdx@alibaba-inc.com)
 */
public class StructWriter extends ArrowFieldWriter<Object> {
    private final Map<String, Method> getterCache = new HashMap<>();
    private final ArrowFieldWriter[] childWriters;

    public StructWriter(ValueVector valueVector, ArrowFieldWriter[] childWriters) {
        super(valueVector);
        this.childWriters = childWriters;
    }

    @Override
    public void doWrite(Object row) {
        if (row == null) {
            ((StructVector) getValueVector()).setNull(getCount());
            for (ArrowFieldWriter writer : childWriters) {
                writer.write(null);
            }
        } else {
            if (getterCache.isEmpty()) {
                cacheGetterMethods(row.getClass());
            }
            ((StructVector) getValueVector()).setIndexDefined(getCount());
            for (ArrowFieldWriter writer : childWriters) {
                String fieldName = writer.getValueVector().getName();
                Method getMethod = getterCache.get(fieldName);
                Object value;
                try {
                    value = getMethod.invoke(row);
                } catch (Exception e) {
                    throw new RuntimeException(getMethod.getName() + " invoke error");
                }
                writer.write(value);
            }
        }
    }

    private void cacheGetterMethods(Class clazz) {
        for (Field field : clazz.getDeclaredFields()) {
            if (field.isSynthetic() || field.getName().startsWith("this$")) {
                continue;
            }
            // 正常字段的 getter 方法
            String getterName = "get" + field.getName().substring(0, 1).toUpperCase() + field.getName().substring(1);
            try {
                Method getter = clazz.getMethod(getterName);
                getterCache.put(field.getName(), getter);
            } catch (NoSuchMethodException e) {
                // 如果没有找到 get 方法，且字段是 boolean 类型，尝试 is 方法
                if (field.getType().equals(boolean.class) || field.getType().equals(Boolean.class)) {
                    String booleanGetterName = "is" + field.getName().substring(0, 1).toUpperCase() + field.getName()
                            .substring(1);
                    try {
                        Method booleanGetter = clazz.getMethod(booleanGetterName);
                        getterCache.put(field.getName(), booleanGetter);
                    } catch (NoSuchMethodException ignored) {
                        throw new IllegalArgumentException("No getter method found for field: " + field);
                    }
                } else {
                    throw new IllegalArgumentException("No getter method found for field: " + field);
                }
            }
        }
    }

    @Override
    public void finish() {
        super.finish();
        for (ArrowFieldWriter<?> fieldsWriter : childWriters) {
            fieldsWriter.finish();
        }
    }

    @Override
    public void reset() {
        super.reset();
        for (ArrowFieldWriter<?> fieldsWriter : childWriters) {
            fieldsWriter.reset();
        }
    }
}
