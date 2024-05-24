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

package tech.dingxin.utils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author dingxin (zhangdingxin.zdx@alibaba-inc.com)
 */
public class POJO {
    private String string;
    private List<String> list;
    private Map<String, String> map;
    private List<List<String>> mapList;
    private Inner inner;

    public static POJO getSampleInstance() {
        POJO pojo = new POJO();
        pojo.setString("name");

        Map<String, String> map = new HashMap<>();
        map.put("key", "value");
        map.put("key2", "value2");
        pojo.setMap(map);

        List<String> list = new ArrayList<>();
        list.add("happy");
        list.add(null);
        pojo.setList(list);

        List<List<String>> list1 = Arrays.asList(list, new ArrayList<String>());
        pojo.setMapList(list1);

        Inner inner = new Inner();
        inner.setInnerString("innerString");
        pojo.setInner(inner);

        return pojo;
    }

    public String getString() {
        return string;
    }

    public void setString(String string) {
        this.string = string;
    }

    public List<String> getList() {
        return list;
    }

    public void setList(List<String> list) {
        this.list = list;
    }

    public Map<String, String> getMap() {
        return map;
    }

    public void setMap(Map<String, String> map) {
        this.map = map;
    }

    public List<List<String>> getMapList() {
        return mapList;
    }

    public void setMapList(List<List<String>> mapList) {
        this.mapList = mapList;
    }

    public Inner getInner() {
        return inner;
    }

    public void setInner(Inner inner) {
        this.inner = inner;
    }

    public static class Inner {
        String innerString;

        public String getInnerString() {
            return innerString;
        }

        public void setInnerString(String innerString) {
            this.innerString = innerString;
        }
    }
}
