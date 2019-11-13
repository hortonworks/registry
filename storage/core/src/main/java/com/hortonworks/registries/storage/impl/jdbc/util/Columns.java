/**
 * Copyright 2017-2019 Cloudera, Inc.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 **/

package com.hortonworks.registries.storage.impl.jdbc.util;

import com.hortonworks.registries.common.Schema;

import java.util.HashMap;

public class Columns {
  private HashMap<String, Schema.Type> colums = new HashMap<>();

  public Schema.Type add(String name, Schema.Type type) {
    return colums.put(name.toLowerCase(), type);
  }

  public Schema.Type getType(String name) {
    return colums.get(name.toLowerCase());
  }

  public Schema.Type remove(String name) {
    return colums.remove(name.toLowerCase());
  }
}
