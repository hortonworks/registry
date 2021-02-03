 /*
  * Copyright 2015-2021 Cloudera, Inc.
  *
  * Licensed to the Apache Software Foundation (ASF) under one
  * or more contributor license agreements.  See the NOTICE file
  * distributed with this work for additional information
  * regarding copyright ownership.  The ASF licenses this file
  * to you under the Apache License, Version 2.0 (the
  * "License"); you may not use this file except in compliance
  * with the License.  You may obtain a copy of the License at
  *
  * http://www.apache.org/licenses/LICENSE-2.0
  *
  * Unless required by applicable law or agreed to in writing, software
  * distributed under the License is distributed on an "AS IS" BASIS,
  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  * See the License for the specific language governing permissions and
  * limitations under the License.
  */
package com.hortonworks.registries.storage.cache.writer;

import com.hortonworks.registries.storage.Storable;
import com.hortonworks.registries.storage.StorableKey;
import com.hortonworks.registries.storage.StorageManager;

/**
 * Created by hlouro on 8/7/15.
 */
public class StorageWriteThrough implements StorageWriter {
    private final StorageManager dao;

    public StorageWriteThrough(StorageManager dao) {
        this.dao = dao;
    }

    public void add(Storable storable) {
        dao.add(storable);
    }

    public void addOrUpdate(Storable storable) {
        dao.addOrUpdate(storable);
    }

    @Override
    public void update(Storable storable) {
        dao.update(storable);
    }

    public Object remove(StorableKey key) {
        return dao.remove(key);
    }
}
