/*
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
package com.hortonworks.registries.storage;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;

/**
 *
 */
public class DeviceInfoTest extends StorableTest {

    public DeviceInfoTest() {
        storableList = new ArrayList<Storable>() {
            {
                long x = 1L;
                int version = 0;
                add(createDeviceInfo(x, "" + version));
                add(createDeviceInfo(x, "deviceinfo-" + (version + 1)));
                add(createDeviceInfo(++x, "" + version));
                add(createDeviceInfo(++x, "" + version));
            }
        };
    }

    private Storable createDeviceInfo(long id, String version) {
        DeviceInfo deviceInfo = new DeviceInfo();
        deviceInfo.setId(id);
        long now = System.currentTimeMillis();
        deviceInfo.setXid("" + now);
        deviceInfo.setName("deviceinfo-" + now);
        deviceInfo.setTimestamp(System.currentTimeMillis());
        deviceInfo.setVersion(version);
        return deviceInfo;
    }

    public static Collection<Class<? extends Storable>> getStorableClasses() {
        return Collections.singletonList(DeviceInfo.class);
    }

}
