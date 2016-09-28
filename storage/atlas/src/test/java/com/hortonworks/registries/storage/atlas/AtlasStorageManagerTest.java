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
package com.hortonworks.registries.storage.atlas;

import com.google.common.io.Files;
import com.hortonworks.registries.common.test.IntegrationTest;
import com.hortonworks.registries.storage.AbstractStoreManagerTest;
import com.hortonworks.registries.storage.DeviceInfo;
import com.hortonworks.registries.storage.DeviceInfoTest;
import com.hortonworks.registries.storage.Storable;
import com.hortonworks.registries.storage.StorableTest;
import com.hortonworks.registries.storage.StorageManager;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 *
 */
@Category(IntegrationTest.class)
//@Ignore
public class AtlasStorageManagerTest extends AbstractStoreManagerTest {
    private static AtlasStorageManager atlasStorageManager;

    @BeforeClass
    public static void setUpClass() throws Exception {
        atlasStorageManager = new AtlasStorageManager();
        atlasStorageManager.init(createProps());
        atlasStorageManager.registerStorables(Collections.singletonList(DeviceInfo.class));
    }

    private static Map<String, Object> createProps() {
        Map<String, Object> props = new HashMap<>();
        Path tempDir = Files.createTempDir().toPath();

        props.put("atlas.graph.storage.backend", "berkleyje");
        props.put("atlas.graph.storage.directory", Paths.get(tempDir.toString(), "berkley"));
        props.put("atlas.graph.index.search.backend", "elasticsearch");
        props.put("atlas.graph.index.search.directory", Paths.get(tempDir.toString(), "elasticsearch"));

        props.put("atlas.graph.index.search.elasticsearch.client-only", false);
        props.put("atlas.graph.index.search.elasticsearch.local-mode", true);
        props.put("atlas.graph.index.search.elasticsearch.create.sleep", 4000);
        props.put("atlas.DeleteHandler.impl", "org.apache.atlas.repository.graph.HardDeleteHandler");
        props.put("atlas.EntityAuditRepository.impl", "org.apache.atlas.repository.audit.NoopEntityAuditRepository");

        return props;
    }

    @Override
    protected void setStorableTests() {
        storableTests = new ArrayList<>();
        storableTests.add(new DeviceInfoTest());
    }

    @Override
    protected StorageManager getStorageManager() {
        return atlasStorageManager;
    }

    @Before
    public void setup() {
        super.setup();
        Collection<Storable> storables = getStorageManager().list(DeviceInfo.NAME_SPACE);
        for (Storable storable : storables) {
            getStorageManager().remove(storable.getStorableKey());
        }
    }

    @Test
    public void testNextId_AutoincrementColumn_IdPlusOne() throws Exception {
        for (StorableTest storableTest : storableTests) {
            Long currentId = getStorageManager().nextId(storableTest.getNameSpace());
            for (int i = 0; i < 10; i++) {
                Long nextId = getStorageManager().nextId(storableTest.getNameSpace());
                Assert.assertTrue(currentId < nextId);
                currentId = nextId;
            }
        }
    }
}
