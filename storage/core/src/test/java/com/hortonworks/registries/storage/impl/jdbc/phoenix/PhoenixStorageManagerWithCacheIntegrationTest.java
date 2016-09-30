package com.hortonworks.registries.storage.impl.jdbc.phoenix;

import com.google.common.cache.CacheBuilder;
import com.hortonworks.registries.storage.impl.jdbc.config.ExecutionConfig;
import com.hortonworks.registries.storage.impl.jdbc.provider.phoenix.factory.PhoenixExecutor;
import com.hortonworks.registries.common.test.HBaseIntegrationTest;
import org.junit.experimental.categories.Category;

/**
 *
 */

@Category(HBaseIntegrationTest.class)
public abstract  class PhoenixStorageManagerWithCacheIntegrationTest extends PhoenixStorageManagerNoCacheIntegrationTest {

    public PhoenixStorageManagerWithCacheIntegrationTest() {
        setConnectionBuilder();
        CacheBuilder  cacheBuilder = CacheBuilder.newBuilder().maximumSize(3);
        jdbcStorageManager = createJdbcStorageManager(new PhoenixExecutor(new ExecutionConfig(-1), connectionBuilder, cacheBuilder));

    }
}