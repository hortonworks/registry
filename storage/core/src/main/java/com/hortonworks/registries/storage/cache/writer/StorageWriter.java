package com.hortonworks.registries.storage.cache.writer;

import com.hortonworks.registries.storage.StorableKey;
import com.hortonworks.registries.storage.Storable;

/**
 * Created by hlouro on 8/7/15.
 */
public interface StorageWriter {
    void add(Storable storable);

    void addOrUpdate(Storable storable);

    Object remove(StorableKey key);
}
