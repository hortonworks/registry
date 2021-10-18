/**
 * Copyright 2016-2021 Cloudera, Inc.
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
package com.hortonworks.registries.schemaregistry.exportimport.reader;

import com.hortonworks.registries.schemaregistry.AggregatedSchemaMetadataInfo;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class ClouderaFileReaderTest {
    private ClouderaFileReader underTest;

    @Test
    public void testParse() throws IOException {
      //given
      InputStream is = getClass().getResourceAsStream("/exportimport/schemaexport.json");
      underTest = new ClouderaFileReader(is);
      AtomicInteger metadataInfoSize = new AtomicInteger();
      AtomicInteger branchSize = new AtomicInteger();
      AtomicInteger versionInfoSize = new AtomicInteger();

      //when
      List<AggregatedSchemaMetadataInfo> metadataInfos = underTest.getMetadataInfos();

      //then
      metadataInfos.stream()
          .forEach(aggregatedSchemaMetadataInfo -> {
            metadataInfoSize.getAndIncrement();
            aggregatedSchemaMetadataInfo.getSchemaBranches().stream()
                .forEach(branch -> {
                  branchSize.getAndIncrement();
                  branch.getSchemaVersionInfos().stream()
                      .forEach(version -> {
                        versionInfoSize.getAndIncrement();
                      });
                });
          });
      assertEquals(6, versionInfoSize.get());
      assertEquals(4, branchSize.get());
      assertEquals(3, metadataInfoSize.get());
    }
}
