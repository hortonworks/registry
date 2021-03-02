/**
 * Copyright 2016-2021 Cloudera, Inc.
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
package com.cloudera.dim.atlas;

import com.hortonworks.registries.common.util.FileStorage;
import com.hortonworks.registries.schemaregistry.AggregatedSchemaBranch;
import com.hortonworks.registries.schemaregistry.AggregatedSchemaMetadataInfo;
import com.hortonworks.registries.schemaregistry.CompatibilityResult;
import com.hortonworks.registries.schemaregistry.SchemaBranch;
import com.hortonworks.registries.schemaregistry.SchemaCompatibility;
import com.hortonworks.registries.schemaregistry.SchemaIdVersion;
import com.hortonworks.registries.schemaregistry.SchemaMetadata;
import com.hortonworks.registries.schemaregistry.SchemaMetadataInfo;
import com.hortonworks.registries.schemaregistry.SchemaValidationLevel;
import com.hortonworks.registries.schemaregistry.SchemaVersion;
import com.hortonworks.registries.schemaregistry.SchemaVersionInfo;
import com.hortonworks.registries.schemaregistry.SchemaVersionKey;
import com.hortonworks.registries.schemaregistry.SchemaVersionLifecycleManager;
import com.hortonworks.registries.schemaregistry.SchemaVersionMergeResult;
import com.hortonworks.registries.schemaregistry.SchemaVersionMergeStrategy;
import com.hortonworks.registries.schemaregistry.SerDesInfo;
import com.hortonworks.registries.schemaregistry.SerDesPair;
import com.hortonworks.registries.schemaregistry.errors.SchemaBranchAlreadyExistsException;
import com.hortonworks.registries.schemaregistry.errors.SchemaBranchNotFoundException;
import com.hortonworks.registries.schemaregistry.errors.SchemaNotFoundException;
import com.hortonworks.registries.schemaregistry.state.CustomSchemaStateExecutor;
import com.hortonworks.registries.schemaregistry.state.SchemaVersionLifecycleContext;
import com.hortonworks.registries.schemaregistry.state.SchemaVersionLifecycleStateMachine;
import com.hortonworks.registries.schemaregistry.state.SchemaVersionLifecycleStateMachineInfo;
import com.hortonworks.registries.schemaregistry.state.SchemaVersionLifecycleStates;
import com.hortonworks.registries.schemaregistry.state.SchemaVersionService;
import com.hortonworks.registries.schemaregistry.state.details.MergeInfo;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import javax.ws.rs.core.MultivaluedHashMap;
import javax.ws.rs.core.MultivaluedMap;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.hortonworks.registries.schemaregistry.ISchemaRegistry.DEFAULT_SCHEMA_VERSION_MERGE_STRATEGY;
import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class AtlasSchemaRegistryTest {
    
    private AtlasSchemaRegistry underTest;
    private AtlasPlugin atlasPlugin;
    private FileStorage fileStorage;
    private SchemaVersionLifecycleManager schemaVersionLifecycleManager;
    private static final Long TIMESTAMP = System.currentTimeMillis();

    @Before
    public void setup() {
        atlasPlugin = Mockito.mock(AtlasPlugin.class);
        fileStorage = Mockito.mock(FileStorage.class);
        schemaVersionLifecycleManager = Mockito.mock(SchemaVersionLifecycleManager.class);
        underTest = new AtlasSchemaRegistry(fileStorage, null);
        underTest.setAtlasClient(atlasPlugin, schemaVersionLifecycleManager);
    }
    
    @Test
    public void addSchemaMetadataReturnValid() {
        //given
        String schemaName = "name";
        SchemaMetadata schemaMetadata = schemaMetadataCreator(schemaName, Optional.empty());
        Long id = 34L;
        when(atlasPlugin.createMeta(schemaMetadata)).thenReturn(id);
        
        //when
        Long actual = underTest.addSchemaMetadata(schemaMetadata);
        
        //then
        assertEquals(id, actual, 0.0);
        verify(atlasPlugin).createMeta(schemaMetadata);
    }
    
    @Test(expected = RuntimeException.class)
    public void addSchemaMetadataExistingMeta() {
        //given
        String schemaName = "existing";
        SchemaMetadata existingschemaMetadata = schemaMetadataCreator(schemaName, Optional.empty());
        SchemaMetadataInfo schemaMetadataInfo = new SchemaMetadataInfo(existingschemaMetadata, 55L, System.currentTimeMillis());
        when(atlasPlugin.getSchemaMetadataInfo(schemaName)).thenReturn(Optional.of(schemaMetadataInfo));
        
        //when
        underTest.addSchemaMetadata(existingschemaMetadata, true);
        
        //then
        verify(atlasPlugin).getSchemaMetadataInfo("existing");
    }
    
    @Test
    public void findAggregatedSchemaMetadataReturnValue() throws Exception {
        //given
        Map<String, String> props = propCreator("Peanut", "Butter");
        String schemaMetadataName = "PBJ";
        Long schemaVersionId = 33L;
        SchemaMetadata schemaMetadata = schemaMetadataCreator(schemaMetadataName, Optional.empty());
        SchemaMetadataInfo schemaMetadataInfo = new SchemaMetadataInfo(schemaMetadata, 43L, TIMESTAMP);
        Collection<SchemaMetadataInfo> schemaMetadataInfos = collectionCreator(schemaMetadataInfo);
        SchemaVersionInfo schemaVersionInfo = new SchemaVersionInfo(schemaVersionId, "versionName", 1, 1L, 
                "schemaText", TIMESTAMP, "description", new Byte("4"));
        Collection<SchemaVersionInfo> schemaVersionInfos = collectionCreator(schemaVersionInfo);
        SchemaBranch schemaBranch = new SchemaBranch(88L, "branchName", schemaMetadataName, "desc", TIMESTAMP);
        Set<SchemaBranch> schemaBranches = setCreator(schemaBranch);
        AggregatedSchemaBranch aggregatedSchemaBranch = new AggregatedSchemaBranch(schemaBranch, schemaVersionId, schemaVersionInfos);
        Collection<AggregatedSchemaBranch> aggregatedSchemaBranches = collectionCreator(aggregatedSchemaBranch);
        SerDesInfo serDesInfo = new SerDesInfo(45L, TIMESTAMP, new SerDesPair());
        Collection<SerDesInfo> serDesInfos = collectionCreator(serDesInfo);
        AggregatedSchemaMetadataInfo aggregatedSchemaMetadataInfo = new AggregatedSchemaMetadataInfo(schemaMetadata, 43L, TIMESTAMP, aggregatedSchemaBranches, serDesInfos);
        Collection<AggregatedSchemaMetadataInfo> aggregatedSchemaMetadataInfos =  collectionCreator(aggregatedSchemaMetadataInfo);
        SchemaVersionLifecycleContext context = new SchemaVersionLifecycleContext(26L, 1, Mockito.mock(SchemaVersionService.class), Mockito.mock(SchemaVersionLifecycleStateMachine.class), Mockito.mock(CustomSchemaStateExecutor.class));
        context.setDetails(null);
        when(schemaVersionLifecycleManager
                .createSchemaVersionLifeCycleContext(schemaVersionId, SchemaVersionLifecycleStates.INITIATED)).thenReturn(context);
        when(schemaVersionLifecycleManager.getSchemaBranches(schemaVersionId)).thenReturn(schemaBranches);
        when(schemaVersionLifecycleManager.getRootVersion(schemaBranch)).thenReturn(schemaVersionInfo);
        when(schemaVersionLifecycleManager.getAllVersions("branchName", schemaMetadataName)).thenReturn(schemaVersionInfos);
        when(schemaVersionLifecycleManager.getAllVersions(schemaMetadataName)).thenReturn(schemaVersionInfos);
        when(atlasPlugin.getSerDesMappingsForSchema(schemaMetadataName)).thenReturn(serDesInfos);
        when(schemaVersionLifecycleManager.getRootVersion(schemaBranch)).thenReturn(schemaVersionInfo);
        when(atlasPlugin.search(Optional.of("Peanut"), Optional.of("Butter"), Optional.of("timestamp,a"))).thenReturn(schemaMetadataInfos);
        
        //when
        Collection<AggregatedSchemaMetadataInfo> actual = underTest.findAggregatedSchemaMetadata(props);
        
        //then
        assertEquals(aggregatedSchemaMetadataInfos, actual);
        verify(schemaVersionLifecycleManager).createSchemaVersionLifeCycleContext(33L, SchemaVersionLifecycleStates.INITIATED);
        verify(schemaVersionLifecycleManager).getSchemaBranches(33L);
        verify(schemaVersionLifecycleManager).getRootVersion(schemaBranch);
        verify(schemaVersionLifecycleManager).getAllVersions("branchName", "PBJ");
        verify(schemaVersionLifecycleManager).getAllVersions("PBJ");
        verify(atlasPlugin).getSerDesMappingsForSchema("PBJ");
        verify(schemaVersionLifecycleManager).getRootVersion(schemaBranch);
        verify(atlasPlugin).search(Optional.of("Peanut"), Optional.of("Butter"), Optional.of("timestamp,a"));
    }
    
    @Test
    public void getAggregatedSchemaMetadataInfoTest() throws Exception {
        //given
        String schemaName = "Coconut";
        Long schemaVersionId = 43L;
        SchemaMetadata schemaMetadata = schemaMetadataCreator(schemaName, Optional.empty());
        SchemaMetadataInfo schemaMetadataInfo = new SchemaMetadataInfo(schemaMetadata, 90L, TIMESTAMP);
        SerDesInfo serDesInfo = new SerDesInfo(54L, TIMESTAMP, new SerDesPair());
        Collection<SerDesInfo> serDesInfos = collectionCreator(serDesInfo);
        SchemaVersionInfo schemaVersionInfo = new SchemaVersionInfo(schemaVersionId, schemaName, 5, "text", TIMESTAMP, "desc");
        Collection<SchemaVersionInfo> schemaVersionInfos = collectionCreator(schemaVersionInfo);
        SchemaBranch schemaBranch = new SchemaBranch("MASTER", schemaName);
        Set<SchemaBranch> schemaBranches = setCreator(schemaBranch);
        SchemaVersionLifecycleContext context = new SchemaVersionLifecycleContext(schemaVersionId, 1, Mockito.mock(SchemaVersionService.class),
                Mockito.mock(SchemaVersionLifecycleStateMachine.class), Mockito.mock(CustomSchemaStateExecutor.class));
        AggregatedSchemaBranch aggregatedSchemaBranch = new AggregatedSchemaBranch(schemaBranch, null, schemaVersionInfos);
        Collection<AggregatedSchemaBranch> aggregatedSchemaBranches = collectionCreator(aggregatedSchemaBranch);
        when(schemaVersionLifecycleManager.getAllVersions(schemaName)).thenReturn(schemaVersionInfos);
        when(schemaVersionLifecycleManager.getSchemaBranches(schemaVersionInfo.getId())).thenReturn(schemaBranches);
        when(schemaVersionLifecycleManager.getAllVersions("MASTER", schemaName)).thenReturn(schemaVersionInfos);
        when(schemaVersionLifecycleManager
                .createSchemaVersionLifeCycleContext(schemaVersionInfo.getId(), SchemaVersionLifecycleStates.INITIATED)).thenReturn(context);
        
        //when
        Collection<AggregatedSchemaBranch> actual = underTest.getAggregatedSchemaBranch(schemaName);
        
        //then
        assertEquals(aggregatedSchemaBranches, actual);
        verify(schemaVersionLifecycleManager).getAllVersions("Coconut");
        verify(schemaVersionLifecycleManager).getSchemaBranches(43L);
        verify(schemaVersionLifecycleManager).getAllVersions("MASTER", "Coconut");
        verify(schemaVersionLifecycleManager).createSchemaVersionLifeCycleContext(43L, SchemaVersionLifecycleStates.INITIATED);
    }
    
    @Test
    public void findSchemaMetadataTest() {
        //given
        String schemaName = "Lime";
        SchemaMetadata schemaMetadata = schemaMetadataCreator(schemaName, Optional.of("Orange"));
        Map<String, String> props = propCreator(schemaName, "Orange");
        SchemaMetadataInfo schemaMetadataInfo =  new SchemaMetadataInfo(schemaMetadata);
        Collection<SchemaMetadataInfo> schemaMetadataInfos = collectionCreator(schemaMetadataInfo);
        when(atlasPlugin.search(Optional.of(schemaName), Optional.of("Orange"), Optional.of("timestamp,a"))).thenReturn(schemaMetadataInfos);
        
        //when
        Collection<SchemaMetadataInfo> actual = underTest.findSchemaMetadata(props);
        
        //then
        assertEquals(schemaMetadataInfos, actual);
        verify(atlasPlugin).search(Optional.of("Lime"), Optional.of("Orange"), Optional.of("timestamp,a"));
    }
    
    @Test
    public void registerSchemaMetadataTest() {
        //given
        String schemaName = "pencil";
        SchemaMetadata schemaMetadata = schemaMetadataCreator(schemaName, Optional.of("pen"));
        Long id = 876L;
        when(atlasPlugin.getSchemaMetadataInfo(schemaName)).thenReturn(Optional.empty());
        when(atlasPlugin.createMeta(schemaMetadata)).thenReturn(id);
        
        //when
        Long actual = underTest.registerSchemaMetadata(schemaMetadata);
        
        //then
        assertEquals(id, actual);
        verify(atlasPlugin).getSchemaMetadataInfo("pencil");
        verify(atlasPlugin).createMeta(schemaMetadata);
    }

    @Test
    public void addSchemaMetadataTest() {
        //given
        String schemaName = "perfume";
        SchemaMetadata schemaMetadata = schemaMetadataCreator(schemaName, Optional.of("dishwasher"));
        Long id = 877L;
        when(atlasPlugin.getSchemaMetadataInfo(schemaName)).thenReturn(Optional.empty());
        when(atlasPlugin.createMeta(schemaMetadata)).thenReturn(id);
        
        //when
        Long actual = underTest.addSchemaMetadata(schemaMetadata);
        
        //then
        assertEquals(id, actual);
        verify(atlasPlugin).getSchemaMetadataInfo("perfume");
        verify(atlasPlugin).createMeta(schemaMetadata);
    }
    
    @Test
    public void updateSchemaMetadataTest() throws Exception {
        //given
        String schemaName = "Tetris";
        SchemaMetadata schemaMetadata = schemaMetadataCreator(schemaName, Optional.of("Tetrimino"));
        SchemaMetadataInfo schemaMetadataInfo = new SchemaMetadataInfo(schemaMetadata);
        when(atlasPlugin.updateMeta(schemaMetadata)).thenReturn(Optional.of(schemaMetadataInfo));
        
        //when
        SchemaMetadataInfo actual = underTest.updateSchemaMetadata(schemaName, schemaMetadata);
        
        //then
        assertEquals(schemaMetadataInfo, actual);
        verify(atlasPlugin).updateMeta(schemaMetadata);
    }

    @Test(expected = IllegalArgumentException.class)
    public void updateSchemaMetadataThrowsIllegalArgumentExceptionTest() {
        //given
        String schemaName = "Tetris";
        SchemaMetadata schemaMetadata = schemaMetadataCreator("not Tetris", Optional.of("Tetrimino"));
        
        //when
        underTest.updateSchemaMetadata(schemaName, schemaMetadata);
        
        //then
    }

    @Test(expected = AtlasUncheckedException.class)
    public void updateSchemaMetadataThrowsAtlasUncheckedExceptionTest() throws Exception {
        //given
        String schemaName = "Tetris";
        SchemaMetadata schemaMetadata = schemaMetadataCreator(schemaName, Optional.of("Tetrimino"));
        when(atlasPlugin.updateMeta(schemaMetadata)).thenThrow(new SchemaNotFoundException());
        
        //when
        underTest.updateSchemaMetadata(schemaName, schemaMetadata);
        
        //then
        verify(atlasPlugin).updateMeta(schemaMetadata);
    }

    @Test
    public void getSchemaMetadataInfoIdTest() {
        //given
        Long schemaMetadataId = 2L;
        SchemaMetadata schemaMetadata = schemaMetadataCreator("yoga", Optional.empty());
        SchemaMetadataInfo schemaMetadataInfo = new SchemaMetadataInfo(schemaMetadata);
        when(atlasPlugin.getSchemaMetadataInfo(schemaMetadataId)).thenReturn(Optional.of(schemaMetadataInfo));
        
        //when
        SchemaMetadataInfo actual = underTest.getSchemaMetadataInfo(schemaMetadataId);
        
        //then
        assertEquals(schemaMetadataInfo, actual);
        verify(atlasPlugin).getSchemaMetadataInfo(2L);
    }
    
    @Test
    public void getSchemaMetadataInfoSchemaNameTest() {
        //given
        String schemaName = "puppy";
        SchemaMetadata schemaMetadata = schemaMetadataCreator("puppy", Optional.empty());
        SchemaMetadataInfo schemaMetadataInfo = new SchemaMetadataInfo(schemaMetadata);
        when(atlasPlugin.getSchemaMetadataInfo(schemaName)).thenReturn(Optional.of(schemaMetadataInfo));
        
        //when
        SchemaMetadataInfo actual = underTest.getSchemaMetadataInfo(schemaName);
        
        //then
        assertEquals(schemaMetadataInfo, actual);
        verify(atlasPlugin).getSchemaMetadataInfo("puppy");
    }
    
    @Test
    public void retrieveSchemaVersionTest() throws Exception {
        //given
        String schemaName = "Juice";
        SchemaVersionKey schemaVersionKey = new SchemaVersionKey(schemaName, 5);
        SchemaMetadata schemaMetadata = schemaMetadataCreator(schemaName, Optional.of("hellobello"));
        SchemaVersionInfo schemaVersionInfo = new SchemaVersionInfo(22L, schemaName, 5, "schemaText", System.currentTimeMillis(), "description");
        SchemaMetadataInfo schemaMetadataInfo = new SchemaMetadataInfo(schemaMetadata);
        when(atlasPlugin.getSchemaMetadataInfo(schemaName)).thenReturn(Optional.of(schemaMetadataInfo));
        when(atlasPlugin.getSchemaVersion(schemaName, 5)).thenReturn(Optional.of(schemaVersionInfo));
        
        //when
        SchemaVersionInfo actual = underTest.retrieveSchemaVersion(schemaVersionKey);
        
        //then
        assertEquals(schemaVersionInfo, actual);
        verify(atlasPlugin).getSchemaMetadataInfo("Juice");
        verify(atlasPlugin).getSchemaVersion("Juice", 5);
    }

    @Test(expected = SchemaNotFoundException.class)
    public void retrieveSchemaVersionThrowsExceptionTest() throws Exception {
        //given
        String schemaName = "Juice";
        SchemaVersionKey schemaVersionKey = new SchemaVersionKey(schemaName, 5);
        when(atlasPlugin.getSchemaMetadataInfo(schemaName)).thenReturn(Optional.empty());
        
        //when
        underTest.retrieveSchemaVersion(schemaVersionKey);
        
        //then
        verify(atlasPlugin).getSchemaMetadataInfo("Juice");
    }
    
    @Test
    public void addSchemaVersionMetadataVersionTest() throws Exception {
        //given
        SchemaMetadata schemaMetadata = schemaMetadataCreator("Water", Optional.empty());
        SchemaVersion schemaVersion = new SchemaVersion("text of version 1", "desc");
        SchemaIdVersion schemaIdVersion = new SchemaIdVersion(970L, 7, 3L);
        when(schemaVersionLifecycleManager.addSchemaVersion(eq(SchemaBranch.MASTER_BRANCH), eq(schemaMetadata), 
                eq(schemaVersion), any(), eq(false))).thenReturn(schemaIdVersion);
        
        //when
        SchemaIdVersion actual = underTest.addSchemaVersion(schemaMetadata, schemaVersion, false);
        
        //then
        assertEquals(schemaIdVersion, actual);
        verify(schemaVersionLifecycleManager).addSchemaVersion(eq(SchemaBranch.MASTER_BRANCH), eq(schemaMetadata),
                eq(schemaVersion), any(), eq(false));
    }

    @Test
    public void addSchemaVersionMetadataIdVersionTest() throws Exception {
        //given
        SchemaMetadata schemaMetadata = schemaMetadataCreator("Water", Optional.empty());
        SchemaVersion schemaVersion = new SchemaVersion("text of version 1", "desc");
        SchemaIdVersion schemaIdVersion = new SchemaIdVersion(971L, 7, 3L);
        when(schemaVersionLifecycleManager.addSchemaVersion(eq(SchemaBranch.MASTER_BRANCH), eq(schemaMetadata), eq(5L),
                eq(schemaVersion), any(), eq(false))).thenReturn(schemaIdVersion);
        
        //when
        SchemaIdVersion actual = underTest.addSchemaVersion(schemaMetadata, 5L, schemaVersion);
        
        //then
        assertEquals(schemaIdVersion, actual);
        verify(schemaVersionLifecycleManager).addSchemaVersion(eq(SchemaBranch.MASTER_BRANCH), eq(schemaMetadata), eq(5L),
                eq(schemaVersion), any(), eq(false));
    }
    
    @Test
    public void addSchemaVersionNameVersionTest() throws Exception {
        //given
        SchemaVersion schemaVersion = new SchemaVersion("schema text", "desc");
        SchemaIdVersion schemaIdVersion = new SchemaIdVersion(971L, 7, 3L);
        String schemaName = "addSchemaVersionTest";
        when(schemaVersionLifecycleManager.addSchemaVersion(SchemaBranch.MASTER_BRANCH, schemaName, schemaVersion, false)).thenReturn(schemaIdVersion);
        
        //when
        SchemaIdVersion actual = underTest.addSchemaVersion(schemaName, schemaVersion, false);
        
        //then
        assertEquals(schemaIdVersion, actual);
        verify(schemaVersionLifecycleManager).addSchemaVersion(SchemaBranch.MASTER_BRANCH, schemaName, schemaVersion, false);
    }
    
    @Test
    public void addSchemaVersionBranchNameVersionTest() throws Exception {
        //given
        String schemaBranchName = "branchName";
        String schemaName = "schemaName";
        SchemaVersion schemaVersion = new SchemaVersion("schema text", "desc");
        SchemaIdVersion schemaIdVersion = new SchemaIdVersion(453L, 4, 23L);
        when(schemaVersionLifecycleManager.addSchemaVersion(schemaBranchName, schemaName, schemaVersion, false)).thenReturn(schemaIdVersion);
        
        //when
        SchemaIdVersion actual = underTest.addSchemaVersion(schemaBranchName, schemaName, schemaVersion, false);
        
        //then
        assertEquals(schemaIdVersion, actual);
        verify(schemaVersionLifecycleManager).addSchemaVersion(schemaBranchName, schemaName, schemaVersion, false);
    }

    @Test(expected = RuntimeException.class)
    public void addSchemaVersionBranchNameVersionThrowsExceptionTest() throws Exception {
        //given
        String schemaBranchName = "branchName";
        String schemaName = "schemaName";
        SchemaVersion schemaVersion = new SchemaVersion("schema text", "desc");
        when(schemaVersionLifecycleManager.addSchemaVersion(schemaBranchName, schemaName, schemaVersion, false)).thenThrow(new SchemaNotFoundException());
        
        //when
        underTest.addSchemaVersion(schemaBranchName, schemaName, schemaVersion, false);
        
        //then
        verify(schemaVersionLifecycleManager).addSchemaVersion("branchName", "schemaName", schemaVersion, false);
    }

    @Test(expected = Error.class)
    public void addSchemaVersionBranchNameVersionThrowsError() throws Exception {
        //given
        String schemaBranchName = "branchName";
        String schemaName = "schemaName";
        SchemaVersion schemaVersion = new SchemaVersion("schema text", "desc");
        when(schemaVersionLifecycleManager.addSchemaVersion(schemaBranchName, schemaName, schemaVersion, false)).thenReturn(null);
        
        //when
        underTest.addSchemaVersion(schemaBranchName, schemaName, schemaVersion, false);
        
        //then
        verify(schemaVersionLifecycleManager).addSchemaVersion("branchName", "schemaName", schemaVersion, false);
    }
    
    @Test
    public void deleteSchemaVersionSchemaVersionKeyTest() throws Exception {
        //given
        SchemaVersionKey schemaVersionKey = new SchemaVersionKey("Michelangelo", 2);
        
        //when
        underTest.deleteSchemaVersion(schemaVersionKey);
        
        //then
        verify(schemaVersionLifecycleManager).deleteSchemaVersion(schemaVersionKey);
    }
    
    @Test
    public void getSchemaVersionInfoSchemaVersionKeyTest() throws Exception {
        //given
        SchemaVersionKey schemaVersionKey = new SchemaVersionKey("Blue", 3);
        SchemaVersionInfo schemaVersionInfo = new SchemaVersionInfo(55L, "Blue", 3, "text of schema", TIMESTAMP, "description");
        when(schemaVersionLifecycleManager.getSchemaVersionInfo(schemaVersionKey)).thenReturn(schemaVersionInfo);
        
        //when
        SchemaVersionInfo actual = underTest.getSchemaVersionInfo(schemaVersionKey);
        
        //then
        assertEquals(schemaVersionInfo, actual);
        verify(schemaVersionLifecycleManager).getSchemaVersionInfo(schemaVersionKey);
    }

    @Test
    public void getSchemaVersionInfoSchemaIdVersionTest() throws Exception {
        //given
        SchemaIdVersion schemaIdVersion = new SchemaIdVersion(588L, 4);
        SchemaVersionInfo schemaVersionInfo = new SchemaVersionInfo(57L, "Blue", 4, "text of schema", TIMESTAMP, "description");
        when(schemaVersionLifecycleManager.getSchemaVersionInfo(schemaIdVersion)).thenReturn(schemaVersionInfo);
        
        //when
        SchemaVersionInfo actual = underTest.getSchemaVersionInfo(schemaIdVersion);
        
        //then
        assertEquals(schemaVersionInfo, actual);
        verify(schemaVersionLifecycleManager).getSchemaVersionInfo(schemaIdVersion);
    }
    
    @Test
    public void getLatestSchemaVersionInfoNameTest() throws Exception {
        //given
        String schemaName = "Cinderella";
        SchemaVersionInfo schemaVersionInfo = new SchemaVersionInfo(11L, "Cinderella", 90, "midnight", TIMESTAMP, "story");
        when(schemaVersionLifecycleManager.getLatestSchemaVersionInfo(schemaName)).thenReturn(schemaVersionInfo);
        
        //when
        SchemaVersionInfo actual = underTest.getLatestSchemaVersionInfo(schemaName);
        
        //then
        assertEquals(schemaVersionInfo, actual);
        verify(schemaVersionLifecycleManager).getLatestSchemaVersionInfo("Cinderella");
        
    }

    @Test
    public void getLatestSchemaVersionInfoBranchNameTest() throws Exception {
        //given
        String schemaName = "Cinderella";
        String branchName = "Shoes";
        SchemaVersionInfo schemaVersionInfo = new SchemaVersionInfo(12L, "Cinderella", 91, "midnight", TIMESTAMP, "story");
        when(schemaVersionLifecycleManager.getLatestSchemaVersionInfo(branchName, schemaName)).thenReturn(schemaVersionInfo);
        
        //when
        SchemaVersionInfo actual = underTest.getLatestSchemaVersionInfo(branchName, schemaName);
        
        //then
        assertEquals(schemaVersionInfo, actual);
        verify(schemaVersionLifecycleManager).getLatestSchemaVersionInfo("Shoes", "Cinderella");
    }
    
    @Test
    public void getAllVersionsNameTest() throws Exception {
        //given
        String schemaName = "allversions";
        SchemaVersionInfo schemaVersionInfo = new SchemaVersionInfo(74L, "allversions", 34, "alltext", TIMESTAMP, "all desc");
        Collection<SchemaVersionInfo> schemaVersionInfos = collectionCreator(schemaVersionInfo);
        when(schemaVersionLifecycleManager.getAllVersions(schemaName)).thenReturn(schemaVersionInfos);
        
        //when
        Collection<SchemaVersionInfo> actual = underTest.getAllVersions(schemaName);
        
        //then
        assertEquals(schemaVersionInfos, actual);
        verify(schemaVersionLifecycleManager).getAllVersions("allversions");

    }

    @Test
    public void getAllVersionsBranchNameTest() throws Exception {
        //given
        String schemaName = "allversionsbranch";
        String branchName = "branchallversion";
        SchemaVersionInfo schemaVersionInfo = new SchemaVersionInfo(75L, "allversionsbranch", 39, "alltext", TIMESTAMP, "all desc");
        Collection<SchemaVersionInfo> schemaVersionInfos = collectionCreator(schemaVersionInfo);
        when(schemaVersionLifecycleManager.getAllVersions(branchName, schemaName)).thenReturn(schemaVersionInfos);
        
        //when
        Collection<SchemaVersionInfo> actual = underTest.getAllVersions(branchName, schemaName);
        
        //then
        assertEquals(schemaVersionInfos, actual);
        verify(schemaVersionLifecycleManager).getAllVersions("branchallversion", "allversionsbranch");

    }
    
    @Test
    public void getAggregatedSchemaBranchTest() throws Exception {
        //given
        String schemaName = "sunshine";
        Long id = 26L;
        SchemaVersionInfo schemaVersionInfo = new SchemaVersionInfo(id, "svinfos", 3, "text", TIMESTAMP, "description");
        Collection<SchemaVersionInfo> schemaVersionInfos = collectionCreator(schemaVersionInfo);
        SchemaBranch schemaBranch = new SchemaBranch(39L, "notMASTER", schemaName, "desc", TIMESTAMP);
        Set<SchemaBranch> schemaBranches = setCreator(schemaBranch);
        SchemaVersionLifecycleContext context = new SchemaVersionLifecycleContext(id, 1, Mockito.mock(SchemaVersionService.class), 
                Mockito.mock(SchemaVersionLifecycleStateMachine.class), Mockito.mock(CustomSchemaStateExecutor.class));
        AggregatedSchemaBranch aggregatedSchemaBranch = new AggregatedSchemaBranch(schemaBranch, id, schemaVersionInfos);
        Collection<AggregatedSchemaBranch> aggregatedSchemaBranches = collectionCreator(aggregatedSchemaBranch);
        when(schemaVersionLifecycleManager.getAllVersions(schemaName)).thenReturn(schemaVersionInfos);
        when(schemaVersionLifecycleManager.getAllVersions("notMASTER", schemaName)).thenReturn(schemaVersionInfos);
        when(schemaVersionLifecycleManager.getSchemaBranches(id)).thenReturn(schemaBranches);
        when(schemaVersionLifecycleManager.getRootVersion(schemaBranch)).thenReturn(schemaVersionInfo);
        when(schemaVersionLifecycleManager
                .createSchemaVersionLifeCycleContext(id, SchemaVersionLifecycleStates.INITIATED)).thenReturn(context);
        
        //when
        Collection<AggregatedSchemaBranch> actual = underTest.getAggregatedSchemaBranch(schemaName);
        
        //then
        assertEquals(aggregatedSchemaBranches, actual);
        verify(schemaVersionLifecycleManager).getAllVersions("sunshine");
        verify(schemaVersionLifecycleManager).getAllVersions("notMASTER", "sunshine");
        verify(schemaVersionLifecycleManager).getSchemaBranches(26L);
        verify(schemaVersionLifecycleManager).getRootVersion(schemaBranch);
        verify(schemaVersionLifecycleManager).createSchemaVersionLifeCycleContext(26L, SchemaVersionLifecycleStates.INITIATED);
    }

    @Test
    public void getAggregatedSchemaBranchMergeInfoNullTest() throws Exception {
        //given
        String schemaName = "sunshine";
        Long id = 26L;
        SchemaVersionInfo schemaVersionInfo = new SchemaVersionInfo(id, "svinfos", 3, "text", TIMESTAMP, "description");
        schemaVersionInfo.setMergeInfo(new MergeInfo("notMaster", id));
        Collection<SchemaVersionInfo> schemaVersionInfos = collectionCreator(schemaVersionInfo);
        SchemaBranch schemaBranch = new SchemaBranch(39L, "notMASTER", "metadataName", "desc", TIMESTAMP);
        Set<SchemaBranch> schemaBranches = setCreator(schemaBranch);
        AggregatedSchemaBranch aggregatedSchemaBranch = new AggregatedSchemaBranch(schemaBranch, id, schemaVersionInfos);
        Collection<AggregatedSchemaBranch> aggregatedSchemaBranches = collectionCreator(aggregatedSchemaBranch);
        when(schemaVersionLifecycleManager.getAllVersions(schemaName)).thenReturn(schemaVersionInfos);
        when(schemaVersionLifecycleManager.getAllVersions("notMASTER", schemaName)).thenReturn(schemaVersionInfos);
        when(schemaVersionLifecycleManager.getSchemaBranches(id)).thenReturn(schemaBranches);
        when(schemaVersionLifecycleManager.getRootVersion(schemaBranch)).thenReturn(schemaVersionInfo);
        when(schemaVersionLifecycleManager
                .createSchemaVersionLifeCycleContext(id, SchemaVersionLifecycleStates.INITIATED)).thenThrow(new SchemaNotFoundException());
        
        //when
        Collection<AggregatedSchemaBranch> actual = underTest.getAggregatedSchemaBranch(schemaName);
        
        //then
        assertEquals(aggregatedSchemaBranches, actual);
        verify(schemaVersionLifecycleManager).getAllVersions("sunshine");
        verify(schemaVersionLifecycleManager).getAllVersions("notMASTER", "sunshine");
        verify(schemaVersionLifecycleManager).getSchemaBranches(26L);
        verify(schemaVersionLifecycleManager).getRootVersion(schemaBranch);
        verify(schemaVersionLifecycleManager).createSchemaVersionLifeCycleContext(26L, SchemaVersionLifecycleStates.INITIATED);
    }

    @Test(expected = RuntimeException.class)
    public void getAggregatedSchemaBranchThrowsExceptionTest() throws Exception {
        //given
        String schemaName = "sunshine";
        Long id = 26L;
        SchemaVersionInfo schemaVersionInfo = new SchemaVersionInfo(id, "svinfos", 3, "text", TIMESTAMP, "description");
        schemaVersionInfo.setMergeInfo(new MergeInfo("notMaster", id));
        Collection<SchemaVersionInfo> schemaVersionInfos = collectionCreator(schemaVersionInfo);
        SchemaBranch schemaBranch = new SchemaBranch(39L, "notMASTER", "metadataName", "desc", TIMESTAMP);
        Set<SchemaBranch> schemaBranches = setCreator(schemaBranch);
        SchemaVersionLifecycleContext context = new SchemaVersionLifecycleContext(id, 1, Mockito.mock(SchemaVersionService.class),
                Mockito.mock(SchemaVersionLifecycleStateMachine.class), Mockito.mock(CustomSchemaStateExecutor.class));
        context.setDetails(new byte[4]);
        AggregatedSchemaBranch aggregatedSchemaBranch = new AggregatedSchemaBranch(schemaBranch, id, schemaVersionInfos);
        Collection<AggregatedSchemaBranch> aggregatedSchemaBranches = collectionCreator(aggregatedSchemaBranch);
        when(schemaVersionLifecycleManager.getAllVersions(schemaName)).thenReturn(schemaVersionInfos);
        when(schemaVersionLifecycleManager.getAllVersions("notMASTER", schemaName)).thenReturn(schemaVersionInfos);
        when(schemaVersionLifecycleManager.getSchemaBranches(id)).thenReturn(schemaBranches);
        when(schemaVersionLifecycleManager.getRootVersion(schemaBranch)).thenReturn(schemaVersionInfo);
        when(schemaVersionLifecycleManager
                .createSchemaVersionLifeCycleContext(id, SchemaVersionLifecycleStates.INITIATED)).thenReturn(context);

        //when
        Collection<AggregatedSchemaBranch> actual = underTest.getAggregatedSchemaBranch(schemaName);

        //then
        assertEquals(aggregatedSchemaBranches, actual);
        verify(schemaVersionLifecycleManager).getAllVersions("sunshine");
        verify(schemaVersionLifecycleManager).getAllVersions("notMASTER", "sunshine");
        verify(schemaVersionLifecycleManager).getSchemaBranches(26L);
        verify(schemaVersionLifecycleManager).getRootVersion(schemaBranch);
        verify(schemaVersionLifecycleManager).createSchemaVersionLifeCycleContext(26L, SchemaVersionLifecycleStates.INITIATED);
    }
    
    
    @Test
    public void getSchemaBranchIdTest() {
        //given
        SchemaBranch schemaBranch = new SchemaBranch("branch", "bread");
        Long schemaBranchId = 43L;
        when(atlasPlugin.getSchemaBranchById(schemaBranchId)).thenReturn(Optional.of(schemaBranch));
        
        //when
        SchemaBranch actual = underTest.getSchemaBranch(schemaBranchId);
        
        //then
        assertEquals(schemaBranch, actual);
        verify(atlasPlugin).getSchemaBranchById(43L);
    }
    
    @Test(expected = SchemaBranchNotFoundException.class)
    public void getSchemaBranchIdThrowsSchemaBranchNotFoundExceptionTest() {
        //given
        Long schemaBranchId = 43L;
        when(atlasPlugin.getSchemaBranchById(schemaBranchId)).thenReturn(Optional.empty());
        
        //when
        underTest.getSchemaBranch(schemaBranchId);
        
        //then
        verify(atlasPlugin).getSchemaBranchById(43L);
    }

    @Test
    public void searchSchemasTest() {
        //given
        SchemaMetadata schemaMetadata = schemaMetadataCreator("hour", Optional.of("minute"));
        SchemaMetadataInfo schemaMetadataInfo = new SchemaMetadataInfo(schemaMetadata, 65L, TIMESTAMP);
        Collection<SchemaMetadataInfo> schemaMetadataInfos = collectionCreator(schemaMetadataInfo);
        MultivaluedMap<String, String> queryMap = new MultivaluedHashMap<>();
        queryMap.add("name", "hour");
        queryMap.add("description", "minute");
        when(atlasPlugin.search(Optional.of("hour"), Optional.of("minute"), Optional.of("timestamp,a"))).thenReturn(schemaMetadataInfos);
        
        //when
        Collection<SchemaMetadataInfo> actual = underTest.searchSchemas(queryMap, Optional.of("timestamp,a"));
        
        //then
        assertEquals(schemaMetadataInfos, actual);
        verify(atlasPlugin).search(Optional.of("hour"), Optional.of("minute"), Optional.of("timestamp,a"));
    }
    
    @Test
    public void getLatestEnabledSchemaVersionInfoTest() throws Exception {
        //given
        String schemaBranchName = "MASTER";
        String schemaName = "green";
        SchemaVersionInfo schemaVersionInfo = new SchemaVersionInfo(77L, "green", 6, "text", TIMESTAMP, "desc");
        when(schemaVersionLifecycleManager.getLatestEnabledSchemaVersionInfo(schemaBranchName, schemaName)).thenReturn(schemaVersionInfo);
        
        //when
        SchemaVersionInfo actual = underTest.getLatestEnabledSchemaVersionInfo(schemaBranchName, schemaName);
        
        //then
        assertEquals(schemaVersionInfo, actual);
        verify(schemaVersionLifecycleManager).getLatestEnabledSchemaVersionInfo("MASTER", "green");
    }
    
    @Test
    public void getSchemaVersionInfoTest() throws Exception {
        //given
        String schemaName = "Brad";
        String schemaText = "Pitt";
        boolean disableCanonicalCheck = false;
        SchemaVersionInfo schemaVersionInfo = new SchemaVersionInfo(645L, "rollerblade", 1, "wuhu", TIMESTAMP, "description");
        when(schemaVersionLifecycleManager.getSchemaVersionInfo(schemaName, schemaText, disableCanonicalCheck)).thenReturn(schemaVersionInfo);
        
        //when
        SchemaVersionInfo actual = underTest.getSchemaVersionInfo(schemaName, schemaText, disableCanonicalCheck);
        
        //then
        assertEquals(schemaVersionInfo, actual);
        verify(schemaVersionLifecycleManager).getSchemaVersionInfo("Brad", "Pitt", false);
    }
    
    @Test
    public void findSchemaVersionByFingerprintTest() throws Exception {
        //given
        SchemaVersionInfo schemaVersionInfo = new SchemaVersionInfo(3L, "cake", 4, "chocolate", TIMESTAMP, "redvelvet");
        String fingerprint = "fingerprint";
        when(schemaVersionLifecycleManager.findSchemaVersionInfoByFingerprint(fingerprint)).thenReturn(schemaVersionInfo);
        
        //when
        SchemaVersionInfo actual = underTest.findSchemaVersionByFingerprint(fingerprint);
        
        //then
        assertEquals(schemaVersionInfo, actual);
        verify(schemaVersionLifecycleManager).findSchemaVersionInfoByFingerprint("fingerprint");
    }
    
    @Test
    public void mergeSchemaVersionTest() throws Exception {
        //given
        SchemaVersionMergeStrategy schemaVersionMergeStrategy = SchemaVersionMergeStrategy.OPTIMISTIC;
        SchemaIdVersion schemaIdVersion = new SchemaIdVersion(80L, 4);
        SchemaVersionMergeResult schemaVersionMergeResult = new SchemaVersionMergeResult(schemaIdVersion, "merge message");
        boolean disableCanonicalCheck = false;
        Long schemaVersionId = 81L;
        when(schemaVersionLifecycleManager.mergeSchemaVersion(schemaVersionId, schemaVersionMergeStrategy, disableCanonicalCheck))
                .thenReturn(schemaVersionMergeResult);
        
        //when
        SchemaVersionMergeResult actual = underTest.mergeSchemaVersion(schemaVersionId, schemaVersionMergeStrategy, disableCanonicalCheck);
        
        //then
        assertEquals(schemaVersionMergeResult, actual);
        verify(schemaVersionLifecycleManager).mergeSchemaVersion(schemaVersionId, schemaVersionMergeStrategy, disableCanonicalCheck);
                
    }
    
    @Test
    public void getSchemaBranchesForVersionTest() {
        //given
        SchemaBranch schemaBranch = new SchemaBranch("not MASTER", "metaData");
        Set<SchemaBranch> schemaBranchSet = setCreator(schemaBranch);
        Long versionId = 3L;
        when(schemaVersionLifecycleManager.getSchemaBranches(versionId)).thenReturn(schemaBranchSet);
        
        //when
        Collection<SchemaBranch> actual = underTest.getSchemaBranchesForVersion(versionId);
        
        //then
        assertEquals(schemaBranchSet, actual);
        verify(schemaVersionLifecycleManager).getSchemaBranches(3L);
    }
    
    
    @Test
    public void checkCompatibilitySchemaTextTest() throws Exception {
        //given
        String schemaName = "guacamole";
        String toSchemaText = "avocado";
        CompatibilityResult compatibilityResult = CompatibilityResult.SUCCESS;
        when(schemaVersionLifecycleManager.checkCompatibility(SchemaBranch.MASTER_BRANCH, schemaName, toSchemaText)).thenReturn(compatibilityResult);
        
        //when
        CompatibilityResult actual = underTest.checkCompatibility(schemaName, toSchemaText);
        
        //then
        assertEquals(compatibilityResult, actual);
        verify(schemaVersionLifecycleManager).checkCompatibility(SchemaBranch.MASTER_BRANCH, "guacamole", "avocado");
    }
    
    @Test
    public void checkCompatibilityBranchSchemaTextTest() throws Exception {
        //given
        String schemaBranchName = "MASTER";
        String schemaName = "almond";
        String toSchemaText = "marzipan";
        CompatibilityResult compatibilityResult = CompatibilityResult.SUCCESS;
        when(schemaVersionLifecycleManager.checkCompatibility(schemaBranchName, schemaName, toSchemaText)).thenReturn(compatibilityResult);
        
        //when
        CompatibilityResult actual = underTest.checkCompatibility(schemaBranchName, schemaName, toSchemaText);
        
        //then
        assertEquals(compatibilityResult, actual);
        verify(schemaVersionLifecycleManager).checkCompatibility("MASTER", "almond", "marzipan");
    }
    
    @Test
    public void getAllVersionsBranchSchemaStateIdsTest() throws Exception {
        //given
        String schemaBranchName = "MASTER";
        String schemaName = "coffee";
        List<Byte> stateIds = new ArrayList<>();
        stateIds.add(new Byte("6"));
        SchemaVersionInfo schemaVersionInfo = new SchemaVersionInfo(76L, "coffee", 5, "light roasted coffee forever", TIMESTAMP, "desc");
        Collection<SchemaVersionInfo> schemaVersionInfos = collectionCreator(schemaVersionInfo);
        when(schemaVersionLifecycleManager.getAllVersions(schemaBranchName, schemaName, stateIds)).thenReturn(schemaVersionInfos);
        
        //when
        Collection<SchemaVersionInfo> actual = underTest.getAllVersions(schemaBranchName, schemaName, stateIds);
        
        //then
        assertEquals(schemaVersionInfos, actual);
        verify(schemaVersionLifecycleManager).getAllVersions("MASTER", "coffee", stateIds);
    }

    @Test
    public void getAllVersionsBranchSchemaTest() throws Exception {
        //given
        String schemaBranchName = "MASTER";
        String schemaName = "coffee";
        List<Byte> stateIds = new ArrayList<>();
        SchemaVersionInfo schemaVersionInfo = new SchemaVersionInfo(77L, "cafe", 9, "light roasted coffee forever", TIMESTAMP, "desc");
        Collection<SchemaVersionInfo> schemaVersionInfos = collectionCreator(schemaVersionInfo);
        when(schemaVersionLifecycleManager.getAllVersions(schemaBranchName, schemaName)).thenReturn(schemaVersionInfos);
        
        //when
        Collection<SchemaVersionInfo> actual = underTest.getAllVersions(schemaBranchName, schemaName, stateIds);
        
        //then
        assertEquals(schemaVersionInfos, actual);
        verify(schemaVersionLifecycleManager).getAllVersions("MASTER", "coffee");
    }
    
    @Test
    public void getSerDesTest() {
        //given
        Long serDesId = 843L;
        SerDesPair serDesPair = new SerDesPair();
        SerDesInfo serDesInfo = new SerDesInfo(843L, TIMESTAMP, serDesPair);
        when(atlasPlugin.getSerdesById(serDesId)).thenReturn(Optional.of(serDesInfo));
        
        //when
        SerDesInfo actual = underTest.getSerDes(serDesId);
        
        //then
        assertEquals(serDesInfo, actual);
        verify(atlasPlugin).getSerdesById(843L);
    }
    
    @Test
    public void uploadFileTest() throws Exception {
        //given
        InputStream inputStream = new InputStream() {
            @Override
            public int read() throws IOException {
                return 4;
            }
        };
        
        //when
        String filename = underTest.uploadFile(inputStream);
        
        //then
        verify(fileStorage).upload(inputStream, filename);
    }
    
    @Test
    public void downloadFileTest() throws Exception {
        //given
        InputStream inputStream = new InputStream() {
            @Override
            public int read() throws IOException {
                return 12;
            }
        };
        String fileId = "owl";
        when(fileStorage.download(fileId)).thenReturn(inputStream);
        
        //when
        InputStream actual = underTest.downloadFile(fileId);
        
        //then
        assertEquals(inputStream, actual);
        verify(fileStorage).download("owl");
    }
    
    @Test
    public void addSerDesTest() {
        //given
        Long id = 64L;
        SerDesPair serializerInfo = new SerDesPair("name", "desc", "64", "ser", "des");
        when(atlasPlugin.addSerdes(serializerInfo)).thenReturn(id);
        
        //when
        Long actual = underTest.addSerDes(serializerInfo);
        
        //then
        assertEquals(id, actual);
        verify(atlasPlugin).addSerdes(serializerInfo);
    }
    
    @Test
    public void mapSchemaWithSerDesTest() throws Exception {
        //given
        String schemaName = "cucumber";
        Long serDesId = 10L;
        
        //when
        underTest.mapSchemaWithSerDes(schemaName, serDesId);
        
        //then
        verify(atlasPlugin).mapSchemaWithSerdes(schemaName, serDesId);
    }

    @Test(expected = AtlasUncheckedException.class)
    public void mapSchemaWithSerDesThrowsExceptionTest() throws Exception {
        //given
        String schemaName = "cucumber";
        Long serDesId = 10L;
        doThrow(new SchemaNotFoundException()).when(atlasPlugin).mapSchemaWithSerdes(schemaName, serDesId);
        
        //when
        underTest.mapSchemaWithSerDes(schemaName, serDesId);
        
        //then
        verify(atlasPlugin).mapSchemaWithSerdes(schemaName, serDesId);
    }
    
    @Test
    public void getSerDesSchemaNameTest() throws Exception {
        //given
        String schemaName = "thyme";
        SerDesInfo serDesInfo = new SerDesInfo(40L, TIMESTAMP, new SerDesPair());
        Collection<SerDesInfo> serDesInfos = collectionCreator(serDesInfo);
        when(atlasPlugin.getAllSchemaSerdes(schemaName)).thenReturn(serDesInfos);
        
        //when
        Collection<SerDesInfo> actual = underTest.getSerDes(schemaName);
        
        //then
        assertEquals(serDesInfos, actual);
        verify(atlasPlugin).getAllSchemaSerdes("thyme");
    }

    @Test(expected = AtlasUncheckedException.class)
    public void getSerDesSchemaNameThrowsExceptionTest() throws Exception {
        //given
        String schemaName = "thyme";
        when(atlasPlugin.getAllSchemaSerdes(schemaName)).thenThrow(new SchemaNotFoundException());
        
        //when
        underTest.getSerDes(schemaName);
        
        //then
        verify(atlasPlugin).getAllSchemaSerdes("thyme");
    }
    
    @Test
    public void enableSchemaVersionTest() throws Exception {
        //given
        Long schemaVersionId = 20L;
        
        //when
        underTest.enableSchemaVersion(schemaVersionId);
        
        //then
        verify(schemaVersionLifecycleManager).enableSchemaVersion(schemaVersionId);
    }

    @Test
    public void deleteSchemaVersionTest() throws Exception {
        //given
        Long schemaVersionId = 21L;
        
        //when
        underTest.deleteSchemaVersion(schemaVersionId);
        
        //then
        verify(schemaVersionLifecycleManager).deleteSchemaVersion(schemaVersionId);
    }

    @Test
    public void archiveSchemaVersionTest() throws Exception {
        //given
        Long schemaVersionId = 22L;
        
        //when
        underTest.archiveSchemaVersion(schemaVersionId);
        
        //then
        verify(schemaVersionLifecycleManager).archiveSchemaVersion(schemaVersionId);
    }

    @Test
    public void disableSchemaVersionTest() throws Exception {
        //given
        Long schemaVersionId = 23L;
        
        //when
        underTest.disableSchemaVersion(schemaVersionId);
        
        //then
        verify(schemaVersionLifecycleManager).disableSchemaVersion(schemaVersionId);
    }

    @Test
    public void startSchemaVersionReviewTest() throws Exception {
        //given
        Long schemaVersionId = 24L;
        
        //when
        underTest.startSchemaVersionReview(schemaVersionId);
        
        //then
        verify(schemaVersionLifecycleManager).startSchemaVersionReview(schemaVersionId);
    }
    
    @Test
    public void mergeSchemaVersionIdTest() throws Exception {
        //given
        Long schemaVersionId = 135L;
        boolean disableCanonicalCheck = true;
        SchemaIdVersion schemaIdVersion = new SchemaIdVersion(4L, 5);
        SchemaVersionMergeResult schemaVersionMergeResult = new SchemaVersionMergeResult(schemaIdVersion, "merge");
        when(schemaVersionLifecycleManager.mergeSchemaVersion(schemaVersionId, SchemaVersionMergeStrategy.valueOf(DEFAULT_SCHEMA_VERSION_MERGE_STRATEGY), disableCanonicalCheck)).thenReturn(schemaVersionMergeResult);
        
        //when
        SchemaVersionMergeResult actual = underTest.mergeSchemaVersion(schemaVersionId, disableCanonicalCheck);
        
        //then
        assertEquals(schemaVersionMergeResult, actual);
        verify(schemaVersionLifecycleManager).mergeSchemaVersion(135L, SchemaVersionMergeStrategy.valueOf(DEFAULT_SCHEMA_VERSION_MERGE_STRATEGY), true);
    }
    
    @Test
    public void transitionStateTest() throws Exception {
        //given
        Long schemaVersionId = 444L;
        Byte targetStateId = new Byte("4");
        byte[] transitionDetails = new byte[0];
        
        //when
        underTest.transitionState(schemaVersionId, targetStateId, transitionDetails);
        
        //then
        verify(schemaVersionLifecycleManager).executeState(schemaVersionId, targetStateId, transitionDetails);
    }
    
    @Test
    public void getSchemaVersionLifecycleStateMachineInfoTest() {
        //given
        SchemaVersionLifecycleStateMachine schemaVersionLifecycleStateMachine = new SchemaVersionLifecycleStateMachine.Builder().build();
        when(schemaVersionLifecycleManager.getSchemaVersionLifecycleStateMachine()).thenReturn(schemaVersionLifecycleStateMachine);
        
        //when
        SchemaVersionLifecycleStateMachineInfo actual = underTest.getSchemaVersionLifecycleStateMachineInfo();
        
        //then
        assertEquals(schemaVersionLifecycleStateMachine.toConfig(), actual);
        verify(schemaVersionLifecycleManager).getSchemaVersionLifecycleStateMachine();
    }
    
    @Test
    public void createSchemaBranchTest() throws Exception {
        //given
        Long schemaVersionId = 29L;
        SchemaBranch schemaBranch = new SchemaBranch("padawan", "yoda");
        SchemaVersionInfo schemaVersionInfo = new SchemaVersionInfo(29L, "obivan", 4, "lightsaber", TIMESTAMP, "desc");
        SchemaIdVersion schemaIdVersion = new SchemaIdVersion(schemaVersionId);
        when(schemaVersionLifecycleManager.getSchemaVersionInfo(schemaIdVersion)).thenReturn(schemaVersionInfo);
        when(atlasPlugin.createBranch(schemaVersionInfo, "padawan")).thenReturn(schemaBranch);
        
        //when
        SchemaBranch actual = underTest.createSchemaBranch(schemaVersionId, schemaBranch);
        
        //then
        assertEquals(schemaBranch, actual);
        verify(schemaVersionLifecycleManager).getSchemaVersionInfo(schemaIdVersion);
        verify(atlasPlugin).createBranch(schemaVersionInfo, "padawan");
    }

    @Test(expected = SchemaBranchAlreadyExistsException.class)
    public void createSchemaBranchThrowBranchExistsExceptionTest() throws Exception {
        //given
        Long schemaVersionId = 29L;
        SchemaBranch schemaBranch = new SchemaBranch("padawan", "yoda");
        SchemaVersionInfo schemaVersionInfo = new SchemaVersionInfo(29L, "obivan", 4, "lightsaber", TIMESTAMP, "desc");
        SchemaIdVersion schemaIdVersion = new SchemaIdVersion(schemaVersionId);
        when(schemaVersionLifecycleManager.getSchemaVersionInfo(schemaIdVersion)).thenReturn(schemaVersionInfo);
        when(atlasPlugin.getSchemaBranch("obivan", "padawan")).thenReturn(Optional.of(schemaBranch));
        
        //when
        underTest.createSchemaBranch(schemaVersionId, schemaBranch);
        
        //then
        verify(schemaVersionLifecycleManager).getSchemaVersionInfo(schemaIdVersion);
        verify(atlasPlugin).getSchemaBranch("obivan", "padawan");
    }

    @Test(expected = RuntimeException.class)
    public void createSchemaBranchThrowRuntimeExceptionTest() throws Exception {
        //given
        Long schemaVersionId = 29L;
        SchemaBranch schemaBranch = new SchemaBranch("padawan", "yoda");
        SchemaVersionInfo schemaVersionInfo = new SchemaVersionInfo(29L, "obivan", 4, "lightsaber", TIMESTAMP, "desc");
        SchemaIdVersion schemaIdVersion = new SchemaIdVersion(schemaVersionId);
        when(schemaVersionLifecycleManager.getSchemaVersionInfo(schemaIdVersion)).thenReturn(schemaVersionInfo);
        when(atlasPlugin.createBranch(schemaVersionInfo, "padawan")).thenThrow(SchemaNotFoundException.class);
        
        //when
        underTest.createSchemaBranch(schemaVersionId, schemaBranch);
        
        //then
        verify(schemaVersionLifecycleManager).getSchemaVersionInfo(schemaIdVersion);
        verify(atlasPlugin).createBranch(schemaVersionInfo, "padawan");
    }
    
    @Test
    public void getSchemaBranchesTest() throws Exception {
        //given
        String schemaName = "binks";
        SchemaBranch schemaBranch = new SchemaBranch("jar-jar", "binks");
        SchemaVersionInfo schemaVersionInfo = new SchemaVersionInfo(30L, "naboo", 2, "bigger fish", TIMESTAMP, "desc");
        Collection<SchemaVersionInfo> schemaVersionInfos = collectionCreator(schemaVersionInfo);
        Set<SchemaBranch> schemaBranches = setCreator(schemaBranch);
        when(schemaVersionLifecycleManager.getAllVersions(schemaName)).thenReturn(schemaVersionInfos);
        when(schemaVersionLifecycleManager.getSchemaBranches(schemaVersionInfo.getId())).thenReturn(schemaBranches);
        
        //when
        Collection<SchemaBranch> actual = underTest.getSchemaBranches(schemaName);
        
        //then
        assertEquals(schemaBranches, actual);
        verify(schemaVersionLifecycleManager).getAllVersions("binks");
        verify(schemaVersionLifecycleManager).getSchemaBranches(30L);
    }

    @Test(expected = RuntimeException.class)
    public void getSchemaBranchesThrowsExceptionTest() throws Exception {
        //given
        String schemaName = "binks";
        SchemaVersionInfo schemaVersionInfo = new SchemaVersionInfo(30L, "naboo", 2, "bigger fish", TIMESTAMP, "desc");
        Collection<SchemaVersionInfo> schemaVersionInfos = collectionCreator(schemaVersionInfo);
        when(schemaVersionLifecycleManager.getAllVersions(schemaName)).thenReturn(schemaVersionInfos);
        when(schemaVersionLifecycleManager.getSchemaBranches(schemaVersionInfo.getId())).thenThrow(SchemaBranchNotFoundException.class);
        
        //when
        underTest.getSchemaBranches(schemaName);
        
        //then
        verify(schemaVersionLifecycleManager).getAllVersions("binks");
        verify(schemaVersionLifecycleManager).getSchemaBranches(30L);
    }
    
    @Test
    public void fetchSchemaVersionInfoTest() throws Exception {
        //given
        Long id = 15L;
        SchemaVersionInfo schemaVersionInfo = new SchemaVersionInfo(6L, "pink", 3, "purple", TIMESTAMP, "mango");
        when(schemaVersionLifecycleManager.fetchSchemaVersionInfo(id)).thenReturn(schemaVersionInfo);
        
        //when
        SchemaVersionInfo actual = underTest.fetchSchemaVersionInfo(id);
        
        //then
        assertEquals(schemaVersionInfo, actual);
        verify(schemaVersionLifecycleManager).fetchSchemaVersionInfo(15L);
    }
    
    private SchemaMetadata schemaMetadataCreator(String name, Optional<String> desc) {
        return new SchemaMetadata.Builder(name)
                .type("type")
                .schemaGroup("schemaGroup")
                .description(desc.orElse("description"))
                .compatibility(SchemaCompatibility.BACKWARD)
                .validationLevel(SchemaValidationLevel.ALL)
                .evolve(false)
                .build();
    }
    
    private Map<String, String> propCreator(String name, String desc) {
        Map<String, String> props = new HashMap<>();
        props.put("name", name);
        props.put("description", desc);
        props.put("_orderByFields", "timestamp,a");
        return props;
    }
    
    private <T> Collection<T> collectionCreator(T element) {
        Collection<T> result = new ArrayList<>();
        result.add(element);
        return result;
    }

    private <T> Set<T> setCreator(T element) {
        Set<T> result = new HashSet<>();
        result.add(element);
        return result;
    }
}
