/*
 * Copyright  (c) 2011-2017, Hortonworks Inc.  All rights reserved.
 *
 * Except as expressly permitted in a written agreement between your
 * company and Hortonworks, Inc, any use, reproduction, modification,
 * redistribution, sharing, lending or other exploitation of all or
 * any part of the contents of this file is strictly prohibited.
 */
package org.hw.qe.schemaregistry.api;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.hortonworks.registries.schemaregistry.CompatibilityResult;
import com.hortonworks.registries.schemaregistry.SchemaBranch;
import com.hortonworks.registries.schemaregistry.SchemaFieldQuery;
import com.hortonworks.registries.schemaregistry.SchemaIdVersion;
import com.hortonworks.registries.schemaregistry.SchemaMetadata;
import com.hortonworks.registries.schemaregistry.SchemaMetadataInfo;
import com.hortonworks.registries.schemaregistry.SchemaProviderInfo;
import com.hortonworks.registries.schemaregistry.SchemaVersion;
import com.hortonworks.registries.schemaregistry.SchemaVersionInfo;
import com.hortonworks.registries.schemaregistry.SchemaVersionKey;
import com.hortonworks.registries.schemaregistry.SerDesInfo;
import com.hortonworks.registries.schemaregistry.SerDesPair;
import com.hortonworks.registries.schemaregistry.client.ISchemaRegistryClient;
import com.hortonworks.registries.schemaregistry.errors.IncompatibleSchemaException;
import com.hortonworks.registries.schemaregistry.errors.InvalidSchemaBranchDeletionException;
import com.hortonworks.registries.schemaregistry.errors.InvalidSchemaException;
import com.hortonworks.registries.schemaregistry.errors.SchemaBranchAlreadyExistsException;
import com.hortonworks.registries.schemaregistry.errors.SchemaBranchNotFoundException;
import com.hortonworks.registries.schemaregistry.errors.SchemaNotFoundException;
import com.hortonworks.registries.schemaregistry.serde.SerDesException;
import com.hortonworks.registries.schemaregistry.state.SchemaLifecycleException;
import com.hortonworks.registries.schemaregistry.state.SchemaVersionLifecycleStateMachineInfo;
import lombok.Getter;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.hw.qe.exceptions.AuthenticationHandlerException;
import org.hw.qe.requests.BaseResponse;
import org.hw.qe.requests.Get;
import org.hw.qe.requests.Post;
import org.hw.qe.requests.RequestUrl;
import org.hw.qe.schemaregistry.api.json.SchemaProviders;
import org.hw.qe.schemaregistry.api.json.Schemas;
import org.hw.qe.schemaregistry.api.utils.WrongResponseCodeException;
import org.hw.qe.util.PojoUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * PrimitiveTypeSchema Registry client to make rest calls to schema registry.
 */
@Getter
final class SchemaRegistryRestClient implements ISchemaRegistryClient {
  private final RequestUrl requestUrl;
  private static final Logger LOG = LoggerFactory.getLogger(SchemaRegistryRestClient.class);

  /**
   * Constructor for schema registry.
   * @param requestURL Request url to make rest calls
   */
  private SchemaRegistryRestClient(RequestUrl requestURL) {
    this.requestUrl = requestURL;
  }

  /**
   * Get all schemaVersions registered with the registry.
   * @return Schemas object with all schemaVersions registered
   */
  public Schemas getSchemas() {
    Schemas schemas = null;
    try {
      Get get = new Get(requestUrl, RegistryURL.SCHEMAS.url);
      schemas = get.execute().getPojo(Schemas.class);
    } catch (URISyntaxException | AuthenticationHandlerException | IOException e) {
      logErrorMessage("Get", RegistryURL.SCHEMAS, e);
    }
    return schemas;
  }

  /**
   * Get a schema versions of a schema with schemaName.
   * @param schemaName Name of the schema to be got
   * @param schemaVersion Version of the schema to be got
   * @return List of schema versions
   */
  private SchemaVersionInfo getSchemaVersion(String schemaName, Integer schemaVersion) {
    SchemaVersionInfo schemaversion = null;
    try {
      Get get = new Get(requestUrl, String.format(
          RegistryURL.INDIVIDUAL_VERSION.url, schemaName, schemaVersion));
      schemaversion = PojoUtils.getPojo(get.execute().getContent(), SchemaVersionInfo.class);
    } catch (URISyntaxException | AuthenticationHandlerException | IOException e) {
      LOG.error("Post failed while {} : {}", RegistryURL.INDIVIDUAL_VERSION.purpose,
          ExceptionUtils.getFullStackTrace(e));
    }
    return schemaversion;
  }

  /**
   * Method to get supported schema Providers.
   * @return Collection of schema providers
   */
  @Override
  public Collection<SchemaProviderInfo> getSupportedSchemaProviders() {
    SchemaProviders schemaProviders = null;
    try {
      BaseResponse response = new Get(requestUrl, RegistryURL.SCHEMA_PROVIDERS.getUrl()).execute();
      schemaProviders = PojoUtils.getPojo(response.getContent(), SchemaProviders.class);
    } catch (Exception e) {
      logErrorMessage("Get", RegistryURL.SCHEMA_PROVIDERS, e);
    }
    assert schemaProviders != null;
    return schemaProviders.getEntities();
  }

  /**
   * Method to register schema.
   * @param schemaMetadata SchemaMetadata
   * @return Schema ID
   */
  @Override
  public Long registerSchemaMetadata(SchemaMetadata schemaMetadata) {
    Long schemaID = null;
    try {
      Map<String, String> headers = new HashMap<>();
      headers.put("_throwErrorIfExists", String.valueOf(true));
      Post post = new Post(requestUrl, RegistryURL.SCHEMAS.getUrl(), headers, schemaMetadata);
      BaseResponse response = post.execute();
      if (response.getStatusCode() != HttpURLConnection.HTTP_CREATED) {
        throw new WrongResponseCodeException(
            String.format("Registering schema metadata %s", schemaMetadata),
            response);
      }
      schemaID = Long.parseLong(response.getContent());
    } catch (URISyntaxException | AuthenticationHandlerException | IOException e) {
      logErrorMessage("Post", RegistryURL.SCHEMAS, e);
    }
    return schemaID;
  }

  @Override
  public Long addSchemaMetadata(SchemaMetadata schemaMetadata) {
    return registerSchemaMetadata(schemaMetadata);
  }

  @Override
  public SchemaMetadataInfo updateSchemaMetadata(String schemaName, SchemaMetadata schemaMetadata) {
    // TODO
    throw new UnsupportedOperationException("Update schema metadata not supported yet.");
  }

  @Override
  public SchemaMetadataInfo getSchemaMetadataInfo(String schemaName) {
    SchemaMetadataInfo schema = null;
    try {
      Get get = new Get(requestUrl, String.format(RegistryURL.INDIVIDUAL_SCHEMA.url, schemaName));
      schema = PojoUtils.getPojo(get.execute().getContent(), SchemaMetadataInfo.class);
    } catch (URISyntaxException | AuthenticationHandlerException | IOException e) {
      LOG.error("Post failed while {} : {}", RegistryURL.INDIVIDUAL_SCHEMA.purpose,
          ExceptionUtils.getFullStackTrace(e));
    }
    return schema;
  }

  @Override
  public SchemaMetadataInfo getSchemaMetadataInfo(Long schemaID) {
    SchemaMetadataInfo schema = null;
    try {
      Get get = new Get(requestUrl, String.format(RegistryURL.SCHEMAS_BY_ID.url, schemaID));
      schema = PojoUtils.getPojo(get.execute().getContent(), SchemaMetadataInfo.class);
    } catch (URISyntaxException | AuthenticationHandlerException | IOException e) {
      LOG.error("Post failed while {} : {}", RegistryURL.INDIVIDUAL_SCHEMA.purpose,
          ExceptionUtils.getFullStackTrace(e));
    }
    return schema;
  }

  @Override
  public void deleteSchema(String schemaName) throws SchemaNotFoundException {
    // TODO
    throw new UnsupportedOperationException("Delete schema not supported yet.");
  }

  /**
   * Method to create a new version of a schema.
   * @param schemaMetadata schemaMetadata to be updated
   * @param schemaVersion Schema version used for updating
   * @return Schema id version of the new schema
   */
  @Override
  public SchemaIdVersion addSchemaVersion(SchemaMetadata schemaMetadata, SchemaVersion schemaVersion)
      throws InvalidSchemaException, IncompatibleSchemaException, SchemaNotFoundException {
    String schemaText = schemaVersion.getSchemaText();
    String schemaName = schemaMetadata.getName();
    SchemaMetadataInfo schemaMetadataInfo = getSchemaMetadataInfo(schemaName);
    if (schemaMetadataInfo == null) {
      throw new SchemaNotFoundException("Schema with name " + schemaName + " not found");
    }
    try {
      Post compatibilityPost = new Post(requestUrl,
          String.format(RegistryURL.COMPATIBILITY_CHECK.getUrl(), schemaName), schemaText);
      BaseResponse baseResponse = compatibilityPost.execute();
      CompatibilityResult compatibilityResponse = PojoUtils.getPojo(
          baseResponse.getContent(), CompatibilityResult.class);
      if (!compatibilityResponse.isCompatible()) {
        LOG.error("Compatibility test failed: " + compatibilityResponse);
      }
    } catch (URISyntaxException | AuthenticationHandlerException | IOException e) {
      LOG.error("Post failed " + ExceptionUtils.getFullStackTrace(e));
    }
    return addSchemaVersion(schemaName, schemaVersion);
  }

  @Override
  public SchemaIdVersion addSchemaVersion(SchemaMetadata schemaMetadata, SchemaVersion schemaVersion, boolean disableCanonicalCheck) throws InvalidSchemaException, IncompatibleSchemaException, SchemaNotFoundException, SchemaBranchNotFoundException {
    throw new UnsupportedOperationException("Add schema version not supported yet.");
  }

  @Override
  public SchemaIdVersion addSchemaVersion(String schemaBranchName, SchemaMetadata schemaMetadata, SchemaVersion schemaVersion)
      throws InvalidSchemaException, IncompatibleSchemaException, SchemaNotFoundException,
      SchemaBranchNotFoundException {
    throw new UnsupportedOperationException("Add schema version not supported yet.");
  }

  @Override
  public SchemaIdVersion addSchemaVersion(String schemaBranchName, SchemaMetadata schemaMetadata, SchemaVersion schemaVersion, boolean disableCanonicalCheck) throws InvalidSchemaException, IncompatibleSchemaException, SchemaNotFoundException, SchemaBranchNotFoundException {
    throw new UnsupportedOperationException("Add schema version not supported yet.");
  }

  @Override
  public SchemaIdVersion uploadSchemaVersion(String schemaName, String description, InputStream inputStream)
      throws InvalidSchemaException, IncompatibleSchemaException, SchemaNotFoundException {
    // TODO
    throw new UnsupportedOperationException("Upload schema version not supported yet.");
  }

  @Override
  public SchemaIdVersion uploadSchemaVersion(String s, String s1, String s2, InputStream inputStream)
      throws InvalidSchemaException, IncompatibleSchemaException, SchemaNotFoundException,
      SchemaBranchNotFoundException {
    throw new UnsupportedOperationException("Upload schema version not supported yet.");
  }

  @Override
  public SchemaIdVersion addSchemaVersion(String schemaName, SchemaVersion schemaVersion)
      throws InvalidSchemaException, IncompatibleSchemaException, SchemaNotFoundException {
    BaseResponse baseResponse = null;
    try {
      Post post = new Post(requestUrl,
          String.format(RegistryURL.SCHEMA_VERSIONS.getUrl(), schemaName), schemaVersion);
      baseResponse = post.execute();
    } catch (URISyntaxException | AuthenticationHandlerException | IOException e) {
      LOG.error("Post failed " + ExceptionUtils.getFullStackTrace(e));
    }
    SchemaIdVersion schemaIdVersion = null;
    try {
      assert baseResponse != null;
      schemaIdVersion = PojoUtils.getPojo(baseResponse.getContent(), SchemaIdVersion.class);
    } catch (IOException e) {
      LOG.error("Pojo utils failed while converting %s to %s type",
          baseResponse.getContent(),
          SchemaIdVersion.class.getName());
    }
    return schemaIdVersion;
  }

  @Override
  public SchemaIdVersion addSchemaVersion(String schemaName, SchemaVersion schemaVersion, boolean disableCanonicalCheck) throws InvalidSchemaException, IncompatibleSchemaException, SchemaNotFoundException, SchemaBranchNotFoundException {
    throw new UnsupportedOperationException("Add schema version not supported yet.");
  }

  @Override
  public SchemaIdVersion addSchemaVersion(String schemaBranchName, String schemaName, SchemaVersion schemaVersion)
      throws InvalidSchemaException, IncompatibleSchemaException, SchemaNotFoundException,
      SchemaBranchNotFoundException {
    throw new UnsupportedOperationException("Add schema version not supported yet.");
  }

  @Override
  public SchemaIdVersion addSchemaVersion(String schemaBranchName, String schemaName, SchemaVersion schemaVersion, boolean disableCanonicalCheck) throws InvalidSchemaException, IncompatibleSchemaException, SchemaNotFoundException, SchemaBranchNotFoundException {
    throw new UnsupportedOperationException("Add schema version not supported yet.");
  }

  @Override
  public void deleteSchemaVersion(SchemaVersionKey schemaVersionKey) throws SchemaNotFoundException {
    // TODO
    throw new UnsupportedOperationException("Delete schema version not supported yet.");
  }

  @Override
  public Collection<SchemaVersionKey> findSchemasByFields(SchemaFieldQuery schemaFieldQuery) {
    // TODO
    throw new UnsupportedOperationException("Find schemas by fields not supported yet.");
  }

  @Override
  public SchemaVersionInfo getSchemaVersionInfo(SchemaVersionKey schemaVersionKey) throws SchemaNotFoundException {
    return getSchemaVersion(schemaVersionKey.getSchemaName(), schemaVersionKey.getVersion());
  }

  @Override
  public SchemaVersionInfo getSchemaVersionInfo(SchemaIdVersion schemaIdVersion) throws SchemaNotFoundException {
    throw new UnsupportedOperationException("Get schema version info not supported yet.");
  }

  /**
   * Get latest schema version of a schema with schemaName.
   * @param schemaName Name of the schema to be got
   * @return List of schema versions
   */
  @Override
  public SchemaVersionInfo getLatestSchemaVersionInfo(String schemaName) throws SchemaNotFoundException {
    SchemaVersionInfo schemaversions = null;
    try {
      Get get = new Get(requestUrl, String.format(RegistryURL.SCHEMA_LATEST_VERSION.url, schemaName));
      schemaversions = PojoUtils.getPojo(get.execute().getContent(), SchemaVersionInfo.class);
    } catch (URISyntaxException | AuthenticationHandlerException | IOException e) {
      logErrorMessage("Post", RegistryURL.SCHEMA_LATEST_VERSION, e);
    }
    return schemaversions;
  }

  @Override
  public SchemaVersionInfo getLatestSchemaVersionInfo(String s, String s1) throws SchemaNotFoundException,
      SchemaBranchNotFoundException {
    throw new UnsupportedOperationException("Get schema version not supported yet.");
  }

  /** Get a schema versions of a schema with schemaName.
   * @param schemaName Name of the schema to be got
   * @return List of schema versions
   */
  @Override
  public Collection<SchemaVersionInfo> getAllVersions(String schemaName) throws SchemaNotFoundException {
    List<SchemaVersionInfo> schemaversions = null;
    try {
      ObjectMapper mapper = new ObjectMapper();
      RegistryURL schemaVersionURL = RegistryURL.buildURL(RegistryURL.SCHEMA_VERSIONS, schemaName);
      Get get = new Get(requestUrl, schemaVersionURL.url);
      schemaversions = mapper.readValue(get.execute().getContent(), new TypeReference<List<SchemaVersionInfo>>(){});
    } catch (URISyntaxException | AuthenticationHandlerException | IOException e) {
      logErrorMessage("Get", RegistryURL.SCHEMA_VERSIONS, e);
    }
    return schemaversions;
  }

  @Override
  public Collection<SchemaVersionInfo> getAllVersions(String s, String s1) throws SchemaNotFoundException,
      SchemaBranchNotFoundException {
    throw new UnsupportedOperationException("Get all versions not supported yet.");
  }

  @Override
  public CompatibilityResult checkCompatibility(String s, String s1) throws SchemaNotFoundException {
    // TODO
    throw new UnsupportedOperationException("Check compatibility not supported yet.");
  }

  @Override
  public CompatibilityResult checkCompatibility(String s, String s1, String s2)
      throws SchemaNotFoundException, SchemaBranchNotFoundException {
    throw new UnsupportedOperationException("Check compatibility version not supported yet.");
  }

  @Override
  public boolean isCompatibleWithAllVersions(String schemaName, String toSchemaText) throws SchemaNotFoundException {
    RegistryURL compatibilityURL = RegistryURL.buildURL(RegistryURL.COMPATIBILITY_CHECK, schemaName);
    try {
      Post compatibilityPost = new Post(requestUrl, compatibilityURL.getUrl(), toSchemaText);
      BaseResponse baseResponse = compatibilityPost.execute();
      CompatibilityResult compatibilityResponse = PojoUtils.getPojo(
          baseResponse.getContent(), CompatibilityResult.class);
      if (!compatibilityResponse.isCompatible()) {
        LOG.error("Compatibility test failed: " + compatibilityResponse);
      }
    } catch (IOException | AuthenticationHandlerException | URISyntaxException e) {
      logErrorMessage("Post", compatibilityURL, e);
    }
    return false;
  }

  @Override
  public boolean isCompatibleWithAllVersions(String s, String s1, String s2) throws SchemaNotFoundException,
      SchemaBranchNotFoundException {
    throw new UnsupportedOperationException("Check compatible with all versions not supported yet.");
  }

  @Override
  public String uploadFile(InputStream inputStream) throws SerDesException {
    throw new UnsupportedOperationException("Upload file not supported yet.");
  }

  @Override
  public InputStream downloadFile(String s) throws FileNotFoundException {
    throw new UnsupportedOperationException("Download file not supported yet.");
  }

  @Override
  public Long addSerDes(SerDesPair serDesPair) {
    throw new UnsupportedOperationException("Add serdesPair not supported yet.");
  }

  @Override
  public void mapSchemaWithSerDes(String s, Long aLong) {

  }

  @Override
  public <T> T getDefaultSerializer(String s) throws SerDesException {
    throw new UnsupportedOperationException("Get default ser not supported yet.");
  }

  @Override
  public <T> T getDefaultDeserializer(String s) throws SerDesException {
    throw new UnsupportedOperationException("Get default deser not supported yet.");
  }

  @Override
  public Collection<SerDesInfo> getSerDes(String s) {
    throw new UnsupportedOperationException("Get serdeser not supported yet.");
  }

  @Override
  public void transitionState(Long aLong, Byte aByte, byte[] bytes) throws SchemaNotFoundException,
      SchemaLifecycleException {
    throw new UnsupportedOperationException("transitionState not supported yet.");
  }

  @Override
  public SchemaVersionLifecycleStateMachineInfo getSchemaVersionLifecycleStateMachineInfo() {
    throw new UnsupportedOperationException("getSchemaVersionLifecycleStateMachineInfo not supported yet.");
  }

  @Override
  public SchemaBranch createSchemaBranch(Long aLong, SchemaBranch schemaBranch) throws
      SchemaBranchAlreadyExistsException, SchemaNotFoundException {
    throw new UnsupportedOperationException("Create schema branch not supported yet.");
  }

  @Override
  public Collection<SchemaBranch> getSchemaBranches(String s) throws SchemaNotFoundException {
    throw new UnsupportedOperationException("Get schema branches not supported yet.");
  }

  @Override
  public void deleteSchemaBranch(Long aLong) throws SchemaBranchNotFoundException,
      InvalidSchemaBranchDeletionException {
    throw new UnsupportedOperationException("Delete schema branch not supported yet.");
  }

  @Override
  public <T> T createSerializerInstance(SerDesInfo serDesInfo) {
    throw new UnsupportedOperationException("createSerializerInstance not supported yet.");
  }

  @Override
  public <T> T createDeserializerInstance(SerDesInfo serDesInfo) {
    throw new UnsupportedOperationException("createDeserializerInstance not supported yet.");
  }

  @Override
  public void close() throws Exception {

  }

  /**
   * Class with all URL constants.
   */
  @Getter
  public static final class RegistryURL {

    private static final RegistryURL SCHEMAS = new RegistryURL(
        "/schemas", " all schemas");
    private static final RegistryURL INDIVIDUAL_SCHEMA =  new RegistryURL(
        SCHEMAS.url + "/%s", " schema %s");
    private static final RegistryURL COMPATIBILITY_CHECK =  new RegistryURL(
        SCHEMAS.url + "/%s/compatibility", "checking compatibility for %s");
    private static final RegistryURL SCHEMA_VERSIONS =  new RegistryURL(
        SCHEMAS.url + "/%s/versions", "getting schema versions for schema %s");
    private static final RegistryURL INDIVIDUAL_VERSION =  new RegistryURL(
        SCHEMAS.url + "/%s/versions/%d", "getting schema %s");
    private static final RegistryURL SCHEMA_LATEST_VERSION =  new RegistryURL(
        SCHEMAS.url + "/%s/versions/latest", "getting the latest version for %s");
    private static final RegistryURL SCHEMA_PROVIDERS =  new RegistryURL(
        "/schemaproviders", "getting all supported schema providers");
    private static final RegistryURL SCHEMAS_BY_ID =  new RegistryURL(
        "/schemasById/%l", "getting schema by id %d");
    private static final RegistryURL SEARCH_FIELD =  new RegistryURL(
        "/search/schemas/fields", "searching for fields");
    private static final RegistryURL SERIALIZERS =  new RegistryURL(
        "/serdes", "getting all serializers");
    private static final RegistryURL FILES =  new RegistryURL(
        "/files", "getting files");
    private static final RegistryURL UPLOAD_FILES = new RegistryURL(
        "/versions/upload", "uploading file");

    private final String url;
    private final String purpose;

    /**
     * Private constructor for RegistryURL.
     * @param url URL for the request
     * @param purpose Purpose of the url
     */
    private RegistryURL(String url, String purpose) {
      this.url = "/api/v1/schemaregistry" + url;
      this.purpose = purpose;
    }

    /**
     * Build a URL object with parameters.
     * @param url Base url for the request
     * @param parameters Parameters to be replaced in URL
     * @return Registry URL to make rest calls
     */
    static RegistryURL buildURL(RegistryURL url, String... parameters) {
      return new RegistryURL(String.format(url.url, Arrays.asList(parameters)),
          String.format(url.purpose, Arrays.asList(parameters)));
    }
  }

  /**
   * Log error message for a failed request.
   * @param requestType Request type
   * @param url URL that failed
   * @param e Failed with Exception
   */
  private static void logErrorMessage(String requestType, RegistryURL url, Exception e) {
    LOG.error("{} request to {} failed with {}",
        requestType, url.getPurpose(), ExceptionUtils.getFullStackTrace(e));
  }

  @Override
  public Collection<SchemaVersionInfo> getAllVersions(String s, String s1, List<Byte> list)
      throws SchemaNotFoundException, SchemaBranchNotFoundException {
    throw new UnsupportedOperationException("GetAllVersions not supported yet.");
  }

}