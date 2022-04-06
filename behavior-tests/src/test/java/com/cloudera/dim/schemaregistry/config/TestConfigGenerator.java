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
package com.cloudera.dim.schemaregistry.config;

import com.hortonworks.registries.common.RegistryConfiguration;
import freemarker.ext.beans.BeansWrapper;
import freemarker.template.Configuration;
import freemarker.template.Template;
import freemarker.template.TemplateExceptionHandler;
import freemarker.template.TemplateModel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import java.io.StringWriter;
import java.util.HashMap;
import java.util.Map;

import static com.google.common.base.Preconditions.checkNotNull;

public class TestConfigGenerator {

    private static final Logger LOG = LoggerFactory.getLogger(TestConfigGenerator.class);

    private final Configuration cfg;

    public TestConfigGenerator() {
        cfg = new Configuration(Configuration.VERSION_2_3_31);
        cfg.setClassForTemplateLoading(getClass(), "/template");
        cfg.setDefaultEncoding("UTF-8");
        cfg.setTemplateExceptionHandler(TemplateExceptionHandler.RETHROW_HANDLER);
        cfg.setLogTemplateExceptions(false);
        cfg.setWrapUncheckedExceptions(true);
        cfg.setFallbackOnNullLoopVariable(false);
    }

    /**
     * Generate a registry.yaml file. You can use it to run Schema Registry.
     * @param configuration         configuration object with prefilled values, this will be used while generating the yaml file
     * @param connectorType         http or https
     * @param schemaRegistryPort    on which port should SR open the HTTP port
     * @param tlsConfig             if TLS is enabled then we need to configure the keystore and trust store
     */
    public String generateRegistryYaml(@Nonnull RegistryConfiguration configuration, String connectorType,
                                       int schemaRegistryPort, TlsConfig tlsConfig) {
        checkNotNull(configuration);
        if (connectorType == null) {
            connectorType = "http";
        }

        Map<String, Object> model = new HashMap<>();
        model.put("config", configuration);
        model.put("connectorType", connectorType);
        model.put("port", String.valueOf(schemaRegistryPort));
        model.put("tlsConfig", tlsConfig);

        return generate("registry.ftl", model);
    }

    public String generateAtlasProperties() {
        Map<String, Object> model = new HashMap<>();

        return generate("atlas-application.properties", model);
    }

    @SuppressWarnings({"unchecked", "rawtype"})
    private String generate(String templateName, Map model) {
        try {
            Template template = cfg.getTemplate(templateName);

            BeansWrapper wrapper = new BeansWrapper(Configuration.VERSION_2_3_31);
            TemplateModel statics = wrapper.getStaticModels();
            model.put("statics", statics);

            try (StringWriter out = new StringWriter()) {
                template.process(model, out);
                return out.toString();
            }
        } catch (Exception ex) {
            throw new IllegalArgumentException("Could not generate with template " + templateName, ex);
        }
    }

    public static class TlsConfig {
        private boolean needClientAuth;
        private String keyStorePath;
        private String keyStorePassword;
        private String certAlias;
        private boolean enableCRLDP;
        private String trustStorePath;
        private String trustStorePassword;

        public TlsConfig() { }

        public TlsConfig(boolean needClientAuth, String keyStorePath, String keyStorePassword, String certAlias,
                         boolean enableCRLDP, String trustStorePath, String trustStorePassword) {
            this.needClientAuth = needClientAuth;
            this.keyStorePath = keyStorePath;
            this.keyStorePassword = keyStorePassword;
            this.certAlias = certAlias;
            this.enableCRLDP = enableCRLDP;
            this.trustStorePath = trustStorePath;
            this.trustStorePassword = trustStorePassword;
        }

        public boolean isNeedClientAuth() {
            return needClientAuth;
        }

        public void setNeedClientAuth(boolean needClientAuth) {
            this.needClientAuth = needClientAuth;
        }

        public String getKeyStorePath() {
            return keyStorePath;
        }

        public void setKeyStorePath(String keyStorePath) {
            this.keyStorePath = keyStorePath;
        }

        public String getKeyStorePassword() {
            return keyStorePassword;
        }

        public void setKeyStorePassword(String keyStorePassword) {
            this.keyStorePassword = keyStorePassword;
        }

        public String getCertAlias() {
            return certAlias;
        }

        public void setCertAlias(String certAlias) {
            this.certAlias = certAlias;
        }

        public boolean isEnableCRLDP() {
            return enableCRLDP;
        }

        public void setEnableCRLDP(boolean enableCRLDP) {
            this.enableCRLDP = enableCRLDP;
        }

        public String getTrustStorePath() {
            return trustStorePath;
        }

        public void setTrustStorePath(String trustStorePath) {
            this.trustStorePath = trustStorePath;
        }

        public String getTrustStorePassword() {
            return trustStorePassword;
        }

        public void setTrustStorePassword(String trustStorePassword) {
            this.trustStorePassword = trustStorePassword;
        }
    }
}
