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

import com.google.common.base.Preconditions;
import com.hortonworks.registries.common.RegistryConfiguration;
import freemarker.ext.beans.BeansWrapper;
import freemarker.template.Configuration;
import freemarker.template.Template;
import freemarker.template.TemplateExceptionHandler;
import freemarker.template.TemplateModel;
import lombok.Data;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.StringWriter;
import java.util.HashMap;
import java.util.Map;

public class RegistryYamlGenerator {

    private static final Logger LOG = LoggerFactory.getLogger(RegistryYamlGenerator.class);

    private RegistryYamlGenerator() {
    }

    /**
     * Generate a registry.yaml file. You can use it to run Schema Registry.
     * @param configuration         configuration object with prefilled values, this will be used while generating the yaml file
     * @param connectorType         http or https
     * @param schemaRegistryPort    on which port should SR open the HTTP port
     * @param tlsConfig             if TLS is enabled then we need to configure the keystore and trust store
     */
    public static String generateRegistryYaml(RegistryConfiguration configuration, String connectorType,
                                              int schemaRegistryPort, TlsConfig tlsConfig, Class classForTemplateLoading,
                                              String configTemplateDir) {
        Preconditions.checkNotNull(configuration);
        if (connectorType == null) {
            connectorType = "http";
        }

        Map<String, Object> model = new HashMap<>();
        model.put("config", configuration);
        model.put("connectorType", connectorType);
        model.put("port", String.valueOf(schemaRegistryPort));
        model.put("tlsConfig", tlsConfig);

        String ret = generate("registry.ftl", model,
                classForTemplateLoading, configTemplateDir);
        return ret;
    }

    public static String generateAtlasProperties(Class classForTemplateLoading,
                                                 String configTemplateDir) {
        Map<String, Object> model = new HashMap<>();

        return generate("atlas-application.properties", model, classForTemplateLoading,
                configTemplateDir);
    }

    @SuppressWarnings({"unchecked", "rawtype"})
    private static String generate(String templateName, Map model, Class classForTemplateLoading, String configTemplateDir) {
        try {
            Configuration cfg = new Configuration(Configuration.VERSION_2_3_31);
            cfg.setClassForTemplateLoading(classForTemplateLoading, configTemplateDir);
            cfg.setDefaultEncoding("UTF-8");
            cfg.setTemplateExceptionHandler(TemplateExceptionHandler.RETHROW_HANDLER);
            cfg.setLogTemplateExceptions(false);
            cfg.setWrapUncheckedExceptions(true);
            cfg.setFallbackOnNullLoopVariable(false);

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

    @Data
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

    }
}
