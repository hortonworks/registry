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
     * @param schemaRegistryPort    on which port should SR open the HTTP port
     */
    public String generateRegistryYaml(@Nonnull RegistryConfiguration configuration, int schemaRegistryPort) {
        checkNotNull(configuration);

        Map<String, Object> model = new HashMap<>();
        model.put("config", configuration);
        model.put("port", String.valueOf(schemaRegistryPort));

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
}
