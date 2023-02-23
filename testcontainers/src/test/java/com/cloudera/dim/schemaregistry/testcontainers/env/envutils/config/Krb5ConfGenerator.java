/**
 * Copyright 2016-2023 Cloudera, Inc.
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

package com.cloudera.dim.schemaregistry.testcontainers.env.envutils.config;

import freemarker.ext.beans.BeansWrapper;
import freemarker.template.Configuration;
import freemarker.template.Template;
import freemarker.template.TemplateExceptionHandler;
import freemarker.template.TemplateModel;
import lombok.AllArgsConstructor;
import lombok.Getter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.StringWriter;
import java.util.HashMap;
import java.util.Map;

import static com.cloudera.dim.schemaregistry.testcontainers.env.envutils.testcontainers.TestcontainersUtils.KDC_CONTAINER_NETWORK_ALIAS;

public class Krb5ConfGenerator {

    private static final Logger LOG = LoggerFactory.getLogger(Krb5ConfGenerator.class);
    public static final String KRB_5_CONF_FTL = "krb5_conf.ftl";

    private Krb5ConfGenerator() {
    }

    public static String generateKrb5ConfContent(boolean generateForContainerizedSrServer,
                                                 Class classForTemplateLoading,
                                                 String configTemplateDir,
                                                 int kdcContainerAdminPort,
                                                 int kdcContainerTgsPort) {
        KerberosConfig kerberosConfig;

        if (generateForContainerizedSrServer) {
            kerberosConfig = new KerberosConfig(
                    KDC_CONTAINER_NETWORK_ALIAS,
                    kdcContainerAdminPort,
                    KDC_CONTAINER_NETWORK_ALIAS,
                    kdcContainerTgsPort);
        } else {
            kerberosConfig = new KerberosConfig(
                    "127.0.0.1",
                    kdcContainerAdminPort,
                    "127.0.0.1",
                    kdcContainerTgsPort);
        }

        Map<String, Object> model = new HashMap<>();
        model.put("kerberosConf", kerberosConfig);

        LOG.info("Generate krb5.conf from template: " + KRB_5_CONF_FTL);
        String ret = generate(KRB_5_CONF_FTL, model,
                classForTemplateLoading, configTemplateDir);
        return ret;
    }


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

    @Getter
    @AllArgsConstructor
    public static class KerberosConfig {
        private String adminServerHost;
        private int adminServerPort;
        private String kdcHost;
        private int kdcPort;
    }

}
