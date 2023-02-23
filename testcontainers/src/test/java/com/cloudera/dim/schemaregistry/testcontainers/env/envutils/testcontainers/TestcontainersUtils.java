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

package com.cloudera.dim.schemaregistry.testcontainers.env.envutils.testcontainers;

import com.cloudera.dim.schemaregistry.testcontainers.env.db.DbType;
import com.cloudera.dim.schemaregistry.testcontainers.env.testsetup.parts.DbSetup;
import com.github.dockerjava.api.command.CreateContainerCmd;
import com.github.dockerjava.api.model.ExposedPort;
import com.github.dockerjava.api.model.PortBinding;
import com.github.dockerjava.api.model.Ports;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.BindMode;
import org.testcontainers.containers.Container;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.wait.strategy.HttpWaitStrategy;
import org.testcontainers.containers.wait.strategy.LogMessageWaitStrategy;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.images.builder.ImageFromDockerfile;

import java.io.IOException;
import java.nio.file.FileVisitOption;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import static com.cloudera.dim.schemaregistry.testcontainers.env.testsetup.TestSetup.*;
import static org.awaitility.Awaitility.await;

public class TestcontainersUtils {

    private static final String POSTGRES_DOCKER_IMAGE_V9_6_24_PUBLIC = "registry.hub.docker.com/bitnami/postgresql:9.6.24";
    private static final String POSTGRES_DOCKER_IMAGE_V12_CLDR = "docker-private.infra.cloudera.com/cloudera_thirdparty/postgres:12";
    private static final String POSTGRES_DOCKER_IMAGE = POSTGRES_DOCKER_IMAGE_V9_6_24_PUBLIC;

    public static final String POSTGRES_CONTAINER_NETWORK_ALIAS = "postgres_network_alias";
    public static final int POSTGRES_PORT_INSIDE_CONTAINER = 5433;


    private static final String MYSQL_DOCKER_IMAGE_5_7_PUBLIC = "registry.hub.docker.com/bitnami/mysql:5.7.41";
    private static final String MYSQL_DOCKER_IMAGE_5_7_CLDR = "docker-private.infra.cloudera.com/cloudera_thirdparty/mysql:5.7";
    private static final String MYSQL_DOCKER_IMAGE = MYSQL_DOCKER_IMAGE_5_7_PUBLIC;
    public static final String MYSQL_CONTAINER_NETWORK_ALIAS = "mysql_network_alias";
    public static final int MYSQL_PORT_INSIDE_CONTAINER = 3307;


    private static final String KDC_DOCKER_IMAGE = "docker-registry.infra.cloudera.com/kubernator-ci/kdc:k8s-v4";
    public static final String MOUNTED_DIR_PATH_IN_KDC_CONTAINER = "/tmp/mountedDir";
    public static final String KDC_CONTAINER_NETWORK_ALIAS = "kdc_network_alias";
    public static final int KDC_PORT = 88;
    public static final int KDC_ADMIN_PORT = 749;
    public static final int KDC_ADMIN_FIXED_PORT = 10749;


    public static final int SR_PORT = 7788;
    public static final int SR_DEBUG_PORT = 5006;
    public static final int SR_FIXED_PORT__DEV_ONLY = 17788;

    private static Logger LOG = LoggerFactory.getLogger(TestcontainersUtils.class);

    private TestcontainersUtils() {
    }

    public static GenericContainer preparePostgresContainer(Network network, DbSetup dbSetup) {
        if (dbSetup.getUsedDbType() != DbType.POSTGRES) {
            throw new IllegalArgumentException();
        }
        GenericContainer ret = new GenericContainer(POSTGRES_DOCKER_IMAGE)
                .withNetworkAliases(POSTGRES_CONTAINER_NETWORK_ALIAS)
                .withNetwork(network)
                .withEnv("POSTGRES_PASSWORD", dbSetup.getDbUserPass())      //for CLDR image
                .withEnv("POSTGRESQL_PASSWORD", dbSetup.getDbUserPass())    //for bitnami image
                .withEnv("POSTGRES_USER", dbSetup.getDbUserName())
                .withEnv("POSTGRESQL_USER", dbSetup.getDbUserName())
                .withEnv("POSTGRESQL_USERNAME", dbSetup.getDbUserName())
                .withEnv("POSTGRES_DB", dbSetup.getDbName())
                .withEnv("POSTGRESQL_DB", dbSetup.getDbName())
                .withEnv("POSTGRESQL_DATABASE", dbSetup.getDbName())
                .withEnv("PGPORT", Integer.toString(dbSetup.getDbPortInsideContainer()))
                .withEnv("POSTGRES_PORT", Integer.toString(dbSetup.getDbPortInsideContainer()))
                .withEnv("POSTGRESQL_PORT", Integer.toString(dbSetup.getDbPortInsideContainer()))
                .withEnv("POSTGRESQL_PORT_NUMBER", Integer.toString(dbSetup.getDbPortInsideContainer()))
                .withExposedPorts(dbSetup.getDbPortInsideContainer());
        if (dbSetup.getIsFixedDbPortUsed()) {
            Consumer<CreateContainerCmd> cmd = (CreateContainerCmd e) -> e.withPortBindings(
                    new PortBinding(Ports.Binding.bindPort(dbSetup.getDbPortFixedForDebugging()),
                            new ExposedPort(dbSetup.getDbPortInsideContainer())));
            ret = ret.withCreateContainerCmdModifier(cmd);
        }
        return ret;
    }

    public static GenericContainer prepareMySqlContainer(Network network, DbSetup dbSetup) {
        if (dbSetup.getUsedDbType() != DbType.MYSQL5
                && dbSetup.getUsedDbType() != DbType.MYSQL8) {
            throw new IllegalArgumentException();
        }
        GenericContainer ret = new GenericContainer(MYSQL_DOCKER_IMAGE)
                .withNetworkAliases(MYSQL_CONTAINER_NETWORK_ALIAS)
                .withNetwork(network)
                .withEnv("MYSQL_ROOT_USER", "root")
                .withEnv("MYSQL_ROOT_PASSWORD", "sr_root_pass")
                .withEnv("MYSQL_DATABASE", dbSetup.getDbName())
                .withEnv("MYSQL_USER", dbSetup.getDbUserName())
                .withEnv("MYSQL_PASSWORD", dbSetup.getDbUserPass())
                .withEnv("MYSQL_TCP_PORT", Integer.toString(dbSetup.getDbPortInsideContainer()))        //for CLDR image
                .withEnv("MYSQL_PORT_NUMBER", Integer.toString(dbSetup.getDbPortInsideContainer()))     //for bitnami image
                .withExposedPorts(dbSetup.getDbPortInsideContainer());
        if (dbSetup.getIsFixedDbPortUsed()) {
            Consumer<CreateContainerCmd> cmd = (CreateContainerCmd e) -> e.withPortBindings(
                    new PortBinding(Ports.Binding.bindPort(dbSetup.getDbPortFixedForDebugging()),
                            new ExposedPort(dbSetup.getDbPortInsideContainer())));
            ret = ret.withCreateContainerCmdModifier(cmd);
        }
        return ret;
    }

    public static GenericContainer prepareSRContainer(String srYamlPath,
                                                      boolean isHttps,
                                                      String serverKeystorePath,
                                                      String serverTruststorePath,
                                                      boolean isKerberized,
                                                      String krb5ConfPath,
                                                      String serverPrincipalKeytabPath,
                                                      Network network,
                                                      GenericContainer... dependsOnContainers) {
        GenericContainer ret = new GenericContainer(
                new ImageFromDockerfile()
                        .withFileFromClasspath("Dockerfile", "docker/schemaregistry/Dockerfile_testcontainers")
                        .withFileFromClasspath("./testcontainers/src/test/resources/entrypoint.sh", "docker/schemaregistry/files/entrypoint.sh")
                        .withFileFromPath("./testcontainers/src/test/resources/custom_registry.yaml", Paths.get(srYamlPath))
                        .withFileFromPath("./registry-dist/build/distributions/schemaregistry-XY.tar.gz", getTarGzPath()))
                .withNetworkAliases("sr")
                .withNetwork(network)
                .dependsOn(dependsOnContainers)
                .withEnv("REGISTRY_OPTS", "-agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=" + Integer.toString(SR_DEBUG_PORT))
                .withExposedPorts(SR_PORT, SR_DEBUG_PORT)
                .withStartupTimeout(Duration.ofMinutes(1));

        HttpWaitStrategy waitStrategyWithoutKerberos = Wait.forHttp("/ui")
                .forPort(SR_PORT)
                .forStatusCodeMatching(status -> status == 200 || status == 401);
        if (isHttps) {
            waitStrategyWithoutKerberos =
                    waitStrategyWithoutKerberos.usingTls();
        }

        LogMessageWaitStrategy waitStrategyWithKerberos =
                Wait.forLogMessage(".*jetty.server.AbstractConnector - Started application.*", 1);

        if (isKerberized) {
            // HttpWaitStrategy mess up the kerberos authN and will not work later in the same JVM
            // so use different wait strategy here
            // o.e.jetty.server.AbstractConnector - Started application@21e3ed31{HTTP/1.1, (http/1.1)}{0.0.0.0:45145}
            ret = ret.waitingFor(waitStrategyWithKerberos);
        } else {
            ret = ret.waitingFor(waitStrategyWithoutKerberos);
        }

        if (isHttps) {
            if (serverKeystorePath == null
                    || serverTruststorePath == null) {
                throw new IllegalArgumentException("If TLS enabled, these must be set.");
            }
            ret = ret.withFileSystemBind(serverKeystorePath, serverKeystorePath, BindMode.READ_ONLY)
                    .withFileSystemBind(serverTruststorePath, serverTruststorePath, BindMode.READ_ONLY);
        }

        if (isKerberized) {
            if (krb5ConfPath != null || serverPrincipalKeytabPath != null) {
                ret = ret.withFileSystemBind(krb5ConfPath, "/etc/krb5.conf", BindMode.READ_ONLY)
                        .withFileSystemBind(serverPrincipalKeytabPath, serverPrincipalKeytabPath, BindMode.READ_ONLY);
            }
        }

        if (SR_USE_FIXED_PORT__DEV_ONLY) {
            Consumer<CreateContainerCmd> cmd = (CreateContainerCmd e) -> e.withPortBindings(
                    new PortBinding(Ports.Binding.bindPort(SR_FIXED_PORT__DEV_ONLY),
                            new ExposedPort(SR_PORT)),
                    new PortBinding(Ports.Binding.bindPort(SR_FIXED_DEBUG_PORT__DEV_ONLY),
                            new ExposedPort(SR_DEBUG_PORT)));
            ret = ret.withCreateContainerCmdModifier(cmd);
        }
        return ret;
    }

    public static GenericContainer prepareKdcContainer(String testTempFolderPath, Network network) {
        GenericContainer ret;
        if (REBUILD_KDC_SERVER_IMAGE_FROM_DOCKERFILE) {
            ret = new GenericContainer(
                    new ImageFromDockerfile()
                            .withFileFromClasspath("Dockerfile", "docker/kdc/Dockerfile")
                            .withFileFromClasspath("resources/docker/kdc/files/krb5.conf", "docker/kdc/files/krb5.conf")
                            .withFileFromClasspath("resources/docker/kdc/files/kadm5.acl", "docker/kdc/files/kadm5.acl")
                            .withFileFromClasspath("resources/docker/kdc/files/kerberos_setup.sh", "docker/kdc/files/kerberos_setup.sh"))
                    .withNetworkAliases(KDC_CONTAINER_NETWORK_ALIAS)
                    .withNetwork(network)
                    .withExposedPorts(KDC_PORT, KDC_ADMIN_PORT)
                    .withFileSystemBind(testTempFolderPath,
                            MOUNTED_DIR_PATH_IN_KDC_CONTAINER, BindMode.READ_WRITE);
        } else {
            ret = new GenericContainer(KDC_DOCKER_IMAGE)
                    .withNetwork(network)
                    .withNetworkAliases(KDC_CONTAINER_NETWORK_ALIAS)
                    .withExposedPorts(KDC_PORT, KDC_ADMIN_PORT)
                    .withFileSystemBind(testTempFolderPath,
                            MOUNTED_DIR_PATH_IN_KDC_CONTAINER, BindMode.READ_WRITE);
        }

        if (KDC_USE_FIXED_PORT__DEV_ONLY) {
            Consumer<CreateContainerCmd> cmd = (CreateContainerCmd e) -> e.withPortBindings(
                    new PortBinding(Ports.Binding.bindPort(KDC_FIXED_PORT__DEV_ONLY),
                            new ExposedPort(KDC_PORT)),
                    new PortBinding(Ports.Binding.bindPort(KDC_ADMIN_FIXED_PORT),
                            new ExposedPort(KDC_ADMIN_PORT)));
            ret = ret.withCreateContainerCmdModifier(cmd);
        }
        return ret;
    }

    protected static Path getTarGzPath() {
        try {
            List<Path> paths = Files.find(Paths.get("src/test/resources"), 4,
                            (path, attr) -> path.toString().endsWith("tar.gz"), FileVisitOption.FOLLOW_LINKS)
                    .collect(Collectors.toList());
            Path path = paths.stream().findFirst().orElseThrow(() -> new RuntimeException("tar.gz file not found"));
            return path;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static void awaitKdcContainerIsUp(GenericContainer kdcContainer) {
        await()
                .atLeast(Duration.ofMillis(10))
                .atMost(Duration.ofSeconds(5))
                .with()
                .pollInterval(Duration.ofMillis(100))
                .until(() -> {
                    try {
                        LOG.debug("Check if KDC container responding...");
                        kdcContainer.execInContainer(
                                createQueryCmdForKDCTestcontainer("list_principals")
                                        .toArray(new String[0]));
                        LOG.debug("KDC responded");
                    } catch (Exception e) {
                        return false;
                    }
                    return true;
                });
    }

    public static String[] chmodKeytabFile(String keytabFilePathInContainer) {
        String[] ret = Arrays.asList(
                        "chmod",
                        "777",
                        keytabFilePathInContainer)
                .toArray(new String[0]);
        return ret;
    }


    private static List<String> createQueryCmdForKDCTestcontainer(String queryCmd) {
        List<String> ret = Arrays.asList(
                "env",
                "KRB5_TRACE=/dev/stdout",
                "kadmin",
                "-p",
                "admin/admin",
                "-w",
                "Hortonworks",
                "-q",
                queryCmd);
        return ret;
    }

    public static void execInTestcontainer(GenericContainer container, String[] cmd) {
        Container.ExecResult result;
        try {
            result = container.execInContainer(cmd);
        } catch (Exception e) {
            LOG.error(e.getMessage(), e);
            throw new RuntimeException(e);
        }
        String stdout = result.getStdout();
        String stdErr = result.getStderr();
        int exitCode = result.getExitCode();

        if (exitCode != 0) {
            LOG.error(stdout);
            LOG.error(stdErr);
            throw new RuntimeException("Non-zero return code");
        }
    }

    public static void addPrincipalToKDC(GenericContainer kdcContainer, String principalName, String principalPassword) {
        execInTestcontainer(kdcContainer, createAddPrincipalCmd(principalName, principalPassword));
    }

    public static void getKeytabToMountedDir(GenericContainer kdcContainer, String principalName, String targetKeytabFileName) {
        execInTestcontainer(kdcContainer, createGetKeytabCmd(principalName, targetKeytabFileName));
    }

    private static String[] createAddPrincipalCmd(String principalName, String principalPassword) {
        List<String> ret = createQueryCmdForKDCTestcontainer(
                String.format("addprinc -expire 2030-01-01 -pwexpire 2030-01-01 -pw %s %s", principalPassword, principalName));
        return ret.toArray(new String[0]);
    }

    private static String[] createGetKeytabCmd(String principalName, String keytabFileName) {
        List<String> ret = createQueryCmdForKDCTestcontainer(
                String.format("ktadd -norandkey -k %s/%s %s", MOUNTED_DIR_PATH_IN_KDC_CONTAINER, keytabFileName, principalName));
        return ret.toArray(new String[0]);
    }


}
