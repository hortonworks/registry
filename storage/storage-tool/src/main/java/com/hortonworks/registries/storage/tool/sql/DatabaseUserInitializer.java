/*
 * Copyright 2016-2019 Cloudera, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hortonworks.registries.storage.tool.sql;

import com.hortonworks.registries.storage.common.DatabaseType;
import com.hortonworks.registries.storage.tool.sql.initenv.DatabaseCreator;
import com.hortonworks.registries.storage.tool.sql.initenv.DatabaseCreatorFactory;
import com.hortonworks.registries.storage.tool.sql.initenv.UserCreator;
import com.hortonworks.registries.storage.tool.sql.initenv.UserCreatorFactory;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;


import java.io.IOException;
import java.net.Authenticator;
import java.net.PasswordAuthentication;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;

public class DatabaseUserInitializer {

    private static final String OPTION_CONFIG_FILE_PATH = "config";
    private static final String OPTION_ADMIN_JDBC_URL = "admin-jdbc-url";
    private static final String OPTION_ADMIN_DB_USER = "admin-username";
    private static final String OPTION_ADMIN_PASSWORD = "admin-password";
    private static final String OPTION_TARGET_USER = "target-username";
    private static final String OPTION_TARGET_PASSWORD = "target-password";
    private static final String OPTION_TARGET_DATABASE = "target-database";

    public static void main(String[] args) throws Exception {
        Options options = new Options();

        options.addOption(
                Option.builder("c")
                        .numberOfArgs(1)
                        .longOpt(OPTION_CONFIG_FILE_PATH)
                        .desc("Config file path")
                        .build()
        );

        options.addOption(
                Option.builder()
                    .hasArg()
                    .longOpt(OPTION_ADMIN_JDBC_URL)
                    .desc("JDBC url to connect DBMS via admin.")
                    .build()
        );

        options.addOption(
                Option.builder()
                    .hasArg()
                    .longOpt(OPTION_ADMIN_DB_USER)
                    .desc("Admin user name: should be able to create and grant privileges.")
                    .build()
        );

        options.addOption(
                Option.builder()
                    .hasArg()
                    .longOpt(OPTION_ADMIN_PASSWORD)
                    .desc("Admin user's password: should be able to create and grant privileges.")
                    .build()
        );

        options.addOption(
                Option.builder()
                    .hasArg()
                    .longOpt(OPTION_TARGET_USER)
                    .desc("Name of target user.")
                    .build()
        );

        options.addOption(
                Option.builder()
                    .hasArg()
                    .longOpt(OPTION_TARGET_PASSWORD)
                    .desc("Password of target user.")
                    .build()
        );

        options.addOption(
                Option.builder()
                    .hasArg()
                    .longOpt(OPTION_TARGET_DATABASE)
                    .desc("Target database.")
                    .build()
        );

        CommandLineParser parser = new DefaultParser();
        CommandLine commandLine = parser.parse(options, args);

        String[] neededOptions = {
                OPTION_CONFIG_FILE_PATH,
                OPTION_ADMIN_JDBC_URL, OPTION_ADMIN_DB_USER, OPTION_ADMIN_PASSWORD,
                OPTION_TARGET_USER, OPTION_TARGET_PASSWORD, OPTION_TARGET_DATABASE
        };

        boolean optNotFound = Arrays.stream(neededOptions).anyMatch(opt -> !commandLine.hasOption(opt));
        if (optNotFound) {
            usage(options);
            System.exit(1);
        }

        String confFilePath = commandLine.getOptionValue(OPTION_CONFIG_FILE_PATH);

        Optional<AdminOptions> adminOptionsOptional = AdminOptions.from(commandLine);
        if (!adminOptionsOptional.isPresent()) {
            usage(options);
            System.exit(1);
        }

        AdminOptions adminOptions = adminOptionsOptional.get();

        Optional<TargetOptions> targetOptionsOptional = TargetOptions.from(commandLine);
        if (!targetOptionsOptional.isPresent()) {
            usage(options);
            System.exit(1);
        }

        TargetOptions targetOptions = targetOptionsOptional.get();

        Map<String, Object> conf;
        try {
            conf = Utils.readConfig(confFilePath);
        } catch (IOException e) {
            System.err.println("Error occurred while reading config file: " + confFilePath);
            System.exit(1);
            throw new IllegalStateException("Shouldn't reach here");
        }

        try (Connection conn = getConnectionViaAdmin(adminOptions)) {
            DatabaseCreator databaseCreator = DatabaseCreatorFactory.newInstance(adminOptions.getDatabaseType(), conn);
            UserCreator userCreator = UserCreatorFactory.newInstance(adminOptions.getDatabaseType(), conn);

            String database = targetOptions.getDatabase();
            String username = targetOptions.getUsername();

            createDatabase(databaseCreator, database);
            createUser(targetOptions, userCreator, username);
            grantPrivileges(databaseCreator, database, username);
        }
    }

    private static void createDatabase(DatabaseCreator databaseCreator, String database) {
        try {
            if (!databaseCreator.exists(database)) {
                databaseCreator.create(database);
                System.out.println("Database " + database + " created.");
            } else {
                System.out.println("Database " + database + " already exists. Skip creating...");
            }
        } catch (SQLException e) {
            System.err.println("Error occurred while creating database!");
            throw new RuntimeException(e);
        }
    }

    private static void createUser(TargetOptions targetOptions, UserCreator userCreator, String username) {
        try {
            if (!userCreator.exists(username)) {
                userCreator.create(username, targetOptions.getPassword());
                System.out.println("User " + username + " created.");
            } else {
                System.out.println("User " + username + " already exists. Skip creating...");
            }
        } catch (SQLException e) {
            System.err.println("Error occurred while creating user!");
            throw new RuntimeException(e);
        }
    }

    private static void grantPrivileges(DatabaseCreator databaseCreator, String database, String username) {
        try {
            databaseCreator.grantPrivileges(database, username);
            System.out.println("Granted privileges on database " + database + " to user " + username + ".");
        } catch (SQLException e) {
            System.err.println("Error occurred while granting privileges!");
            throw new RuntimeException(e);
        }
    }

    private static DatabaseType findDatabaseType(String adminJdbcUrl) {
        String[] jdbcParts = adminJdbcUrl.split(":");
        if (jdbcParts.length < 3) {
            System.err.println("Incorrect format of JDBC url : " + adminJdbcUrl);
            System.exit(1);
            throw new IllegalStateException("Shouldn't reach here");
        }

        if (!jdbcParts[0].equals("jdbc")) {
            System.err.println("Incorrect format of JDBC url : " + adminJdbcUrl);
            System.exit(1);
            throw new IllegalStateException("Shouldn't reach here");
        }

        return DatabaseType.fromValue(jdbcParts[1]);
    }

    private static Connection getConnectionViaAdmin(AdminOptions adminOptions) throws Exception {
        // Connect using the JDBC URL and user/pass from conf
        final Properties info = new Properties();
        info.put("user", adminOptions.getUsername());
        info.put("password", adminOptions.getPassword());
        final String className = JdbcDriverClass.fromDatabaseType(adminOptions.getDatabaseType()).getValue();

        Class.forName(className);
        return DriverManager.getConnection(adminOptions.getJdbcUrl(), info);
    }

    private static void usage(Options options) {
        HelpFormatter formatter = new HelpFormatter();
        formatter.printHelp("DatabaseUserInitializer [options]", options);
    }

    private static Authenticator getBasicAuthenticator(String host, int port, String username, String password) {
        return new Authenticator() {
            @Override
            protected PasswordAuthentication getPasswordAuthentication() {
                if (getRequestorType() == RequestorType.PROXY) {
                    if (getRequestingHost().equalsIgnoreCase(host)) {
                        if (getRequestingPort() == port) {
                            return new PasswordAuthentication(username, password.toCharArray());
                        }
                    }
                }
                return null;
            }
        };
    }

    private static class AdminOptions {
        private final String jdbcUrl;
        private final DatabaseType databaseType;
        private final String username;
        private final String password;

        private AdminOptions(String jdbcUrl, String username, String password) {
            this.jdbcUrl = jdbcUrl;
            this.username = username;
            this.password = password;

            this.databaseType = findDatabaseType(jdbcUrl);
        }

        String getJdbcUrl() {
            return jdbcUrl;
        }

        DatabaseType getDatabaseType() {
            return databaseType;
        }

        String getUsername() {
            return username;
        }

        String getPassword() {
            return password;
        }

        static Optional<AdminOptions> from(CommandLine cli) {
            if (!cli.hasOption(OPTION_ADMIN_JDBC_URL) || !cli.hasOption(OPTION_ADMIN_DB_USER) ||
                    !cli.hasOption(OPTION_ADMIN_PASSWORD)) {
                return Optional.empty();
            }

            return Optional.of(new AdminOptions(cli.getOptionValue(OPTION_ADMIN_JDBC_URL),
                    cli.getOptionValue(OPTION_ADMIN_DB_USER), cli.getOptionValue(OPTION_ADMIN_PASSWORD)));
        }
    }

    private static class TargetOptions {
        private final String username;
        private final String password;
        private final String database;

        private TargetOptions(String username, String password, String database) {
            this.username = username;
            this.password = password;
            this.database = database;
        }

        String getUsername() {
            return username;
        }

        String getPassword() {
            return password;
        }

        String getDatabase() {
            return database;
        }

        static Optional<TargetOptions> from(CommandLine cli) {
            if (!cli.hasOption(OPTION_TARGET_USER) || !cli.hasOption(OPTION_TARGET_PASSWORD) ||
                    !cli.hasOption(OPTION_TARGET_DATABASE)) {
                return Optional.empty();
            }

            return Optional.of(new TargetOptions(cli.getOptionValue(OPTION_TARGET_USER),
                    cli.getOptionValue(OPTION_TARGET_PASSWORD), cli.getOptionValue(OPTION_TARGET_DATABASE)));
        }
    }

    private DatabaseUserInitializer() { }
}
