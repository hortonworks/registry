/*
 * Copyright 2016-2021 Cloudera, Inc.
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
package com.hortonworks.registries.storage.tool.sql.initenv.mysql;

import com.hortonworks.registries.storage.tool.sql.initenv.UserCreator;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public class MySqlUserCreator implements UserCreator {
    private static final String QUERY_USER_TEMPLATE = "SELECT count(*) AS cnt FROM mysql.user WHERE user = '%s'";
    private static final String[] CREATE_USER_TEMPLATE = {
            "create user '%s'@'localhost' IDENTIFIED BY '%s'",
            "create user '%s'@'%%' IDENTIFIED BY '%s'"
    };

    private Connection connection;

    public MySqlUserCreator(Connection connection) {
        this.connection = connection;
    }

    @Override
    public boolean exists(String userName) throws SQLException {
        try (PreparedStatement pstmt = connection.prepareStatement(String.format(QUERY_USER_TEMPLATE, userName));
             ResultSet rs = pstmt.executeQuery()) {
            if (!rs.next()) {
                throw new IllegalStateException("It must return the result.");
            }

            return rs.getInt("cnt") > 0;
        }
    }

    @Override
    public void create(String userName, String password) throws SQLException {
        for (String queryTemplate : CREATE_USER_TEMPLATE) {
            String query = String.format(queryTemplate, userName, password);
            try (PreparedStatement pstmt = connection.prepareStatement(query)) {
                pstmt.executeUpdate();
            }
        }
    }
}
