/*
 * Copyright 2023 Ververica Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.ververica.cdc.connectors.starrocks.sink;

import org.apache.flink.util.Preconditions;
import org.apache.flink.util.StringUtils;

import com.starrocks.connector.flink.catalog.StarRocksCatalog;
import com.starrocks.connector.flink.catalog.StarRocksCatalogException;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

/** A {@link StarRocksCatalog} that can also inspect asynchronous schema change jobs. */
public class StarRocksEnrichedCatalog extends StarRocksCatalog {

    private static final long serialVersionUID = 1L;

    private final String jdbcUrl;
    private final String username;
    private final String password;

    public StarRocksEnrichedCatalog(String jdbcUrl, String username, String password) {
        super(jdbcUrl, username, password);
        this.jdbcUrl = jdbcUrl;
        this.username = username;
        this.password = password;
    }

    /**
     * Returns whether the latest column schema change job ({@code SHOW ALTER TABLE COLUMN}) of the
     * table is still in progress.
     *
     * <p>StarRocks executes {@code ALTER TABLE ... ADD/DROP/MODIFY COLUMN} as an asynchronous job.
     * If the job outlives the client (e.g. the CDC job fails over because of an RPC timeout), the
     * column is not visible yet but issuing the same ALTER again is rejected with "Duplicate
     * column". Callers use this to wait for such a job before retrying.
     */
    public boolean hasUnfinishedColumnAlterJob(String databaseName, String tableName)
            throws StarRocksCatalogException {
        Preconditions.checkArgument(
                !StringUtils.isNullOrWhitespaceOnly(databaseName),
                "Database name cannot be null or empty.");
        Preconditions.checkArgument(
                !StringUtils.isNullOrWhitespaceOnly(tableName),
                "Table name cannot be null or empty.");
        String sql =
                String.format(
                        "SHOW ALTER TABLE COLUMN FROM `%s` WHERE TableName = '%s' "
                                + "ORDER BY CreateTime DESC LIMIT 1",
                        databaseName.replace("`", "``"),
                        tableName.replace("\\", "\\\\").replace("'", "\\'"));
        try (Connection connection = DriverManager.getConnection(jdbcUrl, username, password);
                Statement statement = connection.createStatement();
                ResultSet rs = statement.executeQuery(sql)) {
            if (!rs.next()) {
                return false;
            }
            String state = rs.getString("State");
            return state != null
                    && !"FINISHED".equalsIgnoreCase(state)
                    && !"CANCELLED".equalsIgnoreCase(state);
        } catch (SQLException e) {
            throw new StarRocksCatalogException(
                    String.format(
                            "Failed to query alter jobs of table `%s`.`%s`.",
                            databaseName, tableName),
                    e);
        }
    }
}
