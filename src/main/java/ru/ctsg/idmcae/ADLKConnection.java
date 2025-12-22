/*
 * Copyright (c) 2010-2014 Evolveum
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

package ru.ctsg.idmcae;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

import org.identityconnectors.common.logging.Log;
import org.identityconnectors.common.security.GuardedString;
import org.identityconnectors.framework.common.exceptions.ConnectionFailedException;

import com.evolveum.polygon.common.GuardedStringAccessor;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

public class ADLKConnection {

    private static final Log LOG = Log.getLog(ADLKConnection.class);

    private ADLKConfiguration configuration;
    private HikariDataSource dataSource;

    public ADLKConnection(ADLKConfiguration configuration) {
        LOG.info("Initializing GOK connector configuration"); // Здесь возможно исключение если VPN не подключен
        
        this.configuration = configuration;

        HikariConfig config = new HikariConfig();
        config.setJdbcUrl(this.configuration.getJdbcUrl());
        config.setDriverClassName(this.configuration.getJdbcDriver());
        config.setUsername(this.configuration.getJdbcUser());

        GuardedString clientPassword = configuration.getJdbcPassword();
        GuardedStringAccessor passwordAccessor = new GuardedStringAccessor();
        clientPassword.access(passwordAccessor);

        config.setPassword(passwordAccessor.getClearString());

        this.dataSource = new HikariDataSource(config);
    }

    public Connection getConnection() {
    try {
        LOG.ok("Trying to connect to database...");

        return this.dataSource.getConnection();

    } catch (SQLException e) {
        throw new ConnectionFailedException(
            "Database connection could not be established by the GOK connector: " 
            + e.getLocalizedMessage()
        );
    }
    }

    public void test() {
        Connection connections = getConnection();

        try {
            Statement statement = connections.createStatement();
            statement.executeQuery("SELECT 1");
            LOG.ok("Tест подключению к БД пройден");
        } catch(SQLException error) {
            throw new ConnectionFailedException(
                "Подключится к БД не получилось:" + error
            );
        } finally {
            if (connections !=null){
                try{
                    connections.close();
                } catch(SQLException error) {
                    LOG.warn(error, "Не получилось закрыть подключение"); 
                }
            }
        }
    }

    public void dispose() {
        this.dataSource.close();
    }
}