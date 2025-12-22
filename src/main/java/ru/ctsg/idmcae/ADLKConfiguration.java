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

import org.identityconnectors.framework.spi.AbstractConfiguration;
import org.identityconnectors.common.logging.Log;
import org.identityconnectors.common.security.GuardedString;
import org.identityconnectors.framework.spi.ConfigurationProperty;
import org.identityconnectors.framework.spi.StatefulConfiguration;

public class ADLKConfiguration extends AbstractConfiguration implements StatefulConfiguration {

    private static final Log LOG = Log.getLog(ADLKConfiguration.class);

    
    private String jdbcUrl;
    private String jdbcDriver;
    private String jdbcUser;
    private GuardedString jdbcPassword;

    @Override
    public void release() {
        jdbcUrl = null;
        jdbcDriver = null;
        jdbcUser = null;
        jdbcPassword.dispose();
    }

    @Override
    public void validate() {
        LOG.info("Execution of validate configuration method.");

        if (jdbcUrl == null || jdbcUrl.trim().isEmpty()) {
            throw new IllegalArgumentException("JDBC URL must not be empty");
        }
        if (jdbcDriver == null || jdbcDriver.trim().isEmpty()) {
            throw new IllegalArgumentException("JDBC Driver must not be empty");
        }
        if (jdbcUser == null || jdbcUser.trim().isEmpty()) {
            throw new IllegalArgumentException("Database user must not be empty");
        }
        if (jdbcPassword == null) {
            throw new IllegalArgumentException("Database password must not be null");
        }
    }


    @ConfigurationProperty(
        order = 1,
        displayMessageKey = "JDBC URL",
        helpMessageKey = "Example: jdbc:postgresql://localhost:5432/mydatabase",
        required = true
    )

    //Getters
    public String getJdbcUrl() {
        return jdbcUrl;
    }
    public String getJdbcDriver() {
        return jdbcDriver;
    }
    public String getJdbcUser() {
        return jdbcUser;
    }
    public GuardedString getJdbcPassword() {
        return jdbcPassword;
    }


    //Setters
    public void setJdbcUrl(String jdbcUrl) {
        this.jdbcUrl = jdbcUrl;
    }
    public void setJdbcDriver(String jdbcDriver) {
        this.jdbcDriver = jdbcDriver;
    }
    public void setJdbcUser(String jdbcUser) {
        this.jdbcUser = jdbcUser;
    }
    public void setJdbcPassword(GuardedString jdbcPassword) {
        this.jdbcPassword = jdbcPassword;
    }
}