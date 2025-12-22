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

import java.sql.SQLException;

import org.identityconnectors.common.logging.Log;
import org.identityconnectors.framework.common.exceptions.ConnectionFailedException;
import org.identityconnectors.framework.common.objects.Schema;
import org.identityconnectors.framework.common.objects.filter.Filter;
import org.identityconnectors.framework.spi.Configuration;
import org.identityconnectors.framework.spi.Connector;
import org.identityconnectors.framework.spi.ConnectorClass;
import org.identityconnectors.framework.spi.PoolableConnector;
import org.identityconnectors.framework.spi.operations.CreateOp;
import org.identityconnectors.framework.spi.operations.DeleteOp;
import org.identityconnectors.framework.spi.operations.SchemaOp;
import org.identityconnectors.framework.spi.operations.SearchOp;
import org.identityconnectors.framework.spi.operations.SyncOp;
import org.identityconnectors.framework.spi.operations.TestOp;
import org.identityconnectors.framework.spi.operations.UpdateOp;

@ConnectorClass(displayNameKey = "adlk.connector.display", configurationClass = ADLKConfiguration.class)
public class ADLKConnector implements PoolableConnector, 
    SchemaOp, 
    TestOp, 
    CreateOp, 
    SearchOp<Filter>, 
    UpdateOp, 
    DeleteOp, 
    SyncOp {

    private static final Log LOG = Log.getLog(ADLKConnector.class);

    private ADLKConfiguration configuration;
    private ADLKConnection connection;

    @Override
    public Configuration getConfiguration() {
        return configuration;
    }

    @Override
    public void init(Configuration configuration) {
        this.configuration = (ADLKConfiguration)configuration;
        this.connection = new ADLKConnection(this.configuration);
    }

    @Override
    public Schema schema(){
        return new SchemaDefinitionBuilder().buildSchema();
    }

    @Override
    public void checkAlive() {
        try {
            if (connection != null && !connection.getConnection().isClosed()) {
                return;
            } else {
                throw new ConnectionFailedException("Instance of connection does not exist");
            }
        } catch (SQLException e) {
            throw new ConnectionFailedException("An exception occurred during check-alive. ", e);
        }
    }

    @Override
    public void test() {
        LOG.info("Executing test operation.");

        configuration.validate();
        connection.test();
        connection.dispose();

        LOG.ok("Test OK");
    }

    @Override
    public void dispose() {
        configuration = null;
        if (connection != null) {
            connection.dispose();
            connection = null;
        }
    }
}
