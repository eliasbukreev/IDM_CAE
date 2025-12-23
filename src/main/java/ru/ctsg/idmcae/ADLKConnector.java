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

import java.util.List;
import java.util.Set;

import org.identityconnectors.common.CollectionUtil;
import org.identityconnectors.common.logging.Log;
import org.identityconnectors.framework.common.exceptions.ConnectionFailedException;
import org.identityconnectors.framework.common.exceptions.InvalidAttributeValueException;
import org.identityconnectors.framework.common.objects.Attribute;
import org.identityconnectors.framework.common.objects.ObjectClass;
import org.identityconnectors.framework.common.objects.OperationOptions;
import org.identityconnectors.framework.common.objects.ResultsHandler;
import org.identityconnectors.framework.common.objects.Schema;
import org.identityconnectors.framework.common.objects.SyncResultsHandler;
import org.identityconnectors.framework.common.objects.SyncToken;
import org.identityconnectors.framework.common.objects.Uid;
import org.identityconnectors.framework.common.objects.filter.Filter;
import org.identityconnectors.framework.common.objects.filter.FilterTranslator;
import org.identityconnectors.framework.spi.Configuration;
import org.identityconnectors.framework.spi.ConnectorClass;
import org.identityconnectors.framework.spi.PoolableConnector;
import org.identityconnectors.framework.spi.operations.CreateOp;
import org.identityconnectors.framework.spi.operations.DeleteOp;
import org.identityconnectors.framework.spi.operations.SchemaOp;
import org.identityconnectors.framework.spi.operations.SearchOp;
import org.identityconnectors.framework.spi.operations.SyncOp;
import org.identityconnectors.framework.spi.operations.TestOp;
import org.identityconnectors.framework.spi.operations.UpdateOp;

import ru.ctsg.idmcae.builders.LiveSync;
import ru.ctsg.idmcae.builders.SchemaDefinitionBuilder;
import ru.ctsg.idmcae.processing.AccountProcessing;
import ru.ctsg.idmcae.processing.PermissionProcessing;

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
    private LiveSync liveSync;

    @Override
    public Configuration getConfiguration() {
        return configuration;
    }

    @Override
    public void init(Configuration configuration) {
        this.configuration = (ADLKConfiguration)configuration;
        this.connection = new ADLKConnection(this.configuration);
        this.liveSync = new LiveSync();
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
    public Uid create(
        ObjectClass objectClass,
        Set<Attribute> createAttributes,
        OperationOptions operationOptions) {

        LOG.info("Processing through the create operation using the object class: {0}", objectClass);
        LOG.info("The attribute(s) used for the create query operation:{0} ", 
                createAttributes == null ? "Empty attributes" : createAttributes);
        LOG.info("Evaluating createQuery with the following operation options: {0}", 
                operationOptions == null ? "empty operation options." : operationOptions);

        if (objectClass == null) {
            throw new IllegalArgumentException("Object class attribute can no be null");
        }

        if (createAttributes == null || createAttributes.isEmpty()) {
            throw new IllegalArgumentException("Invalid create attributes.");
        }

        if (objectClass.is(ObjectClass.ACCOUNT_NAME)) {
            LOG.info("Creating account...");
            AccountProcessing accountProcessing = new AccountProcessing();
            return accountProcessing.createAccount(createAttributes, connection.getConnection());

        } else if (objectClass.is("Permission")) {
            LOG.info("Creating permission...");
            PermissionProcessing permissionProcessing = new PermissionProcessing();
            return permissionProcessing.createPermission(createAttributes, connection.getConnection());

        } else {
            throw new IllegalArgumentException(
                "Unsupported object class: " + objectClass.getClass().getName()
            );  
        }
    }

    @Override
    public FilterTranslator<Filter> createFilterTranslator(
        ObjectClass objectClass, 
        OperationOptions options) {

        return new FilterTranslator<>() {
            @Override
            public List<Filter> translate(Filter filter) {
                return CollectionUtil.newList(filter);
            }
        };
    }

    @Override
    public void executeQuery(
        ObjectClass objectClass, 
        Filter filter, 
        ResultsHandler resultsHandler, 
        OperationOptions operationOptions) {

        LOG.info("Processing through the executeQuery operation using the object class: {0}", objectClass);
        LOG.ok("The filter(s) used for the execute query operation:{0} ", 
            filter == null ? 
            "Empty filter, fetching all objects of the object type." : filter);
        LOG.ok("Evaluating executeQuery with the following operation options: {0}", 
            operationOptions == null ? 
            "empty operation options." : operationOptions);

        if (objectClass == null) {
            throw new IllegalArgumentException("Object class attribute can no be null");
        }

        if (resultsHandler == null) {
            LOG.error("Attribute of type ResultsHandler not provided.");
            throw new InvalidAttributeValueException(
                "Attribute of type ResultsHandler is not provided.");
        }

        if (objectClass.is(ObjectClass.ACCOUNT_NAME)) {
            AccountProcessing accountProcessing = new AccountProcessing();
            accountProcessing.executeQuery(filter, resultsHandler, operationOptions, connection.getConnection());
        }

        if (objectClass.is("Permission")) {
            PermissionProcessing permissionProcessing = new PermissionProcessing();
            permissionProcessing.executeQuery(filter, resultsHandler, operationOptions, connection.getConnection());
        }

        LOG.ok("Finished evaluating the execute query operation.");
    }

    @Override
    public Uid update(
        ObjectClass objectClass,
        Uid uid,
        Set<Attribute> updateAttributes,
        OperationOptions operationOptions) {

        LOG.info("Processing through the UPDATE operation using the object class: {0}", objectClass);
        LOG.ok("The attribute(s) used for the UPDATE query operation:{0} ",
            updateAttributes == null ? 
            "Empty attributes" : updateAttributes);
        LOG.ok("Evaluating UpdateQuery with the following operation options: {0}",
            operationOptions == null ? 
            "empty operation options." : operationOptions);

        if (objectClass == null) {
            throw new IllegalArgumentException("Object class attribute can no be null.");
        }

        if (updateAttributes == null || updateAttributes.isEmpty()) {
            throw new IllegalArgumentException("Invalid update attributes.");
        }

        if (objectClass.is(ObjectClass.ACCOUNT_NAME)) {
            LOG.ok("Updating account...");
            AccountProcessing accountProcessing = new AccountProcessing();
            return accountProcessing.updateAccount(uid, updateAttributes, connection.getConnection());
        } else {
            throw new IllegalArgumentException(
                "Unsupported object class: " + objectClass.getClass().getName()
            );
        }
    }

    @Override
    public void delete(
        ObjectClass objectClass, 
        Uid uid, 
        OperationOptions operationOptions) {

        LOG.info("Processing through the DELETE operation using the object class: {0}", objectClass);
        LOG.ok("Evaluating DeleteQuery with the following operation options: {0}", 
            operationOptions == null ? "empty operation options." : operationOptions);
        LOG.ok("Evaluating DeleteQuery with Uid: {0}", uid);

        if (objectClass == null) {
            throw new IllegalArgumentException("Object class attribute can not be null.");
        }

        if (uid == null) {
            throw new IllegalArgumentException("Invalid UID value.");
        }

        if (objectClass.is(ObjectClass.ACCOUNT_NAME)) {
            LOG.ok("Updating account...");
            AccountProcessing accountProcessing = new AccountProcessing();
            accountProcessing.deleteAccount(uid, connection.getConnection());

        } else if (objectClass.is("Permission")) {
            LOG.ok("Updating permission...");
            PermissionProcessing permissionProcessing = new PermissionProcessing();
            permissionProcessing.deletePermission(uid, connection.getConnection());

        } else {
            throw new IllegalArgumentException(
                "Unsupported object class: " + objectClass.getClass().getName()
            );
        }
    }

    @Override
    public void sync(ObjectClass objectClass, SyncToken token, SyncResultsHandler handler, OperationOptions options) {
        liveSync.sync(objectClass, token, handler, options, connection.getConnection());
    }

    @Override
    public SyncToken getLatestSyncToken(ObjectClass objectClass) {
        return liveSync.getLatestSyncToken(objectClass, connection.getConnection());
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
