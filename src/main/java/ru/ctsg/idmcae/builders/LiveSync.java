package ru.ctsg.idmcae.builders;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.Base64;
import java.util.Date;

import org.identityconnectors.common.logging.Log;
import org.identityconnectors.framework.common.exceptions.ConnectorException;
import org.identityconnectors.framework.common.objects.ConnectorObject;
import org.identityconnectors.framework.common.objects.ConnectorObjectBuilder;
import org.identityconnectors.framework.common.objects.ObjectClass;
import org.identityconnectors.framework.common.objects.OperationOptions;
import org.identityconnectors.framework.common.objects.SyncDelta;
import org.identityconnectors.framework.common.objects.SyncDeltaBuilder;
import org.identityconnectors.framework.common.objects.SyncDeltaType;
import org.identityconnectors.framework.common.objects.SyncResultsHandler;
import org.identityconnectors.framework.common.objects.SyncToken;
import org.identityconnectors.framework.spi.SyncTokenResultsHandler;



public class LiveSync {

    private static final Log LOG = Log.getLog(LiveSync.class);

    public void sync(
    ObjectClass objectClass,
    SyncToken token,
    SyncResultsHandler handler,
    OperationOptions options,
    Connection connection) {

        SyncToken currentToken = createSyncToken(null);
        Timestamp timestamp = getTimestampFromToken(token);

        final String sqlQuery = "SELECT a.account_id, a.username, a.full_name, a.email, a.is_active, a.created_at, a.last_modified_at, " +
            "COALESCE(ARRAY_AGG(DISTINCT ap.permission_uid) FILTER (WHERE ap.permission_uid IS NOT NULL), '{}') AS memberOf " +
            "FROM public.accounts a " +
            "LEFT JOIN public.account_permissions ap ON ap.account_id = a.account_id " +
            "WHERE a.last_modified_at > ? " +
            "GROUP BY a.account_id, a.username, a.full_name, a.email, a.is_active, a.created_at, a.last_modified_at";

        try (PreparedStatement preparedStatement = connection.prepareStatement(sqlQuery)) {
            preparedStatement.setTimestamp(1, timestamp);

            ResultSet resultSet = preparedStatement.executeQuery();

            while (resultSet.next()) {
                handleResultSet(resultSet, currentToken, handler);
            }

        } catch (Exception e) {
            throw new ConnectorException("sync() failed: " + e.getMessage(), e);
        }

        ((SyncTokenResultsHandler) handler).handleResult(currentToken);
    }

    private void handleResultSet(ResultSet resultSet, SyncToken currentToken, SyncResultsHandler handler) {

        try {
                String accountId = resultSet.getString("account_id");

                ConnectorObject obj = new ConnectorObjectBuilder()
                    .setUid(accountId)
                    .setObjectClass(ObjectClass.ACCOUNT)
                    .build();

                SyncDelta delta = new SyncDeltaBuilder()
                    .setDeltaType(SyncDeltaType.CREATE_OR_UPDATE)
                    .setObject(obj)
                    .setToken(currentToken)
                    .build();

                handler.handle(delta);
        } catch (SQLException e) {
        throw new ConnectorException("handleResultSet() failed: " + e.getMessage(), e);
    }
    }

    private Timestamp getTimestampFromToken(SyncToken token) {
        
        try {
            byte[] data = Base64.getDecoder().decode(token.getValue().toString());

            try (ByteArrayInputStream baos = new ByteArrayInputStream(data);
                ObjectInputStream objectInputStream = new ObjectInputStream(baos)) {
                    Object obj = objectInputStream.readObject();
                    return (Timestamp) obj;
            }



        } catch (Exception e) {
        throw new ConnectorException("Failed to get Timestamp from SyncToken", e);
    }
    }

    private SyncToken createSyncToken(Timestamp timestamp) {

        LOG.info("createSyncToken()");

        String currentTokenStr = "";
        Timestamp currentTimestamp = (timestamp != null) ? timestamp : new Timestamp(new Date().getTime());

        try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
            ObjectOutputStream objectOutputStream = new ObjectOutputStream(baos)) {

            objectOutputStream.writeObject(currentTimestamp);
            currentTokenStr = Base64.getEncoder().encodeToString(baos.toByteArray());

        } catch (Exception e) {
            throw new ConnectorException("createSyncToken() failed", e);
        }

        return new SyncToken(currentTokenStr);
    }

    public SyncToken getLatestSyncToken(ObjectClass objectClass, Connection connection) {
        LOG.info("getLatestSyncToken()");

        String tableName;
        if (objectClass.is(ObjectClass.ACCOUNT_NAME)) {
            tableName = "public.accounts";
        } else if (objectClass.is("Permission")) {
            tableName = "public.permission";
        } else {
            throw new IllegalArgumentException("Unsupported object class: " + objectClass);
        }

        String sql = "SELECT MAX(last_modified_at) AS last_ts FROM " + tableName;

        try (
         PreparedStatement preparedStatement = connection.prepareStatement(sql);
         ResultSet resultSet = preparedStatement.executeQuery()) {

        Timestamp latestTimestamp = resultSet.getTimestamp("last_ts");

        return createSyncToken(latestTimestamp);

        } catch (Exception e) {
            throw new ConnectorException("Failed to get latest SyncToken for " + objectClass, e);
        }

    }
    
}
