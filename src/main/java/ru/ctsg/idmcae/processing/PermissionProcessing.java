package ru.ctsg.idmcae.processing;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashSet;
import java.util.Set;

import org.identityconnectors.common.logging.Log;
import org.identityconnectors.framework.common.exceptions.ConnectorException;
import org.identityconnectors.framework.common.objects.Attribute;
import org.identityconnectors.framework.common.objects.AttributeBuilder;
import org.identityconnectors.framework.common.objects.ConnectorObject;
import org.identityconnectors.framework.common.objects.ConnectorObjectBuilder;
import org.identityconnectors.framework.common.objects.ObjectClass;
import org.identityconnectors.framework.common.objects.OperationOptions;
import org.identityconnectors.framework.common.objects.ResultsHandler;
import org.identityconnectors.framework.common.objects.Uid;
import org.identityconnectors.framework.common.objects.filter.Filter;

import ru.ctsg.idmcae.ADLKConnection;

public class PermissionProcessing extends Processing{

    private static final Log LOG = Log.getLog(ADLKConnection.class);
    private static final String ATTR_CODE = "code";
    private static final String ATTR_DISPLAY_NAME = "display_name";
    private static final String ATTR_CATEGORY = "category";

    public Uid createPermission(
    Set<Attribute> createAttributes, 
    Connection connection) {
            
        LOG.info("createPermission() attributes: {0}", createAttributes);

        final String sql = """
            INSERT INTO public.permission
            (code, display_name, category, created_at)
            VALUES (?,?,?, CURRENT_TIMESTAMP)
            RETURNING permission_uid
        """;

        LOG.info("Executing PermissionProcessing query: {0}", sql);

        try (PreparedStatement preparedStatement =
                connection.prepareStatement(sql, Statement.RETURN_GENERATED_KEYS)) {

            String code = getString(createAttributes, ATTR_CODE);
            String displayName = getString(createAttributes, ATTR_DISPLAY_NAME);
            String category = getString(createAttributes, ATTR_CATEGORY);

            LOG.info("createPermission() preparedStatement: {0}", preparedStatement);
            LOG.info("createPermission() code: {0}", code);
            LOG.info("createPermission() displayName: {0}", displayName);
            LOG.info("createPermission() category: {0}", category);

            preparedStatement.setString(1, code);
            preparedStatement.setString(2, displayName);
            preparedStatement.setString(3, category);

            int affectedRows = preparedStatement.executeUpdate();
            if (affectedRows == 0) {
                throw new ConnectorException("Failed to create permission. Affected rows is 0");
            }

            try (ResultSet resultSet = preparedStatement.getGeneratedKeys()) {
                if (resultSet.next()) {
                    String permission_uid = resultSet.getString(1);
                    LOG.info("createPermission() permission_uid: {0}", permission_uid);
                    return new Uid(permission_uid);
                } else {
                    throw new ConnectorException("createPermission() failed: No generated key returned.");
                }
            }
        } catch (SQLException e) {
            throw new ConnectorException("createPermission() failed: " + e.getMessage(), e);
        }
    }

    public void executeQuery(
        Filter filter,
        ResultsHandler resultsHandler,
        OperationOptions operationOptions,
        Connection connection) {

        String sql = "SELECT permission_uid, code, display_name FROM public.permission";

        LOG.info("Executing PermissionProcessing query: {0}", sql);
        

        try (PreparedStatement preparedStatement =
                connection.prepareStatement(sql)) {

            LOG.info("executeQuery() preparedStatement: {0}", preparedStatement);


            try (ResultSet resultSet = preparedStatement.executeQuery()) {
                while (resultSet.next()) {
                    Set<Attribute> attrs = new HashSet<>();
                    attrs.add(AttributeBuilder.build("code", resultSet.getString("code")));
                    attrs.add(AttributeBuilder.build("display_name", resultSet.getString("display_name")));

                    ConnectorObject obj = new ConnectorObjectBuilder()
                            .setUid(resultSet.getString("permission_uid"))
                            .setObjectClass(ObjectClass.ALL)
                            .addAttributes(attrs)
                            .build();

                    resultsHandler.handle(obj);
                }
            }

        } catch (SQLException e) {
            throw new ConnectorException("Failed to execute query for accounts: " + e.getMessage(), e);
        }
    }

    public void deletePermission(Uid uid, Connection connection) {
        final String sql = "DELETE FROM public.permission WHERE permission_uid = ?";

        LOG.info("Executing PermissionProcessing query: {0}", sql);

            try (PreparedStatement preparedStatement =
            connection.prepareStatement(sql)) {

                preparedStatement.setString(1, uid.toString());
                
                LOG.info("deletePermission() preparedStatement: {0}", preparedStatement);

                int affectedRows = preparedStatement.executeUpdate();
                if (affectedRows == 0) {
                    throw new ConnectorException("Failed to delete permission. Affected rows is 0");
                }

                LOG.info("Permission delete successfully: {0}", uid);

            } catch (SQLException e) {
            throw new ConnectorException("deletePermission() failed: " + e.getMessage(), e);
        }

    }

}
