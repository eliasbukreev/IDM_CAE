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


public class AccountProcessing extends Processing{

    private static final Log LOG = Log.getLog(ADLKConnection.class);
    private static final String ATTR_USERNAME = "username";
    private static final String ATTR_FULL_NAME = "full_name";
    private static final String ATTR_EMAIL = "email";
    private static final String ATTR_IS_ACTIVE = "is_active";

    public Uid createAccount(
    Set<Attribute> createAttributes, 
    Connection connection) {
            
        LOG.info("createAccount() attributes: {0}", createAttributes);

        final String sql = """
            INSERT INTO public.accounts
            (username, full_name, email, is_active, created_at, last_modified_at)
            VALUES (?,?,?,?, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
            RETURNING account_id
        """;

        LOG.info("Executing AccountProcessing query: {0}", sql);

        try (PreparedStatement preparedStatement =
            connection.prepareStatement(sql, Statement.RETURN_GENERATED_KEYS)) {

            String username = getString(createAttributes, ATTR_USERNAME);
            String fullName = getString(createAttributes, ATTR_FULL_NAME);
            String email = getString(createAttributes, ATTR_EMAIL);
            boolean isActive = getBool(createAttributes, ATTR_IS_ACTIVE, true);

            LOG.info("createAccount() preparedStatement: {0}", preparedStatement);
            LOG.info("createAccount() username: {0}", username);
            LOG.info("createAccount() fullName: {0}", fullName);
            LOG.info("createAccount() email: {0}", email);
            LOG.info("createAccount() isActive: {0}", isActive);

            preparedStatement.setString(1, username);
            preparedStatement.setString(2, fullName);
            preparedStatement.setString(3, email);
            preparedStatement.setBoolean(4, isActive);

            int affectedRows = preparedStatement.executeUpdate();
            if (affectedRows == 0) {
                throw new ConnectorException("Failed to create account. Affected rows is 0");
            }

            try (ResultSet resultSet = preparedStatement.getGeneratedKeys()) {
                if (resultSet.next()) {
                    String accountId = resultSet.getString(1);
                    LOG.info("createAccount() accountId: {0}", accountId);
                    return new Uid(accountId);
                } else {
                    throw new ConnectorException("createAccount() failed: No generated key returned.");
                }
            }
        } catch (SQLException e) {
            throw new ConnectorException("createAccount() failed: " + e.getMessage(), e);
        }
    }

    public void executeQuery(
        Filter filter, 
        ResultsHandler resultsHandler, 
        OperationOptions operationOptions,
        Connection connection) {

        String sql = "SELECT account_id, username FROM public.accounts";

        LOG.info("Executing AccountProcessing query: {0}", sql);
        

        try (PreparedStatement preparedStatement =
                connection.prepareStatement(sql)) {

            LOG.info("executeQuery() preparedStatement: {0}", preparedStatement);


            try (ResultSet resultSet = preparedStatement.executeQuery()) {
                while (resultSet.next()) {
                    Set<Attribute> attrs = new HashSet<>();
                    attrs.add(AttributeBuilder.build("username", resultSet.getString("username")));

                    ConnectorObject obj = new ConnectorObjectBuilder()
                            .setUid(resultSet.getString("account_id"))
                            .setObjectClass(ObjectClass.ACCOUNT)
                            .addAttributes(attrs)
                            .build();

                    resultsHandler.handle(obj);
                }
            }

        } catch (SQLException e) {
            throw new ConnectorException("Failed to execute query for accounts: " + e.getMessage(), e);
        }
    }

    public Uid updateAccount(Uid uid, Set<Attribute> updateAttributes, Connection connection) {

        String sql = "UPDATE public.accounts SET username = ?, full_name = ?, email = ?, is_active= ? WHERE account_id =  ?";

        LOG.info("Executing AccountProcessing query: {0}", sql);

         try (PreparedStatement preparedStatement =
            connection.prepareStatement(sql)) {

                String username = getString(updateAttributes, ATTR_USERNAME);
                String fullName = getString(updateAttributes, ATTR_FULL_NAME);
                String email = getString(updateAttributes, ATTR_EMAIL);
                boolean isActive = getBool(updateAttributes, ATTR_IS_ACTIVE, true);

                LOG.info("updateAccount() preparedStatement: {0}", preparedStatement);
                LOG.info("updateAccount() username: {0}", username);
                LOG.info("updateAccount() fullName: {0}", fullName);
                LOG.info("updateAccount() email: {0}", email);
                LOG.info("updateAccount() isActive: {0}", isActive);

                preparedStatement.setString(1, username);
                preparedStatement.setString(2, fullName);
                preparedStatement.setString(3, email);
                preparedStatement.setBoolean(4, isActive);
                preparedStatement.setString(5, uid.toString());

                int affectedRows = preparedStatement.executeUpdate();
                if (affectedRows == 0) {
                    throw new ConnectorException("Failed to update account. Affected rows is 0");
                }

                LOG.info("Account updated successfully: {0}", uid);
                return uid;
            } catch (SQLException e) {
            throw new ConnectorException("updateAccount() failed: " + e.getMessage(), e);
        }
    }

    public void deleteAccount(Uid uid, Connection connection) {
        final String sql = "DELETE FROM public.accounts WHERE account_id = ?";

        LOG.info("Executing AccountProcessing query: {0}", sql);

            try (PreparedStatement preparedStatement =
            connection.prepareStatement(sql)) {

                preparedStatement.setString(1, uid.toString());
                
                LOG.info("deleteAccount() preparedStatement: {0}", preparedStatement);

                int affectedRows = preparedStatement.executeUpdate();
                if (affectedRows == 0) {
                    throw new ConnectorException("Failed to delete account. Affected rows is 0");
                }

                LOG.info("Account delete successfully: {0}", uid);

            } catch (SQLException e) {
            throw new ConnectorException("deleteAccount() failed: " + e.getMessage(), e);
        }

    }

}
