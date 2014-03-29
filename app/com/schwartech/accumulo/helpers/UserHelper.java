package com.schwartech.accumulo.helpers;

import com.schwartech.accumulo.Accumulo;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.security.Authorizations;
import play.Logger;

import java.util.HashSet;
import java.util.Set;

/**
 * Created by jeff on 3/24/14.
 */
public class UserHelper {

    public static boolean userExists(String username) throws AccumuloSecurityException, AccumuloException {
        return Accumulo.getConnector().securityOperations().listLocalUsers().contains(username);
    }

    public static boolean validateUser(String username, String password) {
        boolean success = false;
        try {
            PasswordToken token = new PasswordToken(password.getBytes());
            success = Accumulo.getConnector().securityOperations().authenticateUser(username, token);
        } catch (Exception e) {
            Logger.error("Error validating user", e);
        }
        return success;
    }

    public static void createUser(String username, String password) throws AccumuloSecurityException, AccumuloException {
        PasswordToken token = new PasswordToken(password);

        if(!getUsernames().contains(username)) {
            Logger.debug("user " + username + " dont exist");
            Accumulo.getConnector().securityOperations().createLocalUser(username, token);
            Logger.debug("user " + username + " created");
        }
    }

    public static void deleteUser(String username) throws AccumuloSecurityException, AccumuloException {
        Accumulo.getConnector().securityOperations().dropLocalUser(username);
    }

    public static Set<String> getUsernames() throws AccumuloSecurityException, AccumuloException {
        return Accumulo.getConnector().securityOperations().listLocalUsers();
    }

    public static void removeRole(String username, String role) throws AccumuloSecurityException, AccumuloException {
        Set<byte[]> roles = new HashSet(getRolesAsSet(username));

        roles.remove(role.getBytes());

        saveUpdateRoles(username, roles);
    }

    private static void saveUpdateRoles(String username, Set<byte[]> roles) throws AccumuloSecurityException, AccumuloException {
        Authorizations newAuths = new Authorizations(roles);
        Accumulo.getConnector().securityOperations().changeUserAuthorizations(username, newAuths);
    }

    public static void addRole(String username, String role) throws AccumuloSecurityException, AccumuloException {
        Set<byte[]> roles = new HashSet(getRolesAsSet(username));
        roles.add(role.getBytes());

        saveUpdateRoles(username, roles);
    }

    public static Authorizations getAuths(String username) throws AccumuloSecurityException, AccumuloException {
        return Accumulo.getConnector().securityOperations().getUserAuthorizations(username);
    }

    public static Set<byte[]> getRolesAsSet(String username) throws AccumuloSecurityException, AccumuloException {
        Authorizations auths = getAuths(username);
        Set<byte[]> roles = new HashSet<byte[]>();
        for (byte[] role : auths) {
            roles.add(role);
        }
        return roles;
    }
}