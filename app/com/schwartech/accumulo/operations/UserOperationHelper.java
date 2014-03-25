package com.schwartech.accumulo.operations;

import com.schwartech.accumulo.AccumuloPlugin;
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
public class UserOperationHelper {
    private AccumuloPlugin plugin;

    public UserOperationHelper(AccumuloPlugin plugin) {
        this.plugin = plugin;
    }

    public boolean validateUser(String username, String password) {
        boolean success = false;
        try {
            PasswordToken token = new PasswordToken(password.getBytes());
            success = plugin.getConnector().securityOperations().authenticateUser(username, token);
        } catch (Exception e) {
            Logger.error("Error validating user", e);
        }
        return success;
    }

    public void createUser(String username, String password) throws AccumuloSecurityException, AccumuloException {
        PasswordToken token = new PasswordToken(password);

        if(!getUsernames().contains(username)) {
            Logger.debug("user " + username + " dont exist");
            plugin.getConnector().securityOperations().createLocalUser(username, token);
            Logger.debug("user " + username + " created");
        }
    }

    public Set<String> getUsernames() throws AccumuloSecurityException, AccumuloException {
        return plugin.getConnector().securityOperations().listLocalUsers();
    }

    public void removeRole(String username, String role) throws AccumuloSecurityException, AccumuloException {
        Set<byte[]> roles = new HashSet(getRolesAsSet(username));

        roles.remove(role.getBytes());

        saveUpdateRoles(username, roles);
    }

    private void saveUpdateRoles(String username, Set<byte[]> roles) throws AccumuloSecurityException, AccumuloException {
        Authorizations newAuths = new Authorizations(roles);
        plugin.getConnector().securityOperations().changeUserAuthorizations(username, newAuths);
    }

    public void addRole(String username, String role) throws AccumuloSecurityException, AccumuloException {
        Set<byte[]> roles = new HashSet(getRolesAsSet(username));
        roles.add(role.getBytes());

        saveUpdateRoles(username, roles);

        if (!plugin.username.equals(username)) {
            addRole(plugin.username, role);
        }
    }

    public Set<byte[]> getRolesAsSet(String username) throws AccumuloSecurityException, AccumuloException {
        Authorizations auths = plugin.getConnector().securityOperations().getUserAuthorizations(username);
        Set<byte[]> roles = new HashSet<byte[]>();
        for (byte[] role : auths) {
            roles.add(role);
        }
        return roles;
    }

//    public String getRolesAsString(String username) throws AccumuloSecurityException, AccumuloException {
//        Authorizations auths = plugin.getConnector().securityOperations().getUserAuthorizations(username);
//        StringBuilder roles = new StringBuilder();
//        for (byte[] auth : auths.getAuthorizations()) {
//            roles.append(new String(auth));
//            roles.append(", ");
//        }
//
//        return trimRoles(roles);
//    }
//
//    private String trimRoles(StringBuilder roles) {
//        if (roles.lastIndexOf(",") > 0) {
//            roles.delete(roles.lastIndexOf(","), roles.lastIndexOf(",")+1);
//        }
//        return roles.toString().trim();
//    }

}