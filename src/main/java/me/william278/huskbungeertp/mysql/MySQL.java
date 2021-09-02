package me.william278.huskbungeertp.mysql;

import me.william278.huskbungeertp.HuskBungeeRTP;
import me.william278.huskbungeertp.config.Group;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.logging.Level;

public class MySQL extends Database {

    private Connection connection;

    public MySQL(HuskBungeeRTP instance) {
        super(instance);
    }

    final String host = HuskBungeeRTP.getSettings().getDatabaseHost();
    final int port = HuskBungeeRTP.getSettings().getDatabasePort();
    final String database = HuskBungeeRTP.getSettings().getDatabaseName();
    final String username = HuskBungeeRTP.getSettings().getDatabaseUsername();
    final String password = HuskBungeeRTP.getSettings().getDatabasePassword();
    final String params = HuskBungeeRTP.getSettings().getDatabaseParams();

    @Override
    public Connection getConnection() {
        try {
            if (connection == null || connection.isClosed()) {
                try {
                    synchronized (HuskBungeeRTP.getInstance()) {
                        Class.forName("com.mysql.cj.jdbc.Driver");
                        connection = (DriverManager.getConnection("jdbc:mysql://" + host + ":" + port + "/" + database + params, username, password));
                    }
                } catch (SQLException ex) {
                    plugin.getLogger().log(Level.SEVERE, "An exception occurred initialising the mySQL database: ", ex);
                } catch (ClassNotFoundException ex) {
                    plugin.getLogger().log(Level.SEVERE, "The mySQL JBDC library is missing! Please download and place this in the /lib folder.");
                }
            }
        } catch (SQLException exception) {
            plugin.getLogger().log(Level.WARNING, "An error occurred checking the status of the SQL connection: ", exception);
        }
        return connection;
    }

    @Override
    public void load() {
        plugin.getLogger().info("Establishing connection to the mySQL Database...");
        connection = getConnection();
        try(Statement tableCreationStatement = connection.createStatement()) {
            // Create player table statement
            tableCreationStatement.execute("CREATE TABLE IF NOT EXISTS " + HuskBungeeRTP.getSettings().getDatabasePlayerTableName() + " ("
                    + "`id` integer AUTO_INCREMENT PRIMARY KEY,"
                    + "`user_uuid` char(36) NOT NULL UNIQUE"
                    + ");");

            // Create group cool-down tables
            for (Group group : HuskBungeeRTP.getSettings().getGroups()) {
                tableCreationStatement.execute("CREATE TABLE IF NOT EXISTS " + group.getGroupDatabaseTableName() + " ("
                        + "`player_id` integer AUTO_INCREMENT PRIMARY KEY,"
                        + "`last_rtp` timestamp NOT NULL DEFAULT (CURRENT_TIMESTAMP),"
                        + "`dest_world` tinytext NOT NULL,"
                        + "`dest_x` double NOT NULL,"
                        + "`dest_y` double NOT NULL,"
                        + "`dest_z` double NOT NULL,"
                        + "`dest_server` tinytext NOT NULL"
                        + ");");
            }
        } catch (SQLException e) {
            plugin.getLogger().log(Level.SEVERE, "An error occurred creating tables: ", e);
        } finally {
            plugin.getLogger().info("Successfully initialized mySQL Database connection.");
        }
        initialize();
    }


}
