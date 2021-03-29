package org.airtel.ug;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.logging.Logger;
import oracle.ucp.jdbc.PoolDataSource;
import oracle.ucp.jdbc.PoolDataSourceFactory;


/**
 *
 * @author benjamin
 */
public class DBConnector {

    private static final Logger LOGGER = Logger.getLogger(DBConnector.class.getName());

    private static final String DB_USER = "PROMOTIONS";
    private static final String DB_PASS = "PROMUSR123";
    private static final String DB_URL = "jdbc:oracle:thin:@(DESCRIPTION=(ADDRESS=(PROTOCOL = TCP)(HOST = 172.27.98.149)(PORT = 1524))(ADDRESS=(PROTOCOL = TCP)(HOST = 172.27.98.150)(PORT = 1524))(LOAD_BALANCE = yes)(CONNECT_DATA = (SERVER = DEDICATED)(SERVICE_NAME = KIKADB)(FAILOVER_MODE = (TYPE = SELECT)(METHOD = BASIC)(RETRIES = 180)(DELAY = 5))))";
    private static PoolDataSource poolDataSource;

    static {
        try {
            System.out.println("Creating Connection Pool");

            //Creating a pool-enabled data source
            poolDataSource = PoolDataSourceFactory.getPoolDataSource();
            poolDataSource.setConnectionPoolName("KIKA_DB_POOL");
            poolDataSource.setConnectionFactoryClassName("oracle.jdbc.pool.OracleDataSource");
            poolDataSource.setURL(DB_URL);
            poolDataSource.setUser(DB_USER);
            poolDataSource.setPassword(DB_PASS);

            //Setting pool properties
            poolDataSource.setInitialPoolSize(5);
            poolDataSource.setMinPoolSize(10);
            poolDataSource.setMaxPoolSize(20);

            poolDataSource.setMaxConnectionReuseCount(100);

        } catch (SQLException ex) {

            ex.printStackTrace(System.out);

        }
    }

    public static Connection connect() throws SQLException {
        Connection connection;

        LOGGER.info("AVAILABLE_DB_CONNECTIONS " + poolDataSource.getAvailableConnectionsCount() + " Connections | " + poolDataSource.getConnectionPoolName() + " | " + Thread.currentThread().getName());
        LOGGER.info("BORROWED_DB_CONNECTIONS " + poolDataSource.getBorrowedConnectionsCount() + " Connections | " + poolDataSource.getConnectionPoolName() + " | " + Thread.currentThread().getName());

        connection = poolDataSource.getConnection();

        return connection;

    }

}//end of class
