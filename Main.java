import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

class UploaderThread implements Runnable {
    String[] columnDatatype;
    String sql = "INSERT INTO ";
    ArrayList<ArrayList<String>> data;
    ConnectionPool pool;
    Connection con;

    public UploaderThread(String[] columnDatatype, int n, BufferedReader reader, ConnectionPool pool, String tableName)
            throws Exception {
        this.columnDatatype = columnDatatype;
        this.con = pool.getConnection();
        this.pool = pool;
        this.sql = sql + tableName + " VALUES";

        this.data = new ArrayList<>();
        String row;
        while (n > 0) {
            row = reader.readLine();
            if (row == null) {
                break;
            }
            String[] data = row.split(",");
            ArrayList<String> dataList = new ArrayList<String>();
            for (int i = 0; i < data.length; i++) {
                dataList.add(data[i]);
            }
            this.data.add(dataList);
            n--;
        }
    }

    private String formatSQL(int n) {
        String result = "(";

        for (int i = 0; i < n; i++) {
            result += "?";
            if (i < n - 1) {
                result += ", ";
            }
        }

        result += ")";
        return result;
    }

    @Override
    public void run() {
        this.sql += formatSQL(this.columnDatatype.length);
        try {
            PreparedStatement ps = con.prepareStatement(sql);
            for (int j = 0; j < data.size(); j++) {
                for (int i = 0; i < columnDatatype.length; i++) {
                    switch (columnDatatype[i]) {
                        case "INT":
                            ps.setInt(i + 1, Integer.parseInt(data.get(j).get(i)));
                            break;
                        case "DECIMAL":
                            ps.setDouble(i + 1, Double.parseDouble(data.get(j).get(i)));
                            break;
                        case "TEXT":
                            ps.setString(i + 1, data.get(j).get(i));
                            break;
                    }
                }
                ps.executeUpdate();
            }
            ps.close();
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            try {
                this.con.commit();
            } catch (SQLException e) {
                e.printStackTrace();
            }

            this.pool.releaseConnection(this.con);
        }
    }
}

class ConnectionPool {
    private static final String JDBC_URL = "URL TO POSTGRES DATABASE";
    private static final String USERNAME = "DATABASE USERNAME";
    private static final String PASSWORD = "DATABASE PASSWORD";

    private List<Connection> connectionPool = new ArrayList<>();
    private int maxPoolSize;

    public ConnectionPool(int maxPoolSize) {
        this.maxPoolSize = maxPoolSize;
        initializeConnectionPool();
    }

    private void initializeConnectionPool() {
        try {
            for (int i = 0; i < maxPoolSize; i++) {
                Connection connection = DriverManager.getConnection(JDBC_URL, USERNAME, PASSWORD);
                connection.setAutoCommit(false);
                connectionPool.add(connection);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    synchronized public Connection getConnection() throws Exception {
        if (connectionPool.isEmpty()) {
            throw new Exception("Connection pool is empty");
        }
        Connection connection = connectionPool.remove(connectionPool.size() - 1);
        return connection;
    }

    synchronized public void releaseConnection(Connection connection) {
        if (connection != null) {
            connectionPool.add(connection);
        }
    }

    public void closeAllConnections() {
        for (Connection connection : connectionPool) {
            try {
                connection.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        connectionPool.clear();
    }
}

public class Main {

    public static String[] inferSQLDataType(String filePath) throws IOException {
        BufferedReader reader = new BufferedReader(new FileReader(filePath));
        reader.readLine();

        String columns[] = reader.readLine().split(",");
        String[] sqlDataTypes = new String[columns.length];
        for (int i = 0; i < columns.length; i++) {
            String column = columns[i].trim();

            try {
                Integer.parseInt(column);
                sqlDataTypes[i] = "INT";
                continue;
            } catch (NumberFormatException e) {
            }

            try {
                Double.parseDouble(column);
                sqlDataTypes[i] = "DECIMAL";
                continue;
            } catch (NumberFormatException e) {
            }

            sqlDataTypes[i] = "TEXT";
        }

        reader.close();

        return sqlDataTypes;
    }

    public static int countLines(String filename) throws IOException {
        InputStream is = new BufferedInputStream(new FileInputStream(filename));
        try {
            byte[] c = new byte[1024];
            int readChars = is.read(c);
            if (readChars == -1) {
                return 0;
            }
            int count = 0;
            while (readChars == 1024) {
                for (int i = 0; i < 1024; i++) {
                    if (c[i] == '\n') {
                        ++count;
                    }
                }
                readChars = is.read(c);
            }
            while (readChars != -1) {
                for (int i = 0; i < readChars; ++i) {
                    if (c[i] == '\n') {
                        ++count;
                    }
                }
                readChars = is.read(c);
            }
            return count == 0 ? 1 : count;
        } finally {
            is.close();
        }
    }

    public static int iterations(String filePath) {
        File f = new File(filePath);
        return (int) Math.ceil((double) f.length() / (Runtime.getRuntime().freeMemory()));
    }

    public static void main(String[] args) throws Exception {
        int connectionsCount = 3;
        ConnectionPool connectionPool;
        int totalTime = 0;
        String filePath = "[PROVIDE CSV FILE PATH]";
        int count;
        long start, end;

        System.out.printf("%-50s", "Initializing connection pool");
        start = System.currentTimeMillis();
        connectionPool = new ConnectionPool(connectionsCount);
        end = System.currentTimeMillis();
        System.out.println(end - start + "ms");
        totalTime += (end - start);

        System.out.printf("%-50s", "Reading number of lines");
        start = System.currentTimeMillis();
        count = countLines(filePath);
        end = System.currentTimeMillis();
        System.out.println(end - start + "ms");
        totalTime += (end - start);

        // Creating table in database
        System.out.printf("%-50s", "Creating table in database");
        start = System.currentTimeMillis();
        Connection con = connectionPool.getConnection();
        Statement stmt = con.createStatement();
        String tableName = new File(filePath).getName().split("\\.")[0];
        String sql = "CREATE TABLE " + tableName + " (";
        String[] columns = new BufferedReader(new FileReader(new File(filePath))).readLine().split(",");
        String[] sqlEquivalent = inferSQLDataType(filePath);

        for (int i = 0; i < columns.length; i++) {
            sql += "\"" + columns[i] + "\" " + sqlEquivalent[i] + ((i == columns.length -
                    1) ? "" : ",");
        }
        sql += ")";
        stmt.executeUpdate("DROP TABLE IF EXISTS " + tableName);
        stmt.executeUpdate(sql);
        stmt.close();
        con.commit();
        connectionPool.releaseConnection(con);
        end = System.currentTimeMillis();
        System.out.println(end - start + "ms");
        totalTime += (end - start);

        int iterations = iterations(filePath);
        if (iterations <= 1) {
            // We can assume that its safe to load whole file in memory
            BufferedReader reader = new BufferedReader(new FileReader(filePath));
            reader.readLine();
            int rowsPerThread = (int) Math.ceil(count / (connectionsCount - 1));
            ArrayList<Thread> threads = new ArrayList<Thread>();
            String[] dataType = inferSQLDataType(filePath);
            System.out.printf("%-50s", "Initializing threads");
            start = System.currentTimeMillis();
            for (int i = 0; i < connectionsCount; i++) {
                threads.add(new Thread(new UploaderThread(dataType, rowsPerThread, reader, connectionPool, tableName)));
            }
            end = System.currentTimeMillis();
            System.out.println(end - start + "ms");
            totalTime += (end - start);
            reader.close();
            System.out.printf("%-50s", "Pushing data into database");
            start = System.currentTimeMillis();

            for (Thread thread : threads) {
                thread.start();
            }

            for (Thread thread : threads) {
                thread.join();
            }
            end = System.currentTimeMillis();
            System.out.println(end - start + "ms");
            totalTime += (end - start);
        } else {
            System.out.println("Intitializing " + iterations + " batches");
            BufferedReader reader = new BufferedReader(new FileReader(filePath));
            reader.readLine();
            int rowsPerThread = (count) / ((connectionsCount - 1) * iterations);
            String[] dataType = inferSQLDataType(filePath);
            for (int i = 0; i < iterations; i++) {
                System.out.printf("%-46s", "\tPushing batch " + (i + 1));
                start = System.currentTimeMillis();

                Thread th = new Thread(new Runnable() {
                    public void run() {
                        ArrayList<Thread> threads = new ArrayList<Thread>();

                        for (int j = 0; j < connectionsCount; j++) {
                            try {
                                threads.add(new Thread(
                                        new UploaderThread(dataType, rowsPerThread, reader, connectionPool,
                                                tableName)));
                            } catch (Exception e) {
                                e.printStackTrace();
                            }
                        }
                        for (Thread thread : threads) {
                            thread.start();
                        }
                        for (Thread thread : threads) {
                            try {
                                thread.join();
                            } catch (InterruptedException e) {
                                e.printStackTrace();
                            }
                        }
                    }
                });
                th.start();
                th.join();
                end = System.currentTimeMillis();
                System.out.println(end - start + "ms");
                totalTime += (end - start);
            }
            reader.close();
        }
        System.out.printf("%-50s%dms", "Total Time: ", totalTime);
    }
}
