import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

/**
 * 类描述：TODO
 *
 * @author yzm
 * @date 2023-11-02 16:53
 **/
public class ClickHouseConnectionTest {

    public static void main(String[] args) {
        String url = "jdbc:clickhouse://hadoop101:8123/default";

        Connection connection = null;

        try {
            Class.forName("ru.yandex.clickhouse.ClickHouseDriver");
            connection = DriverManager.getConnection(url);
            System.out.println("Connected to ClickHouse successfully");
        } catch (ClassNotFoundException e) {
            System.err.println("ClickHouse JDBC driver not found");
            e.printStackTrace();
        } catch (SQLException e) {
            System.err.println("Failed to connect to ClickHouse");
            e.printStackTrace();
        } finally {
            if (connection != null) {
                try {
                    connection.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }
    }
}
