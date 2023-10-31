import java.sql.*;

/**
 * 类描述：TODO
 *
 * @author yzm
 * @date 2023-10-30 14:01
 **/
public class PhoenixJDBCExample {

    public static void main(String[] args) {
        Connection connection = null;
        try {
            Class.forName("org.apache.phoenix.jdbc.PhoenixDriver");
            connection = DriverManager.getConnection("jdbc:phoenix:<zookeeper_quorum>:<zookeeper_client_port>:<hbase_znode_parent>");

            String sql = "SELECT * FROM your_table_name";
            PreparedStatement statement = connection.prepareStatement(sql);
            ResultSet resultSet = statement.executeQuery();

            while (resultSet.next()) {
                // 处理查询结果
            }
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        } catch (SQLException e) {
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
