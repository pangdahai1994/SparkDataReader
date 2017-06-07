package sql;
import java.sql.*;
public class SqlConnector {
	Connection conn;
	public SqlConnector(){
        try{
            Class.forName("com.mysql.jdbc.Driver");
            //System.out.println("加载MySQL驱动");
        }catch(ClassNotFoundException e1){
            System.out.println("找不到MySQL驱动!");
            e1.printStackTrace();
        }
        String url="jdbc:mysql://localhost:3306/spark_movie?useUnicode=true&characterEncoding=utf-8&useSSL=false";
        try {
            conn = DriverManager.getConnection(url,"root","zhuhaichao1248");
            //创建一个Statement对象
            //Statement stmt = conn.createStatement(); //创建Statement对象
            //System.out.println("连接到数据库");
            //stmt.close();
        } catch (SQLException e){
            e.printStackTrace();
        }
	}
    public Connection getconnetion(){
        return conn;
    }
}