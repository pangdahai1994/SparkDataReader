import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.util.Scanner;
import java.sql.*;

import sql.SqlConnector;

public class KResultUpdater {
	static String filepath="";
	static String encoding="UTF-8";
	static void update(){
		//File file=new File(filepath);
		Scanner reader;
		try {
			Connection conn=new SqlConnector().getconnetion();
			Statement stmt = conn.createStatement();
			for (int i=2;i<=12;i++){
				reader = new Scanner(new FileInputStream(filepath+"\\k-meansInfo\\k"+i),encoding);
				double dis=reader.nextDouble();
				String sql= "update k_means_result set TotalDistance=? where K=?";
				PreparedStatement pst = conn.prepareStatement(sql);
				pst.setDouble(1, dis);
				pst.setInt(2, i);
				pst.executeUpdate();
				reader.close();
			}
			stmt.close();
            conn.close();
    		System.out.println("k-means result updated");
		} catch (Exception e) {
			System.out.println("k-means result failed");
			e.printStackTrace();
		}
	}
	static void update(String path){
		filepath=path;
		update();
	}
}
