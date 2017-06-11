import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.InputStreamReader;
import java.sql.*;

import sql.SqlConnector;

public class RecommandUpdater {
	static String filepath="";
	static String encoding="UTF-8";
	static void updateMovie(String path){
		try {
			BufferedReader reader;
			Connection conn=new SqlConnector().getconnetion();
			Statement stmt = conn.createStatement();
			String sql= "truncate reco_by_group";
			PreparedStatement pst = conn.prepareStatement(sql);
			pst.executeUpdate();
			String s1="\\res\\k";
			String s2="\\recoByGroup\\";
			for (int k=4;k<=7;k++){
				for(int l=0;l<k;l++){
					reader = new BufferedReader(new FileReader(new File(path+s1+k+s2+l+"\\part-00000")));
					String line;
					while ((line=reader.readLine())!=null){
						int mid=Integer.parseInt(line.split(",")[0].replace("(", ""));
						double score=Double.parseDouble(line.split(",")[1].replace(")", ""));
						sql= "insert into reco_by_group values (?,?,?,?)";
						pst = conn.prepareStatement(sql);
						pst.setInt(1, k);
						pst.setInt(2, mid);
						pst.setInt(3, l);
						pst.setDouble(4, score);
						pst.executeUpdate();
					}
					reader.close();
				}
			}
			stmt.close();
            conn.close();
    		System.out.println("reco_by_group updated");
		} catch (Exception e) {
			e.printStackTrace();
    		System.out.println("reco_by_group failed");
		}
	}
	
	static void updateRecommand(String path){
		try {
			BufferedReader reader;
			Connection conn=new SqlConnector().getconnetion();
			Statement stmt = conn.createStatement();
			String sql= "truncate reco_by_group";
			PreparedStatement pst = conn.prepareStatement(sql);
			pst.executeUpdate();
			String s1="\\res\\k";
			String s2="\\userGroup\\part-00000";
			for (int k=4;k<=7;k++){
					reader = new BufferedReader(new FileReader(new File(path+s1+k+s2)));
					String line;
					while ((line=reader.readLine())!=null){
						int uid=Integer.parseInt(line.split(",")[0].replace("(", ""));
						int group=Integer.parseInt(line.split(",")[1].replace(")", ""));
						sql= "insert into k"+k+"user_group values (?,?)";
						pst = conn.prepareStatement(sql);
						pst.setInt(1, uid);
						pst.setInt(2, group);
						pst.executeUpdate();
					}
					reader.close();
			}
			stmt.close();
            conn.close();
    		System.out.println("reco_by_group updated");
		} catch (Exception e) {
			e.printStackTrace();
    		System.out.println("reco_by_group failed");
		}
	}
	
	static void updateAll(String path){
		filepath=path;
	}
}
