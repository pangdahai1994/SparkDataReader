import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.InputStreamReader;
import java.sql.*;

import sql.SqlConnector;

public class RankUpdater {
	static String filepath="";
	static String encoding="UTF-8";
	static void updateMovie(String path){
		BufferedReader reader;
		try {
			Connection conn=new SqlConnector().getconnetion();
			Statement stmt = conn.createStatement();
			reader = new BufferedReader(new FileReader(new File(path+"\\someResults\\movieInfo-7min\\part-00000")));
			for (int i=0;i<2000;i++){
				String line = reader.readLine();
				int mid=Integer.parseInt(line.split(",")[0].replace("(", ""));
				int totalscore=Integer.parseInt(line.split(",")[1]);
				int watchernum=Integer.parseInt(line.split(",")[2]);
				int averagescore=Integer.parseInt(line.split(",")[3].replace(")", ""));
				String sql= "insert into movie_rank values (?,?,?,?) on duplicate key update totalscore=?,watchernum=?,averagescore=?";
				PreparedStatement pst = conn.prepareStatement(sql);
				pst.setInt(1, mid);
				pst.setInt(2, totalscore);
				pst.setInt(3, watchernum);
				pst.setInt(4, averagescore);
				pst.setInt(5, totalscore);
				pst.setInt(6, watchernum);
				pst.setInt(7, averagescore);
				pst.executeUpdate();
			}
			reader.close();
			stmt.close();
            conn.close();
    		System.out.println("movie_rank updated");
		} catch (Exception e) {
			e.printStackTrace();
    		System.out.println("movie_rank failed");
		}
	}
	
	static void updateActor(String path){
		BufferedReader reader;
		try {
			Connection conn=new SqlConnector().getconnetion();
			Statement stmt = conn.createStatement();
			reader = new BufferedReader(new InputStreamReader(new FileInputStream(new File(path+"\\someResults\\variance-30min\\actorVar\\part-00000")),"utf-8"));
			for (int i=1;i<8000;i++){
				String line = reader.readLine();
				int aid=i;
				String aname=line.split(",")[0].replace("(", "");
				double totalscore=Double.parseDouble(line.split(",")[1]);
				int fans=(int)Double.parseDouble(line.split(",")[3]);
				double averagescore=Double.parseDouble(line.split(",")[5].replace(")", ""));
				String sql= "insert into actor_rank values (?,?,?,?,?) on duplicate key update aname=?,totalscore=?,fans=?,averagescore=?";
				PreparedStatement pst = conn.prepareStatement(sql);
				pst.setInt(1, aid);
				pst.setString(2, aname);
				pst.setDouble(3, totalscore);
				pst.setInt(4, fans);
				pst.setDouble(5, averagescore);
				pst.setString(6, aname);
				pst.setDouble(7, totalscore);
				pst.setInt(8, fans);
				pst.setDouble(9, averagescore);
				pst.executeUpdate();
			}
			reader.close();
			stmt.close();
            conn.close();
    		System.out.println("actor_rank updated");
		} catch (Exception e) {
			e.printStackTrace();
    		System.out.println("actor_rank failed");
		}
	}
	static void updateAll(String path){
		filepath=path;
		updateMovie(path);
		updateActor(path);
	}
}
