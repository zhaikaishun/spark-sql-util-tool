package me.kaishun.util;
import java.sql.*;
public class DBHelper
{
    Connection _CONN = null;
    String sUser;
    String sPwd;
    String ServerIp;
    String DbName;
    int port;

    public void setDBValue(String user, String pwd, String ip, String Dbname,int port)
    {
        this.sUser = user;
        this.sPwd = pwd;
        this.ServerIp = ip;
        this.DbName = Dbname;
        this.port = port;
    }

	/*
	 * public DBHelper(String sUser, String sPwd, String ServerIp, String
	 * DbName) { this.sUser = sUser; this.sPwd = sPwd; this.ServerIp = ServerIp;
	 * this.DbName = DbName; }
	 */

    // 取得连接
    public Connection GetConn()
    {
        if (_CONN != null)
            return _CONN;
        try
        {
            String sDriverName = "com.mysql.jdbc.Driver";
            String sDBUrl = "jdbc:mysql://" + ServerIp +":"+port+ "/"+DbName+"?useUnicode=true&characterEncoding=utf8";
            Class.forName(sDriverName);
            _CONN = DriverManager.getConnection(sDBUrl, sUser, sPwd);
        } catch (Exception ex)
        {
            ex.printStackTrace();
            return null;
        }
        return _CONN;
    }

    // 关闭连接
    public void CloseConn()
    {
        try
        {
            _CONN.close();
            _CONN = null;
        } catch (Exception ex)
        {
            System.out.println(ex.getMessage());
            _CONN = null;
        }
    }

    // 测试连接
    public boolean TestConn()
    {
        if (GetConn()==null)
            return false;

        CloseConn();
        return true;
    }

    /**
     * 批量得到result
     * @param sSQL
     * @param objParams
     * @return
     */
    public ResultSet GetResultSet(String sSQL, Object[] objParams)
    {
        GetConn();
        ResultSet rs = null;
        try
        {
            PreparedStatement ps = _CONN.prepareStatement(sSQL);
            if (objParams != null)
            {
                for (int i = 0; i < objParams.length; i++)
                {
                    ps.setObject(i + 1, objParams[i]);
                }
            }
            rs = ps.executeQuery();
        } catch (Exception ex)
        {
            System.out.println(ex.getMessage());
            CloseConn();
        } finally
        {
             CloseConn();
        }
        return rs;
    }

    public ResultSet GetResultSet(String sSQL)
    {
        GetConn();
        ResultSet iResult = null;

        try
        {
            Statement ps = _CONN.createStatement();
            iResult = ps.executeQuery(sSQL);
        } catch (Exception ex)
        {
            ex.printStackTrace();
            return iResult;
        }
        finally
        {
//            CloseConn();
        }
        return iResult;
    }


    public Object GetSingle(String sSQL, Object... objParams)
    {
        GetConn();
        try
        {
            PreparedStatement ps = _CONN.prepareStatement(sSQL);
            if (objParams != null)
            {
                for (int i = 0; i < objParams.length; i++)
                {
                    ps.setObject(i + 1, objParams[i]);
                }
            }
            ResultSet rs = ps.executeQuery();
            if (rs.next())
                return rs.getString(1);// 索引从1开始
        } catch (Exception ex)
        {
            System.out.println(ex.getMessage());
        } finally
        {
            CloseConn();
        }
        return null;
    }


    public int updateSql(String sSQL)
    {
        GetConn();
        int iResult = 0;

        try
        {
            Statement ps = _CONN.createStatement();
            iResult = ps.executeUpdate(sSQL);
        } catch (Exception ex)
        {
            System.out.println(ex.getMessage());
            return -1;
        }
        finally
        {
            CloseConn();
        }
        return iResult;
    }

}
