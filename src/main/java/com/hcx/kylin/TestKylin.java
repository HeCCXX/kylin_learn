package com.hcx.kylin;

import org.apache.kylin.jdbc.Driver;

import java.sql.*;

/**
 * @ClassName TestKylin
 * @Description TODO
 * @Author 贺楚翔
 * @Date 2020-03-20 15:04
 * @Version 1.0
 **/
public class TestKylin {
    public static void main(String[] args) throws ClassNotFoundException, SQLException {

        //kylin jdbc驱动
        String driver = "org.apache.kylin.jdbc.Driver";
        // kylin url
        String url = "jdbc:kylin://hadoop1:7070/hcx_kylin";
        //username
        String user = "ADMIN";
        String password = "KYLIN";
        //加载驱动
        Class.forName(driver);
        //获取连接
        Connection connection = DriverManager.getConnection(url, user, password);
        //预编译sql
        String sql = "select dname,sum(sal) from emp e join dept d on e.deptno =d.depto group by dname;";
        PreparedStatement ps = connection.prepareStatement(sql);

        //执行sql
        ResultSet resultSet = ps.executeQuery();

        //获取结果
        while (resultSet.next()){
            System.out.println(resultSet.getString(1)+"\t"+resultSet.getDouble(2));
        }


    }
}
