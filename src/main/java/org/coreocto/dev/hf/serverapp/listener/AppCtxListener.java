package org.coreocto.dev.hf.serverapp.listener;

import org.coreocto.dev.hf.serverapp.db.DBConnMgr;

import java.sql.Connection;
import java.sql.SQLException;

import javax.servlet.ServletContext;
import javax.servlet.ServletContextEvent;
import javax.servlet.ServletContextListener;
import javax.servlet.annotation.WebListener;

import com.google.gson.Gson;

@WebListener
public class AppCtxListener implements ServletContextListener {

    public void contextDestroyed(ServletContextEvent arg0) {
        Connection con = (Connection) arg0.getServletContext().getAttribute("DBConnection");
        try {
            con.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public void contextInitialized(ServletContextEvent arg0) {
        // TODO Auto-generated method stub
        ServletContext ctx = arg0.getServletContext();

        System.out.println("contextPath = " + ctx.getContextPath());

        // initialize DB Connection
        String dbURL = ctx.getInitParameter("dbURL");
        String user = ctx.getInitParameter("dbUser");
        String pwd = ctx.getInitParameter("dbPassword");

        try {
            DBConnMgr connectionManager = new DBConnMgr(dbURL, user, pwd);
            ctx.setAttribute("DBConnection", connectionManager.getConnection());
        } catch (Exception e) {
            e.printStackTrace();
        }

        Gson gson = new Gson();
        ctx.setAttribute("gson", gson);
    }
}
