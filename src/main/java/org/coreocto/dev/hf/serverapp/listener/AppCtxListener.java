package org.coreocto.dev.hf.serverapp.listener;

import com.google.gson.Gson;
import org.apache.log4j.Logger;

import javax.servlet.ServletContext;
import javax.servlet.ServletContextEvent;
import javax.servlet.ServletContextListener;
import javax.servlet.annotation.WebListener;
import java.sql.Connection;
import java.sql.SQLException;

@WebListener
public class AppCtxListener implements ServletContextListener {

    final static Logger LOGGER = Logger.getLogger(AppCtxListener.class);

    public void contextDestroyed(ServletContextEvent arg0) {
        Connection con = (Connection) arg0.getServletContext().getAttribute("DBConnection");
        try {
            con.close();
        } catch (SQLException e) {
            LOGGER.error("error when closing database connection", e);
        }
    }

    public void contextInitialized(ServletContextEvent arg0) {
        // TODO Auto-generated method stub
        ServletContext ctx = arg0.getServletContext();

        LOGGER.debug("contextPath = " + ctx.getContextPath());

        Gson gson = new Gson();
        ctx.setAttribute("gson", gson);
    }
}
