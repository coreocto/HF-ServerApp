package org.coreocto.dev.hf.serverapp.servlet;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import org.apache.log4j.Logger;
import org.coreocto.dev.hf.serverapp.db.DataSource;
import org.coreocto.dev.hf.serverapp.factory.ResponseFactory;

import javax.servlet.ServletContext;
import javax.servlet.ServletException;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

@WebServlet(
        urlPatterns = "/stat",
        name = "StatisticsServlet"
)
public class StatisticsServlet extends HttpServlet {

    final static Logger LOGGER = Logger.getLogger(StatisticsServlet.class);

    protected void doPost(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
        ServletContext ctx = getServletContext();

        Gson gson = (Gson) ctx.getAttribute("gson");

        PrintWriter out = response.getWriter();

        String q = request.getParameter("data");
        String type = request.getParameter("type");

        JsonObject jsonObj = null;

        try {
            jsonObj = gson.fromJson(q, JsonObject.class);
        } catch (Exception ex) {
            response.setStatus(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
            String msg = "error when parsing json into object";
            LOGGER.error(msg, ex);
            out.write(msg);
        }

        if (jsonObj != null) {
            int affectRows = -1;

            try (Connection con = DataSource.getConnection();
                 PreparedStatement pStmnt = con.prepareStatement("INSERT INTO public.tstatistics(cdocid, cstarttime, cendtime, cwordcnt, cfilesize, ctype, cfpr)" + "VALUES (?, ?, ?, ?, ?, ?,?)")) {

                pStmnt.setString(1, jsonObj.get("name").getAsString());
                pStmnt.setLong(2, jsonObj.get("startTime").getAsLong());
                pStmnt.setLong(3, jsonObj.get("endTime").getAsLong());
                if (jsonObj.get("wordCount") != null) {
                    pStmnt.setLong(4, jsonObj.get("wordCount").getAsLong());
                } else {
                    pStmnt.setNull(4, java.sql.Types.BIGINT);
                }
                if (jsonObj.get("fileSize") != null) {
                    pStmnt.setLong(5, jsonObj.get("fileSize").getAsLong());
                } else {
                    pStmnt.setNull(5, java.sql.Types.BIGINT);
                }
                pStmnt.setString(6, type);
                if (jsonObj.get("fpr") != null) {
                    pStmnt.setDouble(7, jsonObj.get("fpr").getAsDouble());
                } else {
                    pStmnt.setNull(7, java.sql.Types.DECIMAL);
                }
                affectRows = pStmnt.executeUpdate();

            } catch (SQLException e) {
                String msg = "error when inserting record into tstatistics";
                LOGGER.error(msg, e);
                affectRows = -1;
            }

            JsonObject jsonObject = null;

            if (affectRows == -1) {
                jsonObject = ResponseFactory.getResponse(ResponseFactory.ResponseType.GENERIC_JSON_ERR);
                response.setStatus(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
            } else {
                jsonObject = ResponseFactory.getResponse(ResponseFactory.ResponseType.GENERIC_JSON_OK);
            }

            out.print(jsonObject.toString());
        }
    }

    protected void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
        response.getWriter().append("Served at: ").append(request.getContextPath());
    }
}
