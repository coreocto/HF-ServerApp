package org.coreocto.dev.hf.serverapp.servlet;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
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
    protected void doPost(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
        ServletContext ctx = getServletContext();

        Connection con = (Connection) ctx.getAttribute("DBConnection");

        Gson gson = (Gson) ctx.getAttribute("gson");

//	Enumeration<String> attrNames = request.getAttributeNames();
//	while (attrNames.hasMoreElements()){
//	    System.out.println("attributes");
//	    System.out.println(attrNames.nextElement());
//	}
//
//	Enumeration<String> paramNames = request.getParameterNames();
//	while (paramNames.hasMoreElements()){
//	    System.out.println("parameters");
//	    System.out.println(paramNames.nextElement());
//	}

        PrintWriter out = response.getWriter();

        String q = request.getParameter("data");
        String type = request.getParameter("type");

        JsonObject jsonObj = gson.fromJson(q, JsonObject.class);

        System.out.println(jsonObj);

        int affectRows = -1;

        try (PreparedStatement pStmnt = con.prepareStatement("INSERT INTO public.tstatistics(cdocid, cstarttime, cendtime, cwordcnt, cfilesize, ctype)" + "VALUES (?, ?, ?, ?, ?, ?)")) {

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
            affectRows = pStmnt.executeUpdate();
        } catch (SQLException e) {
            e.printStackTrace();
        }

        if (affectRows == -1) {
            JsonObject err = ResponseFactory.getResponse(ResponseFactory.ResponseType.GENERIC_JSON_ERR);
            out.print(err.toString());
        } else {
            JsonObject ok = ResponseFactory.getResponse(ResponseFactory.ResponseType.GENERIC_JSON_OK);
            out.print(ok.toString());
        }

        System.out.println("affectRows = " + affectRows);
    }

    protected void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
        response.getWriter().append("Served at: ").append(request.getContextPath());
    }
}
