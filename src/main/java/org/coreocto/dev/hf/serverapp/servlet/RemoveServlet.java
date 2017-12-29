package org.coreocto.dev.hf.serverapp.servlet;

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

@WebServlet(
        urlPatterns = "/remove",
        name = "RemoveServlet"
)
public class RemoveServlet extends HttpServlet {
    protected void doPost(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
        ServletContext ctx = getServletContext();
        PrintWriter out = response.getWriter();
        String docId = request.getParameter("docId");

        Connection con = (Connection) ctx.getAttribute("DBConnection");

        int rowCnt = 0;

        try (PreparedStatement pStmnt = con.prepareStatement("update tdocuments set cdelete = ? where cdocid = ?")) {

            pStmnt.setInt(1, 1);
            pStmnt.setString(2, docId);
            rowCnt = pStmnt.executeUpdate();

        } catch (Exception e) {
            e.printStackTrace();
        }

        if (rowCnt > 0) {
            JsonObject ok = ResponseFactory.getResponse(ResponseFactory.ResponseType.GENERIC_JSON_OK);
            out.write(ok.toString());
        } else {
            JsonObject err = ResponseFactory.getResponse(ResponseFactory.ResponseType.GENERIC_JSON_ERR);
            out.write(err.toString());
        }
    }

    protected void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
        response.getWriter().append("Served at: ").append(request.getContextPath());
    }
}
