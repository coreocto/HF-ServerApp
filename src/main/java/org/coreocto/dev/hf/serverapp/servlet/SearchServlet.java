package org.coreocto.dev.hf.serverapp.servlet;

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.reflect.TypeToken;
import org.coreocto.dev.hf.commonlib.suise.util.SuiseUtil;
import org.coreocto.dev.hf.serverapp.util.JavaBase64Impl;
import org.coreocto.dev.hf.serverapp.util.JavaMd5Impl;
import org.coreocto.dev.hf.serverlib.suise.SuiseServer;

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
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;

@WebServlet(
        urlPatterns = "/search",
        name = "SearchServlet"
)
public class SearchServlet extends HttpServlet {
    protected void doPost(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
        ServletContext ctx = getServletContext();
        SuiseServer server = (SuiseServer) ctx.getAttribute("suiseServer");
        if (server == null) {
            server = new SuiseServer(new SuiseUtil(new JavaBase64Impl(), new JavaMd5Impl(), null));
            ctx.setAttribute("suiseServer", server);
        }

        Connection con = (Connection) ctx.getAttribute("DBConnection");
        Gson gson = (Gson) ctx.getAttribute("gson");

        PrintWriter out = response.getWriter();

        int rowCnt = 0;

        try {
            String q = java.net.URLDecoder.decode(request.getParameter("q"), "utf-8");
            String qid = request.getParameter("qid");

            try (PreparedStatement pStmnt = con.prepareStatement("insert into tquery_statistics (cqueryid,cstarttime,cdata) values (?,?,?)")) {

                pStmnt.setString(1, qid);
                pStmnt.setLong(2, System.currentTimeMillis());
                pStmnt.setString(3, q);
                rowCnt = pStmnt.executeUpdate();

            } catch (Exception e) {
                throw e;
            }

            List<String> searchResult = new ArrayList<>();
            // List<String> searchResult = server.Search(q);

            try (PreparedStatement pStmnt = con.prepareStatement("select cdocid from tdocuments t where exists(select 1 from tdocument_indexes t2 where t.cdocid = t2.cdocid and H(?,R(corder))||R(corder) = ctoken)")) {

                pStmnt.setString(1, q);
                ResultSet rs = pStmnt.executeQuery();

                while (rs.next()) {
                    searchResult.add(rs.getString(1));
                }

            } catch (Exception e) {
                throw e;
            }

            try (PreparedStatement pStmnt = con.prepareStatement("update tquery_statistics set cendtime = ?, cmatchedcnt = ? where cqueryid = ?")) {

                pStmnt.setLong(1, System.currentTimeMillis());
                pStmnt.setInt(2, searchResult.size());
                pStmnt.setString(3, qid);

                rowCnt = pStmnt.executeUpdate();

            } catch (Exception e) {
                throw e;
            }

            JsonElement element = gson.toJsonTree(searchResult, new TypeToken<List<String>>() {
            }.getType());
            JsonArray jsonArray = element.getAsJsonArray();

            JsonObject jsonObj = new JsonObject();
            jsonObj.addProperty("status", "ok");
            jsonObj.addProperty("count", searchResult.size());
            jsonObj.add("files", jsonArray);
            out.write(jsonObj.toString());

            System.out.println(jsonObj.toString());

        } catch (Exception ex) {
            ex.printStackTrace();

            JsonObject jsonObj = new JsonObject();
            jsonObj.addProperty("status", "error");
            out.write(jsonObj.toString());
        }
    }

    protected void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
        response.getWriter().append("Served at: ").append(request.getContextPath());
    }
}
