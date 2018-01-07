package org.coreocto.dev.hf.serverapp.servlet;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import org.coreocto.dev.hf.commonlib.Constants;
import org.coreocto.dev.hf.commonlib.suise.bean.AddTokenResult;
import org.coreocto.dev.hf.commonlib.vasst.bean.TermFreq;
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
import java.util.Map;

@WebServlet(
        urlPatterns = "/upload",
        name = "UploadServlet"
)
public class UploadServlet extends HttpServlet {

    protected void doPost(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
        ServletContext ctx = getServletContext();

        PrintWriter out = response.getWriter();
        String docId = request.getParameter("docId");
        String tokenInJson = request.getParameter("token");
        String ft = request.getParameter("ft");
        String st = request.getParameter("st");
        String weiv = request.getParameter("weiv"); //word encryption iv
        String feiv = request.getParameter("feiv"); //file encryption iv

        if (st == null || st.isEmpty()) {
            st = Constants.SSE_TYPE_SUISE + "";
        }

        Connection con = (Connection) ctx.getAttribute("DBConnection");

        System.out.println("queryStr = " + request.getQueryString());
        System.out.println("tokenInJson = " + tokenInJson);
        System.out.println("docId = " + docId);
        System.out.println("ft = " + ft);
        System.out.println("st = " + st);

        if (docId == null || (st.equalsIgnoreCase(Constants.SSE_TYPE_SUISE + "") && tokenInJson == null)) {
            return;
        }

        int rowCnt = 0;

        // we need to insert a document record here

        try (PreparedStatement pStmnt = con.prepareStatement("insert into tdocuments (cdocid,cdelete,cft,cst,cweiv,cfeiv) select ? as text, ? as integer, cast(? as integer), cast(? as integer), ? as text, ? as text where not exists (select 1 from tdocuments where cdocid = ? and cdelete = ?)")) {

            int paramIdx = 1;

            pStmnt.setString(paramIdx++, docId);
            pStmnt.setInt(paramIdx++, 0);
            pStmnt.setString(paramIdx++, ft);
            pStmnt.setString(paramIdx++, st);
            pStmnt.setString(paramIdx++, weiv);
            pStmnt.setString(paramIdx++, feiv);
            pStmnt.setString(paramIdx++, docId);
            pStmnt.setInt(paramIdx++, 0);
            rowCnt = pStmnt.executeUpdate();

        } catch (Exception e) {
            e.printStackTrace();
        }

        if (st.equalsIgnoreCase(Constants.SSE_TYPE_SUISE + "")) {

            try {
                AddTokenResult addTokenResult = new Gson().fromJson(tokenInJson, AddTokenResult.class);

                //System.out.println("tokenInJson: "+tokenInJson);
                //System.out.println("con: "+con);

                // because the file now save in google drive, the code for dataFileItem would not execute
                // we need to insert a document record here

//                try (PreparedStatement pStmnt = con.prepareStatement("insert into tdocuments (cdocid,cdelete, cweiv) select ? as text, ? as integer, ? as text where not exists (select 1 from tdocuments where cdocid = ? and cdelete = ?)")) {
//
//                    String atrDocId = addTokenResult.getId();
//
//                    pStmnt.setString(1, atrDocId);
//                    pStmnt.setInt(2, 0);
//                    pStmnt.setString(3, weiv);
//                    pStmnt.setString(4, atrDocId);
//                    pStmnt.setInt(5, 0);
//                    rowCnt = pStmnt.executeUpdate();
//
//                } catch (Exception e) {
//                    e.printStackTrace();
//                }

                try (PreparedStatement pStmnt2 = con.prepareStatement("insert into tdocument_indexes (cdocid,ctoken,corder) values (?,?,?)")) {

                    int i = 0;

                    for (String token : addTokenResult.getC()) {
                        pStmnt2.setString(1, addTokenResult.getId());
                        pStmnt2.setString(2, token);
                        pStmnt2.setInt(3, i);
                        rowCnt = pStmnt2.executeUpdate();
                        i++;
                    }

                } catch (Exception e) {
                    e.printStackTrace();
                }

                JsonObject ok = ResponseFactory.getResponse(ResponseFactory.ResponseType.GENERIC_JSON_OK);
                out.write(ok.toString());

            } catch (Exception ex) {
                JsonObject err = ResponseFactory.getResponse(ResponseFactory.ResponseType.GENERIC_JSON_ERR);
                out.write(err.toString());
                throw new ServletException(ex);
            }

        } else if (st.equalsIgnoreCase(Constants.SSE_TYPE_VASST + "")) {

            String terms = request.getParameter("terms");

            try {
                TermFreq termFreq = new Gson().fromJson(terms, TermFreq.class);

                try (PreparedStatement pStmnt = con.prepareStatement("insert into tdoc_term_freq (cdocid,cword,ccount) values (?,?,?)")) {

                    Map<String, Integer> termsMap = termFreq.getTerms();
                    for (String key : termsMap.keySet()) {
                        Integer value = termsMap.get(key);

                        pStmnt.clearParameters();
                        pStmnt.setString(1, docId);
                        pStmnt.setString(2, key);
                        pStmnt.setInt(3, value);
                        rowCnt += pStmnt.executeUpdate();
                    }

                } catch (Exception e) {
                    e.printStackTrace();
                }

            } catch (Exception ex) {
                JsonObject err = ResponseFactory.getResponse(ResponseFactory.ResponseType.GENERIC_JSON_ERR);
                out.write(err.toString());
                throw new ServletException(ex);
            }
        }
    }

    protected void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
        response.getWriter().append("Served at: ").append(request.getContextPath());
    }
}
