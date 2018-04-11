package org.coreocto.dev.hf.serverapp.servlet;

import com.google.gson.Gson;
import org.apache.log4j.Logger;
import org.coreocto.dev.hf.commonlib.Constants;
import org.coreocto.dev.hf.commonlib.crypto.IHashFunc;
import org.coreocto.dev.hf.commonlib.sse.chlh.Index;
import org.coreocto.dev.hf.commonlib.sse.suise.bean.AddTokenResult;
import org.coreocto.dev.hf.commonlib.sse.vasst.bean.TermFreq;
import org.coreocto.dev.hf.commonlib.util.IBase64;
import org.coreocto.dev.hf.serverapp.AppConstants;
import org.coreocto.dev.hf.serverapp.crypto.JavaMd5Impl;
import org.coreocto.dev.hf.serverapp.db.DataSource;
import org.coreocto.dev.hf.serverapp.util.JavaBase64Impl;
import org.coreocto.dev.hf.serverapp.util.StringUtil;

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
import java.util.List;
import java.util.Map;

@WebServlet(
        urlPatterns = "/upload",
        name = "UploadServlet"
)
public class UploadServlet extends HttpServlet {

    final static Logger LOGGER = Logger.getLogger(UploadServlet.class);

    protected void doPost(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
        ServletContext ctx = getServletContext();

        PrintWriter out = response.getWriter();
        String docId = request.getParameter("docId");
        String tokenInJson = request.getParameter("token");
        String ft = request.getParameter("ft");
        String st = request.getParameter("st");
        String weiv = request.getParameter("weiv"); //word encryption iv
        String feiv = request.getParameter("feiv"); //file encryption iv
        String terms = request.getParameter("terms");
        String index_in_json = request.getParameter("index");


        if (st == null || st.isEmpty()) {
            st = Constants.SSE_TYPE_SUISE + "";
        }

        Gson gson = (Gson) ctx.getAttribute("gson");

        IBase64 base64 = new JavaBase64Impl();

        if (docId == null || ((st.equals(Constants.SSE_TYPE_SUISE + "") ||
                st.equals(AppConstants.SSE_TYPE_SUISE_2 + "") ||
                st.equals(AppConstants.SSE_TYPE_SUISE_3 + "")) && tokenInJson == null)) {
            LOGGER.debug("queryStr = " + request.getQueryString());
            LOGGER.debug("tokenInJson = " + tokenInJson);
            LOGGER.debug("docId = " + docId);
            LOGGER.debug("ft = " + ft);
            LOGGER.debug("st = " + st);
            return;
        }

        int rowCnt = 0;

        // we need to insert a document record here

        if (st.equalsIgnoreCase(Constants.SSE_TYPE_SUISE + "") ||
                st.equalsIgnoreCase(AppConstants.SSE_TYPE_SUISE_2 + "") ||
                st.equalsIgnoreCase(AppConstants.SSE_TYPE_SUISE_3 + "") ||
                st.equalsIgnoreCase(Constants.SSE_TYPE_VASST + "") ||
                st.equalsIgnoreCase(AppConstants.SSE_TYPE_VASST_2 + "") ||
                st.equalsIgnoreCase(AppConstants.SSE_TYPE_VASST_3 + "") ||
                st.equalsIgnoreCase(Constants.SSE_TYPE_CHLH + "")) {
            try (
                    Connection con = DataSource.getConnection();
                    PreparedStatement pStmnt = con.prepareStatement("insert into tdocuments (cdocid,cdelete,cft,cst,cweiv,cfeiv,cssetype) select ? as text, ? as integer, cast(? as integer), cast(? as integer), ? as text, ? as text, cast(? as integer) where not exists (select 1 from tdocuments where cdocid = ? and cdelete = ?)")) {

                int paramIdx = 1;

                pStmnt.setString(paramIdx++, docId);
                pStmnt.setInt(paramIdx++, 0);
                pStmnt.setString(paramIdx++, ft);
                pStmnt.setString(paramIdx++, st);
                pStmnt.setString(paramIdx++, weiv);
                pStmnt.setString(paramIdx++, feiv);
                pStmnt.setInt(paramIdx++, Integer.parseInt(st));
                pStmnt.setString(paramIdx++, docId);
                pStmnt.setInt(paramIdx++, 0);
                rowCnt = pStmnt.executeUpdate();

            } catch (SQLException e) {
                response.setStatus(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
                String msg = "error when inserting record to tdocuments";
                LOGGER.error(msg, e);
                out.write(msg);
                rowCnt = -1;
            }
        }

        if (rowCnt == -1) {
            return;
        }

        if (st.equals(Constants.SSE_TYPE_SUISE + "") ||
                st.equals(AppConstants.SSE_TYPE_SUISE_2 + "") ||
                st.equals(AppConstants.SSE_TYPE_SUISE_3 + "")) {

            AddTokenResult addTokenResult = null;

            try {
                addTokenResult = gson.fromJson(tokenInJson, AddTokenResult.class);
            } catch (Exception ex) {
                response.setStatus(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
                String msg = "error when parsing json into object";
                LOGGER.error(msg, ex);
                out.write(msg);
                rowCnt = -1;
            }

            if (rowCnt == -1) {
                return;
            }

            int i = 0;

            try (
                    Connection con = DataSource.getConnection();
                    PreparedStatement pStmnt2 = con.prepareStatement("insert into tdocument_indexes (cdocid,ctoken,corder) values (?,?,?)")) {

                for (String token : addTokenResult.getC()) {
                    pStmnt2.setString(1, addTokenResult.getId());
                    pStmnt2.setString(2, token);
                    pStmnt2.setInt(3, i);
                    rowCnt += pStmnt2.executeUpdate();
                    i++;
                }

            } catch (SQLException e) {
                response.setStatus(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
                String msg = "error when inserting record to tdocument_indexes";
                LOGGER.error(msg, e);
                out.write(msg);
                rowCnt = -1;
            }

        } else if (st.equalsIgnoreCase(Constants.SSE_TYPE_VASST + "") ||
                st.equalsIgnoreCase(AppConstants.SSE_TYPE_VASST_2 + "") ||
                st.equalsIgnoreCase(AppConstants.SSE_TYPE_VASST_3 + "")) {

            TermFreq termFreq = null;

            try {
                termFreq = gson.fromJson(terms, TermFreq.class);
            } catch (Exception ex) {
                response.setStatus(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
                String msg = "error when parsing json into object";
                LOGGER.error(msg, ex);
                out.write(msg);
                rowCnt = -1;
            }

            if (rowCnt == -1) {
                return;
            }

            IHashFunc md5 = new JavaMd5Impl();

            try (Connection con = DataSource.getConnection();
                 PreparedStatement pStmnt = con.prepareStatement("insert into tdoc_term_freq (cdocid,cword,ccount) values (?,?,?)")) {

                Map<String, Integer> termsMap = termFreq.getTerms();
                for (String key : termsMap.keySet()) {
                    Integer value = termsMap.get(key);

                    pStmnt.clearParameters();
                    pStmnt.setString(1, docId);

                    String hash_key = StringUtil.bytesToHex(md5.getHash(key.getBytes(AppConstants.ENCODING_UTF8)));

                    pStmnt.setString(2, hash_key);
                    pStmnt.setInt(3, value);
                    rowCnt += pStmnt.executeUpdate();
                }

            } catch (Exception e) {
                response.setStatus(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
                String msg = "error when inserting record to tdoc_term_freq";
                LOGGER.error(msg, e);
                out.write(msg);
                rowCnt = -1;
            }
        } else if (st.equalsIgnoreCase(Constants.SSE_TYPE_CHLH + "")) {
            Index index = null;
            try {
                index = gson.fromJson(index_in_json, Index.class);
            } catch (Exception ex) {
                response.setStatus(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
                String msg = "error when parsing json into object";
                LOGGER.error(msg, ex);
                out.write(msg);
                rowCnt = -1;
            }

            if (rowCnt == -1) {
                return;
            }

            List<String> bloomFilters = index.getBloomFilters();

            String encDocId = index.getDocId();

            try (Connection con = DataSource.getConnection();
                 PreparedStatement ps = con.prepareStatement("insert into tchlh (cdocid, cbf) values (?,?)")) {

                int size = bloomFilters.size();

                for (int i = 0; i < size; i++) {

                    ps.clearParameters();

                    String s = bloomFilters.get(i);
                    ps.setString(1, encDocId);
                    ps.setString(2, s);
                    rowCnt += ps.executeUpdate();
                }

            } catch (SQLException e) {
                response.setStatus(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
                String msg = "error when inserting record to tdocument_indexes";
                LOGGER.error(msg, e);
                out.write(msg);
                rowCnt = -1;
            }
        }
    }

    protected void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
        response.getWriter().append("Served at: ").append(request.getContextPath());
    }
}
