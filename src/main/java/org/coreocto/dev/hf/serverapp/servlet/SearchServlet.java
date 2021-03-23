package org.coreocto.dev.hf.serverapp.servlet;

import com.google.common.base.Joiner;
import com.google.common.base.Strings;
import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import org.apache.log4j.Logger;
import org.coreocto.dev.hf.commonlib.Constants;
import org.coreocto.dev.hf.commonlib.sse.vasst.bean.RelScore;
import org.coreocto.dev.hf.commonlib.util.IBase64;
import org.coreocto.dev.hf.serverapp.AppConstants;
import org.coreocto.dev.hf.serverapp.bean.DocInfo;
import org.coreocto.dev.hf.serverapp.db.DataSource;
import org.coreocto.dev.hf.serverapp.util.JavaBase64Impl;

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
import java.sql.SQLException;
import java.util.*;

@WebServlet(
        urlPatterns = "/search",
        name = "SearchServlet"
)
public class SearchServlet extends HttpServlet {

    final static Logger LOGGER = Logger.getLogger(SearchServlet.class);

    protected void doPost(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
        ServletContext ctx = getServletContext();

        Gson gson = (Gson) ctx.getAttribute("gson");
        PrintWriter out = response.getWriter();

        IBase64 base64 = new JavaBase64Impl();

        String st = request.getParameter("st");
        String[] qValues = request.getParameterValues("q");

        String qid = request.getParameter("qid");

        String reqDataProtection = request.getParameter("dp");

        boolean dataProtection = true;

        if (reqDataProtection != null && reqDataProtection.equalsIgnoreCase("no")) {
            dataProtection = false;
        }

        if (st == null || st.isEmpty()) {
            st = Constants.SSE_TYPE_SUISE + "";
        }

        int rowCnt = 0;

        if (qid == null || qid.isEmpty()) {
            qid = UUID.randomUUID().toString();
        }

        Joiner joiner = Joiner.on(",").skipNulls();

        String joinedQ = joiner.join(qValues);

        try (Connection con = DataSource.getConnection();
             PreparedStatement pStmnt = con.prepareStatement("insert into tquery_statistics (cqueryid,cstarttime,cdata,cssetype) values (?,?,?,?)")) {

            pStmnt.setString(1, qid);
            pStmnt.setLong(2, System.currentTimeMillis());
            pStmnt.setString(3, joinedQ);
            pStmnt.setInt(4, Integer.parseInt(st));
            rowCnt = pStmnt.executeUpdate();

        } catch (Exception e) {
            LOGGER.error("error when creating new entry for tquery_statistics", e);
            rowCnt = -1;
        }

        if (rowCnt != 1) {
            String msg = "error when inserting tquery_statistics record";
            response.setStatus(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
            out.print(msg);
            LOGGER.error(msg);
        } else {

            int count = 0;
            int totalCnt = 0;

            List<DocInfo> files = new ArrayList<>();

            if (
                    st.equals(Constants.SSE_TYPE_SUISE + "") ||
                            st.equals(AppConstants.SSE_TYPE_SUISE_2 + "") ||
                            st.equals(AppConstants.SSE_TYPE_SUISE_3 + "")
                    ) {

                Map<String, List<DocInfo>> cachedResult = (Map<String, List<DocInfo>>) ctx.getAttribute("suiseCachedResult");
                if (cachedResult == null) {
                    cachedResult = new HashMap<>();
                }

                if (cachedResult.containsKey(joinedQ)) {
                    files = cachedResult.get(joinedQ);
                } else {

                    List<String> uniqueQueryVals = new ArrayList<>();
                    for (String s : qValues) {
                        if (!uniqueQueryVals.contains(s)) {
                            uniqueQueryVals.add(s);
                        }
                    }

                    int numOfQueryTerms = uniqueQueryVals.size();

                    StringBuilder sqlBuilder = new StringBuilder();

                    if (!dataProtection) {
                        sqlBuilder.append("select cdocid, cft, cfeiv from tdocuments t where exists(select 1 from tdocument_indexes t2 where t.cdocid = t2.cdocid and ctoken in (");
                        String singlePlaceHolder = "?,";
                        String placeHolders = Strings.repeat(singlePlaceHolder, numOfQueryTerms).substring(0, singlePlaceHolder.length() * numOfQueryTerms - 1);
                        sqlBuilder.append(placeHolders + "))");
                    } else {
                        sqlBuilder.append("select cdocid, cft, cfeiv from tdocuments t where (select count(*) from tdocument_indexes t2 where t.cdocid = t2.cdocid and (");
                        String singlePlaceHolder = "H(ctoken,?)=? or ";
                        String placeHolders = Strings.repeat(singlePlaceHolder, numOfQueryTerms).substring(0, singlePlaceHolder.length() * numOfQueryTerms - 4);
                        sqlBuilder.append(placeHolders + "))=?");
                    }

                    try (
                            Connection con = DataSource.getConnection();
                            PreparedStatement pStmnt = con.prepareStatement(sqlBuilder.toString());
                    ) {

                        if (dataProtection) {
                            int curQueryIdx = -1;
                            for (int i = 0; i < numOfQueryTerms; i++) {
                                curQueryIdx = i * 2 + 1;
                                pStmnt.setString(curQueryIdx, uniqueQueryVals.get(i));
                                pStmnt.setInt(curQueryIdx + 1, 1);
                            }
                            curQueryIdx += 2;
                            pStmnt.setInt(curQueryIdx, numOfQueryTerms);
                        } else {
                            int curQueryIdx = -1;
                            for (int i = 0; i < numOfQueryTerms; i++) {
                                curQueryIdx = i + 1;
                                pStmnt.setString(curQueryIdx, uniqueQueryVals.get(i));
                            }
                            curQueryIdx++;
                            pStmnt.setInt(curQueryIdx, numOfQueryTerms);
                        }

//                        pStmnt.setString(1, q);
//                        if (dataProtection) {
//                            pStmnt.setInt(2, 1);
//                        }
                        ResultSet result = pStmnt.executeQuery();

                        while (result.next()) {
                            String docId = result.getString(1);
                            Integer type = result.getInt(2);
                            String feiv = result.getString(3);

                            DocInfo docInfo = new DocInfo();
                            docInfo.setName(docId);
                            docInfo.setType(type);
                            docInfo.setFeiv(feiv);

                            files.add(docInfo);
                        }

                    } catch (SQLException ex) {
                        String msg = "error when fetching document info from tdocuments";
                        response.setStatus(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
                        out.print(msg);
                        LOGGER.error(msg, ex);
                    }

                    cachedResult.put(joinedQ, files);
                }

                count = files.size();
            } else if (st.equals(Constants.SSE_TYPE_VASST + "")) {
                //compute the document score
                //each matched document will first compute the TF-IDF (the term freq. will be normalized by max occurrence
                //and each word inside query vector will be computed and normalized with max occurrence also
                //then we will compute the the score using the inner product of two vectors and sort in descending order

                final int MAX_RESULT = 10;

                List<String> uniqueQueryVals = new ArrayList<>();
                for (String s : qValues) {
                    if (!uniqueQueryVals.contains(s)) {
                        uniqueQueryVals.add(s);
                    }
                }

                int numOfQueryTerms = uniqueQueryVals.size();
                String singlePlaceHolder = null;
                if (dataProtection) {
                    singlePlaceHolder = "md5(?),";
                } else {
                    singlePlaceHolder = "?,";
                }
                String placeHolders = Strings.repeat(singlePlaceHolder, numOfQueryTerms).substring(0, singlePlaceHolder.length() * numOfQueryTerms - 1);

                // find the term freq. inside the query vector
                Map<String, Integer> queryTermOccur = new HashMap<>();
                for (String queryTerm : qValues) {
                    if (queryTermOccur.containsKey(queryTerm)) {
                        int occur = queryTermOccur.get(queryTerm);
                        queryTermOccur.put(queryTerm, occur + 1);
                    } else {
                        queryTermOccur.put(queryTerm, 1);
                    }
                }

                // find the max term freq. inside the query vector
                int maxQryTerms = 0;
                for (Integer i : queryTermOccur.values()) {
                    maxQryTerms = Math.max(i, maxQryTerms);
                }

                List<RelScore> relScores = new ArrayList<>();

                List<String> matchedDocIds = new ArrayList<>();

                Map<String, DocInfo> docTypeLookup = new HashMap<>();

                try (
                        Connection con = DataSource.getConnection();
                        //PreparedStatement pStmnt = con.prepareStatement("select cdocid, cft, cfeiv from tdocuments d where exists(select 1 from tdoc_term_freq dtf where d.cdocid = dtf.cdocid and dtf.cword in (" + placeHolders + "))");
                        PreparedStatement pStmnt = con.prepareStatement("select cdocid, cft, cfeiv from tdocuments d where (select count(*) from tdoc_term_freq dtf where d.cdocid = dtf.cdocid and dtf.cword in (" + placeHolders + "))=?");
                ) {

                    for (int i = numOfQueryTerms - 1; i >= 0; i--) {
                        pStmnt.setString(i + 1, uniqueQueryVals.get(i));
                    }
                    pStmnt.setInt(numOfQueryTerms + 1, numOfQueryTerms);

                    ResultSet result = pStmnt.executeQuery();

                    while (result.next()) {
                        String docId = result.getString(1);
                        matchedDocIds.add(docId);
                        int type = result.getInt(2);
                        String feiv = result.getString(3);

                        DocInfo docInfo = new DocInfo();
                        docInfo.setName(docId);
                        docInfo.setType(type);
                        docInfo.setFeiv(feiv);

                        docTypeLookup.put(docId, docInfo);
                    }

                } catch (SQLException ex) {
                    String msg = "error when fetching document info from tdocuments";
                    response.setStatus(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
                    out.print(msg);
                    LOGGER.error(msg, ex);
                }

                int matchedDocCnt = matchedDocIds.size();

                int docCnt = 0;

                try (
                        Connection con = DataSource.getConnection();
                        PreparedStatement pStmnt = con.prepareStatement("select count(*) from tdocuments d where exists(select 1 from tdoc_term_freq dtf where d.cdocid = dtf.cdocid)");
                ) {

                    ResultSet result = pStmnt.executeQuery();

                    if (result.next()) {
                        docCnt = result.getInt(1);
                    }

                } catch (SQLException ex) {
                    String msg = "error when counting documents from tdocuments";
                    response.setStatus(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
                    out.print(msg);
                    LOGGER.error(msg, ex);
                }

                Map<String, RelScore> docScoreMap = new HashMap<>();

                try (
                        Connection con = DataSource.getConnection();
                        PreparedStatement pStmnt = con.prepareStatement("select " +
                                "dtf.ccount, (select max(ccount) from tdoc_term_freq dtf2 where dtf.cword=dtf2.cword) max_ccount, " +
                                "cword " +
                                "from tdoc_term_freq dtf where cdocid = ? and cword in (" + placeHolders + ")");
                ) {

                    //calculate tf-idf
                    for (int i = 0; i < matchedDocCnt; i++) {
                        String curDocId = matchedDocIds.get(i);

                        pStmnt.clearParameters();

                        pStmnt.setString(1, curDocId);

                        for (int j = numOfQueryTerms - 1; j >= 0; j--) {
                            pStmnt.setString(j + 2, uniqueQueryVals.get(j));
                        }

                        ResultSet tmpRs = pStmnt.executeQuery();

                        while (tmpRs.next()) {
                            int tf = tmpRs.getInt(1);  //term freq.
                            int mtf = tmpRs.getInt(2); //max term freq.
                            String word = tmpRs.getString(3); //the encrypted keyword, not necessary
                            double ntf = tf * 1.0 / mtf; //normalized term freq.
                            double tfidf = ntf * Math.log(docCnt * 1.0 / matchedDocCnt);

                            RelScore relScore = null;

                            if (docScoreMap.containsKey(curDocId)) {
                                relScore = docScoreMap.get(curDocId);
                            } else {
                                relScore = new RelScore();
                                relScore.setDocId(curDocId);
                                docScoreMap.put(curDocId, relScore);
                            }
                            double oldScore = relScore.getScore();
                            Integer qryTermFreq = queryTermOccur.get(word);
                            if (qryTermFreq == null) {
                                qryTermFreq = 0;
                            }
                            double queryTermScore = qryTermFreq * 1.0 / maxQryTerms;
                            relScore.setScore(oldScore + (tfidf * queryTermScore));
                        }


                        //end
                    }

                } catch (SQLException ex) {
                    String msg = "error when calculating TF-IDF from tdoc_term_freq";
                    response.setStatus(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
                    out.print(msg);
                    LOGGER.error(msg, ex);
                }

                relScores.addAll(docScoreMap.values());

                Collections.sort(relScores, new Comparator<RelScore>() {
                    public int compare(RelScore o1, RelScore o2) {
                        return (new Double(o2.getScore())).compareTo(o1.getScore());
                    }
                });

                int minResult = Math.min(relScores.size(), MAX_RESULT);

                for (int x = 0; x < minResult; x++) {
                    String docId = relScores.get(x).getDocId();
                    DocInfo tmp = docTypeLookup.get(docId);
                    files.add(tmp);
                }

                count = files.size();
                totalCnt = relScores.size();
            } else if (st.equals(Constants.SSE_TYPE_CHLH + "")) {

                String localQ = qValues[0];

                if (localQ != null && !localQ.isEmpty()) {
                    try (
                            Connection con = DataSource.getConnection();
                            PreparedStatement pStmnt = con.prepareStatement("select cdocid, cft, cfeiv, cweiv from tdocuments d where exists(select 1 from tchlh d2 where search(d2.cbf, ?) = ? and d.cdocid = d2.cdocid)");
                    ) {

                        pStmnt.setString(1, localQ);
                        pStmnt.setInt(2, 1);

                        ResultSet rs = pStmnt.executeQuery();

                        while (rs.next()) {
                            DocInfo tmp = new DocInfo();
                            tmp.setName(rs.getString(1));
                            tmp.setType(rs.getInt(2));
                            tmp.setFeiv(rs.getString(3));
                            tmp.setWeiv(rs.getString(4));
                            files.add(tmp);
                        }


                    } catch (SQLException ex) {
                        String msg = "error when counting documents from tdocuments";
                        response.setStatus(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
                        out.print(msg);
                        LOGGER.error(msg, ex);
                    }
                }

                count = files.size();
            }

            if (totalCnt < count) {
                totalCnt = count;
            }

            try (
                    Connection con = DataSource.getConnection();
                    PreparedStatement pStmnt = con.prepareStatement("update tquery_statistics set cendtime = ?, cmatchedcnt = ? where cqueryid = ?");
            ) {

                pStmnt.setLong(1, System.currentTimeMillis());
                pStmnt.setInt(2, totalCnt);
                pStmnt.setString(3, qid);

                rowCnt = pStmnt.executeUpdate();

            } catch (SQLException ex) {
                String msg = "error when counting documents from tdocuments";
                response.setStatus(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
                out.print(msg);
                LOGGER.error(msg, ex);
            }

            if (rowCnt > 1) {
                LOGGER.error("update statement affected more than one row");
            }


            if (response.getStatus() != HttpServletResponse.SC_INTERNAL_SERVER_ERROR) {
                JsonArray jsonArrayFiles = new JsonArray();

                for (DocInfo docInfo : files) {
                    JsonObject tmp = new JsonObject();
                    tmp.addProperty("name", docInfo.getName());
                    tmp.addProperty("type", docInfo.getType());
                    tmp.addProperty("feiv", docInfo.getFeiv());
                    tmp.addProperty("weiv", docInfo.getWeiv());
                    jsonArrayFiles.add(tmp);
                }

                JsonObject jsonObject = new JsonObject();
                jsonObject.add("files", jsonArrayFiles);
                jsonObject.addProperty("count", count);
                jsonObject.addProperty("totalCount", totalCnt);
                out.write(jsonObject.toString());
            }
        }
    }

    protected void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
        response.getWriter().append("Served at: ").append(request.getContextPath());
    }
}
