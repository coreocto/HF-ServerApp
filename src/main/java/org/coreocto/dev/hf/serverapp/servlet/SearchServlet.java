package org.coreocto.dev.hf.serverapp.servlet;

import com.google.common.base.Strings;
import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import org.apache.log4j.Logger;
import org.coreocto.dev.hf.commonlib.Constants;
import org.coreocto.dev.hf.commonlib.suise.util.SuiseUtil;
import org.coreocto.dev.hf.commonlib.util.Registry;
import org.coreocto.dev.hf.commonlib.vasst.bean.RelScore;
import org.coreocto.dev.hf.serverapp.factory.ResponseFactory;
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

        Connection con = (Connection) ctx.getAttribute("DBConnection");
        Gson gson = (Gson) ctx.getAttribute("gson");

        PrintWriter out = response.getWriter();

        String st = request.getParameter("st");

        if (st == null || st.isEmpty()) {
            st = Constants.SSE_TYPE_SUISE + "";
        }

        int rowCnt = 0;

        String[] qValues = request.getParameterValues("q");

        try {
            String q = (qValues != null && qValues.length > 0) ? qValues[0] : "";
            String qid = request.getParameter("qid");

            if (qid == null || qid.isEmpty()) {
                qid = UUID.randomUUID().toString();
            }

            if (st.equalsIgnoreCase(Constants.SSE_TYPE_SUISE + "")) {

                String inMemSrh = request.getParameter("inMemSrh");

                Map<String, List<String>> inMemHashtable = null;

                SuiseServer server = (SuiseServer) ctx.getAttribute("suiseServer");
                if (server == null) {
                    server = new SuiseServer(new SuiseUtil((Registry) ctx.getAttribute("registry")));
                    ctx.setAttribute("suiseServer", server);
                }

                //load index into memory
//                if (inMemSrh != null && !inMemSrh.isEmpty()) {
//
//                    long loadStartTime = System.currentTimeMillis();
//
//                    inMemHashtable = (Map) ctx.getAttribute("inmem-hashtable");
//                    if (inMemHashtable == null) {
//                        inMemHashtable = new HashMap<>();
//                        ctx.setAttribute("inmem-hashtable", inMemHashtable);
//
//                        try (PreparedStatement pStmnt = con.prepareStatement("select cdocid,ctoken from tdocument_indexes t2 where 1=1 order by cdocid, corder")) {
//
//                            ResultSet rs = pStmnt.executeQuery();
//
//                            AddTokenResult addTokenResult = null;
//
//                            String lastDocId = null;
//
//                            while (rs.next()) {
//                                String curDocId = rs.getString(1);
//                                if (lastDocId == null || !lastDocId.equals(curDocId)) {
//                                    addTokenResult = new AddTokenResult();
//                                    addTokenResult.setId(curDocId);
//                                    addTokenResult.setC(new ArrayList<>());
//                                    addTokenResult.setX(new ArrayList<>());
//                                    lastDocId = curDocId;
//
//                                    server.Add(addTokenResult, null);
//                                }
//                                addTokenResult.getC().add(rs.getString(2));
//                            }
//
//                        } catch (Exception e) {
//                            throw e;
//                        }
//                    }
//
//                    long loadEndTime = System.currentTimeMillis();
//                    System.out.println("loaded index info from database, elapsed time = " + (loadEndTime - loadStartTime) + "ms");
//                    //end load index into memory
//
//                }

                try (PreparedStatement pStmnt = con.prepareStatement("insert into tquery_statistics (cqueryid,cstarttime,cdata) values (?,?,?)")) {

                    pStmnt.setString(1, qid);
                    pStmnt.setLong(2, System.currentTimeMillis());
                    pStmnt.setString(3, q);
                    rowCnt = pStmnt.executeUpdate();

                } catch (Exception e) {
                    LOGGER.error("error when creating new entry for tquery_statistics", e);
                    throw e;
                }

                //List<String> searchResult = new ArrayList<>();
                // List<String> searchResult = server.Search(q);
                JsonArray jsonArray = new JsonArray();

//                if (inMemSrh != null && !inMemSrh.isEmpty()) {
//                    searchResult.addAll(server.Search(q));
//                } else {
                try (PreparedStatement pStmnt = con.prepareStatement("select cdocid, cft, cfeiv from tdocuments t where exists(select 1 from tdocument_indexes t2 where t.cdocid = t2.cdocid and H(?,R(corder))||R(corder) = ctoken)")) {

                    pStmnt.setString(1, q);
                    ResultSet rs = pStmnt.executeQuery();

                    while (rs.next()) {
//                            searchResult.add(rs.getString(1));

                        String docId = rs.getString(1);
                        Integer type = rs.getInt(2);
                        String feiv = rs.getString(3);

                        JsonObject tmp = new JsonObject();
                        tmp.addProperty("name", docId);
                        tmp.addProperty("type", type);
                        tmp.addProperty("feiv", feiv);
                        jsonArray.add(tmp);

                    }

                } catch (Exception e) {
                    LOGGER.error("error when retreiving tdocuments entries from database", e);
                    throw e;
                }
//                }

                try (PreparedStatement pStmnt = con.prepareStatement("update tquery_statistics set cendtime = ?, cmatchedcnt = ? where cqueryid = ?")) {

                    pStmnt.setLong(1, System.currentTimeMillis());
                    pStmnt.setInt(2, jsonArray.size());
                    pStmnt.setString(3, qid);

                    rowCnt = pStmnt.executeUpdate();

                } catch (Exception e) {
                    LOGGER.error("error when updating entry for tquery_statistics", e);
                    throw e;
                }

//                JsonElement element = gson.toJsonTree(searchResult, new TypeToken<List<String>>() {
//                }.getType());
//                JsonArray jsonArray = element.getAsJsonArray();

                JsonObject jsonObj = new JsonObject();
                jsonObj.addProperty("status", "ok");
                jsonObj.addProperty("count", jsonArray.size());
                jsonObj.add("files", jsonArray);
                out.write(jsonObj.toString());

                System.out.println(jsonObj.toString());

            } else if (st.equalsIgnoreCase(Constants.SSE_TYPE_VASST + "")) {

                //compute the document score
                //each matched document will first compute the TF-IDF (the term freq. will be normalized by max occurrence
                //and each word inside query vector will be computed and normalized with max occurrence also
                //then we will compute the the score using the inner product of two vectors and sort in descending order

                final int MAX_RESULT = 10;

                int numOfQueryTerms = qValues.length;

                String placeHolders = Strings.repeat("?,", numOfQueryTerms).substring(0, 2 * numOfQueryTerms - 1);

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

                Map<String, JsonObject> docTypeLookup = new HashMap<>();

                //find the document id which contains the keywords
                try (PreparedStatement pStmnt = con.prepareStatement("select cdocid, cft, cfeiv from tdocuments d where exists(select 1 from tdoc_term_freq dtf where d.cdocid = dtf.cdocid and dtf.cword in (" + placeHolders + "))")) {
                    for (int i = 1; i <= numOfQueryTerms; i++) {
                        pStmnt.setString(i, qValues[i - 1]);
                    }

                    ResultSet rs = pStmnt.executeQuery();
                    while (rs.next()) {
                        String docId = rs.getString(1);
                        matchedDocIds.add(docId);
                        int type = rs.getInt(2);
                        String feiv = rs.getString(3);

                        JsonObject tmp = new JsonObject();
                        tmp.addProperty("name", docId);
                        tmp.addProperty("type", type);
                        tmp.addProperty("feiv", feiv);

                        docTypeLookup.put(docId, tmp);
                    }

                } catch (SQLException e) {
                    LOGGER.error("error when retreiving tdocuments entries from database", e);
                    throw e;
                }
                //end

                int matchedDocCnt = matchedDocIds.size();

                int docCnt = 0;

                //find the total no. of documents
                try (PreparedStatement pStmnt = con.prepareStatement("select count(*) from tdocuments d where exists(select 1 from tdoc_term_freq dtf where d.cdocid = dtf.cdocid)")) {

                    ResultSet rs = pStmnt.executeQuery();
                    if (rs.next()) {
                        docCnt = rs.getInt(1);
                    }

                } catch (SQLException e) {
                    LOGGER.error("error when finding total number of documents from database", e);
                    throw e;
                }
                //end

                Map<String, RelScore> docScoreMap = new HashMap<>();

                //calculate tf-idf
                for (int i = 0; i < matchedDocCnt; i++) {
                    String curDocId = matchedDocIds.get(i);
                    try (PreparedStatement pStmnt = con.prepareStatement("select " +
                            "dtf.ccount, (select max(ccount) from tdoc_term_freq dtf2 where dtf.cword=dtf2.cword) max_ccount, " +
                            "cword " +
                            "from tdoc_term_freq dtf where cdocid = ? and cword in (" + placeHolders + ")")) {

                        pStmnt.setString(1, curDocId);

                        for (int j = 1; j <= numOfQueryTerms; j++) {
                            pStmnt.setString(j + 1, qValues[j - 1]);
                        }

                        ResultSet rs = pStmnt.executeQuery();
                        while (rs.next()) {
                            int tf = rs.getInt(1);  //term freq.
                            int mtf = rs.getInt(2); //max term freq.
                            String word = rs.getString(3); //the encrypted keyword, not necessary
                            double ntf = tf * 1.0 / mtf; //normalized term freq.
                            double tfidf = ntf * Math.log(docCnt * 1.0 / matchedDocCnt);

                            LOGGER.debug("tf=" + tf);
                            LOGGER.debug("mtf=" + mtf);
                            LOGGER.debug("word=" + word);
                            LOGGER.debug("ntf=" + ntf);
                            LOGGER.debug("tfidf=" + tfidf);

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

                    } catch (SQLException e) {
                        LOGGER.error("error when fetching tf-idf information from database", e);
                        throw e;
                    }
                }
                //end

                relScores.addAll(docScoreMap.values());

//                try (PreparedStatement pStmnt = con.prepareStatement("select " +
//                        "dtf.cdocid, dtf.ccount, (select max(ccount) from tdoc_term_freq dtf2 where dtf.cdocid=dtf2.cdocid) max_ccount, " +
//                        "cword " +
//                        "from tdoc_term_freq dtf where cword in (" + placeHolders + ")")) {
//
//                    for (int i = 1; i <= qValues.length; i++) {
//                        pStmnt.setString(i, qValues[i - 1]);
//                    }
//
//                    ResultSet rs = pStmnt.executeQuery();
//                    while (rs.next()) {
//                        String docId = rs.getString(1);
//                        int tf = rs.getInt(2);  //term freq.
//                        int mtf = rs.getInt(3); //max term freq.
//                        double ntf = tf * 1.0 / mtf; //normalized term freq.
//                        int docCnt = rs.getInt(4);
//                        int matchDocCnt = rs.getInt(5);
//                        double idf = Math.log(docCnt * 1.0 / matchDocCnt);
//                        double tfidf = ntf * idf;
//                        String word = rs.getString(6);
//
//                        System.out.println("docId=" + docId);
//                        System.out.println("tf=" + tf);
//                        System.out.println("mtf=" + mtf);
//                        System.out.println("ntf=" + ntf);
//                        System.out.println("docCnt=" + docCnt);
//                        System.out.println("matchDocCnt=" + matchDocCnt);
//                        System.out.println("idf=" + idf);
//                        System.out.println("tfidf=" + tfidf);
//
//                        RelScore relScore = new RelScore();
//                        relScore.setDocId(docId);
//                        relScore.setKeyword(word);
//                        relScore.setScore(tfidf);
//                        relScores.add(relScore);
//                    }
//                } catch (SQLException e) {
//                    throw e;
//                }

                Collections.sort(relScores, new Comparator<RelScore>() {
                    public int compare(RelScore o1, RelScore o2) {
                        return (new Double(o2.getScore())).compareTo(o1.getScore());
                    }
                });

//                for (RelScore score : relScores) {
//                    LOGGER.debug(gson.toJson(score));
//                }

//                System.out.println(gson.toJson(relScores));

                JsonArray jsonArray = new JsonArray();

                int minResult = Math.min(relScores.size(), MAX_RESULT);

                for (int i = 0; i < minResult; i++) {
                    String docId = relScores.get(i).getDocId();
                    JsonObject tmp = docTypeLookup.get(docId);
                    jsonArray.add(tmp);
                }

                JsonObject ok = ResponseFactory.getResponse(ResponseFactory.ResponseType.GENERIC_JSON_OK);
                ok.addProperty("totalCount", relScores.size());
                ok.addProperty("count", jsonArray.size());
                ok.add("files", jsonArray);
                out.write(ok.toString());

//                LOGGER.debug(ok.toString());
            }

        } catch (Exception ex) {
            LOGGER.error("error when processing request", ex);
            JsonObject err = ResponseFactory.getResponse(ResponseFactory.ResponseType.GENERIC_JSON_ERR);
            out.write(err.toString());
        }
    }

    protected void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
        response.getWriter().append("Served at: ").append(request.getContextPath());
    }
}
