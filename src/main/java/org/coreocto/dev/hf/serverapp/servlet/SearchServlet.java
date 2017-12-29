package org.coreocto.dev.hf.serverapp.servlet;

import com.google.common.base.Strings;
import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.reflect.TypeToken;
import org.coreocto.dev.hf.commonlib.Constants;
import org.coreocto.dev.hf.commonlib.suise.bean.AddTokenResult;
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
                if (inMemSrh != null && !inMemSrh.isEmpty()) {

                    long loadStartTime = System.currentTimeMillis();

                    inMemHashtable = (Map) ctx.getAttribute("inmem-hashtable");
                    if (inMemHashtable == null) {
                        inMemHashtable = new HashMap<>();
                        ctx.setAttribute("inmem-hashtable", inMemHashtable);

                        try (PreparedStatement pStmnt = con.prepareStatement("select cdocid,ctoken from tdocument_indexes t2 where 1=1 order by cdocid, corder")) {

                            ResultSet rs = pStmnt.executeQuery();

                            AddTokenResult addTokenResult = null;

                            String lastDocId = null;

                            while (rs.next()) {
                                String curDocId = rs.getString(1);
                                if (lastDocId == null || !lastDocId.equals(curDocId)) {
                                    addTokenResult = new AddTokenResult();
                                    addTokenResult.setId(curDocId);
                                    addTokenResult.setC(new ArrayList<>());
                                    addTokenResult.setX(new ArrayList<>());
                                    lastDocId = curDocId;

                                    server.Add(addTokenResult, null);
                                }
                                addTokenResult.getC().add(rs.getString(2));
                            }

                        } catch (Exception e) {
                            throw e;
                        }
                    }

                    long loadEndTime = System.currentTimeMillis();
                    System.out.println("loaded index info from database, elapsed time = " + (loadEndTime - loadStartTime) + "ms");
                    //end load index into memory

                }

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

                if (inMemSrh != null && !inMemSrh.isEmpty()) {
                    searchResult.addAll(server.Search(q));
                } else {
                    try (PreparedStatement pStmnt = con.prepareStatement("select cdocid from tdocuments t where exists(select 1 from tdocument_indexes t2 where t.cdocid = t2.cdocid and H(?,R(corder))||R(corder) = ctoken)")) {

                        pStmnt.setString(1, q);
                        ResultSet rs = pStmnt.executeQuery();

                        while (rs.next()) {
                            searchResult.add(rs.getString(1));
                        }

                    } catch (Exception e) {
                        throw e;
                    }
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

            } else if (st.equalsIgnoreCase(Constants.SSE_TYPE_VASST + "")) {

                final int MAX_RESULT = 10;

                String placeHolders = Strings.repeat("?,", qValues.length).substring(0, 2 * qValues.length - 1);

//                SearchResult searchResult = new SearchResult();

                List<RelScore> relScores = new ArrayList<>();

                List<String> matchedDocIds = new ArrayList<>();

                Map<String, Integer> docTypeLookup = new HashMap<>();

                //find the document id which contains the keywords
                try (PreparedStatement pStmnt = con.prepareStatement("select cdocid, cft from tdocuments d where exists(select 1 from tdoc_term_freq dtf where d.cdocid = dtf.cdocid and dtf.cword in (" + placeHolders + "))")) {
                    for (int i = 1; i <= qValues.length; i++) {
                        pStmnt.setString(i, qValues[i - 1]);
                    }

                    ResultSet rs = pStmnt.executeQuery();
                    while (rs.next()) {
                        String docId = rs.getString(1);
                        matchedDocIds.add(docId);
                        docTypeLookup.put(docId, rs.getInt(2));
                    }

                } catch (SQLException e) {
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
                    throw e;
                }
                //end

                Map<String, RelScore> docScoreMap = new HashMap<>();

                //calculate tf-idf
                for (int i = 0; i < matchedDocCnt; i++) {
                    String curDocId = matchedDocIds.get(i);
                    try (PreparedStatement pStmnt = con.prepareStatement("select " +
                            "dtf.ccount, (select max(ccount) from tdoc_term_freq dtf2 where dtf.cdocid=dtf2.cdocid) max_ccount, " +
                            "cword " +
                            "from tdoc_term_freq dtf where cdocid = ?")) {

                        pStmnt.setString(1, curDocId);

                        ResultSet rs = pStmnt.executeQuery();
                        while (rs.next()) {
                            int tf = rs.getInt(1);  //term freq.
                            int mtf = rs.getInt(2); //max term freq.
                            String word = rs.getString(3); //the encrypted keyword, not necessary
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
                            relScore.setScore(oldScore + tfidf);
                        }

                    } catch (SQLException e) {
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

                System.out.println(gson.toJson(relScores));

                JsonArray jsonArray = new JsonArray();

                int minResult = Math.min(relScores.size(), MAX_RESULT);

                for (int i = 0; i < minResult; i++) {
                    String docId = relScores.get(i).getDocId();
                    Integer type = docTypeLookup.get(docId);
                    JsonObject tmp = new JsonObject();
                    tmp.addProperty("name", docId);
                    tmp.addProperty("type", type);
                    jsonArray.add(tmp);
                }

                JsonObject ok = ResponseFactory.getResponse(ResponseFactory.ResponseType.GENERIC_JSON_OK);
                ok.addProperty("count", jsonArray.size());
                ok.add("files", jsonArray);
                out.write(ok.toString());

                System.out.println(ok.toString());
            }

        } catch (Exception ex) {
            ex.printStackTrace();
            JsonObject err = ResponseFactory.getResponse(ResponseFactory.ResponseType.GENERIC_JSON_ERR);
            out.write(err.toString());
        }
    }

    protected void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
        response.getWriter().append("Served at: ").append(request.getContextPath());
    }
}
