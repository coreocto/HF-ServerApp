package org.coreocto.dev.hf.serverapp.servlet;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.Files;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.util.Iterator;

import javax.servlet.ServletContext;
import javax.servlet.ServletException;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.fileupload.FileItem;
import org.apache.commons.fileupload.disk.DiskFileItemFactory;
import org.apache.commons.fileupload.servlet.ServletFileUpload;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import org.coreocto.dev.hf.commonlib.suise.bean.AddTokenResult;

@WebServlet(
        urlPatterns = "/upload",
        name = "UploadServlet"
)
public class UploadServlet extends HttpServlet {

    private static final String DATA_DIRECTORY = "data";
    private static final String TOKEN_DIRECTORY = "token";
    private static final int MAX_MEMORY_SIZE = 1024 * 1024 * 2;
    private static final int MAX_REQUEST_SIZE = 1024 * 1024 * 10;

    protected void doPost(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
        ServletContext ctx = getServletContext();

        PrintWriter out = response.getWriter();

        // Check that we have a file upload request
        boolean isMultipart = ServletFileUpload.isMultipartContent(request);

        if (isMultipart) {
            // Create a factory for disk-based file items
            DiskFileItemFactory factory = new DiskFileItemFactory();

            // Sets the size threshold beyond which files are written directly to disk.
            factory.setSizeThreshold(MAX_MEMORY_SIZE);

            // Sets the directory used to temporarily store files that are larger
            // than the configured size threshold. We use temporary directory for
            // java
            factory.setRepository(new File(System.getProperty("java.io.tmpdir")));

            // constructs the folder where uploaded file will be stored
            String uploadFolder = getServletContext().getRealPath("") + File.separator + DATA_DIRECTORY;

            String tokenFolder = getServletContext().getRealPath("") + File.separator + TOKEN_DIRECTORY;

            // Create a new file upload handler
            ServletFileUpload upload = new ServletFileUpload(factory);

            // Set overall request size constraint
            upload.setSizeMax(MAX_REQUEST_SIZE);

            try {
                // Parse the request
                Iterator<FileItem> iter = upload.parseRequest(request).iterator();

                FileItem dataFileItem = null;
                FileItem tokenFileItem = null;

                while (iter.hasNext()) {
                    FileItem tmp = iter.next();
                    if (!tmp.isFormField()) {
                        if ("data".equals(tmp.getFieldName())) {
                            dataFileItem = tmp;
                        } else if ("token".equals(tmp.getFieldName())) {
                            tokenFileItem = tmp;
                        }
                    }
                }

                Connection con = (Connection) ctx.getAttribute("DBConnection");

                int rowCnt = 0;

                if (dataFileItem != null) {
                    File dataFile = new File(uploadFolder + File.separator + dataFileItem.getName());
                    dataFileItem.write(dataFile);

                    try (PreparedStatement pStmnt = con.prepareStatement("insert into tdocuments (cdocid,cdelete) values (?,?)")) {

                        pStmnt.setString(1, dataFileItem.getName());
                        pStmnt.setInt(2, 0);
                        rowCnt = pStmnt.executeUpdate();

                    } catch (Exception e) {
                        e.printStackTrace();
                    }

                }

                rowCnt = 0;

                if (tokenFileItem != null) {
                    File tokenFile = File.createTempFile("suise-", ".token", new File(tokenFolder));
                    tokenFileItem.write(tokenFile);

                    byte[] encoded = Files.readAllBytes(tokenFile.toPath());
                    String jsonStr = new String(encoded, "UTF-8");

                    AddTokenResult addTokenResult = new Gson().fromJson(jsonStr, AddTokenResult.class);

                    // because the file now save in google drive, the code for dataFileItem would not execute
                    // we need to insert a document record here
                    if (dataFileItem == null) {
                        try (PreparedStatement pStmnt = con.prepareStatement("insert into tdocuments (cdocid,cdelete) values (?,?)")) {

                            pStmnt.setString(1, addTokenResult.getId());
                            pStmnt.setInt(2, 0);
                            rowCnt = pStmnt.executeUpdate();

                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                    }

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

                    System.out.println("token file saved at " + tokenFile.getAbsolutePath());
                }

                JsonObject jsonObj = new JsonObject();
                jsonObj.addProperty("status", "ok");

                out.write(jsonObj.toString());

                // displays done.jsp page after upload finished
                // getServletContext().getRequestDispatcher("/done.jsp").forward(request, response);

            } catch (Exception ex) {
                JsonObject jsonObj = new JsonObject();
                jsonObj.addProperty("status", "error");
                throw new ServletException(ex);
            }
        } else {
            String docId = request.getParameter("docId");
            String tokenInJson = request.getParameter("token");

            Connection con = (Connection) ctx.getAttribute("DBConnection");

            if (docId == null || tokenInJson == null) {
                System.out.println("tokenInJson = " + tokenInJson);
                System.out.println("docId = " + docId);
                return;
            }

            int rowCnt = 0;

            try {
                AddTokenResult addTokenResult = new Gson().fromJson(tokenInJson, AddTokenResult.class);

                // because the file now save in google drive, the code for dataFileItem would not execute
                // we need to insert a document record here

                try (PreparedStatement pStmnt = con.prepareStatement("insert into tdocuments (cdocid,cdelete) select ? as text, ? as integer where not exists (select 1 from tdocuments where cdocid = ? and cdelete = ?)")) {

                    pStmnt.setString(1, addTokenResult.getId());
                    pStmnt.setInt(2, 0);
                    pStmnt.setString(3, addTokenResult.getId());
                    pStmnt.setInt(4, 0);
                    rowCnt = pStmnt.executeUpdate();

                } catch (Exception e) {
                    e.printStackTrace();
                }

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

                JsonObject jsonObj = new JsonObject();
                jsonObj.addProperty("status", "ok");

                out.write(jsonObj.toString());

            } catch (Exception ex) {
                JsonObject jsonObj = new JsonObject();
                jsonObj.addProperty("status", "error");
                throw new ServletException(ex);
            }

        }
    }

    protected void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
        response.getWriter().append("Served at: ").append(request.getContextPath());
    }
}
