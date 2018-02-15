package org.coreocto.dev.hf.serverapp;

import org.coreocto.dev.hf.commonlib.Constants;

public class AppConstants {
    public static final int STATUS_OK = 200;
    public static final int STATUS_ERR = 500;
    public static final String MSG_OK = "ok";
    public static final String ENCODING_UTF8 = "UTF-8";
    public static final int SSE_TYPE_SUISE_2 = Constants.SSE_TYPE_SUISE - 1;    //suise + all keyword suffix search
    public static final int SSE_TYPE_SUISE_3 = SSE_TYPE_SUISE_2 - 1;            //suise + all keyword prefix search
}
