package org.coreocto.dev.hf.serverapp.util;

import org.coreocto.dev.hf.commonlib.util.IBase64;

import java.util.Base64;

public class JavaBase64Impl implements IBase64 {

    @Override
    public String encodeToString(byte[] bytes) {
        return Base64.getEncoder().encodeToString(bytes);
    }

    @Override
    public byte[] decodeToByteArray(String s) {
        return Base64.getDecoder().decode(s);
    }
}
