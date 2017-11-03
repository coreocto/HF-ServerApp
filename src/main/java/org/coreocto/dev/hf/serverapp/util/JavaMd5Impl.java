package org.coreocto.dev.hf.serverapp.util;

import org.coreocto.dev.hf.commonlib.util.IMd5;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

public class JavaMd5Impl implements IMd5 {

    private MessageDigest md = null;

    {
        try {
            md = MessageDigest.getInstance("MD5");
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        }
    }

    @Override
    public byte[] getHash(String s) {
        return getHash(s.getBytes());
    }

    @Override
    public byte[] getHash(byte[] bytes) {
        return md.digest(bytes);
    }

}
