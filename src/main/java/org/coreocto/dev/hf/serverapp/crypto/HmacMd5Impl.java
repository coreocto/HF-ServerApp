package org.coreocto.dev.hf.serverapp.crypto;

import org.coreocto.dev.hf.commonlib.crypto.IKeyedHashFunc;
import org.coreocto.dev.hf.serverapp.AppConstants;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import java.io.UnsupportedEncodingException;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;

public class HmacMd5Impl implements IKeyedHashFunc {
    private static final String HMAC_MD5_ALGORITHM = "HmacMD5";

    public byte[] getHash(String key, String s) throws UnsupportedEncodingException, InvalidKeyException, NoSuchAlgorithmException {
        return this.getHash(key.getBytes(AppConstants.ENCODING_UTF8), s.getBytes(AppConstants.ENCODING_UTF8));
    }

    public byte[] getHash(byte[] key, byte[] data) throws NoSuchAlgorithmException, InvalidKeyException {
        SecretKeySpec signingKey = new SecretKeySpec(key, HMAC_MD5_ALGORITHM);
        Mac mac = Mac.getInstance(HMAC_MD5_ALGORITHM);
        mac.init(signingKey);
        return mac.doFinal(data);
    }
}

