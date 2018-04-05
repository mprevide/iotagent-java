package br.com.dojot.auth;

import org.apache.commons.codec.binary.Base64;

public class Auth {

    private static Auth mInstance;

    private Auth() {}

    public static synchronized Auth getInstance() {
        if (mInstance == null) {
            mInstance = new Auth();
        }
        return mInstance;
    }

    /**
     * Generate token to be used in internal communication.
     * @param tenant is the dojot tenant for which the token should be valid for
     * @return JWT token to be used in the requests
     */
    public String getToken(String tenant) {
        StringBuffer payload = new StringBuffer("{\"service\":\"");
        payload.append(tenant);
        payload.append("\",\"username\":\"iotagent\"}");

        System.out.println(payload);

        StringBuffer response = new StringBuffer(Base64.encodeBase64String("jwt schema".getBytes()));
        response.append(".");
        response.append(Base64.encodeBase64String(payload.toString().getBytes()));
        response.append(".");
        response.append(Base64.encodeBase64String("dummy signature".getBytes()));

        return response.toString();
    }
}
