package br.com.dojot;

import br.com.dojot.auth.Auth;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class AuthTest {

    @Test
    public void testValidTokenGeneration() {
        String token = Auth.getInstance().getToken("admin");
        assertEquals(token,
                "and0IHNjaGVtYQ==.eyJzZXJ2aWNlIjoiYWRtaW4iLCJ1c2VybmFtZSI6ImlvdGFnZW50In0=.ZHVtbXkgc2lnbmF0dXJl");
    }
}

