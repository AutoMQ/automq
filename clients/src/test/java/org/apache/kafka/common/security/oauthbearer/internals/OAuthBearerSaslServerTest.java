/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.kafka.common.security.oauthbearer.internals;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import javax.security.auth.callback.Callback;
import javax.security.auth.callback.UnsupportedCallbackException;
import javax.security.auth.login.LoginException;

import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.config.types.Password;
import org.apache.kafka.common.errors.SaslAuthenticationException;
import org.apache.kafka.common.security.JaasContext;
import org.apache.kafka.common.security.auth.AuthenticateCallbackHandler;
import org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule;
import org.apache.kafka.common.security.oauthbearer.OAuthBearerToken;
import org.apache.kafka.common.security.oauthbearer.OAuthBearerTokenCallback;
import org.apache.kafka.common.security.oauthbearer.internals.unsecured.OAuthBearerConfigException;
import org.apache.kafka.common.security.oauthbearer.internals.unsecured.OAuthBearerUnsecuredLoginCallbackHandler;
import org.apache.kafka.common.security.oauthbearer.internals.unsecured.OAuthBearerUnsecuredValidatorCallbackHandler;
import org.junit.Before;
import org.junit.Test;

public class OAuthBearerSaslServerTest {
    private static final String USER = "user";
    private static final Map<String, ?> CONFIGS;
    static {
        String jaasConfigText = "org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule Required"
                + " unsecuredLoginStringClaim_sub=\"" + USER + "\";";
        Map<String, Object> tmp = new HashMap<>();
        tmp.put(SaslConfigs.SASL_JAAS_CONFIG, new Password(jaasConfigText));
        CONFIGS = Collections.unmodifiableMap(tmp);
    }
    private static final AuthenticateCallbackHandler LOGIN_CALLBACK_HANDLER;
    static {
        LOGIN_CALLBACK_HANDLER = new OAuthBearerUnsecuredLoginCallbackHandler();
        LOGIN_CALLBACK_HANDLER.configure(CONFIGS, OAuthBearerLoginModule.OAUTHBEARER_MECHANISM,
                JaasContext.loadClientContext(CONFIGS).configurationEntries());
    }
    private static final AuthenticateCallbackHandler VALIDATOR_CALLBACK_HANDLER;
    static {
        VALIDATOR_CALLBACK_HANDLER = new OAuthBearerUnsecuredValidatorCallbackHandler();
        VALIDATOR_CALLBACK_HANDLER.configure(CONFIGS, OAuthBearerLoginModule.OAUTHBEARER_MECHANISM,
                JaasContext.loadClientContext(CONFIGS).configurationEntries());
    }
    private OAuthBearerSaslServer saslServer;

    @Before
    public void setUp() throws Exception {
        saslServer = new OAuthBearerSaslServer(VALIDATOR_CALLBACK_HANDLER);
    }

    @Test
    public void noAuthorizationIdSpecified() throws Exception {
        byte[] nextChallenge = saslServer
                .evaluateResponse(clientInitialResponse(null));
        assertTrue("Next challenge is not empty", nextChallenge.length == 0);
    }

    @Test
    public void authorizatonIdEqualsAuthenticationId() throws Exception {
        byte[] nextChallenge = saslServer
                .evaluateResponse(clientInitialResponse(USER));
        assertTrue("Next challenge is not empty", nextChallenge.length == 0);
    }

    @Test(expected = SaslAuthenticationException.class)
    public void authorizatonIdNotEqualsAuthenticationId() throws Exception {
        saslServer.evaluateResponse(clientInitialResponse(USER + "x"));
    }

    @Test
    public void illegalToken() throws Exception {
        byte[] bytes = saslServer.evaluateResponse(clientInitialResponse(null, true));
        String challenge = new String(bytes, StandardCharsets.UTF_8);
        assertEquals("{\"status\":\"invalid_token\"}", challenge);
    }

    private byte[] clientInitialResponse(String authorizationId)
            throws OAuthBearerConfigException, IOException, UnsupportedCallbackException, LoginException {
        return clientInitialResponse(authorizationId, false);
    }

    private byte[] clientInitialResponse(String authorizationId, boolean illegalToken)
            throws OAuthBearerConfigException, IOException, UnsupportedCallbackException, LoginException {
        OAuthBearerTokenCallback callback = new OAuthBearerTokenCallback();
        LOGIN_CALLBACK_HANDLER.handle(new Callback[] {callback});
        OAuthBearerToken token = callback.token();
        String compactSerialization = token.value();
        String tokenValue = compactSerialization + (illegalToken ? "AB" : "");
        return new OAuthBearerClientInitialResponse(tokenValue, authorizationId, Collections.emptyMap()).toBytes();
    }
}
