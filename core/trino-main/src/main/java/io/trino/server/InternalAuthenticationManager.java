/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.trino.server;

import com.google.common.collect.ImmutableList;
import com.google.common.hash.Hashing;
import com.google.inject.Inject;
import io.airlift.http.client.HttpRequestFilter;
import io.airlift.http.client.Request;
import io.airlift.log.Logger;
import io.airlift.node.NodeInfo;
import io.jsonwebtoken.JwtException;
import io.jsonwebtoken.JwtParser;
import io.trino.server.security.InternalPrincipal;
import io.trino.server.security.SecurityConfig;
import io.trino.spi.security.Identity;
import jakarta.ws.rs.container.ContainerRequestContext;
import jakarta.ws.rs.core.Response;

import javax.crypto.SecretKey;

import java.time.Instant;
import java.time.ZonedDateTime;
import java.util.Date;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.function.Supplier;

import static io.airlift.http.client.Request.Builder.fromRequest;
import static io.jsonwebtoken.security.Keys.hmacShaKeyFor;
import static io.trino.server.ServletSecurityUtils.setAuthenticatedIdentity;
import static io.trino.server.security.jwt.JwtUtil.newJwtBuilder;
import static io.trino.server.security.jwt.JwtUtil.newJwtParserBuilder;
import static jakarta.ws.rs.core.MediaType.TEXT_PLAIN_TYPE;
import static jakarta.ws.rs.core.Response.Status.UNAUTHORIZED;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.time.temporal.ChronoUnit.MINUTES;
import static java.util.Objects.requireNonNull;

public class InternalAuthenticationManager
        implements HttpRequestFilter
{
    private static final Logger log = Logger.get(InternalAuthenticationManager.class);
    private static final Supplier<Instant> DEFAULT_EXPIRATION_SUPPLIER = () -> ZonedDateTime.now().plusMinutes(6).toInstant();
    // Leave a 5 minute buffer to allow for clock skew and GC pauses
    private static final Function<Instant, Instant> TOKEN_REUSE_THRESHOLD = instant -> instant.minus(5, MINUTES);

    private static final String TRINO_INTERNAL_BEARER = "X-Trino-Internal-Bearer";

    private final SecretKey hmac;
    private final String nodeId;
    private final JwtParser jwtParser;
    private final AtomicReference<InternalToken> currentToken;

    @Inject
    public InternalAuthenticationManager(InternalCommunicationConfig internalCommunicationConfig, SecurityConfig securityConfig, NodeInfo nodeInfo)
    {
        this(getSharedSecret(internalCommunicationConfig, nodeInfo, !securityConfig.getAuthenticationTypes().equals(ImmutableList.of("insecure"))), nodeInfo.getNodeId());
    }

    private static String getSharedSecret(InternalCommunicationConfig internalCommunicationConfig, NodeInfo nodeInfo, boolean authenticationEnabled)
    {
        requireNonNull(nodeInfo, "nodeInfo is null");

        // This check should not be required (as bean validation already checked it),
        // but be extra careful to not use a known secret for authentication.
        if (!internalCommunicationConfig.isRequiredSharedSecretSet()) {
            throw new IllegalArgumentException("Shared secret (internal-communication.shared-secret) is required when internal communications uses HTTPS");
        }

        if (internalCommunicationConfig.getSharedSecret().isEmpty() && authenticationEnabled) {
            throw new IllegalArgumentException("Shared secret (internal-communication.shared-secret) is required when authentication is enabled");
        }

        return internalCommunicationConfig.getSharedSecret().orElseGet(nodeInfo::getEnvironment);
    }

    public InternalAuthenticationManager(String sharedSecret, String nodeId)
    {
        requireNonNull(sharedSecret, "sharedSecret is null");
        requireNonNull(nodeId, "nodeId is null");
        this.hmac = hmacShaKeyFor(Hashing.sha256().hashString(sharedSecret, UTF_8).asBytes());
        this.nodeId = nodeId;
        this.jwtParser = newJwtParserBuilder().verifyWith(hmac).build();
        this.currentToken = new AtomicReference<>(createJwt());
    }

    public static boolean isInternalRequest(ContainerRequestContext request)
    {
        return request.getHeaders().getFirst(TRINO_INTERNAL_BEARER) != null;
    }

    public void handleInternalRequest(ContainerRequestContext request)
    {
        String subject;
        try {
            subject = parseJwt(request.getHeaders().getFirst(TRINO_INTERNAL_BEARER));
        }
        catch (JwtException e) {
            log.error(e, "Internal authentication failed");
            request.abortWith(Response.status(UNAUTHORIZED)
                    .type(TEXT_PLAIN_TYPE.toString())
                    .build());
            return;
        }
        catch (RuntimeException e) {
            throw new RuntimeException("Authentication error", e);
        }

        Identity identity = Identity.forUser("<internal>")
                .withPrincipal(new InternalPrincipal(subject))
                .build();
        setAuthenticatedIdentity(request, identity);
    }

    @Override
    public Request filterRequest(Request request)
    {
        return fromRequest(request)
                .addHeader(TRINO_INTERNAL_BEARER, getOrGenerateJwt())
                .build();
    }

    private String getOrGenerateJwt()
    {
        InternalToken token = currentToken.get();
        if (token.isExpired()) {
            InternalToken newToken = createJwt();
            if (currentToken.compareAndSet(token, newToken)) {
                token = newToken;
            }
            else {
                // Another thread already generated a new token
                token = currentToken.get();
            }
        }
        return token.token();
    }

    private InternalToken createJwt()
    {
        Instant expiration = DEFAULT_EXPIRATION_SUPPLIER.get();
        return new InternalToken(expiration, newJwtBuilder()
                .signWith(hmac)
                .subject(nodeId)
                .expiration(Date.from(expiration))
                .compact());
    }

    private String parseJwt(String jwt)
    {
        return jwtParser
                .parseSignedClaims(jwt)
                .getPayload()
                .getSubject();
    }

    private record InternalToken(Instant expiration, String token)
    {
        public InternalToken
        {
            expiration = TOKEN_REUSE_THRESHOLD.apply(requireNonNull(expiration, "expiration is null"));
            requireNonNull(token, "token is null");
        }

        public boolean isExpired()
        {
            return Instant.now().isAfter(expiration);
        }
    }
}
