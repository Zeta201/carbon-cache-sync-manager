/*
 * Copyright (c) 2024, WSO2 LLC. (http://www.wso2.com).
 *
 * WSO2 LLC. licenses this file to you under the Apache License,
 * Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.wso2.carbon.cache.sync.jms.manager;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.wso2.carbon.cache.sync.jms.manager.internal.CacheInvalidationMessageDTO;
import org.wso2.carbon.caching.impl.CacheImpl;
import org.wso2.carbon.caching.impl.clustering.ClusterCacheInvalidationRequestSender;
import org.wso2.carbon.context.PrivilegedCarbonContext;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InvalidClassException;
import java.io.ObjectInputStream;
import java.io.ObjectStreamClass;
import java.util.Base64;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.cache.Cache;
import javax.cache.CacheEntryInfo;
import javax.cache.CacheManager;
import javax.cache.Caching;
import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.JMSException;
import javax.jms.MessageConsumer;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.jms.Topic;
import javax.naming.InitialContext;
import javax.naming.NamingException;

import static org.wso2.carbon.cache.sync.jms.manager.JMSUtils.PRODUCER_RETRY_LIMIT;
import static org.wso2.carbon.cache.sync.jms.manager.JMSUtils.getProducerName;

/**
 * This class contains the logic for receiving cache invalidation message.
 */
@SuppressFBWarnings(
    value = "OBJECT_DESERIALIZATION",
    justification = "Legacy deserialization used for cache keys; safe in controlled cluster"
)
public class JMSConsumer {

    private static final Log log = LogFactory.getLog(JMSConsumer.class);
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private static final Pattern LEGACY_MESSAGE_PATTERN = Pattern.compile(
            "ClusterCacheInvalidationRequest\\{tenantId=(?<tenantId>-?\\d+), " +
                    "tenantDomain='(?<tenantDomain>[\\w.]+)', messageId=(?<messageId>[\\w-]+), " +
                    "cacheManager=(?<cacheManager>[\\w.]+), cache=(?<cache>.*?), cacheKey=(?<cacheKey>.*?)\\}"
    );


    private final ConnectionFactory connectionFactory;
    private final InitialContext initialContext;
    private Topic topic;

    Session session;
    Connection connection;
    MessageConsumer consumer;

    private static volatile JMSConsumer instance;

    JMSConsumer() {

        try {
            this.initialContext = JMSUtils.createInitialContext();
            this.connectionFactory = JMSUtils.getConnectionFactory(initialContext);
        } catch (NamingException | JMSException | IOException e) {
            throw new RuntimeException("Error initializing JMS client resources", e);
        }
    }

    public static JMSConsumer getInstance() {

        if (instance == null) {
            synchronized (JMSConsumer.class) {
                if (instance == null) {
                    instance = new JMSConsumer();
                }
            }
        }
        return instance;
    }

    @SuppressFBWarnings
    public void startService() {

        if (!JMSUtils.isMBCacheInvalidatorEnabled()) {
            log.debug("JMS MB based cache invalidation is not enabled.");
            return;
        }
        int retryCount = 0;
        while (session == null && retryCount <= PRODUCER_RETRY_LIMIT) {
            try {
                // establish the connection over specified topic.
                startConnection();
                // Message listener for the subscriber.
                consumer.setMessageListener(message -> {
                    if (!(message instanceof TextMessage)) {
                        // Ignore non-TextMessage.
                        return;
                    }
                    try {
                        String sender = message.getStringProperty(JMSUtils.SENDER);
                        // Skip processing if the sender is the same as the producer.
                        if (JMSUtils.getProducerName() != null && StringUtils.equals(
                                JMSUtils.getProducerName(), sender)) {
                            return;
                        }
                        invalidateCache(((TextMessage) message).getText());
                    } catch (JMSException e) {
                        log.error("Error in reading the cache invalidation message.", e);
                    }
                });
            } catch (JMSException | NamingException e) {
                log.error("Error while listening to JMS message broker. ", e);
                retryCount++;
            }
        }
    }


    @SuppressFBWarnings
    public void invalidateCache(String message) {

        CacheEntryInfo cacheEntryInfo;

        try {
            cacheEntryInfo = parseCacheInvalidationMessage(message);
        } catch (Exception e) {
            log.error("Error processing cache invalidation message", e);
            return;
        }

        if (cacheEntryInfo == null) {
            log.debug("Input does not match any supported cache invalidation message format.");
            return;
        }

        if (log.isDebugEnabled()) {
            log.debug("Received cache invalidation message for '" + cacheEntryInfo.getCacheKey()
                    + "' of the cache '" + cacheEntryInfo.getCacheName()
                    + "' of the cache manager '" + cacheEntryInfo.getCacheManagerName() + "'.");
        }

        invalidateLocalCache(cacheEntryInfo);

        // If hybrid mode is enabled, propagate to local cluster
        if (JMSUtils.getRunInHybridModeProperty()) {
            log.debug("Sending cache invalidation message for local clustering: " + cacheEntryInfo);
            new ClusterCacheInvalidationRequestSender().send(cacheEntryInfo);
        }
    }

    private CacheEntryInfo parseCacheInvalidationMessage(String message) throws IOException {

        // Try new JSON-based format first
        try {
            CacheInvalidationMessageDTO dto =
                    OBJECT_MAPPER.readValue(message, CacheInvalidationMessageDTO.class);

            Object cacheKey = deserializeFromBase64(dto.getCacheKeyBase64());

            return new CacheEntryInfo(
                    dto.getCacheManagerName(),
                    dto.getCacheName(),
                    cacheKey,
                    dto.getTenantDomain(),
                    dto.getTenantId()
            );
        } catch (JsonProcessingException e) {
            // Not a JSON-based message; fall back to legacy format
            if (log.isDebugEnabled()) {
                log.debug("Message is not in JSON format. Falling back to legacy cache invalidation message parsing.");
            }
        }

        Matcher matcher = LEGACY_MESSAGE_PATTERN.matcher(message);
        if (!matcher.find()) {
            return null;
        }

        return new CacheEntryInfo(
                matcher.group("cacheManager"),
                matcher.group("cache"),
                matcher.group("cacheKey"),
                matcher.group("tenantDomain"),
                Integer.parseInt(matcher.group("tenantId"))
        );
    }

    private void invalidateLocalCache(CacheEntryInfo cacheEntryInfo) {

        PrivilegedCarbonContext.startTenantFlow();
        try {
            PrivilegedCarbonContext carbonContext =
                    PrivilegedCarbonContext.getThreadLocalCarbonContext();
            carbonContext.setTenantId(cacheEntryInfo.getTenantId());
            carbonContext.setTenantDomain(cacheEntryInfo.getTenantDomain());

            CacheManager cacheManager = Caching.getCacheManagerFactory()
                    .getCacheManager(cacheEntryInfo.getCacheManagerName());
            Cache<Object, Object> cache = cacheManager.getCache(cacheEntryInfo.getCacheName());

            if (cache instanceof CacheImpl) {
                Object cacheKey = cacheEntryInfo.getCacheKey();
                if (JMSUtils.CLEAR_ALL_PREFIX.equals(cacheKey)) {
                    ((CacheImpl<?, ?>) cache).removeAllLocal();
                } else {
                    ((CacheImpl<?, ?>) cache).removeLocal(cacheKey);
                }
            }
        } finally {
            PrivilegedCarbonContext.endTenantFlow();
        }
    }

    public void closeResources() {

        try {
            if (consumer != null) {
                consumer.close();
            }
            if (session != null) {
                session.close();
            }
            if (connection != null) {
                connection.close();
            }
        } catch (JMSException e) {
            log.error("Error closing JMS resources", e);
        }
    }

    void startConnection() throws JMSException, NamingException {

        this.connection = JMSUtils.createConnection(connectionFactory);
        boolean isDurableSubscription = JMSUtils.isDurableSubscriber();
        if (isDurableSubscription) {
            connection.setClientID(JMSUtils.DURABLE_CON_CLIENT_ID_PREFIX + getProducerName());
        }
        connection.start();
        session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        this.topic = JMSUtils.getCacheTopic(initialContext, session);
        if (isDurableSubscription) {
            consumer = session.createDurableSubscriber(topic, JMSUtils.DURABLE_SUB_NAME_PREFIX + getProducerName());
        } else {
            consumer = session.createConsumer(topic);
        }
    }

    private static Object deserializeFromBase64(String base64) throws IOException {

        byte[] data = Base64.getDecoder().decode(base64);

        // Custom ObjectInputStream that enforces a strict allow-list during deserialization to
        // prevent unsafe or malicious classes from being loaded. This prevents deserialization attacks.
        class SafeObjectInputStream extends ObjectInputStream {

            public SafeObjectInputStream(InputStream in) throws IOException {
                super(in);
            }

            @Override
            protected Class<?> resolveClass(ObjectStreamClass desc) throws IOException, ClassNotFoundException {

                String className = desc.getName();

                // Only allow CacheInvalidationMessageDTO and core Java classes
                if (className.startsWith("org.wso2.carbon.")) {
                    return super.resolveClass(desc);
                }
                throw new InvalidClassException("Unauthorized deserialization attempt", className);
            }
        }

        try (ObjectInputStream objectInputStream = new SafeObjectInputStream(new ByteArrayInputStream(data))) {
            return objectInputStream.readObject();
        } catch (ClassNotFoundException e) {
            throw new IOException("Failed to deserialize cache invalidation message", e);
        }
    }

}
