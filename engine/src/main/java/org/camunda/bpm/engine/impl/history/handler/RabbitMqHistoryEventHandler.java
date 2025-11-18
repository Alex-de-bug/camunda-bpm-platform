/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH
 * under one or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information regarding copyright
 * ownership. Camunda licenses this file to you under the Apache License,
 * Version 2.0; you may not use this file except in compliance with the License.
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
package org.camunda.bpm.engine.impl.history.handler;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeoutException;

import org.camunda.bpm.engine.ProcessEngineException;
import org.camunda.bpm.engine.impl.history.event.HistoryEvent;
import org.camunda.bpm.engine.impl.util.EnsureUtil;
import org.camunda.bpm.engine.impl.util.JsonUtil;

import com.google.gson.Gson;
import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.BuiltinExchangeType;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

/**
 * {@link HistoryEventHandler} implementation that publishes every produced history event
 * to a RabbitMQ exchange. Use {@link #builder()} to configure connection credentials,
 * exchanges and routing-keys that should receive the serialized events.
 */
public class RabbitMqHistoryEventHandler implements HistoryEventHandler, AutoCloseable {

  protected static final String DEFAULT_CONNECTION_NAME = "camunda-history-handler";

  protected final ConnectionFactory connectionFactory;
  protected final String exchangeName;
  protected final String routingKey;
  protected final boolean mandatory;
  protected final boolean persistentDelivery;
  protected final boolean declareExchange;
  protected final boolean durableExchange;
  protected final BuiltinExchangeType exchangeType;
  protected final String connectionName;
  protected final Map<String, Object> staticHeaders;
  protected final Gson gson;
  protected final AMQP.BasicProperties messageProperties;

  protected final Object channelLock = new Object();
  protected volatile Connection connection;
  protected volatile Channel channel;

  protected RabbitMqHistoryEventHandler(ConnectionFactory connectionFactory,
                                        String exchangeName,
                                        String routingKey,
                                        boolean mandatory,
                                        boolean persistentDelivery,
                                        boolean declareExchange,
                                        BuiltinExchangeType exchangeType,
                                        boolean durableExchange,
                                        String connectionName,
                                        Map<String, Object> staticHeaders,
                                        Gson gson) {
    EnsureUtil.ensureNotNull("client configuration", connectionFactory);
    EnsureUtil.ensureNotNull("json mapper", gson);
    if (declareExchange) {
      EnsureUtil.ensureNotEmpty("exchangeName", exchangeName);
    }

    this.connectionFactory = connectionFactory;
    this.exchangeName = exchangeName != null ? exchangeName : "";
    this.routingKey = routingKey != null ? routingKey : "";
    this.mandatory = mandatory;
    this.persistentDelivery = persistentDelivery;
    this.declareExchange = declareExchange;
    this.exchangeType = exchangeType != null ? exchangeType : BuiltinExchangeType.TOPIC;
    this.durableExchange = durableExchange;
    this.connectionName = connectionName != null ? connectionName : DEFAULT_CONNECTION_NAME;
    this.staticHeaders = staticHeaders == null ? Collections.<String, Object>emptyMap() : Collections.unmodifiableMap(new HashMap<>(staticHeaders));
    this.gson = gson;
    this.messageProperties = buildMessageProperties();
  }

  public static Builder builder() {
    return new Builder();
  }

  @Override
  public void handleEvent(HistoryEvent historyEvent) {
    if (historyEvent == null) {
      return;
    }

    publish(historyEvent);
  }

  @Override
  public void handleEvents(List<HistoryEvent> historyEvents) {
    if (historyEvents == null || historyEvents.isEmpty()) {
      return;
    }

    for (HistoryEvent historyEvent : historyEvents) {
      handleEvent(historyEvent);
    }
  }

  protected void publish(HistoryEvent historyEvent) {
    byte[] payload = gson.toJson(historyEvent).getBytes(StandardCharsets.UTF_8);

    try {
      Channel rabbitChannel = ensureChannel();
      synchronized (channelLock) {
        rabbitChannel.basicPublish(exchangeName, routingKey, mandatory, messageProperties, payload);
      }
    } catch (IOException e) {
      throw new ProcessEngineException("Unable to publish history event to RabbitMQ", e);
    }
  }

  protected Channel ensureChannel() {
    Channel localChannel = channel;
    if (localChannel == null || !localChannel.isOpen()) {
      synchronized (channelLock) {
        if (channel == null || !channel.isOpen()) {
          channel = openChannel();
        }
        localChannel = channel;
      }
    }
    return localChannel;
  }

  protected Channel openChannel() {
    try {
      Channel newChannel = ensureConnection().createChannel();
      if (declareExchange && exchangeName != null && !exchangeName.isEmpty()) {
        newChannel.exchangeDeclare(exchangeName, exchangeType, durableExchange);
      }
      return newChannel;
    } catch (IOException e) {
      throw new ProcessEngineException("Unable to open RabbitMQ channel", e);
    }
  }

  protected Connection ensureConnection() {
    Connection localConnection = connection;
    if (localConnection == null || !localConnection.isOpen()) {
      synchronized (channelLock) {
        if (connection == null || !connection.isOpen()) {
          try {
            connection = connectionFactory.newConnection(connectionName);
          } catch (IOException | TimeoutException e) {
            throw new ProcessEngineException("Unable to open RabbitMQ connection", e);
          }
        }
        localConnection = connection;
      }
    }
    return localConnection;
  }

  @Override
  public void close() {
    synchronized (channelLock) {
      closeChannel();
      closeConnection();
    }
  }

  protected void closeChannel() {
    if (channel != null) {
      try {
        if (channel.isOpen()) {
          channel.close();
        }
      } catch (IOException | TimeoutException e) {
        throw new ProcessEngineException("Unable to close RabbitMQ channel", e);
      } finally {
        channel = null;
      }
    }
  }

  protected void closeConnection() {
    if (connection != null) {
      try {
        if (connection.isOpen()) {
          connection.close();
        }
      } catch (IOException e) {
        throw new ProcessEngineException("Unable to close RabbitMQ connection", e);
      } finally {
        connection = null;
      }
    }
  }

  protected AMQP.BasicProperties buildMessageProperties() {
    AMQP.BasicProperties.Builder builder = new AMQP.BasicProperties.Builder()
        .contentType("application/json")
        .deliveryMode(persistentDelivery ? 2 : 1);

    if (!staticHeaders.isEmpty()) {
      builder.headers(staticHeaders);
    }

    return builder.build();
  }

  public static class Builder {

    protected ConnectionFactory connectionFactory;
    protected String uri;
    protected String host = ConnectionFactory.DEFAULT_HOST;
    protected int port = ConnectionFactory.DEFAULT_AMQP_PORT;
    protected String username = ConnectionFactory.DEFAULT_USER;
    protected String password = ConnectionFactory.DEFAULT_PASS;
    protected String virtualHost = ConnectionFactory.DEFAULT_VHOST;
    protected String connectionName = DEFAULT_CONNECTION_NAME;

    protected String exchange = "camunda.history";
    protected BuiltinExchangeType exchangeType = BuiltinExchangeType.TOPIC;
    protected boolean declareExchange = true;
    protected boolean durableExchange = true;
    protected String routingKey = "history.events";
    protected boolean mandatory = true;
    protected boolean persistentDelivery = true;
    protected Map<String, Object> headers = new HashMap<>();
    protected Gson gson = JsonUtil.getGsonMapper();

    public Builder connectionFactory(ConnectionFactory connectionFactory) {
      this.connectionFactory = connectionFactory;
      return this;
    }

    public Builder uri(String uri) {
      this.uri = uri;
      return this;
    }

    public Builder host(String host) {
      this.host = host;
      return this;
    }

    public Builder port(int port) {
      this.port = port;
      return this;
    }

    public Builder username(String username) {
      this.username = username;
      return this;
    }

    public Builder password(String password) {
      this.password = password;
      return this;
    }

    public Builder virtualHost(String virtualHost) {
      this.virtualHost = virtualHost;
      return this;
    }

    public Builder connectionName(String connectionName) {
      this.connectionName = connectionName;
      return this;
    }

    public Builder exchange(String exchange) {
      this.exchange = exchange;
      return this;
    }

    public Builder exchangeType(BuiltinExchangeType exchangeType) {
      this.exchangeType = exchangeType;
      return this;
    }

    public Builder declareExchange(boolean declareExchange) {
      this.declareExchange = declareExchange;
      return this;
    }

    public Builder durableExchange(boolean durableExchange) {
      this.durableExchange = durableExchange;
      return this;
    }

    public Builder routingKey(String routingKey) {
      this.routingKey = routingKey;
      return this;
    }

    public Builder mandatory(boolean mandatory) {
      this.mandatory = mandatory;
      return this;
    }

    public Builder persistentDelivery(boolean persistentDelivery) {
      this.persistentDelivery = persistentDelivery;
      return this;
    }

    public Builder header(String key, Object value) {
      if (key != null && value != null) {
        headers.put(key, value);
      }
      return this;
    }

    public Builder headers(Map<String, Object> headers) {
      if (headers != null) {
        this.headers.clear();
        this.headers.putAll(headers);
      }
      return this;
    }

    public Builder gson(Gson gson) {
      this.gson = gson;
      return this;
    }

    public RabbitMqHistoryEventHandler build() {
      ConnectionFactory configuredFactory = connectionFactory != null ? connectionFactory : createConnectionFactory();
      return new RabbitMqHistoryEventHandler(
          configuredFactory,
          exchange,
          routingKey,
          mandatory,
          persistentDelivery,
          declareExchange,
          exchangeType,
          durableExchange,
          connectionName,
          headers,
          gson);
    }

    protected ConnectionFactory createConnectionFactory() {
      ConnectionFactory factory = new ConnectionFactory();
      try {
        if (uri != null && !uri.isEmpty()) {
          factory.setUri(uri);
        } else {
          factory.setHost(host);
          factory.setPort(port);
          factory.setUsername(username);
          factory.setPassword(password);
          factory.setVirtualHost(virtualHost);
        }
      } catch (Exception e) {
        throw new ProcessEngineException("Unable to configure RabbitMQ connection factory", e);
      }
      return factory;
    }
  }
}

