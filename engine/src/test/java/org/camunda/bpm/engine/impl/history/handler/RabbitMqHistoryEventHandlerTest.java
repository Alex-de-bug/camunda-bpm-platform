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

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.Arrays;

import org.camunda.bpm.engine.ProcessEngineException;
import org.camunda.bpm.engine.impl.history.event.HistoryEvent;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.BuiltinExchangeType;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

public class RabbitMqHistoryEventHandlerTest {

  protected ConnectionFactory connectionFactory;
  protected Connection connection;
  protected Channel channel;

  @Before
  public void setUp() throws Exception {
    connectionFactory = mock(ConnectionFactory.class);
    connection = mock(Connection.class);
    channel = mock(Channel.class);

    when(connectionFactory.newConnection(anyString())).thenReturn(connection);
    when(connection.isOpen()).thenReturn(true);
    when(connection.createChannel()).thenReturn(channel);
    when(channel.isOpen()).thenReturn(true);
  }

  @Test
  public void shouldPublishSingleEvent() throws Exception {
    RabbitMqHistoryEventHandler handler = RabbitMqHistoryEventHandler.builder()
        .connectionFactory(connectionFactory)
        .exchange("camunda.history")
        .routingKey("history.events")
        .build();

    HistoryEvent event = new HistoryEvent();
    event.setId("42");
    event.setEventType("test");

    handler.handleEvent(event);

    verify(connectionFactory).newConnection(anyString());
    verify(connection).createChannel();
    verify(channel).exchangeDeclare("camunda.history", BuiltinExchangeType.TOPIC, true);

    ArgumentCaptor<byte[]> bodyCaptor = ArgumentCaptor.forClass(byte[].class);
    verify(channel).basicPublish(eq("camunda.history"), eq("history.events"), eq(true), any(AMQP.BasicProperties.class), bodyCaptor.capture());

    String body = new String(bodyCaptor.getValue());
    org.assertj.core.api.Assertions.assertThat(body).contains("\"id\":\"42\"");
  }

  @Test
  public void shouldPublishMultipleEvents() throws Exception {
    RabbitMqHistoryEventHandler handler = RabbitMqHistoryEventHandler.builder()
        .connectionFactory(connectionFactory)
        .build();

    HistoryEvent eventOne = new HistoryEvent();
    eventOne.setId("one");
    HistoryEvent eventTwo = new HistoryEvent();
    eventTwo.setId("two");

    handler.handleEvents(Arrays.asList(eventOne, eventTwo));

    verify(channel, times(2)).basicPublish(eq("camunda.history"), eq("history.events"), eq(true), any(AMQP.BasicProperties.class), any(byte[].class));
  }

  @Test
  public void shouldWrapCloseExceptions() throws Exception {
    RabbitMqHistoryEventHandler handler = RabbitMqHistoryEventHandler.builder()
        .connectionFactory(connectionFactory)
        .build();

    handler.handleEvent(new HistoryEvent());

    doThrow(new IOException("boom")).when(channel).close();

    assertThatThrownBy(handler::close)
        .isInstanceOf(ProcessEngineException.class)
        .hasMessageContaining("Unable to close RabbitMQ channel");
  }

  @Test
  public void shouldCloseResources() throws Exception {
    RabbitMqHistoryEventHandler handler = RabbitMqHistoryEventHandler.builder()
        .connectionFactory(connectionFactory)
        .build();

    handler.handleEvent(new HistoryEvent());
    handler.close();

    verify(channel).close();
    verify(connection).close();
  }
}

