/**
 * Copyright 2016-2023 Cloudera, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 **/
package com.hortonworks.registries.schemaregistry.client;

import com.hortonworks.registries.schemaregistry.exceptions.RegistryRetryableException;
import com.hortonworks.registries.schemaregistry.retry.RetryExecutor;
import com.hortonworks.registries.schemaregistry.retry.policy.BackoffPolicy;
import com.hortonworks.registries.shaded.javax.ws.rs.client.Client;
import com.hortonworks.registries.shaded.javax.ws.rs.client.WebTarget;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.net.URI;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.same;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

public class SchemaRegistryClientRetryTest {
  private final String url1 = "url1";
  private final String url2 = "url2";

  private SchemaRegistryClient.RegistryRetryableBlock<String, RuntimeException, RuntimeException> mockRetryableBlock;
  private Client mockClient;
  private BackoffPolicy mockBackoffPolicy;
  private UrlSelector mockUrlSelector;
  private SchemaRegistryClient client;

  @BeforeEach
  void setup() {
    mockRetryableBlock = mock(SchemaRegistryClient.RegistryRetryableBlock.class);
    mockClient = mock(Client.class);

    mockBackoffPolicy = mock(BackoffPolicy.class);
    mockUrlSelector = mock(UrlSelector.class);
    RetryExecutor retryExecutor = new RetryExecutor.Builder()
        .retryOnException(RegistryRetryableException.class)
        .backoffPolicy(mockBackoffPolicy)
        .build();
    client = new SchemaRegistryClient(null, false, false, mockClient, mockUrlSelector, null, null,
        null, null, null, retryExecutor);

    when(mockUrlSelector.select()).thenReturn(url1, url2, url1, url2);
    when(mockClient.target(anyString())).thenAnswer(i -> {
      WebTarget mockTarget = mock(WebTarget.class);
      when(mockTarget.path(any())).thenReturn(mockTarget);
      when(mockTarget.getUri()).thenReturn(URI.create(i.getArgument(0)));
      return mockTarget;
    });
  }

  @Test
  void testFirstTrySucceeds() {
    String expected = "success";
    when(mockRetryableBlock.run(any())).thenReturn(expected);

    String actual = client.runRetryableBlock(mockRetryableBlock);

    assertSame(expected, actual);

    ArgumentCaptor<SchemaRegistryClient.SchemaRegistryTargets> targetCaptor = targetCaptor();
    verify(mockUrlSelector).select();
    verify(mockUrlSelector, never()).urlWithError(any(), any());
    verify(mockClient).target(url1);
    verify(mockRetryableBlock).run(targetCaptor.capture());
    verifyNoInteractions(mockBackoffPolicy);

    assertEquals(url1, targetCaptor.getValue().getRootTarget().getUri().toString());
  }

  @Test
  void testFirstTryFailsWithoutRetry() {
    RegistryRetryableException exception = new RegistryRetryableException("Test exception");
    when(mockRetryableBlock.run(any())).thenThrow(exception);
    when(mockBackoffPolicy.mayBeSleep(anyInt(), anyLong())).thenReturn(false);

    assertThrows(RegistryRetryableException.class, () -> client.runRetryableBlock(mockRetryableBlock));

    ArgumentCaptor<SchemaRegistryClient.SchemaRegistryTargets> targetCaptor = targetCaptor();
    verify(mockUrlSelector).select();
    verify(mockUrlSelector).urlWithError(eq(url1), same(exception));
    verify(mockClient).target(url1);
    verify(mockRetryableBlock).run(targetCaptor.capture());
    verify(mockBackoffPolicy).mayBeSleep(eq(1), anyLong());

    assertEquals(url1, targetCaptor.getValue().getRootTarget().getUri().toString());
  }

  @Test
  void testSecondTrySucceeds() {
    RegistryRetryableException exception = new RegistryRetryableException("Test exception");
    String expected = "success2";
    when(mockRetryableBlock.run(any())).thenThrow(exception).thenReturn(expected);
    when(mockBackoffPolicy.mayBeSleep(anyInt(), anyLong())).thenReturn(true);

    String actual = client.runRetryableBlock(mockRetryableBlock);

    assertSame(expected, actual);

    ArgumentCaptor<SchemaRegistryClient.SchemaRegistryTargets> targetCaptor = targetCaptor();
    verify(mockUrlSelector, times(2)).select();
    verify(mockUrlSelector).urlWithError(eq(url1), same(exception));
    verify(mockClient).target(url1);
    verify(mockClient).target(url2);
    verify(mockRetryableBlock, times(2)).run(targetCaptor.capture());
    verify(mockBackoffPolicy).mayBeSleep(eq(1), anyLong());

    List<SchemaRegistryClient.SchemaRegistryTargets> targets = targetCaptor.getAllValues();
    assertEquals(url1, targets.get(0).getRootTarget().getUri().toString());
    assertEquals(url2, targets.get(1).getRootTarget().getUri().toString());
  }

  @Test
  void testAllAttemptsFailWithRetry() {
    RegistryRetryableException exception = new RegistryRetryableException("Test exception");
    when(mockRetryableBlock.run(any())).thenThrow(exception);
    when(mockBackoffPolicy.mayBeSleep(anyInt(), anyLong())).thenReturn(true, true, true, false);

    assertThrows(RegistryRetryableException.class, () -> client.runRetryableBlock(mockRetryableBlock));

    ArgumentCaptor<SchemaRegistryClient.SchemaRegistryTargets> targetCaptor = targetCaptor();
    verify(mockUrlSelector, times(4)).select();
    verify(mockUrlSelector, times(2)).urlWithError(eq(url1), same(exception));
    verify(mockUrlSelector, times(2)).urlWithError(eq(url2), same(exception));
    // Use atLeast to tolerate caching of targets
    verify(mockClient, atLeast(1)).target(url1);
    verify(mockClient, atLeast(1)).target(url2);
    verify(mockRetryableBlock, times(4)).run(targetCaptor.capture());
    ArgumentCaptor<Integer> attemptCaptor = ArgumentCaptor.forClass(int.class);
    verify(mockBackoffPolicy, times(4)).mayBeSleep(attemptCaptor.capture(), anyLong());

    List<SchemaRegistryClient.SchemaRegistryTargets> targets = targetCaptor.getAllValues();
    assertEquals(url1, targets.get(0).getRootTarget().getUri().toString());
    assertEquals(url2, targets.get(1).getRootTarget().getUri().toString());
    assertEquals(url1, targets.get(2).getRootTarget().getUri().toString());
    assertEquals(url2, targets.get(3).getRootTarget().getUri().toString());

    List<Integer> attempts = attemptCaptor.getAllValues();
    assertEquals(IntStream.range(1, 5).boxed().collect(Collectors.toList()), attempts);
  }

  private static ArgumentCaptor<SchemaRegistryClient.SchemaRegistryTargets> targetCaptor() {
    return ArgumentCaptor.forClass(SchemaRegistryClient.SchemaRegistryTargets.class);
  }
}
