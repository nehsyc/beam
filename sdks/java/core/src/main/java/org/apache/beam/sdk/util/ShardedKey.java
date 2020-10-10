/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.beam.sdk.util;

import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkArgument;

import com.google.auto.value.AutoValue;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Collections;
import java.util.List;
import org.apache.beam.sdk.coders.ByteArrayCoder;
import org.apache.beam.sdk.coders.StructuredCoder;
import org.apache.beam.sdk.util.common.ElementByteSizeObserver;

/**
 * A sharded key consisting of a user key and a shard id represented by bytes.
 *
 * <p>This is a more generic definition of {@link org.apache.beam.sdk.values.ShardedKey}.
 */
@AutoValue
public abstract class ShardedKey<K> {

  public static <K> ShardedKey<K> of(K key) {
    return new AutoValue_ShardedKey(new byte[0], key);
  }

  public static <K> ShardedKey<K> of(K key, byte[] shardId) {
    checkArgument(shardId != null, "Shard id should not be null!");
    return new AutoValue_ShardedKey(shardId, key);
  }

  @SuppressWarnings("mutable")
  public abstract byte[] getShardId();

  public abstract K getKey();

  public static class Coder<K> extends StructuredCoder<ShardedKey<K>> {

    private final ByteArrayCoder shardCoder = ByteArrayCoder.of();
    private final org.apache.beam.sdk.coders.Coder<K> keyCoder;

    private Coder(org.apache.beam.sdk.coders.Coder<K> coder) {
      keyCoder = coder;
    }

    public static <K> ShardedKey.Coder<K> of(org.apache.beam.sdk.coders.Coder<K> keyCoder) {
      return new ShardedKey.Coder<K>(keyCoder);
    }

    public org.apache.beam.sdk.coders.Coder<K> getKeyCoder() {
      return keyCoder;
    }

    @Override
    public void encode(ShardedKey<K> shardedKey, OutputStream outStream) throws IOException {
      // The encoding should follow the order:
      //   length of shard id
      //   shard id
      //   encoded user key
      shardCoder.encode(shardedKey.getShardId(), outStream);
      keyCoder.encode(shardedKey.getKey(), outStream);
    }

    @Override
    public ShardedKey<K> decode(InputStream inStream) throws IOException {
      byte[] shardId = shardCoder.decode(inStream);
      K key = keyCoder.decode(inStream);
      return ShardedKey.of(key, shardId);
    }

    @Override
    public List<? extends org.apache.beam.sdk.coders.Coder<?>> getCoderArguments() {
      return Collections.singletonList(keyCoder);
    }

    @Override
    public void verifyDeterministic() throws NonDeterministicException {
      shardCoder.verifyDeterministic();
      keyCoder.verifyDeterministic();
    }

    @Override
    public boolean consistentWithEquals() {
      return shardCoder.consistentWithEquals() && keyCoder.consistentWithEquals();
    }

    @Override
    public boolean isRegisterByteSizeObserverCheap(ShardedKey<K> shardedKey) {
      return shardCoder.isRegisterByteSizeObserverCheap(shardedKey.getShardId())
          && keyCoder.isRegisterByteSizeObserverCheap(shardedKey.getKey());
    }

    @Override
    public Object structuralValue(ShardedKey<K> shardedKey) {
      return ShardedKey.of(keyCoder.structuralValue(shardedKey.getKey()), shardedKey.getShardId());
    }

    @Override
    public void registerByteSizeObserver(ShardedKey<K> shardedKey, ElementByteSizeObserver observer)
        throws Exception {
      shardCoder.registerByteSizeObserver(shardedKey.getShardId(), observer);
      keyCoder.registerByteSizeObserver(shardedKey.getKey(), observer);
    }
  }
}
