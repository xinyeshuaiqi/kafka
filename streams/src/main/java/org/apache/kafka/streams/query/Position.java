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
package org.apache.kafka.streams.query;


import org.apache.kafka.common.annotation.InterfaceStability.Evolving;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

@Evolving
public class Position {
    private final ConcurrentHashMap<String, ConcurrentHashMap<Integer, Long>> position;

    private Position(final ConcurrentHashMap<String, ConcurrentHashMap<Integer, Long>> position) {
        this.position = position;
    }

    public static Position emptyPosition() {
        return new Position(new ConcurrentHashMap<>());
    }

    public static Position fromMap(final Map<String, ? extends Map<Integer, Long>> map) {
        return new Position(deepCopy(map));
    }

    public Position withComponent(final String topic, final int partition, final long offset) {
        position
            .computeIfAbsent(topic, k -> new ConcurrentHashMap<>())
            .put(partition, offset);
        return this;
    }

    public Position copy() {
        return new Position(deepCopy(position));
    }

    public Position merge(final Position other) {
        if (other == null) {
            return this;
        } else {
            final ConcurrentHashMap<String, ConcurrentHashMap<Integer, Long>> copy =
                deepCopy(position);
            for (final Entry<String, ConcurrentHashMap<Integer, Long>> entry : other.position.entrySet()) {
                final String topic = entry.getKey();
                final Map<Integer, Long> partitionMap =
                    copy.computeIfAbsent(topic, k -> new ConcurrentHashMap<>());
                for (final Entry<Integer, Long> partitionOffset : entry.getValue().entrySet()) {
                    final Integer partition = partitionOffset.getKey();
                    final Long offset = partitionOffset.getValue();
                    if (!partitionMap.containsKey(partition)
                        || partitionMap.get(partition) < offset) {
                        partitionMap.put(partition, offset);
                    }
                }
            }
            return new Position(copy);
        }
    }

    public Set<String> getTopics() {
        return Collections.unmodifiableSet(position.keySet());
    }

    public Map<Integer, Long> getBound(final String topic) {
        return Collections.unmodifiableMap(position.get(topic));
    }

    private static ConcurrentHashMap<String, ConcurrentHashMap<Integer, Long>> deepCopy(
        final Map<String, ? extends Map<Integer, Long>> map) {
        if (map == null) {
            return new ConcurrentHashMap<>();
        } else {
            final ConcurrentHashMap<String, ConcurrentHashMap<Integer, Long>> copy =
                new ConcurrentHashMap<>(map.size());
            for (final Entry<String, ? extends Map<Integer, Long>> entry : map.entrySet()) {
                copy.put(entry.getKey(), new ConcurrentHashMap<>(entry.getValue()));
            }
            return copy;
        }
    }

    @Override
    public String toString() {
        return "Position{" +
            "position=" + position +
            '}';
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        final Position position1 = (Position) o;
        return Objects.equals(position, position1.position);
    }

    @Override
    public int hashCode() {
        throw new UnsupportedOperationException("This mutable object is not suitable as a hash key");
    }
}
