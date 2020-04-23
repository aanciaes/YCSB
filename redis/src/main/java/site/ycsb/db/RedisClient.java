/**
 * Copyright (c) 2012 YCSB contributors. All rights reserved.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License. You
 * may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License. See accompanying
 * LICENSE file.
 * <p>
 * Redis client binding for YCSB.
 * <p>
 * All YCSB records are mapped to a Redis *hash field*.  For scanning
 * operations, all keys are saved (by an arbitrary hash) in a sorted set.
 * <p>
 * Redis client binding for YCSB.
 * <p>
 * All YCSB records are mapped to a Redis *hash field*.  For scanning
 * operations, all keys are saved (by an arbitrary hash) in a sorted set.
 * <p>
 * Redis client binding for YCSB.
 * <p>
 * All YCSB records are mapped to a Redis *hash field*.  For scanning
 * operations, all keys are saved (by an arbitrary hash) in a sorted set.
 * <p>
 * Redis client binding for YCSB.
 * <p>
 * All YCSB records are mapped to a Redis *hash field*.  For scanning
 * operations, all keys are saved (by an arbitrary hash) in a sorted set.
 */

/**
 * Redis client binding for YCSB.
 *
 * All YCSB records are mapped to a Redis *hash field*.  For scanning
 * operations, all keys are saved (by an arbitrary hash) in a sorted set.
 */

package site.ycsb.db;

import java.io.Closeable;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.Vector;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.Protocol;
import redis.clients.jedis.commands.BasicCommands;
import redis.clients.jedis.commands.JedisCommands;
import site.ycsb.ByteIterator;
import site.ycsb.DB;
import site.ycsb.DBException;
import site.ycsb.Status;
import site.ycsb.StringByteIterator;

/**
 * YCSB binding for <a href="http://redis.io/">Redis</a>.
 *
 * See {@code redis/README.md} for details.
 */
public class RedisClient extends DB {

  private JedisCommands jedis;
  private JedisCluster jedisCluster;
  private boolean isCluster = false;

  public static final String HOST_PROPERTY = "redis.host";
  public static final String PORT_PROPERTY = "redis.port";
  public static final String PASSWORD_PROPERTY = "redis.password";
  public static final String CLUSTER_PROPERTY = "redis.cluster";
  public static final String TLS_PROPERTY = "redis.tls";
  public static final String TRUSTSTORE_PROPERTY = "redis.tls.truststore.path";
  public static final String TRUSTSTORE_PASSWORD_PROPERTY = "redis.tls.truststore.password";
  public static final String KEYSTORE_PROPERTY = "redis.tls.keystore.path";
  public static final String KEYSTORE_PASSWORD_PROPERTY = "redis.tls.keystore.password";

  public static final String INDEX_KEY = "_indices";

  public void init() throws DBException {
    Properties props = getProperties();
    int port;

    String portString = props.getProperty(PORT_PROPERTY);
    if (portString != null) {
      port = Integer.parseInt(portString);
    } else {
      port = Protocol.DEFAULT_PORT;
    }
    String host = props.getProperty(HOST_PROPERTY);

    boolean clusterEnabled = Boolean.parseBoolean(props.getProperty(CLUSTER_PROPERTY));
    String password = props.getProperty(PASSWORD_PROPERTY);
    if (clusterEnabled) {
      isCluster = true;
      Set<HostAndPort> jedisClusterNodes = new HashSet<>();
      jedisClusterNodes.add(new HostAndPort(host, port));
      if (password != null) {
        jedisCluster = new JedisCluster(jedisClusterNodes, 2000, 2000, 5, "redis", new JedisPoolConfig());
      } else {
        jedisCluster = new JedisCluster(jedisClusterNodes);
      }
    } else {
      boolean tlsEnabled = Boolean.parseBoolean(props.getProperty(TLS_PROPERTY));

      if (tlsEnabled) {
        System.setProperty("javax.net.ssl.keyStore", props.getProperty(KEYSTORE_PROPERTY));
        System.setProperty("javax.net.ssl.keyStorePassword", props.getProperty(KEYSTORE_PASSWORD_PROPERTY));
        System.setProperty("javax.net.ssl.trustStore", props.getProperty(TRUSTSTORE_PROPERTY));
        System.setProperty("javax.net.ssl.trustStorePassword", props.getProperty(TRUSTSTORE_PASSWORD_PROPERTY));
      }
      jedis = new Jedis(host, port, tlsEnabled);
      ((Jedis) jedis).connect();
    }

    if (password != null) {
      ((BasicCommands) jedis).auth(password);
    }
  }

  public void cleanup() throws DBException {
    try {
      if (isCluster) {
        ((Closeable) jedisCluster).close();
      } else {
        ((Closeable) jedis).close();
      }
    } catch (IOException e) {
      throw new DBException("Closing connection failed.");
    }
  }

  /*
   * Calculate a hash for a key to store it in an index. The actual return value
   * of this function is not interesting -- it primarily needs to be fast and
   * scattered along the whole space of doubles. In a real world scenario one
   * would probably use the ASCII values of the keys.
   */
  private double hash(String key) {
    return key.hashCode();
  }

  // XXX jedis.select(int index) to switch to `table`

  @Override
  public Status read(String table, String key, Set<String> fields,
                     Map<String, ByteIterator> result) {
    if (fields == null) {
      StringByteIterator.putAllAsByteIterators(result, jedis.hgetAll(key));
    } else {
      String[] fieldArray =
          (String[]) fields.toArray(new String[fields.size()]);

      List<String> values;

      if (isCluster) {
        values = jedisCluster.hmget(key, fieldArray);
      } else {
        values = jedis.hmget(key, fieldArray);
      }

      Iterator<String> fieldIterator = fields.iterator();
      Iterator<String> valueIterator = values.iterator();

      while (fieldIterator.hasNext() && valueIterator.hasNext()) {
        result.put(fieldIterator.next(),
            new StringByteIterator(valueIterator.next()));
      }
      assert !fieldIterator.hasNext() && !valueIterator.hasNext();
    }
    return result.isEmpty() ? Status.ERROR : Status.OK;
  }

  @Override
  public Status insert(String table, String key,
                       Map<String, ByteIterator> values) {

    if (isCluster) {
      if (jedisCluster.hmset(key, StringByteIterator.getStringMap(values))
          .equals("OK")) {
        jedisCluster.zadd(INDEX_KEY, hash(key), key);
        return Status.OK;
      }
    } else {
      if (jedis.hmset(key, StringByteIterator.getStringMap(values))
          .equals("OK")) {
        jedis.zadd(INDEX_KEY, hash(key), key);
        return Status.OK;
      }
    }

    return Status.ERROR;
  }

  @Override
  public Status delete(String table, String key) {
    if (isCluster) {
      return jedisCluster.del(key) == 0 && jedisCluster.zrem(INDEX_KEY, key) == 0 ? Status.ERROR
          : Status.OK;
    } else {
      return jedis.del(key) == 0 && jedis.zrem(INDEX_KEY, key) == 0 ? Status.ERROR
          : Status.OK;
    }
  }

  @Override
  public Status update(String table, String key,
                       Map<String, ByteIterator> values) {
    if (isCluster) {
      return jedisCluster.hmset(key, StringByteIterator.getStringMap(values))
          .equals("OK") ? Status.OK : Status.ERROR;
    } else {
      return jedis.hmset(key, StringByteIterator.getStringMap(values))
          .equals("OK") ? Status.OK : Status.ERROR;
    }
  }

  @Override
  public Status scan(String table, String startkey, int recordcount,
                     Set<String> fields, Vector<HashMap<String, ByteIterator>> result) {
    Set<String> keys;
    if (isCluster) {
      keys = jedisCluster.zrangeByScore(INDEX_KEY, hash(startkey),
          Double.POSITIVE_INFINITY, 0, recordcount);
    } else {
      keys = jedis.zrangeByScore(INDEX_KEY, hash(startkey),
          Double.POSITIVE_INFINITY, 0, recordcount);
    }

    HashMap<String, ByteIterator> values;
    for (String key : keys) {
      values = new HashMap<String, ByteIterator>();
      read(table, key, fields, values);
      result.add(values);
    }

    return Status.OK;
  }
}
