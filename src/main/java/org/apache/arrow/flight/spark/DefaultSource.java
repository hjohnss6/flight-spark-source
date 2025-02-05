/*
 * Copyright (C) 2019 The flight-spark-source Authors
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
 */

package org.apache.arrow.flight.spark;

import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.arrow.flight.Location;
import org.apache.spark.sql.connector.catalog.TableProvider;
import org.apache.spark.sql.connector.expressions.Transform;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.connector.catalog.Table;
import org.apache.spark.sql.sources.DataSourceRegister;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;

public class DefaultSource implements TableProvider, DataSourceRegister {
  private SparkSession spark;

  private SparkSession getSparkSession() {
    if (spark == null) {
      spark = SparkSession.getActiveSession().get();
    }
    return spark;
  }

  private FlightTable makeTable(CaseInsensitiveStringMap options) {
    String uri = options.getOrDefault("uri", "grpc://localhost:47470");
    Location location;
    try {
      location = new Location(uri);
    } catch (URISyntaxException e) {
      throw new RuntimeException(e);
    }

    String sql = options.getOrDefault("path", "");
    String username = options.getOrDefault("username", "");
    String password = options.getOrDefault("password", "");
    String trustedCertificates = options.getOrDefault("trustedCertificates", "");
    String clientCertificate = options.getOrDefault("clientCertificate", "");
    String clientKey = options.getOrDefault("clientKey", "");
    String token = options.getOrDefault("token", "");
    List<FlightClientMiddlewareFactory> middleware = new ArrayList<>();
    if (!token.isEmpty()) {
      middleware.add(new TokenClientMiddlewareFactory(token));
    }


    Broadcast<FlightClientOptions> clientOptions = JavaSparkContext.fromSparkContext(getSparkSession().sparkContext()).broadcast(
      new FlightClientOptions(username, password, trustedCertificates, clientCertificate, clientKey, uri, middleware)
    );

    return new FlightTable(
      String.format("{} Location {} Command {}", shortName(), location.getUri().toString(), sql),
      location,
      sql,
      clientOptions
    );
  }

  @Override
  public StructType inferSchema(CaseInsensitiveStringMap options) {
    return makeTable(options).schema();
  }

  @Override
  public String shortName() {
    return "flight";
  }

  @Override
  public Table getTable(StructType schema, Transform[] partitioning, Map<String, String> options) {
    return makeTable(new CaseInsensitiveStringMap(options));
  }
}
