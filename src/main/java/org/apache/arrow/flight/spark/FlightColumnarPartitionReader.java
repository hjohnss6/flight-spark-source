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

import org.apache.arrow.flight.FlightClient;
import org.apache.arrow.flight.FlightStream;
import org.apache.arrow.flight.Location;
import org.apache.arrow.flight.grpc.CredentialCallOption;
import org.apache.arrow.util.AutoCloseables;
import org.apache.spark.sql.connector.read.PartitionReader;
import org.apache.spark.sql.vectorized.ColumnVector;
import org.apache.spark.sql.vectorized.ColumnarBatch;

import java.io.IOException;
import java.util.List;

public class FlightColumnarPartitionReader implements PartitionReader<ColumnarBatch> {
  private final FlightClientFactory clientFactory;
  private final FlightClient client;
  private final FlightStream stream;

  public FlightColumnarPartitionReader(FlightClientOptions clientOptions, FlightPartition partition) {
    List<Location> locations = partition.getEndpoint().get().getLocations();
    // TODO - Should we handle multiple locations?
    Location location;
    if (locations.isEmpty()) {
      location = clientOptions.getDefaultLocation();
    } else {
      location = locations.get(0);
    }
    clientFactory = new FlightClientFactory(location, clientOptions);
    client = clientFactory.apply();
    CredentialCallOption callOption = clientFactory.getCallOption();
    stream = client.getStream(partition.getEndpoint().get().getTicket(), callOption);
  }

  // This is written this way because the Spark interface iterates in a different way.
  // E.g., .next() -> .get() vs. .hasNext() -> .next()
  @Override
  public boolean next() throws IOException {
    try {
      return stream.next();
    } catch (RuntimeException e) {
      throw new IOException(e);
    }
  }

  @Override
  public ColumnarBatch get() {
    ColumnarBatch batch = new ColumnarBatch(
      stream.getRoot().getFieldVectors()
        .stream()
        .map(FlightArrowColumnVector::new)
        .toArray(ColumnVector[]::new)
    );
    batch.setNumRows(stream.getRoot().getRowCount());
    return batch;
  }

  @Override
  public void close() throws IOException {
    try {
      AutoCloseables.close(stream, client, clientFactory);
    } catch (Exception e) {
      throw new IOException(e);
    }
  }
}
