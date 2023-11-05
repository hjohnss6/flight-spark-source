"""Minimal flight server to explore."""
import pathlib
import logging

import pyarrow as pa
import pyarrow.flight as af
import pyarrow.parquet as pq

logging.basicConfig(
    format="[%(asctime)s %(funcName)s():%(lineno)s]%(levelname)s: %(message)s",
    level=logging.INFO
)


class FlightServer(af.FlightServerBase):

    def __init__(self, location="grpc://0.0.0.0:8815",
                repo=pathlib.Path("./datasets"), **kwargs):
        super(FlightServer, self).__init__(location, **kwargs)
        self._location = location
        self._repo = repo


    def _make_flight_info(self, dataset_path):
        """Create flight info for path."""
        logging.info("="*60)
        logging.info(f"Dataset path: {dataset_path}")
        full_dataset_path = self._repo / dataset_path
        ds = pq.ParquetDataset(full_dataset_path)
        schema = ds.schema
        logging.info(f"Schema: {schema}")

        descriptor = af.FlightDescriptor.for_path(
            dataset_path.encode('utf-8')
        )
        logging.info(f"Descriptor: {descriptor}")
        endpoints = [af.FlightEndpoint(dataset_path, [self._location])]
        logging.info(f"Endpoints: {endpoints}")
        
        return af.FlightInfo(schema, descriptor, endpoints, 0, 0)


    def list_flights(self, context, criteria):
        """List the flights."""
        logging.info("="*60)
        logging.info("Criteria: {criteria}")
        for dataset in self._repo.iterdir():
            yield self._make_flight_info(dataset.name)


    def get_flight_info(self, context, descriptor):
        """Return the flight info."""
        logging.info("="*60)
        logging.info(f"Getting flight info for {descriptor}")
        logging.info(descriptor)
        return self._make_flight_info(descriptor.command.decode('utf-8'))


    def do_get(self, context, ticket):
        """Return data for ticket."""
        logging.info("="*60)
        logging.info(f"Ticket: {ticket}")

        dataset_path = ticket.ticket.decode('utf-8')
        logging.info(f"Path: {dataset_path}")
        dataset_path = self._repo / dataset_path

        # Read
        reader = pa.dataset.dataset(dataset_path, partitioning="hive")
        reader = reader.scanner()
        batches = reader.to_batches()
        logging.info(f"Batches {batches}")

        # return stream
        return af.GeneratorStream(
            reader.projected_schema, batches)


    def get_schema(self, context, fligtdesc: af.FlightDescriptor):
        logging.info("="*60)
        """Returns the schema for a certain flight descriptor."""
        logging.info(f"FlightDescriptor: {fligtdesc}")

        logging.info(f"FlightDescriptorType: {fligtdesc.descriptor_type}")
        if fligtdesc.descriptor_type == af.DescriptorType["CMD"]:
            dataset_path = fligtdesc.command.decode()
        else: dataset_path = fligtdesc.path.decode()
        logging.info(f"Dataset path: {dataset_path}")

        info = self._make_flight_info(dataset_path)

        return af.SchemaResult(info.schema)


if __name__ == '__main__':
    server = FlightServer()
    server._repo.mkdir(exist_ok=True)
    server.serve()
