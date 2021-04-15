#!/usr/bin/env python3
import os
import sys
import json
from datetime import datetime

import singer
from singer import utils
from singer.catalog import Catalog, CatalogEntry
from singer.schema import Schema

# Import my little context manager
from .client import SevenRoomsClient


REQUIRED_CONFIG_KEYS = [
    "client_id",
    "client_secret",
    "start_date"
]
LOGGER = singer.get_logger()


class DateRangeError(Exception):
    pass


def get_abs_path(path):
    return os.path.join(os.path.dirname(os.path.realpath(__file__)), path)


def load_schemas():
    """ Load schemas from schemas folder """
    schemas = {}
    for filename in os.listdir(get_abs_path('schemas')):
        path = get_abs_path('schemas') + '/' + filename
        file_raw = filename.replace('.json', '')
        with open(path) as file:
            schemas[file_raw] = Schema.from_dict(json.load(file))
    return schemas


def discover():
    schemas, field_metadata = get_schemas()
    streams = []
    for stream_id, schema in raw_schemas.items():
        # TODO: populate any metadata and stream's key properties here..
        stream_metadata = []
        key_properties = []
        streams.append(
            CatalogEntry(
                tap_stream_id=stream_id,
                stream=stream_id,
                schema=schema,
                key_properties=key_properties,
                metadata=stream_metadata,
                replication_key=None,
                is_view=None,
                database=None,
                table=None,
                row_count=None,
                stream_alias=None,
                replication_method=None,
            )
        )
    return Catalog(streams)


def sync(client, config, state, catalog):
    """ Sync data from tap source """
    # Loop over selected streams in catalog
    for stream in catalog.get_selected_streams(state):
        LOGGER.info("Syncing stream:" + stream.tap_stream_id)

        bookmark_column = stream.replication_key # TODO: Remove this part.

        singer.write_schema(
            stream_name=stream.tap_stream_id,
            schema=stream.schema,
            key_properties=stream.key_properties,
        )

        # TODO: pull_data()
        today = datetime.now()
        day = state.get(stream.tap_stream_id) or config.get('start_date')
        end_date = config['end_date'][:10] if 'end_date' in config and config['end_date'] else str(today.strftime('%Y-%m-%d'))

        while day < end_date and day <= today:
            tap_data = client.request_data(stream, day)

            for row in tap_data():
                # TODO: place type conversions or transformations here

                # write one or more rows to the stream:
                singer.write_records(stream.tap_stream_id, [row])
                if bookmark_column:
                    # update bookmark to latest value
                    singer.write_state({stream.tap_stream_id: row[bookmark_column]})


def do_discover():
    LOGGER.info('Starting discover')
    catalog = discover()
    json.dump(catalog.to_dict(), sys.stdout, indent=2)
    LOGGER.info('Finished discover')


@utils.handle_top_exception(LOGGER)
def main():
    # Parse command line arguments
    args = utils.parse_args(REQUIRED_CONFIG_KEYS)
    config = args.config

    with SevenRoomsClient(config=config) as client:

        # If discover flag was passed, run discovery mode and dump output to stdout
        if args.discover:
            do_discover()
        # Otherwise run in sync mode
        else:

            # If we are supplying the catalog, use that.
            if args.catalog:
                catalog = args.catalog
            # Otherwise run discovery
            else:
                catalog = discover()

            sync(
                client=client,
                config=args.config,
                state=args.state,
                catalog=catalog
            )


if __name__ == "__main__":
    main()
