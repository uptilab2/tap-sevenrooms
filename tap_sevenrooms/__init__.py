#!/usr/bin/env python3
import os
import sys
import json
from datetime import datetime, timedelta

import singer
from singer import utils, metadata
from singer.catalog import Catalog, CatalogEntry, Schema
from .schema import get_schemas, flatten_streams

# Import my little context manager
from .client import SevenRoomsClient
from .streams import STREAMS

DATE_FORMAT = "%Y-%m-%d"
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


def discover():
    schemas, field_metadata = get_schemas()
    catalog = Catalog([])

    flat_streams = flatten_streams()
    for stream_name, schema_dict in schemas.items():
        schema = Schema.from_dict(schema_dict)
        mdata = metadata.to_map(field_metadata[stream_name])

        stream = flat_streams.get(stream_name, {})
        if stream.get('replication_method') == 'INCREMENTAL':
            for field_name in stream.get('replication_keys'):
                metadata.write(mdata, ('properties', field_name), 'inclusion', 'automatic')
        catalog.streams.append(CatalogEntry(
            stream=stream_name,
            tap_stream_id=stream_name,
            key_properties=stream.get('key_properties', None),
            schema=schema,
            metadata=metadata.to_list(mdata)
        ))

    return catalog


# Currently syncing sets the stream currently being delivered in the state.
# If the integration is interrupted, this state property is used to identify
#  the starting point to continue from.
# Reference: https://github.com/singer-io/singer-python/blob/master/singer/bookmarks.py#L41-L46
def update_currently_syncing(state, stream_name):
    if (stream_name is None) and ('currently_syncing' in state):
        del state['currently_syncing']
    else:
        singer.set_currently_syncing(state, stream_name)
    singer.write_state(state)


def sync(client, config, state, catalog):
    """ Sync data from tap source """

    # Get selected_streams from catalog, based on state last_stream
    #   last_stream = Previous currently synced stream, if the load was interrupted
    last_stream = singer.get_currently_syncing(state)
    LOGGER.info(f'last/currently syncing stream: {last_stream}')

    selected_streams = []
    flat_streams = flatten_streams()

    # Loop over selected streams in catalog
    for stream in catalog.get_selected_streams(state):
        selected_streams.append(stream.stream)
        parent_stream = flat_streams.get(stream.stream, {}).get('parent_stream')
        if parent_stream and parent_stream not in selected_streams:
            selected_streams.append(parent_stream)
    LOGGER.info(f'selected_streams: {selected_streams}')

    if not selected_streams:
        return

    # Loop through selected_streams
    for stream_name, endpoint_config in STREAMS.items():
        if stream_name in selected_streams:
            LOGGER.info(f"Syncing stream: {stream.tap_stream_id}")
            update_currently_syncing(state, stream_name)
            path = endpoint_config.get('path', stream_name)

            # Key used in the response array.
            data_key = endpoint_config.get('data_key', 'results')

            # This is used to determine if we are using to_date from_date in the query params
            use_dates = endpoint_config.get('use_dates', True)

            # This is any additionnal params that may be used for the request to the API
            params = endpoint_config.get('params', None)

            if params:
                for param_key, param_value in params.items():
                    # If param is format placeholder, use the key to get the value from the config.
                    if param_value == '{}' and config.get(param_key):
                        params[param_key] = param_value.format(config[param_key])

            # replication_ind defaults to True, set to False when you shouldn't replicate parent
            replication_ind = endpoint_config.get('replication_ind', True)
            if replication_ind:
                # Get the selected fields used for syncing.
                mdata = metadata.to_map(stream.metadata)
                mdata_list = singer.metadata.to_list(mdata)
                selected_fields = []
                for entry in mdata_list:
                    field = None
                    try:
                        field = entry['breadcrumb'][1]
                        if entry.get('metadata', {}).get('selected', False):
                            selected_fields.append(field)
                    except IndexError:
                        continue

                LOGGER.info(f'Stream: {stream_name}, selected_fields: {selected_fields}')
                singer.write_schema(
                    stream_name=stream.tap_stream_id,
                    schema=stream.schema.to_dict(),
                    key_properties=stream.key_properties,
                )
            else:
                selected_fields = None

            # Here we loop through any children streams and lookup info for each row.
            children = endpoint_config.get('children')
            children_to_sync = []
            if children:
                for child_stream_name, child_endpoint_config in children.items():
                    if child_stream_name in selected_streams:
                        # replication_ind defaults to True, set to False when you shouldn't replicate child
                        child_replication_ind = child_endpoint_config.get('replication_ind', True)
                        if child_replication_ind:

                            child_stream = catalog.get_stream(child_stream_name)

                            singer.write_schema(
                                stream_name=child_stream_name,
                                schema=child_stream.schema.to_dict(),
                                key_properties=child_stream.key_properties,
                            )

                            # Add the stream and it's config data to the list of children
                            children_to_sync.append((child_stream, child_endpoint_config))

                            # Get the selected fields used for syncing.
                            child_mdata = metadata.to_map(child_stream.metadata)
                            child_mdata_list = singer.metadata.to_list(child_mdata)
                            child_selected_fields = []
                            for entry in child_mdata_list:
                                field = None
                                try:
                                    field = entry['breadcrumb'][1]
                                    if entry.get('metadata', {}).get('selected', False):
                                        child_selected_fields.append(field)
                                except IndexError:
                                    continue
                            LOGGER.info(f'Stream: {child_stream_name}, selected_fields: {child_selected_fields}')

            today = datetime.now()
            day = state.get(stream.tap_stream_id) or config.get('start_date')
            end_date = utils.strptime_to_utc(config['end_date'][:10]) if 'end_date' in config and config['end_date'] else today.strftime(DATE_FORMAT)

            day = utils.strptime_to_utc(day)
            end_date = utils.strptime_to_utc(end_date)

            LOGGER.info(f'Sync data from {day} to {end_date}')

            # We sync the fields for each day
            while day <= end_date:

                if not use_dates:
                    # The case of items not iterable by date then skip to end_date and don't include params in request
                    day = end_date
                    tap_data = client.request_data(stream=stream, endpoint=path, data_key=data_key, day=day, use_dates=False, additional_params=params)
                else:
                    tap_data = client.request_data(stream=stream, endpoint=path, data_key=data_key, day=day, additional_params=params)

                # write one or more rows to the stream:
                singer.write_records(stream.tap_stream_id, tap_data)
                state[stream.tap_stream_id] = day.strftime(DATE_FORMAT)
                singer.write_state(state)

                for row in tap_data:
                    # Handle the child streams and get the data for those
                    if children_to_sync:
                        parent_id_field = endpoint_config.get('key_properties')
                        if parent_id_field:
                            # We are using the parent ID in the path or other settings of the child.
                            parent_id = row.get(parent_id_field[0])
                            if parent_id:
                                for child_stream, child_endpoint_config in children_to_sync:
                                    LOGGER.info(f'Syncing: {child_stream.tap_stream_id}, parent_stream: {stream_name}, parent_id: {parent_id}')

                                    # Child path is written with {} in the place of where the parent ID should go.
                                    # we can use .format() to insert the parent ID into the URL route.
                                    child_path = child_endpoint_config.get('path', child_stream.tap_stream_id).format(str(parent_id))

                                    # This is any additionnal params that may be used for the request to the API
                                    child_params = child_endpoint_config.get('params', None)

                                    if child_params:
                                        for child_param_key, child_param_value in child_params.items():
                                            # If param is format placeholder, we can replace it with a child value of the same name from the parent or config
                                            if child_param_value == '{}':
                                                if config.get(child_param_key):
                                                    child_params[child_param_key] = param_value.format(config[param_key])
                                                elif row.get(child_param_key):
                                                    child_params[child_param_key] = param_value.format(row[child_param_key])

                                    child_data_key = child_endpoint_config.get('data_key', 'results')

                                    child_tap_data = client.request_data(
                                        stream=child_stream,
                                        endpoint=child_path,
                                        data_key=child_data_key,
                                        day=day,
                                        additional_params=child_params
                                    )

                                    # write one or more rows to the stream:
                                    singer.write_records(child_stream.tap_stream_id, child_tap_data)
                                    state[child_stream.tap_stream_id] = day.strftime(DATE_FORMAT)
                                    singer.write_state(state)
                day += timedelta(days=1)


@utils.handle_top_exception(LOGGER)
def main():
    # Parse command line arguments
    args = utils.parse_args(REQUIRED_CONFIG_KEYS)
    config = args.config

    with SevenRoomsClient(config=config) as client:

        state = {}
        if args.state:
            state = args.state

        if args.discover:
            # If discover flag was passed, run discovery mode and dump output to stdout
            LOGGER.info('Starting discover')
            catalog = discover()
            json.dump(catalog.to_dict(), sys.stdout, indent=2)
            LOGGER.info('Finished discover')
        else:
            # Otherwise run in sync mode
            if args.catalog:
                # If we are supplying the catalog, use that.
                catalog = args.catalog
            else:
                # Otherwise run discovery
                catalog = discover()

            sync(
                client=client,
                config=args.config,
                state=state,
                catalog=catalog
            )


if __name__ == "__main__":
    main()
