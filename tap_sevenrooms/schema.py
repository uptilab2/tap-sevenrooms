import os
import json
import singer
from singer import metadata
from tap_sevenrooms.streams import STREAMS

logger = singer.get_logger()


def get_abs_path(path):
    return os.path.join(os.path.dirname(os.path.realpath(__file__)), path)


def get_schemas():
    schemas = {}
    field_metadata = {}

    flat_streams = flatten_streams()
    for stream_name, stream_metadata in flat_streams.items():
        replication_ind = stream_metadata.get('replication_ind', True)
        if replication_ind:
            schema_path = get_abs_path('schemas/{}.json'.format(stream_name))
            with open(schema_path) as file:
                schema = json.load(file)
            schemas[stream_name] = schema

            metadata.new()

            # Documentation:
            # https://github.com/singer-io/getting-started/blob/master/docs/DISCOVERY_MODE.md#singer-python-helper-functions
            # Reference:
            # https://github.com/singer-io/singer-python/blob/master/singer/metadata.py#L25-L44
            mdata = metadata.get_standard_metadata(
                schema=schema,
                key_properties=stream_metadata.get('key_properties', None),
                valid_replication_keys=stream_metadata.get('replication_keys', None),
                replication_method=stream_metadata.get('replication_method', None)
            )
            field_metadata[stream_name] = mdata
    return schemas, field_metadata


def flatten_streams():
    # De-nest children nodes for Discovery mode
    flat_streams = {}
    # Loop through parents
    for stream_name, endpoint_config in STREAMS.items():
        flat_streams[stream_name] = {
            'key_properties': endpoint_config.get('key_properties'),
            'replication_method': endpoint_config.get('replication_method'),
            'replication_keys': endpoint_config.get('replication_keys'),
            'replication_ind': endpoint_config.get('replication_ind', True)
        }
        # Loop through children
        children = endpoint_config.get('children')
        if children:
            for child_stream_name, child_enpoint_config in children.items():
                flat_streams[child_stream_name] = {
                    'key_properties': child_enpoint_config.get('key_properties'),
                    'replication_method': child_enpoint_config.get('replication_method'),
                    'replication_keys': child_enpoint_config.get('replication_keys'),
                    'replication_ind': child_enpoint_config.get('replication_ind', True),
                    'parent_stream': stream_name
                }
    return flat_streams
