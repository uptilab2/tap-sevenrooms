STREAMS = {
    'client': {
        'path': 'clients/export',
        'data_key': 'results',
        'key_properties': ['id'],
        'replication_method': 'FULL_TABLE'
    },
    'reservation': {
        'path': 'reservations/export',
        'data_key': 'results',
        'key_properties': ['id'],
        'replication_method': 'INCREMENTAL',
        'replication_key': 'updated'
    },
    'requests': {
        'path': 'requests',
        'key_properties': ['id'],
        'replication_method': 'INCREMENTAL',
        'replication_key': 'updated'
    },
    'venues': {
        'path': 'venues',
        'data_key': 'results',
        'key_properties': ['id'],
        'replication_method': 'FULL_TABLE',
        'children': {
            'charges': {
                'path': 'venues/{}/charges',
                'data_key': 'charges',
                'key_properties': ['id'],
                'replication_method': 'INCREMENTAL',
                'replication_key': 'updated'
            }
        }
    }
}
