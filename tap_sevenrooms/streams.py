STREAMS = {
    'clients': {
        'path': 'clients/export',
        'data_key': 'results',
        'key_properties': ['id'],
        'replication_method': 'FULL_TABLE',
        'use_dates': False,
        'params': {
            'venue_group_id': '{}'
        }
    },
    'reservations': {
        'path': 'reservations/export',
        'data_key': 'results',
        'key_properties': ['id'],
        'replication_method': 'INCREMENTAL',
        'replication_keys': ['updated'],
        'params': {
            'venue_group_id': '{}'
        }
    },
    'venues': {
        'path': 'venues',
        'data_key': 'results',
        'key_properties': ['id'],
        'replication_method': 'FULL_TABLE',
        'params': {
            'venue_group_id': '{}'
        },
        'children': {
            'charges': {
                'path': 'venues/{}/charges',
                'data_key': 'charges',
                'key_properties': ['id'],
                'replication_method': 'INCREMENTAL',
                'replication_keys': ['updated']
            }
        }
    }
}
