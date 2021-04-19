# tap-sevenrooms

This is a [Singer](https://singer.io) tap that produces JSON-formatted data
following the [Singer
spec](https://github.com/singer-io/getting-started/blob/master/SPEC.md).

This tap:

- Pulls raw data from [SevenRooms Reservations API](https://api-docs.sevenrooms.com/)
- Extracts the following resources:
  - [Reservations](https://api-docs.sevenrooms.com/api-reference/reservations/create-reservations-export)
- Outputs the schema for each resource
- Incrementally pulls data based on the input state

## Config
API login is performed via ID and SECRET if no API token is found.
```
{
  "base_url: string,
  "client_id": string,
  "client_secret": string,
  "start_date": string (YYYY-MM-DD),
}
```

---

Copyright &copy; 2018 Stitch
