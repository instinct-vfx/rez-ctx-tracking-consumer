# Test fixtures

`rez-<version>/` holds real context-tracking messages captured from that rez
version by [`tools/capture_rez_fixtures.py`](../../tools/capture_rez_fixtures.py).
Each file has the following fields:

- `rez_version`: the rez version that produced the message.
- `routing_key`: the routing key rez published with.
- `payload`: the decoded message body (rez sends `json.dumps(payload)`).

Host, user and local paths are replaced with placeholders.

| File | Scenario |
|---|---|
| `created_default.json` | New resolve with a dependency and a variant, default `context_tracking_context_fields` |
| `sourced_default.json` | The same context loaded again from an `.rxt` file |
| `created_failed.json` | Conflicting request (`foo`, `!bar`), `status: failed` |
| `created_extra_fields.json` | `context_tracking_extra_fields = {"show": "demo", "site": "${NO_SUCH_VAR}"}`; rez drops `site` |
| `created_all_fields.json` | Every field `ResolvedContext.to_dict()` can produce |

To regenerate, or to add a new rez version:

```sh
uv venv /tmp/rez-venv && uv pip install --python /tmp/rez-venv rez
/tmp/rez-venv/bin/python tools/capture_rez_fixtures.py tests/fixtures/rez-<version>
```
