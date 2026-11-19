"""Group EVB window checks by any subset of tags.

EVB tags (e.g. ``plant`` and ``unit_id``) become one text column each when the data is converted to
Stream DaQ's native format, and ``groupby_column`` groups windows by a single column. This custom
source adds a ``group`` column that combines the tags chosen per task in ``group_by_tags``:

- no tags: leave ``groupby_column`` out of the task;
- one tag: no custom source needed, set ``groupby_column`` to the tag itself (e.g. ``"unit_id"``);
- two or more tags: use this source and set ``groupby_column`` to ``"group"``.

Load it when starting the server:

    streamdaq serve --files custom-code/

then create tasks with a payload like ``task.json``.
"""

import json
from typing import Any

import pathway as pw

from streamdaq.custom import source
from streamdaq.io.utils import ensure_unique_mqtt_client_id
from streamdaq.schema.evb.definitions import EVBKeyNames, EVBSchema

GROUP_COLUMN = "groupby_column"


def _group_key(*tag_values: str | None) -> tuple[str | None, ...]:
    """Combine tag values into one key, e.g. ``("StreamdaqFactory", "012332")``.

    A tuple is written as a real JSON list, e.g. ``"group": ["StreamdaqFactory", "012332"]``,
    with a tag that is missing from a message shown as ``null``.
    """
    return tuple(tag_values)

@pw.udf
def adapt_evbschema(message: str) -> list[pw.Json]:
    full_evb = json.loads(
        message,
        parse_constant=lambda _: None,
    )
    if (top_measurement_field := EVBKeyNames.MEASUREMENTS.get_matched(full_evb)) is None:
        raise TypeError(
            'Expected a top-level "measurements" field inside the EVB message'
        )

    return [pw.Json(m) for m in full_evb[top_measurement_field]]

@source(data_format="compact")
def mqtt_evb_grouped(uri: str, topic: str, group_by_tags: list[str]):
    """Reads EVB messages from MQTT and adds a ``group`` column combining ``group_by_tags``.

    The keyword arguments come from the payload's ``connector_params``.
    """
    uri = ensure_unique_mqtt_client_id(uri)
    raw_table = pw.io.mqtt.read(
        uri=uri,
        topic=topic,
        format="plaintext",
        qos=0,
    ).assert_append_only().select(
        measurements=adapt_evbschema(pw.this.data)
    )


    def post_transform(native_table: pw.Table) -> pw.Table:
        # Tags can be missing from a message, so their columns are ``str | None``: Pathway's ``+``
        # does not accept optional text, hence the helper function
        tag_columns = [native_table[tag] for tag in group_by_tags]
        group = pw.apply_with_type(_group_key, tuple[str | None, ...], *tag_columns)
        return native_table.with_columns(**{GROUP_COLUMN: group})

    return raw_table, post_transform
