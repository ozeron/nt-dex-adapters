# Fixture capture

This directory is reserved for a small public-feed capture script. Store full capture sessions outside Git. Copy selected public messages into the venue crate's `test_data/` directory with their endpoint, subscription, and UTC capture time. Preserve payload bytes for parser tests.

Capture REST instrument and book snapshots alongside WebSocket book, trade, and quote messages so fixture tests can check instrument mapping and book bootstrap. Redact credentials and discard private feed payloads before adding fixtures.
