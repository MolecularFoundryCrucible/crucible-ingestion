"""capabilities() is sent as-is in PUT /workers/{worker_id}. The API drops or rewrites an
entry that breaks its rules and reports it in the response's ignored list, so the rules
are enforced here instead."""

import re

from crucible_ingestion.ingestors.registry import SELECTABLE, capabilities

NAME = re.compile(r"^[A-Za-z0-9_.-]{1,128}$")
EXTENSION = re.compile(r"^\.[A-Za-z0-9][A-Za-z0-9._-]{0,31}$")


def test_capabilities_fit_the_worker_registration_rules():
    caps = capabilities()
    assert [c["name"] for c in caps] == list(SELECTABLE)
    assert len(caps) <= 500, "the API refuses a registration with more than 500 capabilities"
    for cap in caps:
        assert set(cap) == {"name", "description", "file_extensions"}
        assert NAME.match(cap["name"]), cap["name"]
        assert cap["description"] is None or len(cap["description"]) <= 2000, cap["name"]
        exts = cap["file_extensions"]
        assert len(exts) <= 50, cap["name"]
        assert len(set(exts)) == len(exts), f"{cap['name']}: repeated extension"
        for ext in exts:
            assert EXTENSION.match(ext), f"{cap['name']}: {ext!r}"
            assert ext == ext.lower(), f"{cap['name']}: {ext!r} is not lowercase"
