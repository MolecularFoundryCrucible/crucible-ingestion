"""Pytest hooks for the ingestion tests.

Only hooks live here, because pytest will not pick them up anywhere else. The data files,
the parsing and the baseline comparison are all in test_dry_run_parse.py.
"""


def pytest_addoption(parser):
    parser.addoption(
        "--check-values", action="store_true", default=False,
        help="assert the values the schema pinned, not just its key paths and types")


def parametrized_file(nodeid):
    """The data file a test ran against, or None if it wasn't parametrized over one."""
    if not nodeid.endswith("]"):
        return None
    return nodeid[nodeid.index("[") + 1:-1]


def pytest_terminal_summary(terminalreporter):
    """Report one result per data file rather than one per check.

    Several checks run against each file, so pytest's own totals are a multiple of the
    number of files and one unparseable file reads as a handful of failures. The unit
    worth reporting is the file: either it still produces the packet its baseline
    describes, or it doesn't and these are the checks that say so.
    """
    failed_checks = {}
    for outcome in ("passed", "failed", "error", "skipped"):
        for report in terminalreporter.stats.get(outcome, []):
            name = parametrized_file(report.nodeid)
            if name is None:
                continue
            checks = failed_checks.setdefault(name, set())
            if outcome in ("failed", "error"):
                checks.add(report.nodeid.split("::")[-1].split("[")[0])

    if not failed_checks:
        return

    broken = {name: checks for name, checks in failed_checks.items() if checks}
    terminalreporter.write_sep("=", "data files")
    terminalreporter.line(
        f"{len(failed_checks) - len(broken)} of {len(failed_checks)} files "
        f"match their baseline")
    for name in sorted(broken):
        terminalreporter.line(f"  {name}", red=True)
        for check in sorted(broken[name]):
            terminalreporter.line(f"      {check}")
