from pg_statviz.modules.blocking import (calc_findings, count_by_locktype,
                                         find_locktypes)


def snapshot(*pairs):
    """Build a blocking JSONB payload from (lock_type, blocked_count) pairs.

    Same shape as pgstatviz.lock and pgstatviz.wait: one aggregated
    {dimension, count} object per group, never raw per-session rows.
    """
    return [{'lock_type': lt, 'blocked_count': n} for lt, n in pairs]


def test_find_locktypes_collects_distinct_types_in_order():
    details = [snapshot(('relation', 2), ('transactionid', 1)),
               snapshot(('relation', 3))]
    assert find_locktypes(details) == ['relation', 'transactionid']


def test_find_locktypes_handles_empty_and_none_snapshots():
    assert find_locktypes([]) == []
    assert find_locktypes([None, [], None]) == []


def test_find_locktypes_skips_entries_without_lock_type():
    details = [[{'blocked_count': 1}, {'lock_type': 'tuple'}]]
    assert find_locktypes(details) == ['tuple']


def test_count_by_locktype_reads_the_aggregated_count():
    # The count comes straight from the JSONB, not from array length.
    details = [snapshot(('relation', 4)),
               snapshot(('transactionid', 9)),
               snapshot(('relation', 1))]
    assert count_by_locktype(details, 'relation') == [4, 0, 1]


def test_count_by_locktype_zero_for_empty_snapshots():
    assert count_by_locktype([None, [], snapshot(('tuple', 3))],
                             'tuple') == [0, 0, 3]


def test_count_by_locktype_unknown_type_is_all_zero():
    assert count_by_locktype([snapshot(('relation', 5))], 'advisory') == [0]


def test_count_by_locktype_missing_count_reads_as_zero():
    assert count_by_locktype([[{'lock_type': 'tuple'}]], 'tuple') == [0]


def test_calc_findings_clean_when_never_blocked():
    assert calc_findings([0, 0, 0], [0, 0, 0]) == []


def test_calc_findings_warns_on_any_blocking():
    findings = calc_findings([0, 2, 0], [0, 1, 0])
    assert len(findings) == 1
    assert findings[0]['severity'] == 'WARNING'
    assert '2' in findings[0]['message']


def test_calc_findings_escalates_when_blocking_is_sustained():
    # Blocked in more than half the snapshots: contention, not a blip.
    findings = calc_findings([3, 4, 5, 0], [1, 1, 2, 0])
    assert len(findings) == 1
    assert findings[0]['severity'] == 'CRITICAL'


def test_calc_findings_handles_empty_input():
    assert calc_findings([], []) == []
