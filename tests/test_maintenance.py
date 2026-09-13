import os

from vaws_diagnostics.maintenance import prune


def test_retention_preserves_fresh_and_unrelated_files(tmp_path):
    folder = tmp_path / 'events' / 'test'
    folder.mkdir(parents=True)
    old = folder / ('1-' + 'a' * 32 + '.jsonl')
    old.write_text('old')
    os.utime(old, (1, 1))
    fresh = folder / ('2-' + 'b' * 32 + '.jsonl')
    fresh.write_text('fresh')
    unrelated = folder / 'state.db'
    unrelated.write_text('preserve')
    result = prune(tmp_path, max_bytes=1)
    assert not old.exists()
    assert fresh.exists() and unrelated.exists()
    assert result['removed_files'] == 1 and result['limited']
