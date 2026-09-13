from types import SimpleNamespace

from vaws_diagnostics import configure
from vaws_diagnostics import cli
from vaws_diagnostics.health import Health, read_health
from vaws_diagnostics.outbox import Outbox, QueueFull


def test_full_intake_queue_does_not_prevent_publication(tmp_path, monkeypatch):
    recorder = configure('cycle-test', root=tmp_path / 'logs')
    queue = Outbox(tmp_path / 'state' / 'queue.db')
    calls = []
    def ingest(*args, **kwargs):
        raise QueueFull('private details never printed')
    monkeypatch.setattr(cli, 'ingest', ingest)
    monkeypatch.setattr(cli, 'publish_one', lambda *args: calls.append('published') or {'status': 'published'})
    with Health(tmp_path / 'state', recorder) as health:
        result = cli.run_cycle(SimpleNamespace(root=[str(tmp_path / 'logs')]), queue, object(), recorder, health)
        assert calls == ['published'] and result['status'] == 'degraded'
        assert result['ingestion'][0] == {'status': 'degraded', 'error_type': 'QueueFull'}
        assert not read_health(tmp_path / 'state')['healthy']
