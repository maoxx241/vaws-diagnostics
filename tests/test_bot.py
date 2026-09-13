import json
import subprocess

import pytest

from vaws_diagnostics.bot import Grok, diagnose_one
from vaws_diagnostics.outbox import Outbox
from vaws_diagnostics.reporter import TransportError


def test_profile_with_hooks_is_rejected_before_model_call(tmp_path, monkeypatch):
    calls = []
    def run(command, **kwargs):
        calls.append(command)
        return subprocess.CompletedProcess(command, 0, json.dumps({'hooks': ['unsafe hook']}), '')
    monkeypatch.setattr(subprocess, 'run', run)
    with pytest.raises(TransportError, match='profile_not_isolated'):
        Grok(home=tmp_path / 'home', work=tmp_path / 'work').diagnose({})
    assert len(calls) == 1


def test_comment_lost_reply_reconciles_without_model_rerun(tmp_path, monkeypatch):
    monkeypatch.setattr('vaws_diagnostics.bot.sanitize_diagnosis', lambda text: text)
    now = [1000.0]
    queue = Outbox(tmp_path / 'bot.db', clock=lambda: now[0])
    queue.enqueue('a', 'o', {'issue_number': 1, 'evidence': {}})
    class Model:
        calls = 0
        def diagnose(self, _):
            self.calls += 1
            return 'Observed: transport failure. Submission outcome needs reconciliation.'
    class GitHub:
        repository = 'example/project'
        comments = []
        def request(self, method, path, payload=None):
            if method == 'GET':
                return self.comments
            self.comments.append({'body': payload['body'], 'html_url': 'https://github.com/example/project/issues/1#issuecomment-1'})
            raise TransportError('lost', uncertain=True)
    model, github = Model(), GitHub()
    assert diagnose_one(queue, github, model)['status'] == 'uncertain'
    now[0] += 301
    assert diagnose_one(queue, github, model)['status'] == 'reconciled'
    assert model.calls == 1
    assert len(github.comments) == 1
