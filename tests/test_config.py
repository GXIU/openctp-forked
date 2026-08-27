from live.config import load_config


def test_load_replay_config():
    cfg = load_config("configs/live_replay.yaml")
    assert cfg.source.mode == "replay"
    assert cfg.source.replay_dates == ("20220104",)
    assert cfg.graph.path.endswith("cqr_bar.yaml")
    assert [i.md_symbol for i in cfg.instruments] == ["sh603019", "sh600000"]
    assert cfg.bars.freq_min == 3
    assert cfg.sinks.jsonl.enabled


def test_unknown_key_rejected(tmp_path):
    p = tmp_path / "bad.yaml"
    p.write_text(
        """
source:
  mode: replay
  bogus: 1
instruments: [600000]
graph:
  path: /x.yaml
"""
    )
    import pytest

    with pytest.raises(ValueError, match="unknown keys"):
        load_config(p)
