from __future__ import annotations

import json

from mmap_entropy.cli import main
from mmap_entropy.reader import RingReader


def test_cli_create_write_read_and_stats(tmp_path, capsys):
    path = tmp_path / "cli.ring"
    assert main(["create", str(path), "--slots", "4", "--slot-size", "16"]) == 0
    assert "created" in capsys.readouterr().out

    assert main(["stat", str(path), "--json"]) == 0
    status = json.loads(capsys.readouterr().out)
    assert status["slots"] == 4
    assert status["slot_size"] == 16

    assert (
        main(
            [
                "write",
                str(path),
                "--source",
                "synthetic",
                "--count",
                "3",
                "--interval",
                "0",
                "--json",
            ]
        )
        == 0
    )
    stats = json.loads(capsys.readouterr().out)
    assert stats["published"] == 3

    assert main(["read", str(path), "--timeout", "1", "--bytes", "4"]) == 0
    output = capsys.readouterr().out.strip()
    assert len(output) == 8
    bytes.fromhex(output)

    assert main(["list", str(tmp_path), "--json"]) == 0
    rings = json.loads(capsys.readouterr().out)
    assert rings[0]["path"] == str(path)


def test_cli_read_empty_ring(tmp_path, capsys):
    path = tmp_path / "empty.ring"
    main(["create", str(path), "--slots", "2", "--slot-size", "16"])
    capsys.readouterr()
    assert main(["read", str(path), "--timeout", "0"]) == 1
    assert "no fresh data" in capsys.readouterr().err


def test_cli_refuses_existing_ring(tmp_path, capsys):
    path = tmp_path / "existing.ring"
    main(["create", str(path)])
    capsys.readouterr()
    assert main(["create", str(path)]) == 2
    assert "already exists" in capsys.readouterr().err


def test_discovery_ignores_broken_files(tmp_path):
    from mmap_entropy import RingWriter, SyntheticSource

    RingWriter.create(tmp_path / "a.ring", source=SyntheticSource(), slots=2, slot_size=64).close()
    RingWriter.create(tmp_path / "b.ring", source=SyntheticSource(), slots=2, slot_size=64).close()
    (tmp_path / "broken.ring").write_text("not a ring")
    assert [path.name for path in RingReader.discover(tmp_path)] == ["a.ring", "b.ring"]
