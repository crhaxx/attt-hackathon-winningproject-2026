"""Read-only Git access for ingestion: snapshot identity, file blobs and commit diffs.

Git is the only source of identity: a commit identifies the repository snapshot, a blob id
identifies file contents. Nothing here writes to the inspected checkout (status runs with
--no-optional-locks so it does not even refresh the index).
"""
from __future__ import annotations

import re
import subprocess
from dataclasses import dataclass
from pathlib import Path

REGULAR_MODES = {"100644", "100755"}  # collection policy: tracked regular files only


class GitError(RuntimeError):
    pass


def git(repo: Path, *args, input: bytes | None = None) -> bytes:
    p = subprocess.run(["git", "--no-optional-locks", "-C", str(repo), *args],
                       input=input, capture_output=True)
    if p.returncode != 0:
        raise GitError(f"git {' '.join(args[:3])} failed: {p.stderr.decode(errors='replace').strip()[:300]}")
    return p.stdout


def safe_url(url: str | None) -> str | None:
    """Remote URL without embedded credentials (https://user:token@host/... -> https://host/...)."""
    if not url:
        return None
    return re.sub(r"^([a-z][a-z0-9+.-]*://)[^@/]*@", r"\1", url.strip())


@dataclass
class Snapshot:
    repo: Path
    head: str
    identity: str            # root commit id(s): stable across clones of the same repository
    remote: str | None       # informational, credentials stripped
    files: dict[str, str]    # path -> blob id (tracked regular files at HEAD)
    sizes: dict[str, int]    # path -> blob size in bytes


def toplevel(repo: Path) -> Path:
    try:
        return Path(git(repo, "rev-parse", "--show-toplevel").decode().strip())
    except (GitError, FileNotFoundError) as e:
        raise GitError(f"{repo} is not a Git checkout ({e})") from None


def dirty_tracked(repo: Path) -> list[str]:
    """Staged or unstaged changes to tracked files (untracked files are outside the corpus)."""
    out = git(repo, "status", "--porcelain=v1", "-z", "--untracked-files=no", "--ignore-submodules=none")
    return [e for e in out.decode(errors="replace").split("\0") if e.strip()]


def snapshot(repo: Path) -> Snapshot:
    try:
        head = git(repo, "rev-parse", "--verify", "HEAD^{commit}").decode().strip()
    except GitError:
        raise GitError(f"{repo} has no commits (HEAD is unborn)") from None
    roots = sorted(git(repo, "rev-list", "--max-parents=0", "HEAD").decode().split())
    try:
        remote = safe_url(git(repo, "config", "--get", "remote.origin.url").decode())
    except GitError:
        remote = None
    files, sizes = {}, {}
    for entry in git(repo, "ls-tree", "-r", "-l", "-z", "--full-tree", head).decode("utf-8", "surrogateescape").split("\0"):
        if not entry:
            continue
        meta, path = entry.split("\t", 1)
        mode, kind, oid, size = meta.split()
        if kind == "blob" and mode in REGULAR_MODES:
            files[path], sizes[path] = oid, int(size)
    return Snapshot(repo, head, ",".join(roots), remote, files, sizes)


def read_blobs(repo: Path, oids: list[str]) -> dict[str, bytes]:
    """Contents of many blobs in one `git cat-file --batch` call."""
    unique = sorted(set(oids))
    if not unique:
        return {}
    out = git(repo, "cat-file", "--batch", input=("\n".join(unique) + "\n").encode())
    blobs, pos = {}, 0
    for _ in unique:
        nl = out.index(b"\n", pos)
        oid, kind, size = out[pos:nl].decode().split()
        start = nl + 1
        blobs[oid] = out[start:start + int(size)]
        pos = start + int(size) + 1
    return blobs


def has_commit(repo: Path, commit: str) -> bool:
    try:
        git(repo, "cat-file", "-e", f"{commit}^{{commit}}")
        return True
    except GitError:
        return False


def diff_paths(repo: Path, old: str, new: str) -> dict[str, str]:
    """path -> A/M/D/T between two commits; renames are reported as delete + add."""
    out = git(repo, "diff", "--name-status", "--no-renames", "--no-ext-diff", "-z", old, new).decode(
        "utf-8", "surrogateescape").split("\0")
    changes = {}
    for status, path in zip(out[0::2], out[1::2]):
        if status:
            changes[path] = status[0]
    return changes
