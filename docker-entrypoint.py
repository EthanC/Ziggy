"""Initialize the writable container directory and drop root privileges."""

from __future__ import annotations

import errno
import os
import sys
from pathlib import Path
from typing import NoReturn

_DEFAULT_ID = "1000"
_MAX_LINUX_ID = (1 << 32) - 2
_WRITABLE_DIRECTORY = "/ziggy"


def _fail(message: str) -> NoReturn:
    print(f"ziggy: {message}", file=sys.stderr)  # noqa: T201
    raise SystemExit(2)


def _read_linux_id(name: str) -> int:
    raw_value = os.environ.get(name, _DEFAULT_ID)
    if not raw_value.isascii() or not raw_value.isdigit():
        _fail(
            f"{name} must be a positive numeric Linux ID between 1 and "
            f"{_MAX_LINUX_ID}; got {raw_value!r}"
        )
    value = int(raw_value)
    if not 1 <= value <= _MAX_LINUX_ID:
        _fail(
            f"{name} must be a positive numeric Linux ID between 1 and "
            f"{_MAX_LINUX_ID}; got {raw_value!r}"
        )
    return value


def _chown_tree(path: str, uid: int, gid: int) -> None:
    Path(path).mkdir(parents=True, exist_ok=True)
    for _root, directories, files, directory_fd in os.fwalk(
        path, follow_symlinks=False
    ):
        try:
            os.fchown(directory_fd, uid, gid)
        except OSError as error:
            if error.errno != errno.EROFS:
                raise
        for name in (*directories, *files):
            try:
                os.chown(
                    name,
                    uid,
                    gid,
                    dir_fd=directory_fd,
                    follow_symlinks=False,
                )
            except OSError as error:
                if error.errno != errno.EROFS:
                    raise


def main() -> NoReturn:
    """Apply requested IDs, then replace this process with the application."""
    uid = _read_linux_id("PUID")
    gid = _read_linux_id("PGID")
    command = sys.argv[1:]
    skip_chown = bool(command and command[0] == "--skip-chown")
    if skip_chown:
        command = command[1:]
    if not command:
        _fail("no command was provided")

    current_uid = os.getuid()
    current_gid = os.getgid()
    if current_uid == 0:
        if not skip_chown:
            _chown_tree(_WRITABLE_DIRECTORY, uid, gid)
        os.setgroups([])
        os.setgid(gid)
        os.setuid(uid)
    elif (current_uid, current_gid) != (uid, gid):
        print(  # noqa: T201
            "ziggy: container started as non-root "
            f"{current_uid}:{current_gid}; requested PUID:PGID {uid}:{gid} "
            "were not applied",
            file=sys.stderr,
        )

    os.execvp(command[0], command)  # noqa: S606 - entrypoint must preserve argv.


if __name__ == "__main__":
    main()
