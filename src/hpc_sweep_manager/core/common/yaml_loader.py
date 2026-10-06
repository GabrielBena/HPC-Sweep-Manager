"""The one YAML loader HSM reads sweep files and ``.hsm/config.yaml`` with, and its dumper.

PyYAML's ``SafeLoader`` resolves numbers by YAML 1.1: ``1e-1`` (no dot) stays a
string, ``010`` is octal 8, and an unquoted ``12:00:00`` is the base-60 int
43200. This loader swaps in YAML 1.2's decimal ints and floats; everything else
(bools such as ``yes``/``no``, null, timestamps) stays as PyYAML has it.
"""

from __future__ import annotations

import re
from typing import Any

import yaml

_INT, _FLOAT = "tag:yaml.org,2002:int", "tag:yaml.org,2002:float"


class YAML12Loader(yaml.SafeLoader):
    """``SafeLoader`` with YAML 1.2 numbers: decimal ints only, ``1e-1``/``.5``/``.inf`` floats."""


YAML12Loader.yaml_implicit_resolvers = {
    first: [(tag, rx) for tag, rx in resolvers if tag not in (_INT, _FLOAT)]
    for first, resolvers in yaml.SafeLoader.yaml_implicit_resolvers.items()
}
YAML12Loader.add_implicit_resolver(_INT, re.compile(r"^[-+]?[0-9]+$"), list("-+0123456789"))
YAML12Loader.add_implicit_resolver(
    _FLOAT,
    re.compile(
        r"^(?:[-+]?(?:\.[0-9]+|[0-9]+(?:\.[0-9]*)?)(?:[eE][-+]?[0-9]+)?"
        r"|[-+]?\.(?:inf|Inf|INF)|\.(?:nan|NaN|NAN))$"
    ),
    list("-+.0123456789"),
)
# SafeLoader's int constructor reads a leading 0 as octal; YAML 1.2 decimal doesn't.
YAML12Loader.add_constructor(_INT, lambda loader, node: int(loader.construct_scalar(node)))


def load_yaml(stream: Any) -> Any:
    """``yaml.safe_load`` with YAML 1.2 numbers (see :class:`YAML12Loader`)."""
    return yaml.load(stream, Loader=YAML12Loader)  # safe: a SafeLoader subclass


class YAMLDumper(yaml.SafeDumper):
    """Quotes every string a YAML 1.1 *or* 1.2 reader would take for something else.

    So a file HSM rewrites reads back the same here (``"1e-3"`` stays a string) and in an
    older HSM (``"12:00:00"`` never becomes 43200).
    """


YAMLDumper.yaml_implicit_resolvers = {
    first: YAML12Loader.yaml_implicit_resolvers.get(first, []) + resolvers
    for first, resolvers in yaml.SafeDumper.yaml_implicit_resolvers.items()
}


def dump_yaml(data: Any) -> str:
    """Block-style YAML, keys in order, every ambiguous string quoted (see :class:`YAMLDumper`)."""
    return yaml.dump(data, Dumper=YAMLDumper, default_flow_style=False, sort_keys=False)
