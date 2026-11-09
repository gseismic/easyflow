"""解析 DAG 时从本地源码加载依赖，使摘要对应实际编译的字节。"""

from __future__ import annotations

import hashlib
import importlib
import sys
import sysconfig
import threading
from collections.abc import Iterator
from contextlib import contextmanager
from importlib.machinery import PathFinder, SourceFileLoader
from pathlib import Path
from types import ModuleType

_IMPORT_LOCK = threading.RLock()


class LocalSourceFinder:
    def __init__(self, dag_path: Path) -> None:
        self.roots = {dag_path.parent.resolve(), Path.cwd().resolve()}
        for parent in dag_path.parents:
            if (parent / "pyproject.toml").is_file() or (parent / ".git").exists():
                self.roots.add(parent.resolve())
                break
        self.excluded = {Path(__file__).resolve().parents[1]}
        self.excluded.update(
            Path(sysconfig.get_path(name)).resolve()
            for name in ("stdlib", "platstdlib", "purelib", "platlib")
        )

    def is_local(self, path: Path) -> bool:
        return (
            path.suffix == ".py"
            and any(path.is_relative_to(root) for root in self.roots)
            and not any(path.is_relative_to(root) for root in self.excluded)
        )

    def find_spec(self, fullname, path=None, target=None):
        spec = PathFinder.find_spec(fullname, path, target)
        if (
            spec is not None
            and isinstance(spec.loader, SourceFileLoader)
            and self.is_local(Path(spec.loader.path).resolve())
        ):
            spec.loader = _LocalSourceLoader(fullname, spec.loader.path)
            return spec
        return None


class _LocalSourceLoader(SourceFileLoader):
    def exec_module(self, module: ModuleType) -> None:
        source = self.get_data(self.path)
        module.__easyflow_source_sha256__ = hashlib.sha256(source).hexdigest()
        exec(self.source_to_code(source, self.path), module.__dict__)


@contextmanager
def local_source_imports(dag_path: Path) -> Iterator[None]:
    finder = LocalSourceFinder(dag_path)
    with _IMPORT_LOCK:
        # 父包也一并刷新，防止它继续引用传递依赖的旧函数或模块。
        for name, module in list(sys.modules.items()):
            loader = getattr(module, "__loader__", None)
            if not isinstance(loader, SourceFileLoader):
                continue
            if not finder.is_local(Path(loader.path).resolve()):
                continue
            sys.modules.pop(name, None)
            parent_name, _, child_name = name.rpartition(".")
            parent = sys.modules.get(parent_name)
            if parent is not None and getattr(parent, child_name, None) is module:
                delattr(parent, child_name)
        importlib.invalidate_caches()
        # 保留自定义 finder 的优先级，仅替换普通本地 Python 文件的加载。
        position = sys.meta_path.index(PathFinder)
        sys.meta_path.insert(position, finder)
        try:
            yield
        finally:
            sys.meta_path.remove(finder)
