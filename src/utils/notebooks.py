import kfp.dsl
from _pytest.monkeypatch import MonkeyPatch


def patch_kfp():
    def _get_path(self):
        return self.uri

    def primitive_decorator(*args, **kwargs):
        return lambda func: func

    monkeypatch = MonkeyPatch()
    monkeypatch.setattr(kfp.dsl, "component", primitive_decorator)
    monkeypatch.setattr(kfp.dsl.Artifact, "_get_path", _get_path)
