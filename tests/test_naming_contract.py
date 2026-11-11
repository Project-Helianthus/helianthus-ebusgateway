import importlib.util
from pathlib import Path
import shutil
import pytest

ROOT = Path(__file__).resolve().parents[1]
spec = importlib.util.spec_from_file_location('naming_contract', ROOT / 'scripts/check_naming_contract.py')
naming = importlib.util.module_from_spec(spec)
spec.loader.exec_module(naming)


def test_current_contract():
    naming.check(ROOT)


@pytest.mark.parametrize('old,new', [
    ('group: 0x02, addr: 0x000F', 'group: 0x03, addr: 0x000F'),
    ('addr: 0x000F', 'addr: 0x0011'),
    ('valueType: configValueFloat32, min: 0.1', 'valueType: configValueUint16, min: 0.1'),
    ('min: 0.1', 'min: 0.2'),
    ('max: 4.0', 'max: 5.0'),
    ('"circuit_heating_curve_factor":', '"heatingCurve":'),
])
def test_each_setter_axis_fails_closed(tmp_path, old, new):
    for relative in ('docs/architecture/naming-inventory.json', 'graphql/mutations.go'):
        dest = tmp_path / relative
        dest.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(ROOT / relative, dest)
    path = tmp_path / 'graphql/mutations.go'
    text = path.read_text()
    # Whitespace is irrelevant to the binding comparison; preserve mutation semantics.
    import re
    text = re.sub(r'\s+', ' ', text)
    old = re.sub(r'\s+', ' ', old)
    if old not in text and old.endswith(':'):
        text = text.replace('"circuit_heating_curve_factor" :', old)
    assert old in text
    path.write_text(text.replace(old, new, 1))
    with pytest.raises(ValueError, match='setter binding'):
        naming.check(tmp_path)
