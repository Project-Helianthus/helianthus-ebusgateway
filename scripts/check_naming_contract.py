#!/usr/bin/env python3
"""Check the bounded public naming adoption without network or Git access."""
from pathlib import Path
import hashlib
import json
import re


def check(root):
    root = Path(root)
    inventory = json.loads((root / 'docs/architecture/naming-inventory.json').read_text())
    for row in inventory['rows'] + inventory['bounded_declarations']:
        if row.get('status') not in {'ready', 'unchanged', 'provisional', 'structural', 'native-identity', 'removed'}:
            raise ValueError('undefined inventory status')
    source = (root / 'graphql/mutations.go').read_text()
    compact = lambda value: re.sub(r'\s+', '', value)
    for row in inventory['bounded_declarations']:
        if row['kind'] == 'setter' and row['status'] == 'ready':
            declaration = '"' + row['target'] + '":{' + row['preserve_constraints'] + '}'
            if compact(declaration) not in compact(source):
                raise ValueError('setter binding or constraints changed: ' + row['target'])
    if re.search(r'"(?:frostProtC|frost_prot_c)"\s*:', source):
        raise ValueError('withdrawn writer accepted')
    tools = (root / 'mcp/server_tools.go').read_text()
    if re.search(r'Name:\s*tool(?:Devices|Invoke)LegacyName', tools):
        raise ValueError('legacy MCP registration')
    dispatch = (root / 'mcp/server.go').read_text()
    if re.search(r'case[^:]*tool(?:Devices|Invoke)LegacyName', dispatch):
        raise ValueError('legacy MCP dispatch')
    portal = (root / 'portal/web/src/app.js').read_bytes()
    built = (root / 'portal/static/assets/app.js').read_bytes()
    if built != b'// Generated from portal/web/src/app.js. DO NOT EDIT.\n' + portal:
        raise ValueError('Portal source/build drift')
    manifest = json.loads((root / 'portal/static/assets/manifest.json').read_text())
    for name, entry in manifest['assets'].items():
        data = (root / 'portal/static/assets' / name).read_bytes()
        if entry['bytes'] != len(data) or entry['sha256'] != hashlib.sha256(data).hexdigest():
            raise ValueError('asset manifest drift')
    for old in ('?.semanticSnapshot', 'semanticSnapshot(assetIds:'):
        if old in portal.decode():
            raise ValueError('old Portal GraphQL contract')


if __name__ == '__main__':
    check(Path(__file__).resolve().parents[1])
    print('Bounded naming adoption and Portal parity verified; provisional rows remain provisional.')
