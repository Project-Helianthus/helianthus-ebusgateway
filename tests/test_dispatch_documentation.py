import pathlib
import unittest

ROOT = pathlib.Path(__file__).resolve().parents[1]


class DispatchDocumentationTest(unittest.TestCase):
    def test_semreg_dependency_owner(self):
        readme = (ROOT / 'README.md').read_text()
        self.assertIn('https://github.com/Project-Helianthus/helianthus-ebusgateway/blob/main/go.mod', readme)
        self.assertIn('exact SemReg dependency is owned by', readme)
        self.assertNotIn('accepted Gateway baseline pins SemReg at', readme)

    def test_dispatcher_owner_references(self):
        source = (ROOT / 'cmd/gateway/vaillant_b503_dispatcher.go').read_text()
        header = source.split('import (', 1)[0]
        for reference in ('b503Bus interface', 'generation-bound substrate',
                          'source provider', 'bus.Send', 'liveMonitorMu',
                          'dedicates', 'stale epoch'):
            with self.subTest(reference=reference):
                self.assertIn(reference, header)
        self.assertNotIn('execution-plans', header)
        self.assertNotIn('Plan canonical', header)


if __name__ == '__main__':
    unittest.main()
