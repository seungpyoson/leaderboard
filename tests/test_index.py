import subprocess
import unittest
from pathlib import Path


class FrontendTests(unittest.TestCase):
    def test_unavailable_filters_and_columns(self):
        script = r"""
const assert = require('node:assert/strict');
const fs = require('node:fs');
const vm = require('node:vm');
const html = fs.readFileSync(process.argv[1], 'utf8');
const code = html.match(/<script>([\s\S]*?)<\/script>/)[1].replace(/\binit\(\);\s*$/, '');
function element(filter) {
    const attrs = {};
    return {
        dataset: {filter}, hidden: false, textContent: '', title: '',
        classList: {toggle() {}},
        setAttribute(key, value) { attrs[key] = value; },
        getAttribute(key) { return attrs[key]; },
    };
}
const filters = ['all', 'HFT/MM', 'DIR'].map(element);
const columns = {volume: element(), efficiency: element()};
const notice = element();
const document = {
    querySelectorAll(selector) {
        assert.equal(selector, '.filter-btn');
        return filters;
    },
    querySelector(selector) {
        const match = selector.match(/^th\[data-col="(volume|efficiency)"\]$/);
        assert.ok(match, selector);
        return columns[match[1]];
    },
    getElementById(id) {
        assert.equal(id, 'availability');
        return notice;
    },
};
const context = vm.createContext({document});
vm.runInContext(code, context);
vm.runInContext(`profiles = [{style: null, volume: null, efficiency: null}];
    activeFilter = 'DIR'; updateAvailability();`, context);
assert.equal(filters[0].hidden, false);
assert.equal(filters[1].hidden, true);
assert.equal(filters[2].hidden, true);
assert.equal(vm.runInContext('activeFilter', context), 'all');
assert.equal(notice.hidden, false);
assert.match(notice.textContent, /volume and efficiency unavailable/);
assert.match(columns.volume.textContent, /unavailable/);
assert.equal(columns.efficiency.getAttribute('aria-disabled'), 'true');
vm.runInContext(`profiles = [{style: 'HFT/MM', volume: 100, efficiency: 2}];
    updateAvailability();`, context);
assert.equal(filters[1].hidden, false);
assert.equal(filters[2].hidden, true);
assert.equal(notice.hidden, true);
assert.equal(columns.volume.getAttribute('aria-disabled'), 'false');
vm.runInContext(`profiles = [{style: 'HFT/MM'}, {style: 'DIR'}];
    updateAvailability();`, context);
assert.equal(filters[1].hidden, false);
assert.equal(filters[2].hidden, false);
"""
        index = Path(__file__).resolve().parents[1] / "index.html"
        result = subprocess.run(["node", "-e", script, str(index)], capture_output=True, text=True)
        self.assertEqual(result.returncode, 0, result.stderr)
