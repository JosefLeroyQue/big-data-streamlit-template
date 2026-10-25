"""Test-only shim: mongomock 4.3.0 predates pymongo 4.18's `sort` kwarg on ReplaceOne."""
import mongomock.collection as _mc

_orig = _mc.BulkOperationBuilder.add_replace


def _add_replace(self, selector, replacement, upsert=False, collation=None, hint=None, sort=None):
    return _orig(self, selector, replacement, upsert, collation, hint)


_mc.BulkOperationBuilder.add_replace = _add_replace
