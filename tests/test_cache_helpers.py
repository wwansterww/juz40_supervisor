"""Unit tests for cache.py classification/empty-detection helpers.

These drive two correctness-critical behaviours: which TTL each endpoint gets
(a regression here silently makes data stale or hammers the API) and whether a
200-with-empty body is treated as a glitch (the "report came back empty" fix).
"""

import unittest

from cache import _looks_empty, _suspicious_empty, _ttl_for

BASE = "https://api.juz40-edu.kz"
URL_STUDENTS = f"{BASE}/v3/headteacher/groups/G/students?month=2"
URL_SUMMARY = f"{BASE}/v3/headteacher/groups/G/themes/T/lessons/summary"
URL_PROGRESSES = f"{BASE}/v2/headteacher/groups/G/lessons/L/progresses"
URL_THEMES = f"{BASE}/v1/headteacher/groups/G/themes?week=1&month=2"
URL_GROUPS = f"{BASE}/v1/headteacher/courses/C/groups"
URL_COURSES = f"{BASE}/v2/headteacher/subjects/S/courses?size=50&page=0&product=SMART"


class TestLooksEmpty(unittest.TestCase):
    def test_empty_containers(self):
        for v in ([], {}, None, ""):
            self.assertTrue(_looks_empty(v), v)

    def test_empty_envelopes(self):
        self.assertTrue(_looks_empty({"students": []}))
        self.assertTrue(_looks_empty({"themes": []}))
        self.assertTrue(_looks_empty({"content": []}))

    def test_non_empty(self):
        self.assertFalse(_looks_empty([1]))
        self.assertFalse(_looks_empty({"students": [{"id": 1}]}))
        # A dict with no recognised payload key is assumed full (conservative).
        self.assertFalse(_looks_empty({"months": [1, 2]}))


class TestSuspiciousEmpty(unittest.TestCase):
    def test_suspicious_endpoints(self):
        self.assertTrue(_suspicious_empty(URL_STUDENTS, {"students": []}))
        self.assertTrue(_suspicious_empty(URL_SUMMARY, []))
        self.assertTrue(_suspicious_empty(URL_PROGRESSES, []))
        self.assertTrue(_suspicious_empty(URL_GROUPS, []))

    def test_not_suspicious_when_legit_empty_possible(self):
        # An empty week of themes / an empty product page can be legitimate, so
        # they are NOT retried (would just slow every report down).
        self.assertFalse(_suspicious_empty(URL_THEMES, {"themes": []}))
        self.assertFalse(_suspicious_empty(URL_COURSES, {"content": []}))

    def test_not_suspicious_when_data_present(self):
        self.assertFalse(_suspicious_empty(URL_STUDENTS, {"students": [{"id": 1}]}))


class TestTtlFor(unittest.TestCase):
    def test_each_endpoint_gets_intended_ttl(self):
        # Regression guard for the old bug where every "/groups/..." URL
        # collapsed into the 900s bucket.
        self.assertEqual(_ttl_for(URL_STUDENTS), 300)
        self.assertEqual(_ttl_for(URL_PROGRESSES), 1800)
        self.assertEqual(_ttl_for(URL_SUMMARY), 3600)
        self.assertEqual(_ttl_for(URL_THEMES), 3600)
        self.assertEqual(_ttl_for(URL_GROUPS), 900)
        self.assertEqual(_ttl_for(URL_COURSES), 1800)


if __name__ == "__main__":
    unittest.main()
