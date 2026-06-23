"""Regression tests for the inactive-group ("special curator") filter.

A group whose month-scoped students list comes back with <= 1 student is an
inactive / placeholder group and must be EXCLUDED from the report — otherwise it
shows up as an empty "-" row. This filter used to live in a separate
_is_group_active pre-pass; it now lives inside build_group_all_weeks. A real
fetch FAILURE must NOT drop the group (fall back to the groups-list count).

This pins the behaviour so the filter can't silently disappear again.
"""

import asyncio
import unittest

import subjects.base_builder as bb


def _trivial_make_builder():
    return bb.make_builder(
        extract_metrics_fn=lambda sr, name: {"x": None},
        merge_metrics_fn=lambda lst: {"x": None},
        empty_metrics_fn=lambda: {"x": None},
        metrics_to_row_fn=lambda base, m: {**base, **m},
    )


class TestGroupFilter(unittest.IsolatedAsyncioTestCase):
    async def _build(self, group, students_resp, fetch_raises=False):
        orig = bb.api_get_async

        async def fake(url, token, client):
            if "/students" in url:
                if fetch_raises:
                    raise RuntimeError("network down")
                return students_resp
            if "/themes" in url:
                return {"themes": []}   # empty week → no metrics, but group still builds
            return {}

        bb.api_get_async = fake
        try:
            _, build_group_all_weeks, _, _ = _trivial_make_builder()
            return await build_group_all_weeks(group, "tok", 5, None, asyncio.Semaphore(5))
        finally:
            bb.api_get_async = orig

    async def test_empty_month_students_excluded(self):
        # 0 students for the month on a SUCCESSFUL fetch ⇒ inactive ⇒ dropped,
        # even though the groups-list carried a studentCount. (This is exactly
        # the "empty -row for a special curator" case.)
        self.assertIsNone(await self._build({"id": "g", "studentCount": 29}, {"students": []}))

    async def test_single_student_excluded(self):
        self.assertIsNone(await self._build({"id": "g", "studentCount": 1}, {"students": [{"id": 1}]}))

    async def test_active_group_included(self):
        r = await self._build({"id": "g"}, {"students": [{"id": i} for i in range(30)]})
        self.assertIsNotNone(r)
        self.assertIn(30, r["base"].values())   # student count carried through

    async def test_fetch_failure_falls_back(self):
        # A transient FETCH FAILURE must not erase a real group.
        r = await self._build({"id": "g", "studentCount": 25}, None, fetch_raises=True)
        self.assertIsNotNone(r)
        self.assertIn(25, r["base"].values())


if __name__ == "__main__":
    unittest.main()
