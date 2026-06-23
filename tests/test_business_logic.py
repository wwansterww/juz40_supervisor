"""Unit tests for the core report business logic in subjects.base_builder.

These functions encode the tricky rules the whole product depends on — what
counts as "submitted", who counts as a student who "left the course", and how
per-lesson counts/averages are recomputed with left students excluded from both
numerator and denominator. They're pure (no network/Redis), so they're cheap to
test and exactly where a silent regression would corrupt every report.

Run:  .venv/bin/python -m unittest discover -s tests
"""

import unittest

from subjects.base_builder import (
    is_submitted,
    is_left_course,
    to_int,
    get_student_id,
    _recalc_item,
    _collect_left_ids,
)


class TestIsSubmitted(unittest.TestCase):
    def test_finished_flag(self):
        self.assertTrue(is_submitted({"finished": True}))

    def test_finish_or_submission_time(self):
        self.assertTrue(is_submitted({"finishTime": "2026-02-01"}))
        self.assertTrue(is_submitted({"submissionTime": "2026-02-01"}))

    def test_submissions_list(self):
        self.assertTrue(is_submitted({"submissions": [{"x": 1}]}))

    def test_submission_text(self):
        self.assertTrue(is_submitted({"submissionText": "answer"}))
        self.assertFalse(is_submitted({"submissionText": "   "}))

    def test_score_nonzero_counts(self):
        self.assertTrue(is_submitted({"score": 42}))

    def test_score_zero_excluded_by_default(self):
        # 0 is the platform's "no work" placeholder unless the theme says
        # otherwise (САБАҚ ТАПСЫРУ / ҚАЙТАЛАУ ТЕСТ pass include_zero_score=True).
        self.assertFalse(is_submitted({"score": 0}))
        self.assertTrue(is_submitted({"score": 0}, include_zero_score=True))

    def test_empty_is_not_submitted(self):
        self.assertFalse(is_submitted({}))


class TestIsLeftCourse(unittest.TestCase):
    def test_left_marker_score(self):
        self.assertTrue(is_left_course({"score": 0.1}))
        self.assertTrue(is_left_course({"score": "0,1"}))   # comma decimal
        self.assertTrue(is_left_course({"score": "0.1"}))

    def test_comment_phrases(self):
        self.assertTrue(is_left_course({"comments": [{"commentText": "курстан шықты"}]}))
        self.assertTrue(is_left_course({"parentComments": [{"commentText": "оқудан шықты"}]}))
        self.assertTrue(is_left_course({"comment": "шығып кетті"}))

    def test_not_left(self):
        self.assertFalse(is_left_course({"score": 50}))
        self.assertFalse(is_left_course({}))
        self.assertFalse(is_left_course({"comment": "жарайсың"}))


class TestToInt(unittest.TestCase):
    def test_various(self):
        self.assertEqual(to_int("5"), 5)
        self.assertEqual(to_int("5.7"), 5)
        self.assertEqual(to_int(5.9), 5)
        self.assertEqual(to_int("abc"), 0)
        self.assertEqual(to_int(None), 0)


class TestGetStudentId(unittest.TestCase):
    def test_prefers_student_id(self):
        self.assertEqual(get_student_id({"studentId": "A", "username": "B"}), "A")

    def test_falls_back_to_username(self):
        self.assertEqual(get_student_id({"username": "B"}), "B")

    def test_falls_back_to_name(self):
        self.assertEqual(
            get_student_id({"studentFirstname": "Aman", "studentLastname": "Bek"}),
            "Aman_Bek",
        )


class TestRecalcItem(unittest.TestCase):
    def test_left_students_excluded_from_both_sides(self):
        item = {"studentsCount": 10}
        progresses = (
            [{"studentId": f"L{i}", "score": 0.1} for i in range(2)]          # left
            + [{"studentId": f"S{i}", "score": 80, "finished": True} for i in range(5)]  # submitted
            + [{"studentId": f"N{i}", "score": 0} for i in range(3)]          # not submitted
        )
        left_ids = {"L0", "L1"}
        out = _recalc_item(item, progresses, left_ids)

        # 10 students, 2 left → denominator 8, not 10.
        self.assertEqual(out["studentsCount"], 8)
        self.assertEqual(out["totalStudentsCount"], 8)
        # Only the 5 with real work count as submitted.
        self.assertEqual(out["submittedCount"], 5)
        self.assertEqual(out["notSubmittedCount"], 3)
        # Average over submitted scores only.
        self.assertEqual(out["averageScore"], 80)

    def test_no_submissions_gives_none_average(self):
        item = {"studentsCount": 3}
        progresses = [{"studentId": f"N{i}", "score": 0} for i in range(3)]
        out = _recalc_item(item, progresses, set())
        self.assertEqual(out["submittedCount"], 0)
        self.assertIsNone(out["averageScore"])

    def test_forced_count_from_parent(self):
        # Children inherit the parent's corrected count; already_excluded avoids
        # subtracting the same left student twice.
        item = {"studentsCount": 99}
        progresses = [{"studentId": "S0", "score": 70, "finished": True}]
        out = _recalc_item(item, progresses, {"L0"}, forced_count=8,
                           already_excluded={"L0"})
        # forced_count 8, L0 already excluded by parent → not subtracted again.
        self.assertEqual(out["studentsCount"], 8)


class TestCollectLeftIds(unittest.TestCase):
    def test_collects_across_lessons(self):
        all_progresses = [
            [{"studentId": "A", "score": 0.1}, {"studentId": "B", "score": 50}],
            [{"studentId": "C", "comment": "курстан шықты"}],
        ]
        self.assertEqual(_collect_left_ids(all_progresses), {"A", "C"})


if __name__ == "__main__":
    unittest.main()
