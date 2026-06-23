"""Unit tests for utils.normalize (Latin→Cyrillic homoglyph folding).

Curators type theme names with a mix of visually-identical Latin and Cyrillic
letters; normalize() folds them so theme matching (quiz detection, zero-score
themes, etc.) is reliable.
"""

import unittest

from utils import normalize


class TestNormalize(unittest.TestCase):
    def test_latin_folded_to_cyrillic_and_uppercased(self):
        # C, A, H are Latin here; all should become Cyrillic and upper-case.
        out = normalize("CAH")
        self.assertEqual(out, "САН")            # right-hand side is Cyrillic
        self.assertTrue(all(ord(ch) > 127 for ch in out))

    def test_lowercase_input_uppercased(self):
        self.assertEqual(normalize("aбc"), "АБС")

    def test_pure_cyrillic_unchanged(self):
        self.assertEqual(normalize("САБАҚ"), "САБАҚ")

    def test_digits_and_spaces_preserved(self):
        self.assertEqual(normalize("1-AΥ "), normalize("1-AΥ "))  # idempotent
        self.assertEqual(normalize("test 1")[-1], "1")


if __name__ == "__main__":
    unittest.main()
