"""Unit tests for NiceNum"""

import unittest
import doctest

import gracc_reporting.NiceNum as NiceNum


class TestNiceNum(unittest.TestCase):
    """Test niceNum from NiceNum"""
    def test_nicenum_from_doctest(self):
        """Run doctests in niceNum"""
        doctest.testmod(NiceNum, verbose=False)
