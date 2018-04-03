import unittest
import doctest

class TestNiceNum(unittest.TestCase):
    """Test niceNum from NiceNum"""
    def test_nicenum_from_doctest(self):
        """Run doctests in niceNum"""
        import gracc_reporting.NiceNum as NiceNum
        doctest.testmod(NiceNum, verbose=False)
