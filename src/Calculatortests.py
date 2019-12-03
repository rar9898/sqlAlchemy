import unittest

from Calculator import Calculator

class MyTestCase(unittest.TestCase):

    def setUp(self) -> None:
        self.Calculator = Calculator()


    def test_instantiate_Calculator(self):
        self.assertIsInstance(self.Calculator, Calculator)

    def test_result_property_Calculator(self):
        self.assertEqual(self.Calculator.result, 0)

if __name__ == '__main__':
            unittest.main()