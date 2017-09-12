import unittest

from meerkat_consul import app


class MeerkatConsulTestCase(unittest.TestCase):

    def setUp(self):
        """Setup for testing"""
        app.config['TESTING'] = True
        self.app = app.test_client()

    def tearDown(self):
        pass

    def test_index(self):
        rv = self.app.get('/')
        self.assertEqual(rv.status_code, 200)
        self.assertIn(b'{"name":"meerkat_consul"}', rv.data)

if __name__ == '__main__':
    unittest.main()