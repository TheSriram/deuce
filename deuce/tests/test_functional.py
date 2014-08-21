from unittest import TestCase
from webtest import TestApp
from deuce.tests import FunctionalTest


class TestRootController(FunctionalTest):

    def test_get(self):
        # Require project ID for root as well
        response = self.app.get('/', headers={'x-auth-token': 'good'},
            expect_errors=True)
        assert response.status_int == 400

        # Require auth token for root as well
        response = self.app.get('/', headers={'x-project-id': 'blah'},
            expect_errors=True)
        assert response.status_int == 401

        response = self.app.get('/', headers={'x-project-id': 'blah',
            'x-auth-token': 'good', 'x-storage-url': ''})
        assert response.status_int == 200

    def test_get_10(self):
        response = self.app.get('/v1.0', headers={'x-project-id': 'blah',
            'x-auth-token': 'good', 'x-storage-url': ''})
        assert response.status_int == 302

    def test_get_not_found(self):
        response = self.app.get('/a/bogus/url',
            headers={'x-project-id': 'blah', 'x-auth-token': 'good',
                     'x-storage-url': ''},
            expect_errors=True)

        assert response.status_int == 404
