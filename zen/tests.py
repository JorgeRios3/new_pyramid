import unittest
import transaction

from pyramid import testing

from .models import DBSession


@unittest.skip("por el momento este no")
class TestMyViewSuccessCondition(unittest.TestCase):
    def setUp(self):
        self.config = testing.setUp()
        from sqlalchemy import create_engine

        engine = create_engine("sqlite://")
        from .models import (
            Base,
            MyModel,
        )

        DBSession.configure(bind=engine)
        Base.metadata.create_all(engine)
        with transaction.manager:
            model = MyModel(name="one", value=55)
            DBSession.add(model)

    def tearDown(self):
        DBSession.remove()
        testing.tearDown()

    def test_passing_view(self):
        from .views import my_view

        request = testing.DummyRequest()
        info = my_view(request)
        self.assertEqual(info["one"].name, "one")
        self.assertEqual(info["project"], "zen")


@unittest.skip("este otro tampoco")
class TestMyViewFailureCondition(unittest.TestCase):
    def setUp(self):
        self.config = testing.setUp()
        from sqlalchemy import create_engine

        engine = create_engine("sqlite://")
        from .models import (
            Base,
            MyModel,
        )

        DBSession.configure(bind=engine)

    def tearDown(self):
        DBSession.remove()
        testing.tearDown()

    def test_failing_view(self):
        from .views import my_view

        request = testing.DummyRequest()
        info = my_view(request)
        self.assertEqual(info.status_int, 500)


class TestCornice(unittest.TestCase):
    def setUp(self):
        self.config = testing.setUp()

    def tearDown(self):
        pass

    def test_configuration(self):
        self.assertTrue(not self.config is None)

    def test_zenversion_token_invalido(self):
        from .views import Zenversion

        request = testing.DummyRequest()
        z = Zenversion(request)
        response = z.get()
        self.assertIn("error", response)

    def test_zenversion_token_valido(self):
        from .views import Zenversion, cached_results

        cached_results.dicAuthToken["tokengood"] = True
        request = testing.DummyRequest()
        request.headers["authorization"] = "notgood notgood tokengood"
        z = Zenversion(request)
        response = z.get()
        self.assertIn("zenversion", response)

    def test_adrede_1(self):
        self.assertIn(3, [0, 1])

    def test_adrede_2(self):
        self.assertEqual([1, 1], [0, 1])
