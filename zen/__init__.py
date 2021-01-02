from pyramid.config import Configurator
from pyramid_beaker import session_factory_from_settings
from sqlalchemy import engine_from_config
from .views import raiz, raiz2
import redis
from .models import (
    DBSession,
    Base,
    DBSession2,
    Base2
    )
redis_host, redis_port, redis_db  = "127.0.0.1", 6379, 5
redis_conn  = redis.Redis(host = redis_host, port = redis_port, db = redis_db)

def main(global_config, **settings):
    pyraconfig = "prod"
    if "TEST" in settings.get("sqlalchemy.url",""):
        pyraconfig = "test"
    redis_host = settings.get("redis.host")
    redis_port = settings.get("redis.port")
    redis_db = settings.get("redis.db")
    redis_conn  = redis.Redis(host = redis_host, port = redis_port, db = redis_db)
    redis_conn.set("pyraconfig",pyraconfig)

    engine = engine_from_config(settings, 'sqlalchemy.')
    engine2 = engine_from_config(settings, 'sqlalchemy2.')
    session_factory = session_factory_from_settings( settings )
    DBSession.configure(bind=engine)
    Base.metadata.bind = engine
    DBSession2.configure(bind=engine2)
    Base2.metadata.bind = engine2
    
    config = Configurator(settings=settings)
    config.set_session_factory( session_factory )
    config.add_static_view('app', 'app', cache_max_age=3600)
    config.add_static_view('assets', 'assets', cache_max_age=3600)
    config.add_static_view('fonts', 'fonts', cache_max_age=3600)
    config.add_route( 'raiz', '/')
    config.add_route( 'zeniclar', '/zeniclar/')
    config.add_view( raiz, route_name = 'raiz')
    config.add_view( raiz2, route_name = 'zeniclar')
    config.add_route('token', '/api/token')
    
    config.add_route('pruebapost','/api/pruebapost')
    config.add_route('revoke','/api/revoke')
    config.add_route('routeauth', '/api/routeauth')
    config.add_route('lotespinares', '/api/lotespinares')
    config.add_route('foo', '/api/foo')
    config.add_route('printtest','/api/printtest')
    config.add_route('printtest2','/api/printtest2')
    config.add_route('listprinters','/api/listprinters')
    config.add_route('userprinter','/api/userprinter')
    config.add_route('useremail','/api/useremail')
    config.add_route('deleteprinter','/api/deleteprinter')
    config.add_route('otro','/api/otro')
    config.add_route('oauth', '/api/oauth')
    config.add_route('refreshtoken', '/api/refreshgoogletoken')
    config.add_route('ropiclar', '/api/ropiclar')
    config.add_route('logosmall','/image/logosmall.jpg')
    config.include("cornice")
    config.scan("zen.views")
    return config.make_wsgi_app()
