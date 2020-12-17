#!/bin/env python
# -*- coding: iso-8859-15 -*-

from cornice.resource import resource, view
from models import ( 
     DBSession, Base,Mfa_User, CuentaPagare, Cuenta, Documento, MedioPublicitario, Prospecto,
     DocumentoPagare, ReciboPagare, MovimientoPagare, Inmueble, GerentesVentas, Vendedor, d_e )
import traceback
from traceback import print_exc
from pyramid.response import Response
from pyramid.httpexceptions import HTTPFound
from pyramid.view import view_config
import sys
import transaction
from utils import aletras
from datetime import datetime
import json
import rethinkdb as rdb
from urllib2 import urlopen
from expiringdict import ExpiringDict
from hashlib import md5, sha1
from cloud_spooler import CloudSpooler, CLIENT_ID, CLIENT_KEY, ICLAR_REDIRECT_URI, requests

from repoze.lru import lru_cache
from pyramid.events import subscriber, NewRequest, ApplicationCreated

from mako.template import Template
from mako.runtime import Context
from cStringIO import StringIO
import xhtml2pdf.pisa as pisa
import redis
import pyodbc
from painter import paint
import tsql
import uuid

redis_host, redis_port, redis_db  = "10.0.1.124", 6379, 5
#redis_conn  = redis.Redis(host = redis_host, port = redis_port, db = redis_db)
redis_conn = None
formato_comas = "{:,.2f}"

def l_traceback(msg=""):
	exc_type, exc_value, exc_traceback = sys.exc_info()
	error = "".join(traceback.format_tb(exc_traceback))
	return error

@subscriber( NewRequest )
def anyrequest( event ):
	print( paint.blue( "request at {} is {}".format(datetime.now().isoformat(), event.request.path_qs) ))

@subscriber( ApplicationCreated )
def arranque( event ):
	global redis_conn
	settings = event.app.registry.settings
	redis_host = settings.get("redis.host","")
	redis_port = int(settings.get("redis.port", ""))
	redis_db = int(settings.get("redis.db", ""))
	try:
		assert cloudSpooler, "No esta cloudSpooler"

		assert redis_host and redis_port and redis_db, "Faltan parametros en el ini sobre el redis"
	except:
		print_exc()

	rdb.connect(settings.get("rethinkdb.host"), settings.get("rethinkdb.port")).repl()
	redis_conn = redis.Redis(host = redis_host, port = redis_port, db = redis_db )
	cd = redis_conn.get("zeniclar-current-deploy")
	try:
		assert cd, "No hay redis, o registro de current-deploy para zeniclar"
		print( paint.blue("Current Deploy {}".format(cd)))
		cloudSpooler.redis_conn = redis_conn
		token_req = get_google_token( redis_conn.get("google_code"), True)
		assert token_req.get("success") , "No se puedo obtener token para Google Cloud Print"
	except:
		print_exc()

	
	print( paint.yellow( "Arrancando la Web App a las {}".format(datetime.now().isoformat()) ))
	#print( paint.green("rethinkdb.host = {}").format(inivalues.get("rethinkdb.host","no esta")))
	for x in "zen_token_hub zen_track_casas_ofertas zen_track_prospectos".split(" "):
		print "{} tenia {}".format( x, rdb.db("iclar").table(x).count().run() )
		rdb.db("iclar").table(x).delete().run()
		print "{} tiene {}".format( x, rdb.db("iclar").table(x).count().run() )


def pdfFileName(tipo="indeterminado", llave="X" ):
	tipo = tipo.upper()
	fecha = datetime.now().isoformat()
	#fecha = fecha.isoformat()
	for x in "-:.T":
		fecha = fecha.replace(x, "")
	nombre = "{}_{}_{}.pdf".format(tipo,llave,fecha)
	return nombre

class Datos(object):
	def __setattr__(self, name, value):
		if isinstance(value, unicode):
			self.__dict__[name]= value.encode("ascii", "xmlcharrefreplace")
		else:
			self.__dict__[name]= value

def preparaQuery(sql):
		sqlx = sql.replace('\t', ' '); sql = sqlx.replace('\n', ' ')
		return sql

def ofertaReciente(etapa=0, oferta=0, asignada=False):
	session = DBSession
	if not etapa and not oferta:
		if asignada:
			query=preparaQuery("""select top 1 o.fk_etapa as fk_etapa, o.oferta as oferta  
			from ofertas_compra o join cuenta c on o.cuenta=c.codigo and c.fk_inmueble is not null 
			order by fk_etapa desc, oferta desc""")
		else:
			query="select top 1 fk_etapa, oferta  from ofertas_compra order by fk_etapa desc, oferta desc"
	else:
		query="select fk_etapa, oferta  from ofertas_compra  where fk_etapa={} and oferta={} ".format(etapa, oferta)
	r = session.execute(query)
	for x in r:
		etapa=x.fk_etapa
		oferta=x.oferta
	assert etapa >0 and oferta>0, "no tiene nada"
	query="select fk_desarrollo from etapa where codigo ={} ".format(etapa)
	r = session.execute(query)
	for x in r:
		desarrollo=x.fk_desarrollo
	assert desarrollo >0, "no tiene desarrollo"
	query="select fk_empresa from desarrollo where codigo ={} ".format(desarrollo)
	r = session.execute(query)
	for x in r:
		empresa=x.fk_empresa
	assert empresa >0, "no tiene empresa"
	return [empresa,desarrollo,etapa,oferta]

def obtenerRap(cliente=None):
	
	datos=Datos()
	session = DBSession
	meses = ("", "Enero", "Febrero", "Marzo", "Abril", "Mayo", "Junio", "Julio", "Agosto",
			         "Septiembre", "Octubre", "Noviembre", "Diciembre")
	formato_fecha="a {} de {} de {}"
	fecha=datetime.now()
	datos.fecha=formato_fecha.format(fecha.day,meses[fecha.month],fecha.year)
	manzana=""
	desarrollo=""
	etapa=0
	cuenta=""
	lote=""
	nombre=""
	referencia=""
	query="""

		select d.descripcion as desarrollo, c.fk_etapa as etapa, 
		i.iden2 as manzana, i.iden1 as lote, c.codigo as cuenta,
		x.nombre as nombre
		from cuenta c join inmueble i on c.fk_inmueble = i.codigo
		join etapa e on i.fk_etapa = e.codigo
		join desarrollo d on e.fk_desarrollo = d.codigo
		join cliente x on c.fk_cliente = x.codigo
		where c.fk_cliente={}
	""".format(cliente)
	query=preparaQuery(query)
	#print "este el query que truena",query
	datos.manzana=""
	datos.desarrollo=""
	datos.etapa=""
	datos.lote=""
	datos.cuenta=""
	datos.nombre=""
	datos.referencia=""
	datos.cliente=cliente
	r = session.execute(query)
	for x in r:
		datos.manzana=x.manzana
		datos.desarrollo=x.desarrollo
		datos.etapa=x.etapa
		datos.lote=x.lote
		datos.cuenta=x.cuenta
		datos.nombre=x.nombre.decode("iso-8859-1")
	query="""
		select top 1 referencia as referencia from referencias_rap where cliente={}
	""".format(cliente)
	if datos.cuenta:
		query="{} and cuenta={}".format(query,datos.cuenta)
	query="{} order by cuenta desc ".format(query)
	query=preparaQuery(query)
	print "este es el query", query
	r = session.execute(query)
	for x in r:
		datos.referencia=x.referencia

	return datos

def obtenerCaracteristicas(etapa=0, oferta=0):
	etapa,oferta = ofertaReciente( etapa, oferta, asignada = True)[2:]
	meses = ("", "Enero", "Febrero", "Marzo", "Abril", "Mayo", "Junio", "Julio", "Agosto", "Septiembre", "Octubre", "Noviembre", "Diciembre")
	formato_fecha="{} de {} de {}"
	cliente, imueble, idprecioetapa="","",""
	session = DBSession

	sql = """
			select e.descripcion as descripcion, p.cuenta as cuenta,
			convert(varchar(10), p.fecha_oferta, 103) as fecha_oferta,
			case when p.fecha_asignacion is null then 'null' else convert(varchar(10), p.fecha_asignacion, 103) end as fecha_asignacion,
			isnull(i.iden2, '') as iden2, isnull(i.iden1, '') as iden1,
			p.fk_preciosetapaasignacion as fk_preciosetapaasignacion,
			p.precioasignacion as precioasignacion, p.fk_preciosetapaoferta as fk_preciosetapaoferta, 
			p.preciooferta as preciooferta, 
			convert(varchar(10), o.fecha_asignacion, 103) as fechaasignacion,
			c.fk_inmueble as fk_inmueble , m.iden2 as iiden2, m.iden1 as iiden1, 
			p.inmueble as inmueble, isnull(r.idprospecto, 0) as idprospecto, 
			c.fk_cliente as fk_cliente, t.nombre as nombre
			from gixpreciosetapaofertaasignacion p
			join etapa e on e.codigo = p.fk_etapa
			left join inmueble i on i.codigo = p.inmueble
			join ofertas_compra o on o.fk_etapa = p.fk_etapa and o.oferta = p.oferta
			join cuenta c on c.codigo = p.cuenta
			join inmueble m on m.codigo = c.fk_inmueble
			left join gixprospectos r on r.cuenta = c.codigo
			join cliente t on t.codigo = c.fk_cliente
			where p.fk_etapa = {} and p.oferta = {}
			""".format(etapa, oferta)
	sql=preparaQuery(sql)
	r=session.execute(sql)
	datos=Datos()
	print "los datos del query son "
	idprecioetapa = ""
	for x in r:
		datos.descripcion_etapa=x.descripcion.decode("iso-8859-1")
		datos.cuenta=x.cuenta
		dia,mes,ano=x.fecha_oferta.split("/")
		if dia[0] == "0":
			dia = dia[1:]
		mes=int(mes)
		datos.fecha_oferta=formato_fecha.format(dia,meses[mes],ano)
		datos.fecha_asignacion=x.fecha_asignacion
		datos.iden2=x.iden2.decode("iso-8859-1")
		datos.iden1=x.iden1
		datos.fk_preciosetapaasignacion=x.fk_preciosetapaasignacion
		datos.precioasignacion=x.precioasignacion
		datos.precioasignacion_comas=formato_comas.format(x.precioasignacion)
		datos.fk_preciosetapaoferta=x.fk_preciosetapaoferta
		datos.preciooferta=x.preciooferta
		datos.fechaasignacion=x.fechaasignacion
		datos.fk_inmueble=x.fk_inmueble
		datos.iiden2=x.iiden2
		datos.iiden1=x.iiden1
		datos.inmueble=x.inmueble
		datos.idprospecto=x.idprospecto
		datos.fk_cliente=x.fk_cliente
		datos.nombre=x.nombre.decode("iso-8859-1")
		inmueble=x.fk_inmueble
		idprecioetapa=x.fk_preciosetapaasignacion
		datos.oferta=oferta
		datos.etapa=etapa
		#print x
	print "el idprecioetapa es",idprecioetapa
	assert idprecioetapa <> "", "el inmueble no tiene idprecioetapa, lo cual puede ser que no este asignada"
	sql = """
		select p.id, p.cantidad as cantidad, c.descripcion as descripcion from gixpreciosetapacaracteristicas p
		join gixcaracteristicasinmuebles c on c.id = p.fk_idcaracteristica
		where p.fk_idpreciosetapa = {} order by c.descripcion
		""".format(idprecioetapa)
	sql=preparaQuery(sql)
	r=session.execute(sql)
	print "listan caracteristicas"
	caracteristicas=0

	listacaracteristicas=[]
	for x in r:
		datos.descripcion=x.descripcion.decode("iso-8859-1")
		datos.cantidad=float(x.cantidad)
		print datos.cantidad, datos.descripcion
		listacaracteristicas.append(dict(cantidad=datos.cantidad,descripcion=datos.descripcion))

		caracteristicas+=1
	datos.listacaracteristicas=listacaracteristicas
	print "datos.listacaracteristicas", datos.listacaracteristicas
	jump=[ "<br>" for salto in range(caracteristicas,18)]
	jump="".join(jump)
	datos.jump=jump
	return datos

def obtenerOferta(etapa=0, oferta=0):
	session = DBSession
	meses = ("", "Enero", "Febrero", "Marzo", "Abril", "Mayo", "Junio", "Julio", "Agosto", "Septiembre", "Octubre", "Noviembre", "Diciembre")
	formato_fecha="{} de {} de {}"
	query = """
		select convert(varchar(10), o.fecha_oferta, 103) as fecha,
		e.razonsocial as razonsocial,
		c.nombre as nombre, c.rfc as rfc, c.imss as imss , c.domicilio as domicilio,
		c.telefonocasa as telefonocasa, d.descripcion as descripcion,
		d.localizacion as localizacion, d.estado as estado, o.precio as precio,
		o.monto_credito as monto_credito, o.apartado as apartado,
		o.oferta as oferta, fk_etapa as etapa
		from ofertas_compra o
		join cliente c on o.cliente = c.codigo
		join empresa e on {} = e.codigo
		join desarrollo d on {} = d.codigo
		where o.fk_etapa = {} and o.oferta = {}
		""".format(*ofertaReciente(etapa, oferta))
	sql=preparaQuery(query)
	resultado=session.execute(sql)
	datos=Datos()
	for r in resultado:
		dia,mes,ano=r.fecha.split("/")
		mes=int(mes)
		if dia[0] == "0":
			dia = dia[1:]
		datos.fecha=formato_fecha.format(dia,meses[mes],ano)
		
		datos.razonsocial=r.razonsocial.decode("iso-8859-1")
		datos.nombre=r.nombre.decode("iso-8859-1")
		datos.rfc=r.rfc
		datos.imss=r.imss
		datos.domicilio=r.domicilio.decode("iso-8859-1")
		datos.telefonocasa=r.telefonocasa
		datos.descripcion=r.descripcion.decode("iso-8859-1")
		datos.localizacion=r.localizacion.decode("iso-8859-1")
		datos.estado=r.estado
		datos.precio=float(r.precio)
		datos.precio_letras=aletras(float(r.precio), tipo="pesos")
		datos.precio_comas=formato_comas.format(r.precio)
		datos.monto_credito=float(r.monto_credito)
		datos.monto_credito_letras=aletras(float(r.monto_credito), tipo="pesos")
		datos.monto_credito_comas=formato_comas.format(r.monto_credito)
		datos.apartado=float(r.apartado)
		datos.apartado_letras=aletras(float(r.apartado), tipo="pesos")
		datos.apartado_comas=formato_comas.format(r.apartado)
		datos.oferta=r.oferta
		datos.etapa=r.etapa
		print "valores", [(x,getattr(datos,x)) for x in datos.__dict__]
		
	return datos

def obtenerOtro():
	datos = Datos()

	datos.fecha=datetime.now().isoformat()
	return datos

def obtenerAnexo(etapa=0, oferta=0, precalificacion=0, avaluo=0, subsidio=0, pagare=0):
	session = DBSession
	datos=Datos()
	psql = preparaQuery
	meses = ("", "Enero", "Febrero", "Marzo", "Abril", "Mayo", "Junio", "Julio", "Agosto",
			         "Septiembre", "Octubre", "Noviembre", "Diciembre")
	formato_fecha="{} de {} de {}"
	fecha=datetime.now()
	datos.fecha=formato_fecha.format(fecha.day,meses[fecha.month],fecha.year)
	print "fecha",datos.fecha
	cuenta=0
	if not etapa and not oferta:
		query="""select top 1  c.codigo as cuenta,
		o.fk_etapa as fk_etapa, o.oferta as oferta  
		from ofertas_compra o 
		join cuenta c on o.cuenta=c.codigo
		and c.fk_inmueble is not null 
		order by o.fk_etapa desc, o.oferta desc"""
		query=preparaQuery(query)
		print query
	else:
		query="select cuenta, fk_etapa, oferta  from ofertas_compra  where fk_etapa={} and oferta={} ".format(etapa, oferta)	
	r = session.execute(query)

	for x in r:
		print x.cuenta
		cuenta=x.cuenta
		datos.etapa=x.fk_etapa
		datos.oferta=x.oferta

	assert cuenta, "no tiene cuenta"
	sql="""select c.nombre as nombre, i.iden2 as iden2, i.iden1 as iden1 
	from cliente c join cuenta x on c.codigo=x.fk_cliente 
	join inmueble i on x.fk_inmueble=i.codigo 
	where x.codigo={}""".format(cuenta)
	r=session.execute(psql(sql))
	nombre=""
	for x in r:
		nombre=x.nombre.decode("iso-8859-1")
		datos.iden2=x.iden2.decode("iso-8859-1")
		datos.iden1=x.iden1
	assert nombre, "no tiene nombre"

	print "cuenta", cuenta
	sql_apartado = """select isnull( sum(m.cantidad),0) as apartados from movimiento m join documento d on m.fk_documento = d.codigo 
	where d.fk_cuenta =  {}   and m.cargoabono = 'A' and m.relaciondepago = 'Apartado'
	and d.fk_tipo not in (15,16)""".format(cuenta)

	
	r=session.execute(psql(sql_apartado))
	for x in r:
		apartados=x.apartados
	print apartados
	#apartados=r.scalar()

	sql_abonos = """select isnull( sum(m.cantidad),0) as abonos from movimiento
	 m join documento d on m.fk_documento = d.codigo 
	where d.fk_cuenta =  {}   and m.cargoabono = 'A' 
	and m.relaciondepago <> 'Apartado'
	and d.fk_tipo not in (15,16)""".format(cuenta)
	r=session.execute(psql(sql_abonos))
	for x in r:
		abonos=x.abonos

	sql_prerecibos = """select isnull( sum(cantidad),0) as prerecibos from prerecibo where 
	fk_cuenta ={} and fk_recibo is null """.format(cuenta)
	r=session.execute(psql(sql_prerecibos))
	for x in r:
		prerecibos=x.prerecibos

	f1="${:,.2f}"
	f2="({})"
	v= f1.format(apartados)
	if apartados<0:
		v=f2.format(v)
	datos.apartados=v
	v= f1.format(abonos)
	if abonos<0:
		v=f2.format(v)
	datos.abonos=v
	v= f1.format(prerecibos)
	if prerecibos<0:
		v=f2.format(v)
	datos.prerecibos=v
	datos.nombre=nombre

	print "vale datos"
	print "valores", [(x,getattr(datos,x)) for x in datos.__dict__]
	return datos

def pdfCreate(datos, template="calar.html",tipo="oferta"):

	filename = "./zen/report/{}".format(template)
	mytemplate = Template(filename = filename, input_encoding="utf-8")
	buf = StringIO()
	
	try:
		ctx = Context(buf, datos=datos)
		mytemplate.render_context(ctx)
		html=buf.getvalue().encode("utf-8")
	except:
		print_exc()
		return ( False, "")

	llave = "X"
	if tipo=="oferta":
		llave="{:02d}_{:010d}".format(int(datos.etapa),int(datos.oferta))
	elif tipo=="caracteristicas":
		llave="{:02d}_{:010d}".format(int(datos.etapa),int(datos.oferta))
	elif tipo=="anexo":
		llave="{:02d}_{:010d}".format(int(datos.etapa),int(datos.oferta))	
	elif tipo=="rap":
		llave="{:010d}".format(int(datos.cliente))

	
	try:
		nombre=pdfFileName(tipo=tipo, llave=llave)
		with open(nombre, "wb") as f:	
			pdf=pisa.CreatePDF(StringIO(html), f)
		if not pdf.err:
			print "si se genero pdf "
			return (True, nombre )
		else:
			print "no se genero"
			return (False, "")
	except:
		print_exc()
		return ( False, "")
	return (False, "")


def record(row):
	return dict( id = row.id, extended = row.extended, author = row.author, title = row.title, intro = row.intro , published_at = row.publishedAt.isoformat() )

def errorJson(d, error = 400):
	return Response(json.dumps(d), content_type = "application/json", status_int = error ) 

def raiz( request ):
    app = request.params.get("app", "zeniclar")
    current = redis_conn.get("{}-current-deploy".format(app))
    version = request.params.get("version", "")
    if version:
        current = version
    html = redis_conn.get(current)
    return Response(html, content_type = "text/html", status_int = 200)

raiz2 = raiz

def dec_enc( what ):
	return what.decode("iso-8859-1").encode("utf-8")

def ayp( lista , valor ):
	lista.append( valor )
	print valor
	return

def get_token(request):
	token = ""
	valid = True
	try:
		stoken = request.headers.get("authorization", "")
		assert stoken, "header for authorization not present"
		token = stoken.split(" ")[-1]
		assert str(token) in cached_results.dicAuthToken, "token {} invalido".format( token )
	except:
		print_exc()
		valid = False
	return (valid, token )

class EAuth( object ):
	def auth( self, content = None, get_token = False ):
		"""
			content servira para preguntar si hay que extraer del json embebido alguna estructura
			con dicho nombre...
			Ej.  que, dic = self.auth( u"buroinfo" )
			if not que:
				#aqui se supone que el methodo auth preparo el error 401
				return dic

			Si get_token es puesto True quiere decir que en una tercer variable regresara el token
		"""
		token = ""
        
		try:
			#regresa tres palabras en el header Authorization separadas por espacio y la ultima es el token
			stoken = self.request.headers.get("authorization","")
			print "authorization contiene", stoken or " !basura " 
			
			token = stoken.split(" ")[-1]
			
		except:
			print_exc()

		ok = True 
		print "el token de autorizacion es ", token  
		error_message = "" 
		try:
			assert str(token) in cached_results.dicAuthToken, "token {} invalido".format( token )
		except AssertionError,e:
			print_exc()
			self.request.response.status = 401
			ok = False
			error_message = e.args[0]
		
		if not ok:
			if get_token:
				return  ( False, dict( error = error_message ), token )
			else:
				return  ( False, dict( error = error_message ) )
		if content:
			if get_token:
				return ( True, self.request.json_body.get(content), token )
			else:
				return ( True, self.request.json_body.get(content) )
		if get_token:
			return ( True, dict(), token)
		return ( True, dict ())




@resource( collection_path='api/prospectos', path='api/prospecto/{id}')
class ProspectoData( EAuth ):
	def __init__( self, request ):
		self.request = request
		self.modelo = "prospecto"

	def collection_get(self):
		que , record, token = self.auth(self.modelo , get_token = True)
		if not que:
			return record
		try:
			query = DBSession.query( Prospecto ).order_by( Prospecto.id )
			
			return dict( buroinfos = [x.cornice_json for x in query.all()] )
			
		except:
			traceback.print_exc()
			return dict()

	@view(renderer='json')
	def get(self):
		
		que , record, token = self.auth(self.modelo, get_token = True)
		if not que:
			return record

		try:
			record = DBSession.query( Prospecto ).get(int(self.request.matchdict['id']))
			return record.cornice_json
		except:
			pass
		return dict()
			
	def store( self, record , id = None):
		fmap = dict( apellidopaterno = "apellidopaterno1",
		apellidomaterno = "apellidomaterno1",
		nombre = "nombre1",
		fechadenacimiento = "fechadenacimiento",
		fechaasignacion = "fechaasignacion",
		telefonocasa = "telefonocasa",
		telefonooficina = "telefonooficina",
		extensionoficina = "extensionoficina",
		telefonocelular = "telefoncelular",
		lugardetrabajo = "lugardetrabajo",
		idmediopublicitario = "idmediopublicitario",
		mediopublicitariosugerido = "mediopublicitariosugerido",
		contado = "contado",
		hipotecaria = "hipotecaria",
		gerente = "idgerente",
		vendedor = "idvendedor",
		rfc = "rfc",
		curp = "curp",
		afiliacionimss = "afiliacionimss"
		)
		if not id:
			prospecto = Prospecto()
		else:
			try:
				prospecto = DBSession.query( Prospecto ).get( id )
				print "processing id ", id
			except:
				self.request.response.status = 400
				return dict( error = "problema al grabar")

		for x in record:
			try:
				field = fmap.get(str(x))
				value = record.get(x)
				
				if field == "cuenta":
				
					try:
						#print "cuenta vale ", value
						#assert DatoBuro.haycuenta( value ) is True, "la cuenta no existe"
						#if not id:
						#	assert DatoBuro.existeconcuenta( value ) is False, "ya hay registro con la misma cuenta"
						assert 1 == 1, "nada"
					except AssertionError, e:
						self.request.response.status = 400
						return  dict( error = e.args)
				try:
					print field, value
					if field in ("cuenta", "estado", "exportar"):
						pass
					else:
						value = str(value)
					setattr( prospecto, field, value )
				except AssertionError, e:
					self.request.response.status = 400
					return dict( error = e.args)
			except:
				print "algo no pronosticado ocurre en asignacion de campos"
				traceback.print_exc()
				
		#print "comenzare a grabar"
		#print "ciudad", dburo.ciudad
		try:
			assert 1 == 2, "escape momentaneo"
			nuevo_id = ""
			with transaction.manager:
				DBSession.add( prospecto )
				if DBSession.new:
					DBSession.flush()
					print "hice flush()"
				try:
					nuevo_id = prospecto.id
					print "el id es, ", nuevo_id
				except:
					pass
			try:
				
				print "el id es ", nuevo_id
				record["id"] = nuevo_id
				#if id:
					#incomprensible doble pasada de dato sin explicacion alguna
					#dato = DBSession.query( DatoBuro ).get(int(nuevo_id))
					#dato.ciudad = myciudad
					#with transaction.manager:
					#	DBSession.add( dato )
                    
				return dict( prospecto = record)
			except:
				return dict()
		except:
			traceback.print_exc()
			self.request.response.status = 400
			return dict( error = "problema al grabar")
		
	def deleterecord( self, id ):
		try:
			return dict() #escape momentaneo
			with transaction.manager:
				DBSession.delete( DBSession.query( Prospecto ).get( id  ) )
		except:
			traceback.print_exc()
			self.request.response.status = 400
			return dict( error = "problema al borrar")
		return dict()

	@view(renderer = 'json')
	def collection_post(self):
		print "inserting Prospecto"
		que, record, token = self.auth(self.modelo, get_token = True)
		if not que:
			return record
		return self.store( record )

	@view( renderer = 'json')
	def put(self):
		print "updating Prospecto"
		que , record = self.auth(self.modelo, get_token = True)
		if not que:
			return record
		id = int( self.request.matchdict['id'])
		return self.store( record = record, id = id )	

	@view( renderer = 'json')
	def delete(self):
		print "deleting Prospecto"
		que , record = self.auth(self.modelo , get_token = True)
		if not que:
			return record
		id = int( self.request.matchdict['id'])
		return self.deleterecord( id )

class QueryAndErrors( object ):
	def edata_error(self,mensaje):
		
		return dict(errors = dict( resultado = [ mensaje ]))

	def logqueries(self,queries):
		
		if self.hazqueries:
			hoy = datetime.now().isoformat()
			print(paint.blue("Grabando en rethinkdb los queries {}".format(hoy)))
			t_queries = rdb.db("iclar").table("zen_queries")
			t_queries.delete().run()

			for i,x in enumerate(queries,1):
				t_queries.insert( dict( fecha = hoy, query = x, tipo = x.split(" ")[0] , consecutivo = i )).run()


@resource( collection_path='api/hijos', path='api/hijoss/{id}')
class HijoRest( EAuth, QueryAndErrors ):
	def __init__(self, request):
		self.request = request
		self.modelo = "chilpayate"

	def get(self):
		que, record, token = self.auth(get_token = True)
		if not que:
			return record
		return dict()

	def collection_get(self):
		#print self.request.headers
		que, record = self.auth()
		if not que:
			return record
		self.request.response.status = 400
		return dict( error = "Pendiente de implementar")

	def collection_post(self):
		print "inserting Cliente"
		que, record, token = self.auth(self.modelo, get_token = True)
		user = cached_results.dicTokenUser.get( token )
		if not que:
			return record
		user = cached_results.dicTokenUser.get( token )
		usuario = user.get("usuario", "")
		perfil = user.get("perfil", "")
		if perfil not in ("admin", "comercial", "subdireccioncomercial"):
			
			self.request.response.status = 400
			error = "perfil no autorizado"
			return self.edata_error( error)
		if perfil == "admin":
			self.hazqueries = True
		else:
			self.hazqueries = False
		return self.store( record )

	def store( self, record , id = None):
		print "Voy a generar el hijo"
		queries = []
		
		error = "Pendiente de implementar"
		print record
		try:
			engine = Base.metadata.bind
			c = engine.connect().connection
			cu = c.cursor()
			if not record["meses"] :
				record["meses"] = 0
			for x in "cliente meses anos sexo".split(" "):
				assert record.get(x, "") <> "", "{} esta vacio".format(x)
				if x == "sexo":
					assert record.get("sexo", "" ) in ("M", "F"), "sexo debe ser M o F"
				elif x == "meses":
					assert int(record.get(x)) >= 0, "meses esta mal"
				else:
					assert int(record.get(x)) > 0, "{} debe ser entero".format(x)
			sql = "insert into hijos(fk_cliente, anios, meses, fecha) values ({},{},{}, getdate())".format(record.get("cliente"), record.get("anos"), record.get("meses"))
			cu.execute(sql)
			c.commit()
		except AssertionError, e:
			print_exc()
			error = e.args[0]
			self.request.response.status = 400
			return dict(error = error )
		except:
			print_exc()
			error = l_traceback()
			self.request.response.status = 400
			return dict(error = error )
		
		record["id"] = str(uuid.uuid4())

		return dict(hijo = record )
		#self.request.response.status = 400
		#return dict(errors = dict( resultado = [error]))

@resource( collection_path='api/clientes', path='api/clientes/{id}')
class ClienteRest( EAuth, QueryAndErrors ):
	def __init__(self, request):
		self.request = request
		self.modelo = "cliente"

	def get(self):
		que, record, token = self.auth(get_token = True)
		if not que:
			return record
		return dict()

	def collection_get(self):
		#print self.request.headers
		que, record = self.auth()
		if not que:
			return record
		self.request.response.status = 400
		return dict( error = "Pendiente de implementar")

	def collection_post(self):
		print "inserting Cliente"
		que, record, token = self.auth(self.modelo, get_token = True)
		user = cached_results.dicTokenUser.get( token )
		if not que:
			return record
		user = cached_results.dicTokenUser.get( token )
		usuario = user.get("usuario", "")
		perfil = user.get("perfil", "")
		if perfil not in ("admin", "comercial", "subdireccioncomercial"):
			
			self.request.response.status = 400
			error = "perfil no autorizado"
			return self.edata_error( error)
		if perfil == "admin":
			self.hazqueries = True
		else:
			self.hazqueries = False
		return self.store( record )

	def store( self, record , id = None):
		print "Voy a generar el cliente"
		queries = []
		error = "Pendiente de implementar"
		ses = DBSession

		#por lo pronto regresar el numero de cliente mayor.
		print record
		cliente = 1
		for x in ses.execute("select max(codigo) as cliente from cliente"):
			cliente = x.cliente
		record["id"] = cliente
		print "el cliente es ", cliente
		return dict( cliente = record )
		#self.request.response.status = 400
		#return dict(errors = dict( resultado = [error]))

@resource( collection_path='api/ofertas', path='api/ofertas/{id}')
class OfertaDeCompra( EAuth, QueryAndErrors ):
	def __init__(self, request):
		self.request = request
		self.modelo = "oferta"

	def get(self):
		que, record, token = self.auth(get_token = True)
		if not que:
			return record
		return dict()

	def collection_get(self):
		#print self.request.headers
		que, record = self.auth()
		if not que:
			return record
		self.request.response.status = 400
		return dict( error = "Pendiente de implementar")

	def store( self, record , id = None):
		print "Voy a generar la Oferta"
		
		
		
		queries = []
		error = "Pendiente de implementar"
		
		#self.request.response.status = 400
		#return dict(errors = dict( resultado = ["esta tronando como ejote"]))

		ses = DBSession
		i = record.get("inmueble", 0)
		try:
			localerror = ""
			assert i, "inmueble vacio"
		except AssertionError, e:
			print_exc()
			localerror = e.args[0]
		if localerror:
			error = localerror
			
			self.request.response.status = 400
			return self.edata_error( error)

		sql = """
		select i.codigo as inmueble, e.codigo as etapa, d.codigo as desarrollo 
		from inmueble i join etapa e 
		on i.fk_etapa = e.codigo
		join desarrollo d on e.fk_desarrollo = d.codigo
		where i.codigo = {}
		""".format( i )
		queries.append(sql)
		for x in ses.execute(sql):
			inmueble = x.inmueble
			etapa = x.etapa
			desarrollo = x.desarrollo
			found = True
		if found:
			print "Inmueble {}, etapa {}, desarrollo {}".format(inmueble, etapa,desarrollo)
		else:
			error = "no hay inmueble {}".format(i)
			self.logqueries(queries)
			self.request.response.status = 400
			return self.edata_error( error)
			
		precio_id = record.get("precio",0)
		try:
			localerror = ""
			assert precio_id, "no tiene precio"
		except AssertionError, e:
			print_exc()
			localerror = e.args[0]
		if localerror:
			error = localerror
			self.logqueries(queries)
			self.request.response.status = 400
			return self.edata_error( error)

		found = False
		sql = """select precio from gixpreciosetapa
		 where  id = {} and fk_etapa = {}""".format(precio_id, etapa)
		sql = sql.replace("\n"," ")
		sql = sql.replace("\t", " ")
		queries.append(sql)
		precio = 0
		for x in ses.execute(sql):
			precio = x.precio
			found = True
		if not found:
			error = "no hay precio en base de datos"
			self.logqueries(queries)
			self.request.response.status = 400
			return self.edata_error( error)
		
		print(paint.blue("el precio es {}".format(precio)))

		prospecto = record.get("prospecto", 0)
		try:
			localerror = ""
			assert prospecto, "prospecto vacio"
		except AssertionError, e:
			print_exc()
			localerror = e.args[0]
		if localerror:
			error = localerror
			self.logqueries(queries)
			self.request.response.status = 400
			return self.edata_error( error)

		precalificacion = record.get("precalificacion", 0)
		localerror = ""
		if not precalificacion:
			precalificacion = 0
		try:
			precalificacion = float(precalificacion)
		except:
			localerror = "la precalificacion debe ser numerica"
		if localerror:
			error = localerror
			self.logqueries(queries)
			self.request.response.status = 400
			return self.edata_error( error)

		avaluo = record.get("avaluo", 0)
		localerror = ""
		if not avaluo:
			avaluo = 0
		try:
			avaluo = float(avaluo)
		except:
			localerror = "el avaluo debe ser numerica"
		if localerror:
			error = localerror
			self.logqueries(queries)
			self.request.response.status = 400
			return self.edata_error( error)

		subsidio = record.get("subsidio", 0)
		localerror = ""
		if not subsidio:
			subsidio = 0
		try:
			subsidio = float(subsidio)
		except:
			localerror = "el avaluo debe ser numerica"
		if localerror:
			error = localerror
			self.logqueries(queries)
			self.request.response.status = 400
			return self.edata_error( error)

		pagare = record.get("pagare", 0)
		localerror = ""
		if not pagare:
			pagare = 0
		try:
			pagare = float(pagare)
		except:
			localerror = "el pagare debe ser numerica"
		if localerror:
			error = localerror
			self.logqueries(queries)
			self.request.response.status = 400
			return self.edata_error( error)

		prerecibo = record.get("prerecibo", 0)
		localerror = ""
		if not prerecibo:
			prerecibo = 0
		try:
			prerecibo = float(prerecibo)
		except:
			localerror = "el monto de prerecibo debe ser numerico"
		if localerror:
			error = localerror
			self.logqueries(queries)
			self.request.response.status = 400
			return self.edata_error( error)

		prereciboadicional = record.get("prereciboadicional", 0)
		localerror = ""
		if not prereciboadicional:
			prereciboadicional = 0
		try:
			prereciboadicional = float(prereciboadicional)
		except:
			localerror = "el monto de prerecibo adicional debe ser numerico"
		if localerror:
			error = localerror
			self.logqueries(queries)
			self.request.response.status = 400
			return self.edata_error( error)

		suma = precalificacion + avaluo + subsidio + pagare + prerecibo + prereciboadicional
		localerror = ""
		if suma <> precio:
			localerror = "precios es distinto a suma de precalificacion, avaluo, subsidio, pagare y prerecibos"

		if localerror:
			error = localerror
			self.logqueries(queries)
			self.request.response.status = 400
			return self.edata_error( error)
		

		sql = """
		select idvendedor as vendedor, idgerente as gerente, afiliacionimss, fechacierre, congelado, cuenta from gixprospectos where idprospecto = {}
		""".format(prospecto) 
		queries.append(sql)
		found = False
		p_afiliacionimss = ''
		for x in ses.execute(sql):
			p_afiliacionimss = x.afiliacionimss or ""
			p_fechacierre = x.fechacierre
			p_congelado = x.congelado
			p_cuenta = x.cuenta
			p_vendedor = x.vendedor
			p_gerente = x.gerente
			found = True
		localerror = ""
		if found:
			if p_congelado:
				print "esta congelado"
				localerror = "prospecto congelado"
			if p_fechacierre:
				print "prospecto ya tuvo fecha de cierre"
				localerror = "prospecto con fecha de cierre"
			if p_cuenta:
				print "prospecto con cuenta asignada"
				localerror ="prospecto con cuenta asignada"
		else:
			print "no existe el prospecto"
			localerror = "no existe el prospecto"

		if localerror:
			error = localerror
			self.logqueries(queries)
			self.request.response.status = 400
			return self.edata_error( error)
		localerror = ""

		found = False
		sql = "select vendedor as empresavendedora,es_subvendedor, vendedorvirtual, desactivado from vendedor where codigo = {}".format(p_vendedor)
		queries.append(sql)
		for x in ses.execute(sql):
			es_subvendedor = x.es_subvendedor
			vendedorvirtual = x.vendedorvirtual
			desactivado = x.desactivado
			empresavendedora = x.empresavendedora
			found = True
		if not found:
			localerror = "no existe el vendedor"

		else:
			if not es_subvendedor:
				localerror = "No es subvendedor"
			elif vendedorvirtual:
				localerror = "es vendedor virtual y no se puede"
			elif desactivado:
				localerror = "el vendedor esta desactivado"

		if localerror:
			error = localerror
			self.logqueries(queries)
			self.request.response.status = 400
			return self.edata_error( error)
		localerror = ""
		print(paint.blue("vendedor {}, es_subvendedor {}, vendedorvirtual {}, desactivado {}".format(p_vendedor,es_subvendedor,vendedorvirtual, desactivado)))
		
		sql = "select fk_vendedor as vendedor, porcentaje from porcentaje_comision where fk_vendedor in ({}, {}) and fk_desarrollo = {}".format(empresavendedora, p_vendedor, desarrollo)
		queries.append(sql)
		found = False
		conteo = 0
		for x in ses.execute( sql ):
			print(paint.blue("comision: vendedor {}, porcentaje {} ".format(x.vendedor, x.porcentaje)))
			conteo = conteo + 1
		if conteo < 2:
			localerror = "falta porcentaje de comision de subvendedor o vendedor"
		if localerror:
			error = localerror
			self.logqueries(queries)
			self.request.response.status = 400
			return self.edata_error( error)
		
		cliente = record.get("cliente", 0)
		
		try:
			localerror = ""
			assert cliente, "cliente vacio"
		except AssertionError, e:
			print_exc()
			localerror = e.args[0]
		if localerror:
			error = localerror
			self.logqueries(queries)
			self.request.response.status = 400
			return self.edata_error( error)
		
		found = False
		sql = """
		select imss from cliente where codigo = {}
		""".format(cliente)
		queries.append(sql)
		imss = ""
		localerror = ""
		for x in ses.execute(sql):
			imss = x.imss or ""
			found = True
		if found:
			if not p_afiliacionimss and not imss:
				localerror = "afiliaciones de prospecto y cliente vacias"
				#print error
			else:
				#if p_afiliacionimss.strip() == imss.strip():
				if True: #quita esta linea y pon la de arriba luego!!
					pass
				else:
					localerror = "no coinciden las afiliaciones de imss de prospecto y vendedor"
					#print error
		else:
			localerror = "no existe el cliente"
			#print error
		if localerror:
			error = localerror
			return self.edata_error( error)
		
		

		apartado = record.get("apartado",0)
		if apartado == "":
			apartado = 0
		localerror = ""
		try:
			apartado = float(apartado)
		except:
			localerror = "apartado no es numerico"
		if localerror:
			error = localerror
			self.logqueries(queries)
			self.request.response.status = 400
			return self.edata_error( error)
		
		precio = float(precio)
		resto = precio - apartado

		try:
			localerror = ""
			assert apartado , "apartado vacio"
		except AssertionError, e:
			print_exc()
			localerror = e.args[0]
		if localerror:
			error = localerror
			self.logqueries(queries)
			self.request.response.status = 400
			return self.edata_error( error)

		print(paint.blue("apartado = {}, resto = {}".format(str(apartado),str(resto))))


		sql = "select contrato + 1 as siguientecontrato from desarrollo where codigo = {}".format(desarrollo)
		queries.append(sql)
		for x in ses.execute(sql):
			contrato = x.siguientecontrato
		sql = "update desarrollo set contrato = {} where codigo = {}".format(contrato,desarrollo)
		queries.append(sql)
		print(paint.blue(sql))
		#ses.execute(sql)
		print(paint.blue("contrato es {}".format(contrato)))

		sql = "select max(codigo) + 1 as siguientecuenta from cuenta"
		queries.append(sql)
		for x in ses.execute(sql):
			cuenta = x.siguientecuenta
		hoy = datetime.now()
		fechadeventa = record.get("fechadeventa", "")
		if not fechadeventa:
			fechadeventa = "{:04d}/{:02d}/{:02d}".format(hoy.year, hoy.month, hoy.day)
		sql = """
					insert into cuenta
					(codigo, fecha, saldo, fk_cliente, fk_inmueble, fk_tipo_cuenta, contrato, tipo_contrato, fk_etapa)
					values ({}, '{}', {}, {}, 0, 2, {}, '3', {})
		""".format(cuenta, fechadeventa, precio, cliente, contrato, etapa)
		sql = sql.replace("\n"," ")
		sql = sql.replace("\t", " ")
		queries.append(sql)
		print(paint.blue(sql))

		sql = """
		select precio, sustentable, id from gixpreciosetapa where fk_etapa = {} and activo = 1 and precio = {}
		""".format(etapa,precio)
		queries.append(sql)
		found = False
		for x in ses.execute(sql):
			sustentable = x.sustentable
			sustentable1 = 0
			gixprecioetapaid = x.id
			if sustentable:
				sustentable1 = -1
			found = True
		localerror = ""
		if not found:
			localerror = "no hay precio en etapa , activo"
		if localerror:
			error = localerror
			self.logqueries(queries)
			self.request.response.status = 400
			return self.edata_error( error)

		referenciarap = record.get("referencia",0)
		localerror = ""
		if not referenciarap:
			localerror = "falta referenciarap"
		if localerror:
			error = localerror
			self.logqueries(queries)
			self.request.response.status = 400
			return self.edata_error( error)
		sql = """
		select referencia from referencias_rap where cuenta = 0 and cliente = {}
		""".format(cliente)
		queries.append(sql)
		found = False
		localerror = ""
		for x in ses.execute(sql):
			referencia = x.referencia
			found = True
		if not found:
			localerror = "no existe la referencia rap asociada al cliente"

		if localerror:
			error = localerror
			self.logqueries(queries)
			self.request.response.status = 400
			return self.edata_error( error)

		localerror = ""
		tipocuenta = record.get("tipocuenta", "")
		if not tipocuenta:
			localerror = "no esta el tipo de cuenta"
		if localerror:
			error = localerror
			self.logqueries(queries)
			self.request.response.status = 400
			return self.edata_error( error)
		 
		montocredito = record.get("montocredito",0)
		localerror = ""
		if not montocredito and tipocuenta == "infonavit":
			localerror = "si es tipo cuenta infonavit debe tener monto de credito"
		if localerror:
			error = localerror
			self.logqueries(queries)
			self.request.response.status = 400
			return self.edata_error( error)

		anticipocomision = record.get("anticipocomision",0)
		localerror = ""
		try:
			anticipocomision = float(anticipocomision)
		except:
			localerror = "anticipo comision no es numerico"
		if localerror:
			error = localerror
			self.logqueries(queries)
			self.request.response.status = 400
			return self.edata_error( error)

		localerror = ""
		seguro = record.get("seguro",0)
		sql = """
		insert into ofertas_compra
							(fk_etapa, oferta, cliente, vendedor, subvendedor, fecha_oferta, gastos_admin, apartado,
							monto_credito, asignada, referencia_rap, precio, anticipo_comision, cuenta, cancelada,
							precio_seguro, habilitada, preciosustentable)
							values ({}, {}, {}, {}, {}, '{}', 0, {}, {}, 0, '{}', {}, {}, {}, 0, {}, -1, {}) 
		""".format(etapa, contrato, cliente, empresavendedora, p_vendedor, fechadeventa, apartado,
							       montocredito, referenciarap, precio, anticipocomision, cuenta, seguro,
							       sustentable1)
		sql = sql.replace("\n"," ")
		sql = sql.replace("\t", " ")
		queries.append(sql)
		print(paint.blue(sql))
		found = False
		sql = "update referencias_rap set cuenta = {} where referencia = '{}'".format(cuenta, referenciarap)
		queries.append(sql)
		print(paint.blue(sql))
		found = False
		sql = "select max(codigo) + 1 as codigocomision from comision"
		queries.append(sql)
		codigocomision = 0
		for x in ses.execute(sql):
			codigocomision = x.codigocomision
			found = True
		if not found:
			localerror = "no se puede obtener consecutivo siguiente de codigo comision"

		if localerror:
			error = localerror
			self.logqueries(queries)
			self.request.response.status = 400
			return self.edata_error( error)
		sql = """
		insert comision (codigo, aplicar, cantidad, saldo_cantidad, iva, saldo_iva,
							        total, saldo_total, fk_inmueble, fk_vendedor, fk_cuenta, cancelada,
							        cuenta_anterior, cuenta_original)
							        values ({}, 'N', {}, {}, {}, {}, {}, {}, {}, {}, {}, 'N', 0, 0)
		""".format(codigocomision, anticipocomision, anticipocomision,
							               anticipocomision * 0.16, anticipocomision * 0.16,
							               anticipocomision * 1.16, anticipocomision * 1.16,
							               0, empresavendedora, cuenta)


		sql = sql.replace("\n"," ")
		sql = sql.replace("\t", " ")
		queries.append(sql)
		print(paint.blue(sql))

		found = False
		sql = "select max(codigo) + 1 as codigocomision from comision"
		queries.append(sql)
		codigocomision = 0
		for x in ses.execute(sql):
			codigocomision = x.codigocomision
			found = True
		if not found:
			localerror = "no se puede obtener consecutivo siguiente de codigo comision al subvendedor"

		if localerror:
			error = localerror
			self.logqueries(queries)
			self.request.response.status = 400
			return self.edata_error( error)
		sql = """
		insert comision (codigo, aplicar, cantidad, saldo_cantidad, iva, saldo_iva,
							        total, saldo_total, fk_inmueble, fk_vendedor, fk_cuenta, cancelada,
							        cuenta_anterior, cuenta_original)
							        values ({}, 'N', {}, {}, {}, {}, {}, {}, {}, {}, {}, 'N', 0, 0)
		""".format(codigocomision, anticipocomision, anticipocomision,
							               anticipocomision * 0.16, anticipocomision * 0.16,
							               anticipocomision * 1.16, anticipocomision * 1.16,
							               0, p_vendedor, cuenta)


		sql = sql.replace("\n"," ")
		sql = sql.replace("\t", " ")
		queries.append(sql)
		print(paint.blue(sql))	

		sql = """
		insert into gixpreciosetapaofertaasignacion
								(fk_etapa, oferta, fecha_oferta, fk_preciosetapaoferta,
								preciooferta, cuenta)
								values ({}, {}, '{}', {}, {}, {})
								""".format(etapa, contrato, fechadeventa, gixprecioetapaid,
								       precio, cuenta)
		sql = sql.replace("\n"," ")
		sql = sql.replace("\t", " ")
		queries.append(sql)
		print(paint.blue(sql))
		if apartado:
			queries.append("delta apartado true")
			found = False
			sql = "select max(codigo) + 1 as codigodocumento from documento"
			queries.append(sql)
			codigodocumento = 0
			for x in ses.execute(sql):
				codigodocumento = x.codigodocumento
				found = True
			if not found:
				localerror = "no se puede obtener consecutivo siguiente de codigo de documento"

			if localerror:
				error = localerror
				self.logqueries(queries)
				self.request.response.status = 400
				return self.edata_error( error)
			sql = """
			insert into documento (codigo, fechadeelaboracion, fechadevencimiento, fechadevencimientovar,
						saldo, cargo, abono, fk_tipo, fk_cuenta, referencia)
						values ({}, '{}', '{}', '{}', {}, {}, 0, 7, {}, '{}')
						""".format(codigodocumento, fechadeventa, fechadeventa, fechadeventa, apartado, apartado,
						       cuenta, referenciarap)
			sql = sql.replace("\n"," ")
			sql = sql.replace("\t", " ")
			queries.append(sql)
			print(paint.blue(sql))

			found = False
			sql = "select max(codigo) + 1 as codigomovimiento from movimiento"
			queries.append(sql)
			codigomovimiento = 0
			for x in ses.execute(sql):
				codigomovimiento = x.codigomovimiento
				found = True
			if not found:
				localerror = "no se puede obtener consecutivo siguiente de codigo de movimiento"
			if localerror:
				error = localerror
				self.logqueries(queries)
				self.request.response.status = 400
				return self.edata_error( error)
			sql = """
			insert into movimiento (codigo, cantidad, fecha, relaciondepago, cargoabono,
							fk_tipo, fk_documento)
							values ({}, {}, '{}', '1/1', 'C', 7, {})
							""".format(codigomovimiento, apartado, fechadeventa, codigodocumento)
			sql = sql.replace("\n"," ")
			sql = sql.replace("\t", " ")
			queries.append(sql)
			queries.append("delta apartado true end")
			print(paint.blue(sql))
		else:
			queries.append("delta apartado false")
			queries.append("delta apartado false end")

		if resto:
			queries.append("delta resto true")
			found = False
			sql = "select max(codigo) + 1 as codigodocumento from documento"
			queries.append(sql)
			codigodocumento = 0
			for x in ses.execute(sql):
				codigodocumento = x.codigodocumento
				found = True
			if not found:
				localerror = "no se puede obtener consecutivo siguiente de codigo de documento"

			if localerror:
				error = localerror
				self.logqueries(queries)
				self.request.response.status = 400
				return self.edata_error( error)
			sql = """
			insert into documento (codigo, fechadeelaboracion, fechadevencimiento, fechadevencimientovar,
						saldo, cargo, abono, fk_tipo, fk_cuenta, referencia)
						values ({}, '{}', '{}', '{}', {}, {}, 0, 2, {}, '{}')
						""".format(codigodocumento, fechadeventa, fechadeventa, fechadeventa, resto, resto,
						       cuenta, referenciarap)
			sql = sql.replace("\n"," ")
			sql = sql.replace("\t", " ")
			queries.append(sql)
			print(paint.blue(sql))

			found = False
			sql = "select max(codigo) + 1 as codigomovimiento from movimiento"
			queries.append(sql)
			codigomovimiento = 0
			for x in ses.execute(sql):
				codigomovimiento = x.codigomovimiento
				found = True
			if not found:
				localerror = "no se puede obtener consecutivo siguiente de codigo de movimiento"
			if localerror:
				error = localerror
				self.logqueries(queries)
				self.request.response.status = 400
				return self.edata_error( error)
			sql = """
			insert into movimiento (codigo, cantidad, fecha, relaciondepago, cargoabono,
							fk_tipo, fk_documento)
							values ({}, {}, '{}', '1/1', 'C', 2, {})
							""".format(codigomovimiento, resto, fechadeventa, codigodocumento)
			sql = sql.replace("\n"," ")
			sql = sql.replace("\t", " ")
			queries.append(sql)
			print(paint.blue(sql))
			queries.append("delta resto true end")
		else:
			queries.append("delta resto false")
			queries.append("delta resto false end")

		sql = """
					update gixprospectos set cuenta = {}, fechacierre = convert(varchar(10), getdate(), 111)
					where idprospecto = {}
				""".format(cuenta, prospecto)
		sql = sql.replace("\n"," ")
		sql = sql.replace("\t", " ")
		queries.append(sql)
		print(paint.blue(sql))
		#inicia asignacion de inmueble
		print(paint.yellow("inicia asignacion de inmueble"))

			
		sql = """
		update cuenta set fk_inmueble = {}, bruto_precalificacion = {}, avaluoimpuesto_precalificacion = {}, subsidio_precalificacion = {}, pagare_precalificacion = {} where codigo = {}
		""".format(inmueble, precalificacion, avaluo, subsidio, pagare, cuenta)

		sql = sql.replace("\n"," ")
		sql = sql.replace("\t", " ")
		queries.append(sql)
		print(paint.blue(sql))

		sql = """
			update anticipocomision set fk_inmueble = {} where fk_cuenta = {}
		""".format(inmueble, cuenta)
		sql = sql.replace("\n"," ")
		sql = sql.replace("\t", " ")
		queries.append(sql)
		print(paint.blue(sql))

		sql = """
			update comision set fk_inmueble = {} where fk_cuenta = {}
		""".format( inmueble, cuenta)
		sql = sql.replace("\n"," ")
		sql = sql.replace("\t", " ")
		queries.append(sql)
		print(paint.blue(sql))

		sql = """
			update inmueble set fechadeventa = '{}' , precio = {} where codigo = {}
		""".format(fechadeventa, precio, inmueble)

		sql = sql.replace("\n"," ")
		sql = sql.replace("\t", " ")
		queries.append(sql)
		print(paint.blue(sql))

		sql = """
		update ofertas_compra set fecha_asignacion = '{}', asignada = -1, monto_precalificacion = {} where oferta = {} and cuenta = {}
		""".format(fechadeventa,precalificacion, contrato, cuenta)

		sql = sql.replace("\n"," ")
		sql = sql.replace("\t", " ")
		queries.append(sql)
		print(paint.blue(sql))

		

		if prerecibo:
			queries.append("delta prerecibo true")
			sql = """
			insert into prerecibo ( fk_cuenta, fecha, cantidad, referencia) values ( {},'{}',{},'{}')"
			""".format(cuenta,fechadeventa,prerecibo,referenciarap)
			sql = sql.replace("\n"," ")
			sql = sql.replace("\t", " ")
			queries.append(sql)
			print(paint.blue(sql))
			queries.append("delta prerecibo true end")
		else:
			queries.append("delta prerecibo false")
			queries.append("delta prerecibo false end")

		if prereciboadicional:
			queries.append("delta prereciboadicional true")
			sql = """
			insert into prerecibo ( fk_cuenta, fecha, cantidad, referencia) values ( {},'{}',{},'{}')"
			""".format(cuenta,fechadeventa,prereciboadicional,referenciarap)
			sql = sql.replace("\n"," ")
			sql = sql.replace("\t", " ")
			queries.append(sql)
			print(paint.blue(sql))
			queries.append("delta prereciboadicional true end")

		else:
			queries.append("delta prereciboadicional false")
			queries.append("delta prereciboadicional false end")
		
		self.logqueries(queries)
		self.request.response.status = 400
		error = "truena pero llega al final"
		return self.edata_error( error)
		
	#@view(renderer = 'json')
	def collection_post(self):
		print "inserting Oferta"
		que, record, token = self.auth(self.modelo, get_token = True)
		user = cached_results.dicTokenUser.get( token )
		if not que:
			return record
		user = cached_results.dicTokenUser.get( token )
		usuario = user.get("usuario", "")
		perfil = user.get("perfil", "")
		if perfil not in ("admin", "comercial", "subdireccioncomercial"):
			
			self.request.response.status = 400
			error = "perfil no autorizado"
			return self.edata_error( error)
		if perfil == "admin":
			self.hazqueries = True
		else:
			self.hazqueries = False
		return self.store( record )




@resource( collection_path='api/zenusuarios', path='api/zenusuarios/{id}')
class UsuarioZen( EAuth ):
	def __init__(self, request):
		self.request = request

	def get(self):
		
		que, record, token = self.auth(get_token = True)
		if not que:
			return record
		user = cached_results.dicTokenUser.get( token )
		usuario = user.get("usuario", "")
		perfil = user.get("perfil", "")
		return dict( zenusuario = dict( id = "1", usuario = usuario, perfil = perfil ))

@resource( collection_path='api/zenversions', path='api/zenversions/{id}')
class Zenversion( EAuth ):
    
    def __init__(self, request):
		self.request = request
    
    def collection_get(self):
    	print self.request.headers
    	que, record = self.auth()
    	if not que:
    		return record
        return dict()

    def get(self):
		que, record = self.auth()
		print "voy en get de Zenversion class"
		if not que:
			return record
		print "voy a regresar valor en Zenversion class"
		return dict( zenversion = dict( id = "1", version = "2014052801" ))

@resource( collection_path='api/prospectosbusquedas', path='api/prospectosbusquedas/{id}')
class ProspectosBusquedas(EAuth):
	def __init__(self, request):
		self.request = request

	def collection_get(self):
		if True:
			que, record, token = self.auth(get_token = True)
			if not que:
				return record
		print "voy en ProspectosBusquedas.collection_get()"
		user = cached_results.dicTokenUser.get( token )
		_vendedor = user.get("vendedor", 0)
		_gerente = user.get("gerente", 0)
		clase = Prospecto
		query = DBSession.query( clase ).filter( clase.congelado == False)
		req = self.request.params
		ROWS_PER_PAGE = 20
		page = 1
		try:
			page = int(req.get(u"page",1))
		except:
			pass
		gerente = 0
		#print self.request.params
		try:
			gerente = int(req.get(u"gerente",0))
		except:
			pass
		vendedor = 0
		try:
			vendedor = int(req.get(u"vendedor",0))
		except:
			pass
		if _vendedor:
			vendedor = _vendedor
		if _gerente:
			gerente = _gerente

		mediopublicitario = int(req.get(u"mediopublicitario",0))
		tipofecha = req.get(u"tipofecha","")
		fechainicial = req.get(u"fechainicial","")
		fechafinal = req.get(u"fechafinal","")
		tipocuenta = req.get(u"tipocuenta","")
		numeroprospecto = int(req.get(u"numeroprospecto",0))
		nombreprospecto = req.get(u"nombreprospecto","")
		afiliacion = req.get(u"afiliacion","")
		s_cierre = req.get(u"sincierre","")
		sincierre = False
		if s_cierre == "1":
			sincierre = True
		
		if vendedor > 0 :
			query = query.filter( clase.idvendedor == vendedor )
		if vendedor == 0 and gerente > 0:
			query = query.filter( clase.idgerente == gerente )
		#if prospecto > 0:
		#	query = query.filter( clase.id == prospecto )
		if mediopublicitario > 0:
			query = query.filter( clase.idmediopublicitario == mediopublicitario)
		if tipocuenta:
			if tipocuenta == "infonavit":
				query = query.filter(clase.contado == False).filter(clase.hipotecaria == False)
			elif tipocuenta == "contado":
				query = query.filter(clase.contado == True)
			elif tipocuenta == "hipotecaria":
				query = query.filter(clase.hipotecaria == True)
		#print "tipofecha", tipofecha		
		if numeroprospecto:
			query = query.filter( clase.idprospecto == numeroprospecto)
		if nombreprospecto:
			np = nombreprospecto
			query = query.filter( clase.apellidopaterno1.like("%{}%".format(np) ) )
		if afiliacion:
			query = query.filter( clase.afiliacionimss.like("%{}%".format(afiliacion)) )
		if tipofecha:
			if tipofecha == "alta":
				fecha = clase.fechaasignacion
			else:
				fecha = clase.fechacierre
			print "fechainicial", fechainicial, "fechafinal", fechafinal
			nohagas_nada = False
			if fechainicial <> "" and fechafinal <> "":
				fini = self.get_date(fechainicial)		
				ffin = self.get_date(fechafinal, minimal_date = False)
			elif fechainicial == "" and fechafinal <> "":
				fini = self.get_date("")
				ffin = self.get_date(fechafinal, minimal_date = False)
			elif fechainicial <> "" and fechafinal == "":
				fini = self.get_date(fechainicial)		
				ffin = self.get_date("", minimal_date = False)
			elif fechainicial == "" and fechafinal == "":
				fini = self.get_date("")
				ffin = self.get_date("", minimal_date = False)
			else:
				nohagas_nada = True
			if nohagas_nada is False:
				query = query.filter( fecha >= fini).filter( fecha <= ffin )
				if tipofecha == "alta" and sincierre:
					query = query.filter( clase.fechacierre == None )
		query = query.order_by(clase.idprospecto.desc())
		rows = query.count()
			
		pages = rows / ROWS_PER_PAGE 
		more = rows % ROWS_PER_PAGE
		if more:
			pages += 1
		if page > pages:
			page = pages
		left_slice = ( page - 1 ) * ROWS_PER_PAGE 
		right_slice  = left_slice + ROWS_PER_PAGE
		if right_slice > rows:
			right_slice = rows

		#return dict( prospectosbusquedas = [ x.busq_cornice_json for x in query.all() ])
		return { 'meta': { 'page': page, 'pages': pages, 'rowcount': rows, 'rowcountformatted': "{:,}".format(rows) },
		 'prospectosbusquedas': self.include_rn(( x.busq_cornice_json for x in query[left_slice: right_slice] ),left_slice)}

	def include_rn(self, rows, first):
		l = []
		c = first + 1
		for row in rows:
			row.update(num=c, numf="{:,}".format(c))
			c += 1
			l.append(row)
		return l

	def get(self):
		if True:
			que, record, token = self.auth(get_token = True)
			if not que:
				return record
		user = cached_results.dicTokenUser.get( token )
		_vendedor = user.get("vendedor", 0)
		_gerente = user.get("gerente", 0)
		id = int(self.request.matchdict['id'])  
		
		clase = Prospecto
		ses = DBSession
		qo = ses.query( clase ).filter( clase.id == id )
		q = qo.filter( clase.gerente == _gerente)
		q = q.filter( clase.vendedor == _vendedor)
		try:
			assert q.count() == 1, "Incorrecto, debe regresar un registro"
		except AssertionError, e:
			print_exc()
			return dict()
		r = qo.one()
		return r.busqueda_cornice_json

	def get_date(self, dateValue, minimal_date = True):
		if minimal_date:
			fecha = datetime( day = 1, month = 1, year = 1999)
		else:
			fecha = datetime( day = 31, month = 12, year = 2100)
		try:
			d, m, y = [ int(x) for x in dateValue.split("/")]
			fecha = datetime(day = d, month = m, year = y )
		except:
			pass
		return fecha

@resource( collection_path='api/cuantosprospectos', path='api/cuantosprospectos/{id}')
class CuantosProspectos(EAuth):
	def __init__(self, request):
		self.request = request

	def collection_get(self):
		if True:
			que, record, token = self.auth(get_token = True)
			if not que:
				return record
		user = cached_results.dicTokenUser.get( token )
		_vendedor = user.get("vendedor", 0)
		_gerente = user.get("gerente", 0)
		clase = Prospecto
		query = DBSession.query( clase.idprospecto ).filter( clase.congelado == False)
		req = self.request.params
		gerente = 0
		#print self.request.params
		try:
			gerente = int(req.get(u"gerente",0))
		except:
			pass
		vendedor = 0
		try:
			vendedor = int(req.get(u"vendedor",0))
		except:
			pass
		if _vendedor:
			vendedor = _vendedor
		if _gerente:
			gerente = _gerente

		mediopublicitario = int(req.get(u"mediopublicitario",0))
		tipofecha = req.get(u"tipofecha","")
		fechainicial = req.get(u"fechainicial","")
		fechafinal = req.get(u"fechafinal","")
		tipocuenta = req.get(u"tipocuenta","")
		s_cierre = req.get(u"sincierre","")
		sincierre = False
		if s_cierre == "1":
			sincierre = True
		
		if vendedor > 0 :
			query = query.filter( clase.idvendedor == vendedor )
		if vendedor == 0 and gerente > 0:
			query = query.filter( clase.idgerente == gerente )
		if mediopublicitario > 0:
			query = query.filter( clase.idmediopublicitario == mediopublicitario)
		if tipocuenta:
			if tipocuenta == "infonavit":
				query = query.filter(clase.contado == False).filter(clase.hipotecaria == False)
			elif tipocuenta == "contado":
				query = query.filter(clase.contado == True)
			elif tipocuenta == "hipotecaria":
				query = query.filter(clase.hipotecaria == True)
		#print "tipofecha", tipofecha		
		if tipofecha:
			if tipofecha == "alta":
				fecha = clase.fechaasignacion
			else:
				fecha = clase.fechacierre
			print "fechainicial", fechainicial, "fechafinal", fechafinal
			nohagas_nada = False
			if fechainicial <> "" and fechafinal <> "":
				fini = self.get_date(fechainicial)		
				ffin = self.get_date(fechafinal, minimal_date = False)
			elif fechainicial == "" and fechafinal <> "":
				fini = self.get_date("")
				ffin = self.get_date(fechafinal, minimal_date = False)
			elif fechainicial <> "" and fechafinal == "":
				fini = self.get_date(fechainicial)		
				ffin = self.get_date("", minimal_date = False)
			elif fechainicial == "" and fechafinal == "":
				fini = self.get_date("")
				ffin = self.get_date("", minimal_date = False)
			else:
				nohagas_nada = True
			if nohagas_nada is False:
				query = query.filter( fecha >= fini).filter( fecha <= ffin )
				if tipofecha == "alta" and sincierre:
					query = query.filter( clase.fechacierre == None )	
		print query
		cuantos = query.count()

		return dict( cuantosprospectos = [ dict( id = "1", cuantos = cuantos, cuantosformateado = "{:,}".format(cuantos) ),])

	def get_date(self, dateValue, minimal_date = True):
		if minimal_date:
			fecha = datetime( day = 1, month = 1, year = 1999)
		else:
			fecha = datetime( day = 31, month = 12, year = 2100)
		try:
			d, m, y = [ int(x) for x in dateValue.split("/")]
			fecha = datetime(day = d, month = m, year = y )
		except:
			pass
		return fecha

	def get(self):
		que, record = self.auth()
		if not que:
			return record
		clase = Prospecto
		query = DBSession.query( clase )

		cuantos = query.count()
		return dict( cuantosprospecto = dict( id = "1", cuantos = cuantos, cuantosformateado = "{:,}".format(cuantos) ))
	
@resource( collection_path='api/gtevdors', path='api/gtevdors/{id}')
class Gtevdors( EAuth ):

	def __init__(self, request):
		self.request = request

    #def collection_get(self):
    #	print self.request.headers
    #	que, record = self.auth()
    #	if not que:
    #		return record
	#	return dict()

	def get(self):
		if True:
			que, record, token = self.auth(get_token = True)
			if not que:
				return record
		user = cached_results.dicTokenUser.get( token )
		vendedor = user.get("vendedor", 0)
		gerente = user.get("gerente", 0)
		id_vendedor = 0
		id_gerente = 0
		nombre_vendedor = ""
		nombre_gerente = ""
		try:
			
			if vendedor:
				try:
					v = DBSession.query( Vendedor ).\
						filter( Vendedor.codigo == int( vendedor )).\
						filter( Vendedor.desactivado == False ).one()
					nombre_vendedor = d_e(v.nombre)
					id_vendedor = v.id
					
				except:
					print_exc()
					print "saliendo en el vendedor"
					nombre_vendedor = ""
					id_vendedor = 0

				try:
					g = DBSession.query( GerentesVentas).\
						get( int(gerente) )
					nombre_gerente = d_e(g.nombre)
					id_gerente = g.id
				except:
					print_exc()
					print "saliendo en el vendedor zona gerente"
					nombre_gerente = ""
					id_gerente = 0

					
			elif gerente:
				try:
					v = DBSession.query( Vendedor ).\
						filter( Vendedor.desactivado == False ).\
						order_by( Vendedor.nombre)
				except:
					print_exc()
					print "saliendo en el gerente"
					nombre_gerente = ""
					id_gerente = 0
			else:
				pass
		except:
			print_exc()
			print "sali por quien sabe que causa"
		
		return dict(gtevdor = dict( id = 1, idvendedor = id_vendedor, nombrevendedor = nombre_vendedor, idgerente = id_gerente, nombregerente = nombre_gerente))

@resource( collection_path='api/mediospublicitarios' , path = 'api/mediospublicitarios/{id}')
class MediosPublicitarios( EAuth):
	def __init__(self , request ):
		self.request = request

	def collection_get(self):
		if True:
			que, record = self.auth()
			if not que:
				return record
		clase = MedioPublicitario
		query = DBSession.query( clase ).\
				filter( clase.estatus == "A" ).\
				order_by( clase.descripcion)
		#return dict(foo="bar")
		return { 'mediospublicitarios': [ x.cornice_json for x in query.all() ]} 

def limpia_sql(sql):
	sql = sql.replace("\n"," ")
	sql = sql.replace("\t", " ")
	return sql

@resource( collection_path='api/clientesofertas' , path = 'api/clientesofertas/{id}')
class ClientesOfertas(EAuth):
	def __init__(self , request ):
		self.request = request

	def collection_get(self):
		if True:
			que, record = self.auth()
			if not que:
				return record
		
		cliente = self.request.params.get("cliente","")
		sql = """select top 1 x.codigo as id , x.nombre as nombre, isnull(c.codigo,0) as cuenta from cliente x
			left join cuenta c on x.codigo = c.fk_cliente 
			where x.codigo = {}""".format(cliente)

		resul = []
		try:
			for x in DBSession.execute( limpia_sql(sql) ):
				pass
				d = dict( id = x.id, nombre = x.nombre.decode("iso-8859-1").encode("utf-8"), cuenta = x.cuenta)
				resul.append(d)
				
		except:
			print_exc()
		print resul
		return dict(clientesofertas = resul )

@resource( collection_path='api/clientessinofertas' , path = 'api/clientessinofertas/{id}')
class ClientesSinOfertas(EAuth):
	def __init__(self , request ):
		self.request = request

	def collection_get(self):
		if True:
			que, record = self.auth()
			if not que:
				return record
		
		#cliente = self.request.params.get("cliente","")
		sql = """select top 1 x.codigo as id , x.nombre as nombre, isnull(c.codigo,0) as cuenta from cliente x
			left join cuenta c on x.codigo = c.fk_cliente 
			where c.codigo = 0 or c.codigo is null"""

		resul = []
		try:
			for x in DBSession.execute( limpia_sql(sql) ):
				pass
				d = dict( id = x.id, nombre = x.nombre.decode("iso-8859-1").encode("utf-8"), cuenta = x.cuenta)
				resul.append(d)
				
		except:
			print_exc()
		print resul
		return dict(clientessinofertas = resul )


@resource( collection_path='api/prospectosofertas' , path = 'api/prospectosofertas/{id}')
class ProspectosOfertas(EAuth):
	def __init__(self , request ):
		self.request = request

	def collection_get(self):
		if True:
			que, record = self.auth()
			if not que:
				return record
		
		afiliacion = self.request.params.get("afiliacion","")
		prospecto = self.request.params.get("prospecto","")
		resul =[] 
		
		if afiliacion:
			additionalWhere = "rtrim(ltrim(p.afiliacionimss)) = '{}'".format(afiliacion.strip())

		if prospecto:
			additionalWhere = "p.idprospecto = {}".format( prospecto )
		try:
			assert afiliacion or prospecto, "no hay afiliacion o prospecto"
		except:
			print_exc()
			return dict( prospectosofertas = resul)

		DIRECTOR_VENTAS = 9 #gerente que tiene el director de ventas
		sql = """
			select top 1 p.idprospecto as prospecto, 
			rtrim(ltrim(p.apellidopaterno1)) + ' ' + rtrim(ltrim(p.apellidomaterno1)) + ' ' + rtrim(ltrim(p.nombre1)) as nombre,
			rtrim(ltrim(p.afiliacionimss)) as afiliacion,
			p.idvendedor as vendedor, 
			v.nombre as nombrevendedor,
			p.idgerente as gerente, g.nombre as nombregerente from gixprospectos p
			join vendedor v on p.idvendedor = v.codigo
			join  gerentesventas g on p.idgerente = g.codigo
			where {} and p.fechacierre is null 
			and (p.cuenta = 0 or p.cuenta is null ) 
			and p.congelado = 0 and p.idgerente <> {} 
			order by p.idprospecto desc
		""".format(additionalWhere, DIRECTOR_VENTAS)
		sql = sql.replace("\n"," ")
		sql = sql.replace("\t", " ")
		try:
			for x in DBSession.execute( sql ):
				pass
				d = dict( id = x.prospecto, nombre = x.nombre.decode("iso-8859-1").encode("utf-8"), afiliacion = x.afiliacion, \
					vendedor = x.vendedor, nombrevendedor = x.nombrevendedor.decode("iso-8859-1").encode("utf-8"), \
					gerente = x.gerente, nombregerente = x.nombregerente.decode("iso-8859-1").encode("utf-8"))
				resul.append(d)
				
		except:
			print_exc()
		print resul
		return dict(prospectosofertas = resul )

@resource( collection_path='api/preciosinmuebles' , path = 'api/preciosinmuebles/{id}')
class PreciosInmuebles(EAuth):
	def __init__(self , request ):
		self.request = request

	def collection_get(self):
		if True:
			que, record = self.auth()
			if not que:
				return record
		sql = """select precio, sustentable, id, fk_etapa as etapa from gixpreciosetapa
		 where  activo = 1 order by precio"""
		sql = sql.replace("\n"," ")
		sql = sql.replace("\t", " ")
		resul =[] 
		try:
			for x in DBSession.execute( sql ):
				resul.append( dict ( id = x.id, precio = formato_comas.format(x.precio), sustentable = x.sustentable, etapa = x.etapa, precioraw = x.precio ))
		except:
			print_exc()
		print resul
		return dict(preciosinmuebles = resul )


@resource( collection_path='api/referenciasrapconclientesincuentas' , path = 'api/referenciasrapconclientesincuentas/{id}')
class RapConClienteSinCuentas(EAuth):
	def __init__(self , request ):
		self.request = request

	def get(self):
		que, record = self.auth()
		if not que:
			return record
		cual = self.request.matchdict['id']
		sql = """ select top 1 referencia from referencias_rap where cliente = {} and cuenta = 0 order by fecha desc
		""".format(cual)
		referencia = 0
		for x in DBSession.execute(sql):
			referencia = x.referencia
		if not referencia:
			error = "no esta en referencias rap"
			self.request.response.status = 400
			return dict( error = error)
		return dict( referenciasrapconclientesincuenta = dict( id = 1, referencia = referencia ))

@resource( collection_path='api/parametrosetapas' , path = 'api/parametrosetapas/{id}')
class ParametrosEtapas(EAuth):
	def __init__(self , request ):
		self.request = request

	def get(self):
		que, record = self.auth()
		if not que:
			return record
		cual = self.request.matchdict['id']
		
		sql = """select precio, valor_gastos as gastosadministrativos, precio_seguro as precioseguro, valor_apartado as apartado,
		 		anticipo_comision as antcipocomision, gastos_a_cuenta
				from PrecioEtapaDefault where fk_etapa = {}
			""".format(cual)
		sql = sql.replace("\n"," ")
		sql = sql.replace("\t", " ")
		d = dict()
		
		for x in DBSession.execute( sql ):
			d = dict( id = cual, gastosadministrativos = x.gastosadministrativos, precioseguro = x.precioseguro, apartado = x.apartado, anticipocomision = x.antcipocomision)
		if len(d) > 0:
			return dict( parametrosetapa = d)

		self.request.response.status = 401
		return dict(error = "no se encuentra en parametrosetapas")

@resource( collection_path='api/etapasofertas' , path = 'api/etapasofertas/{id}')
class EtapasOfertas(EAuth):
	def __init__(self , request ):
		self.request = request

	def collection_get(self):
		if True:
			que, record = self.auth()
			if not que:
				return record
		
		sql = """
		select distinct i.fk_etapa as netapa, e.descripcion as etapa, d.descripcion as desarrollo from inmueble i 
		join etapa e on i.fk_etapa = e.codigo 
		join desarrollo d on e.fk_desarrollo = d.codigo 
		where i.codigo not in ( select distinct fk_inmueble from cuenta ) and i.fk_etapa > 39
		"""
		sql = sql.replace("\n"," ")
		sql = sql.replace("\t", " ")
		resul =[] 
		try:
			for x in DBSession.execute( sql ):
				resul.append( dict ( id = x.netapa, nombre = "{} - {}".format(x.desarrollo.decode("iso-8859-1").encode("utf-8"), x.etapa.decode("iso-8859-1").encode("utf-8"))))
		except:
			print_exc()
		print resul
		return dict(etapasofertas = resul )


@resource( collection_path='api/manzanasdisponibles' , path = 'api/manzanasdisponibles/{id}')
class ManzanasDisponibles(EAuth):
	def __init__(self , request ):
		self.request = request

	def collection_get(self):
		if True:
			que, record = self.auth()
			if not que:
				return record
		
		etapa = self.request.params.get("etapa", "")
		additionalWhere = ""
		if etapa:
			additionalWhere = " and i.fk_etapa = {}".format( etapa )
		sql = """
		select distinct i.iden2 as manzana from inmueble i
		where i.codigo not in ( select distinct fk_inmueble from cuenta ) and i.fk_etapa > 39
		{} order by 1""".format ( additionalWhere )

		sql = """
		select distinct i.iden2 as manzana from inmueble i left join cuenta c 
		on i.codigo = c.fk_inmueble
		where c.fk_inmueble is null and i.fk_etapa > 39 {} order by 1
		""".format( additionalWhere)

		sql = sql.replace("\n"," ")
		sql = sql.replace("\t", " ")
		resul =[] 
		wasAnError = False
		try:
			for i,x in enumerate(DBSession.execute( sql ),1):
				resul.append( dict( id = i, manzana = x.manzana.decode("iso-8859-1").encode("utf-8").strip()))
		except:
			print_exc()
			wasAnError = True
		print resul
		return dict(manzanasdisponibles = resul )
		 
@resource( collection_path='api/inmueblesdisponibles' , path = 'api/inmueblesdisponibles/{id}')
class InmueblesDisponibles(EAuth):
	def __init__(self , request ):
		self.request = request

	def collection_get(self):
		if True:
			que, record = self.auth()
			if not que:
				return record
		
		etapa = self.request.params.get("etapa", "")
		
		additionalWhere = ""
		if etapa:
			additionalWhere = " and i.fk_etapa = {}".format( etapa )

		env = redis_conn.get("pyraconfig")
		if etapa:
			return dict( inmueblesdisponibles = tsql.inmueblesdisponibles(etapa = int(etapa), env = env ) )
		#sql = """
		#select i.iden2 as manzana, iden1 as lote, i.codigo as codigo from inmueble i
		#where i.codigo not in ( select distinct fk_inmueble from cuenta ) and i.fk_etapa > 39
		#{} order by i.iden2,i.iden1""".format ( additionalWhere )

		sql = """
		select i.iden2 as manzana , iden1 as lote, i.codigo as codigo from inmueble i 
		left join cuenta c on i.codigo = c.fk_inmueble
		where c.fk_inmueble is null and i.fk_etapa > 39 {} order by i.iden2,i.iden1""".format( additionalWhere)

		sql = sql.replace("\n"," ")
		sql = sql.replace("\t", " ")
		#resul =[] 
		#try:
		#	for x in DBSession.execute( sql ):
		#		resul.append( dict( id = x.codigo, manzana = x.manzana.decode("iso-8859-1").encode("utf-8").strip(), lote = x.lote))
		#except:
		#	print_exc()
		#print resul
		de = "InmueblesDisponibles"
		print "armando query de {} a las {}".format(de,datetime.now().isoformat())
		try:
		    #cn = DBSession.connection()
		    #c = cn.connection
		    stream = True
		    #cu = c.cursor()
		except:
			print_exc()
		resul =  {"inmueblesdisponibles" : [{ "id" : x.codigo, "manzana" : x.manzana.decode("iso-8859-1").encode("utf-8").strip(), "lote" : x.lote } for x in DBSession.execute(sql) ]}
		#resul =  {"inmueblesdisponibles" : [{ "id" : x.codigo, "manzana" : x.manzana.decode("iso-8859-1").encode("utf-8").strip(), "lote" : x.lote } for x in c.execution_options(stream_results=stream).execute(sql) ]}
		#cu.execute(sql)
		
		#resul =  {"inmueblesdisponibles" : [{ "id" : x[2], "manzana" : x[0].decode("iso-8859-1").encode("utf-8").strip(), "lote" : x[1] } for x in cu.fetchall() ]}
		print "resolviendo query de {} a las {}".format(de,datetime.now().isoformat())
		#cu.close()
		return resul
		#return dict(inmueblesdisponibles = resul )
		 
@resource( collection_path='api/vendedors' , path = 'api/vendedors/{id}')
class Vendedores( EAuth):
	def __init__(self , request ):
		self.request = request

	def collection_get(self):
		if True:
			que, record, token = self.auth(get_token = True)
			if not que:
				return record
		user = cached_results.dicTokenUser.get( token )
		try:
			vendedor = user.get("vendedor", 0)
			if vendedor:

				query = DBSession.query( Vendedor ).\
					filter( Vendedor.codigo == int( vendedor )).\
					filter( Vendedor.desactivado == False )
			else:

				query = DBSession.query( Vendedor ).\
					filter( Vendedor.desactivado == False ).\
					order_by( Vendedor.nombre)
		except:
			print_exc()
			return dict(vendedors = [])
		
		return { 'vendedors': [ x.cornice_json for x in query.all() ]} 

@resource( collection_path='api/usuarios', path = 'api/usuarios/{id}')
class UsuariosRest( EAuth ):
	def __init__(self, request ):
		self.request = request

	def collection_get(self):
		if True:
			que, record, token = self.auth(get_token = True)
			if not que:
				return record
			user = cached_results.dicTokenUser.get( token )
			try:
				assert user.get("perfil", "") == "admin", "El usuario no es administrador"
			except AssertionError, e:
				print_exc()
				return  dict( )
		t = rdb.db("iclar").table("usuarios").order_by("usuario")
		#return dict( usuarios = [ x for x in t.run() ])
		return dict( usuarios = [ dict( id = x.get("id"), usuario = x.get("usuario"), password = x.get("password"), perfil = x.get("zen_profile")) for x in t.run() ])

@resource( collection_path='api/gerentesventa' , path = 'api/gerentesventa/{id}')
class GerentesVtas( EAuth):
	def __init__(self , request ):
		self.request = request

	def collection_get(self):
		if True:
			que, record, token = self.auth(get_token = True)
			if not que:
				return record
		try:
			user = cached_results.dicTokenUser.get( token )
			gerente = user.get("gerente", 0)
			if gerente:
				query = DBSession.query( GerentesVentas ).\
					filter( GerentesVentas.codigo == int( gerente )).\
					filter( GerentesVentas.activo == True )
			else:
				query = DBSession.query( GerentesVentas ).\
					filter( GerentesVentas.activo == True ).\
					order_by( GerentesVentas.nombre)
		except:
			print_exc()
			return dict( gerentesventas = [])
		#return dict(foo="bar")
		return { 'gerentesventas': [ x.cornice_json for x in query.all() ]}

@resource( collection_path='api/ofertasrecientes' , path = 'api/ofertasrecientes/{id}')
class OfertasRecientes( EAuth):
	def __init__(self , request ):
		self.request = request

	def collection_get(self):
		if True:
			que, record, token = self.auth(get_token = True)
			if not que:
				return record
		try:
			user = cached_results.dicTokenUser.get( token )
			p = self.request.params
			etapa = p.get("etapa",0)
			cuantos = p.get("cuantos",100)
			additionalWhere = ""
			if etapa:
				additionalWhere = "where o.fk_etapa = {}".format(etapa)
			if cuantos:
				top = "top {}".format(cuantos)
			print("ofertasrecientes etapa {} cuantos {}".format(etapa, cuantos))
			sql = """
				select {} o.fk_etapa as etapa, o.oferta as oferta, o.cliente as cliente, 
				cte.nombre as nombrecliente, o.cuenta as cuenta, coalesce(i.iden2,'') as manzana, coalesce(i.iden1,'') as lote,
				cta.saldo as saldo, cta.fecha as fecha
				from ofertas_compra o join cliente cte on o.cliente = cte.codigo 
				join cuenta cta on o.cuenta = cta.codigo 
				left join inmueble i on cta.fk_inmueble = i.codigo  {} order by cta.codigo desc
				""".format(top, additionalWhere)
			
			return dict(ofertasrecientes = [dict(id = i, etapa = x.etapa, oferta = x.oferta, cliente = x.cliente, nombrecliente = x.nombrecliente.decode("iso-8859-1").encode("utf-8"), cuenta = x.cuenta, manzana = x.manzana, lote = x.lote, saldo = x.saldo, fecha = x.fecha.isoformat()) for i,x in enumerate(DBSession.execute(sql),1)])

		except:
			print_exc()
			return dict( ofertasrecientes = [])

@resource( collection_path='api/prospectosrecientes' , path = 'api/prospectosrecientes/{id}')
class ProspectosRecientes( EAuth):
	def __init__(self , request ):
		self.request = request

	def collection_get(self):
		if True:
			que, record, token = self.auth(get_token = True)
			if not que:
				return record
		try:
			user = cached_results.dicTokenUser.get( token )
			gerente = user.get("gerente", 0)
			vendedor = user.get("vendedor", 0)
			p = self.request.params
			afiliacion_valida = p.get("afiliacionvalida", "")
			if afiliacion_valida in "01":
				pass
			else:
				print "forzando afiliacion_valida por valor distinto a 0 o 1, valor ", afiliacion_valida
				afiliacion_valida = ""

			cuantos = 10
			if afiliacion_valida == "0":
				cuantos = 100
			limite = p.get("limite", cuantos)
			
			clase = Prospecto
			s = DBSession
			query = s.query( clase ).filter( clase.congelado == False)

			if gerente:
				query = query.filter( clase.idgerente == gerente )
			if vendedor:
				query = query.filter( clase.idvendedor == vendedor )
			query = query.order_by( clase.idprospecto.desc()).limit(limite)
			
		except:
			print_exc()
			return dict( prospectosrecientes = [])
		#return dict(foo="bar")
		if afiliacion_valida == "":
			return { 'prospectosrecientes': [ x.reciente_cornice_json for x in query.all() ]}
		if afiliacion_valida == "1":
			return { 'prospectosrecientes': [ x.reciente_cornice_json for x in query.all() if clase.is_luhn_valid( x.afiliacionimss)  ]}
		if afiliacion_valida == "0":
			return { 'prospectosrecientes': [ x.reciente_cornice_json for x in query.all() if clase.is_luhn_valid( x.afiliacionimss) == False  ][0:10]}

@lru_cache( maxsize = 500 )
def cantidad_a_palabras( que ):
    texto, texto2 = "", ""

    try:
        cual = aletras( que )
        texto = cual.encode("UTF-8")
        texto2 = texto.split("PESO")[0].strip()
    except:
        pass
    return ( texto, texto2 )

@resource(collection_path='api/pesos', path='api/pesos/{id}')
class Pesos(object):
	def __init__(self, request):
		self.request = request

	@view(renderer='json')
	def get(self):
		id = int(self.request.matchdict['id'])  
		que = id / 100.0 
		formato_comas = "{:,.2f}"
		
		texto, texto2 = cantidad_a_palabras( que )
		return dict( id = id, texto = texto, texto2 = texto2, importeformateado = formato_comas.format( que ) )

@resource(collection_path='api/gravatars', path='api/gravatars/{id}')
class Gravatar(EAuth):		
	def __init__(self , request ):
		self.request = request
	
	@view(renderer='json')
	def get(self):	
		que, record, token = self.auth(get_token = True)
		if not que:
			return record
		user = cached_results.dicTokenUser.get( token )
		usuario = user.get("usuario", "")
		usuarios = rdb.db("iclar").table("usuarios")
		for x in usuarios.filter( rdb.row["usuario"] == usuario.upper()).run():
			gv = ""
			gvemail = ""
			try:
				gv = x["gravatar"]

			except:
				pass
			try:
				gvemail = x["gravataremail"]
			except:
				pass
		return dict( gravatar = dict( id = "1", gravatar = gv, gravataremail = gvemail))


	@view(renderer='json')
	def put(self):
		que, record, token = self.auth(content = "gravatar", get_token = True)
		if not que:
			return record
		user = cached_results.dicTokenUser.get( token )
		usuario = user.get("usuario", "")
		gravataremail = ""
		try:
			gravataremail = record.get("gravataremail", "")
		except:
			print "no obtuve valor para gravataremail"
		if gravataremail:

			try:
				gravatar = md5( gravataremail ).hexdigest()
				rdb.db("iclar").table("usuarios").filter(rdb.row["usuario"] == usuario.upper()).update( dict( gravatar = gravatar, gravataremail = gravataremail )).run()
				print "gravatar actualizado"
			except:
				print "fallo actualizacion del gravatar"
		else:
			print "no habia valor de gravataremail"
		
		return dict(gravatar= dict(id=1, gravatar = gravatar, gravataremail = gravataremail))


@resource(collection_path='api/printers', path='api/printers/{id}')
class PrinterRest(EAuth):
	def __init__(self, request):
		print "entrando a la clase PrinterRest"
		self.request = request

	def collection_get(self):
		if True:
			que, record, token = self.auth(get_token = True)
			print "valores que, record, token", que, record, token
			if not que:
				return record
		try:
			user = cached_results.dicTokenUser.get( token )
			print "resolviendo printers... para", user
			assert user, "no hay usuario asociado a token"
			printers = printers_info(user)
			error = ""
			if isinstance(printers, dict):
				error = printers.get("error", "")
			print printers
			if error:
				print(paint.red("hubo error al llama printers_info"))
				raise
			return dict( printers = printers )	
		except:
			print_exc()
			return dict( printers = [])
		#return dict(foo="bar")
		
		
	@view(renderer='json')
	def get(self):
		if True:
			que, record, token = self.auth(get_token = True)
			if not que:
				return record
		try:
			user = cached_results.dicTokenUser.get( token )
			print "resolviendo printers... para", user
			assert user, "no hay usuario asociado a token"
			
			cual = self.request.matchdict['id']
			print "cual es", cual
			return dict( printer = printers_info(user, cual).get('printers','')[0])
		except:
			print_exc()
			return dict() 

	
@resource(collection_path='api/mfausers', path='api/mfausers/{id}')
class MfaUser(object):
	def __init__(self, request):
		self.request = request

	def collection_get(self):
		return {'mfausers': [ x.cornice_json for x in DBSession.query ( Mfa_User ).all() ] }

	@view(renderer='json')
	def get(self):
		try:
			#row = DBSession.query( Mfa_User ).get(int(self.request.matchdict['id']))
			row = Mfa_User.byId( int( self.request.matchdict['id']))
			return row.cornice_json
		except:
			print_exc()
			return dict() 
	
	@view(renderer='json')
	def put(self):
		id = self.request.matchdict['id']
		row = self.request.json.get(u"mfauser")
		user = row.get(u'active')
		active  = row.get(u"active")
		print "PUT", row.keys()
		try:
			u = DBSession.query( Mfa_User ).get( id )
			u.user = user
			u.active = active
			DBSession.add( u )
			DBSession.flush()
			transaction.commit()
		except:
			transaction.abort()

		return dict()


def resumen(y = None, m = None, d = None):
    c = dict()
    try:
        if y:
            f = datetime( year = y, month = m, day = d, hour = 19, minute = 30).isoformat()
                   
            foo = rdb.db("iclar").table("historia_resumen").\
                                filter(rdb.row["fecha"] > f ).\
                                order_by("fecha").limit(1).run()
        else:
            foo = rdb.db("iclar").table("resumen_reciente").run()
        for x in foo:
            c = x
    except:
        print_exc()

    return c


@view_config( route_name = "ropiclar", renderer= "json", request_method = "GET")
def resumenoperativo( request ):
    y = 0
    m = 0
    d = 0
    try:
        y = int(request.params["y"])
        m = int(request.params["m"])
        d = int(request.params["d"])
    except:
        pass
    if y and m and d:
        return resumen( y, m, d)
    return resumen()


def resumen_operativo_iclar(request):
    pass

@view_config( route_name = "printtest", renderer= "json", request_method = "GET")
def print_test(request):
    printer = request.params.get("printer", "")
    if not printer:
        return dict( printed = "0")
    file_to_print = "solfea.pdf"
    conf_file = "./zen/conf.json"
    params = json.load(open(conf_file))

    print_pdf( file_to_print, params.get(printer))
    return dict( printed = "1" )

@view_config( route_name = "printtest2", renderer= "json", request_method = "GET")
def print_test2(request):
	printer = request.params.get("printer", "")
	tipo = request.params.get("tipo", "caracteristicas")
	oferta = int(request.params.get("oferta", 0))
	etapa = int(request.params.get("etapa",0))
	cliente = int(request.params.get("cliente",0))
	if not printer:
		return dict( printed = "0")
	try:
		#dFunctions = dict( oferta = obtenerOferta, anexo = obtenerAnexo, caracteristicas = obtenerCaracteristicas)
		#datos = dFunctions.get(tipo)(etapa, oferta)
		func_name = "obtener{}{}".format(tipo[0].upper(),tipo[1:])
		if tipo in ("caracteristicas oferta anexo".split(" ")):
			datos = eval(func_name)(etapa, oferta)
		elif tipo in (["rap"]):
			datos = eval(func_name)(cliente)
		elif tipo in (["otro"]):
			datos = eval(func_name)()
		else:
			return dict()
		template = "{}.html".format(tipo)
		ok, nombre = pdfCreate(datos = datos, template=template, tipo = tipo)
		conf_file = "./zen/conf.json"
		params = json.load(open(conf_file))

		print_pdf( nombre, params.get(printer))
		return dict(ok = nombre, printed = "1")
	except:
		print_exc()
		
	return dict()

@view_config( route_name = "otro", renderer= "json", request_method = "GET")
def otro(request):
	value = printdispatcher(request)
	if value.get("printed", "0") == "0":
		request.response.status_code = 400
	return value

def printdispatcher(request):
    r = request.params
    printer = r.get("printer", "")
    email = r.get("email", "")
    if email:
        printer = "email"
        print(paint.yellow("email is {}".format(email)))
    if not printer:
        return dict( name = "", printed = "0", error = "impresora no especificada")
    pdf = r.get("pdf","")
    #user = r.get("user","")
    copies = int(r.get("copies","1"))
    tipo = r.get("tipo", "caracteristicas")
    oferta = int(r.get("oferta", 0))
    etapa = int(r.get("etapa",0))
    cliente = int(r.get("cliente",0))
    validToken, token = get_token( request )
    #token = r.get("token", "")
	
    try:
		
        assert validToken, "Token invalido"
        user = cached_results.dicTokenUser[token].get("id","")
        func_name = "obtener{}{}".format(tipo[0].upper(),tipo[1:])
        if tipo in ("caracteristicas oferta anexo".split(" ")):
            datos = eval(func_name)(etapa, oferta)
        elif tipo in (["rap"]):
            datos = eval(func_name)(cliente)
        elif tipo in (["otro", "recientesofertas"]):
            datos = eval(func_name)()
        else:
            return dict()
        template = "{}.html".format(tipo)
        reprint = False
        if pdf:
            name = pdf
            reprint = True
        else:
            ok, name = pdfCreate(datos = datos, template=template, tipo = tipo)
		
        if printer == "email":
            table = rdb.db("printing").table("email_jobs")
        else:
            table = rdb.db("printing").table("jobs")
        ts = rdb.expr( datetime.now( rdb.make_timezone('00:00') ))
        if printer == "email":
            table.insert(dict(filepath = name, user = user , email = email, timestamp = ts)).run()
        else:
            table.insert(dict(filepath=name, target=printer, copies = copies, user=user, reprint = reprint,  timestamp = ts  )).run()
        print(paint.blue("dispatching {}".format(name)))
		#print_pdf( nombre, printer )
        return dict(name = name, printed = "1", error = "")
    except:
        print_exc()

		
    return dict(name = "", printed = "0", error = "hubo error")

@view_config( route_name = "listprinters", renderer = "json", request_method = "GET")
def listprinters(request):
	ptable = rdb.db("printing").table("printers")
	valid = True
	try:
		validToken, token = get_token(request)
		assert validToken , "token invalido"
		user = cached_results.dicTokenUser.get( token )
		assert user.get("perfil", "") == "admin", "Solo un admin puede incorporar"
	except:
		print_exc()
		valid = False

	if not valid:
		request.response.status = 400
		#return errorJson(dict( success = valid , error = "hubo error"))
    
	rlist = request.params.get("rlist", "")
	if rlist:
		printers = []
		for x in ptable.run():
			printers.append(x)
		return printers
	printers = printers_info(match = False)
	for printer in printers:
		registered_printer = False
		for x in ptable.filter(rdb.row["printerid"] == printer.get("printerid")).run():
			registered_printer = True
		if not registered_printer:
			ptable.insert(dict(printerid = printer.get("printerid"), name = printer.get("name"), displayname = printer.get("displayname"))).run()
	return printers

def dump_request_response(request):
	for x in dir(request.response):
		if x in (["status_int", "status", "status_code"]):
			x = "{} = {}".format(x,getattr(request.response, x))
		print(paint.yellow("{}".format(x)))

@view_config( route_name = "deleteprinter", renderer = "json", request_method = "POST")
def deleteprinter(request):
	error = ""
	valid = True
	try:
		printers =rdb.db("printing").table("printers")
		r = request.params
		validToken, token = get_token(request)
		assert validToken, "token invalido"
		user = cached_results.dicTokenUser.get( token )
		assert user.get("perfil", "") == "admin", "Solo un admin puede eliminar"
		print( paint.blue("Aqui vamos"))
		printerid = r.get("printerid", "")
		assert printerid, "No se especifico printerid"
		#assert valid == False, "forzando error para probar, borrar despues"
		assert printers.filter(dict(printerid = printerid)).count().run() == 1, "No existe impresora"
		printers.filter(dict(printerid = printerid)).delete().run()
	except:
		print_exc()
		error = "falla eliminacion"
		valid = False
	print(paint.blue("en deleteprinter is valid es {}".format(valid)))
	if not valid:
		try:
			dump_request_response(request)
			request.response.status_code = 400

			#print(paint.yellow("pase el status_code igual a 400\n {}".format("\n".join(dir(request.response)))))

		except:
			print_exc()
		#return errorJson(dict(success = valid, error = error ))
	return dict( success = valid, error = error )



@view_config( route_name = "useremail", renderer = "json", request_method = "POST")
def useremail(request):
	try:
		#print cached_results.dicTokenUser
		ptable = rdb.db("printing").table("useremail")
		#printers =rdb.db("printing").table("printers")
		r = request.params
		email = r.get("email","")
		query = r.get("query",  "")
		valid, tokenValue = get_token(request)
		print(paint.blue("Chequeo de token es {} y vale {}".format(valid, tokenValue)))
		#token = r.get("token", "")
		token = tokenValue
		user = cached_results.dicTokenUser.get( token )
		print user
		
		
		usuario = user.get("id")
		assert usuario , "no existe usuario"
		if query:
			r_email = ""
			for x in ptable.filter(rdb.row["user"] == usuario).run():
				r_email = x.get("email", "")
			return dict( success = "1", email = r_email) 
		ptable.filter(rdb.row["user"] == usuario).delete().run()
		
		if email:
			ptable.insert(dict(user = usuario, email = email) ).run()
			print "adding user and email"

		
		return dict( success = "1")

	except:
		print_exc()
		return dict( success = "0")

@view_config( route_name = "userprinter", renderer = "json", request_method = "POST")
def userprinter(request):
	try:
		#print cached_results.dicTokenUser
		ptable = rdb.db("printing").table("userprinter")
		printers =rdb.db("printing").table("printers")
		r = request.params
		printerid = r.get("printerid", "")
		copies = int(r.get("copies", 0))
		valid, tokenValue = get_token(request)
		print(paint.blue("Chequeo de token es {} y vale {}".format(valid, tokenValue)))
		#token = r.get("token", "")
		token = tokenValue
		user = cached_results.dicTokenUser.get( token )
		print user
		
		assert printerid , "no hay printerid"
		print "copies ", copies
		assert copies in (0,1,2,3) , "copies invalido"
		usuario = user.get("id")
		assert printers.filter(rdb.row['printerid']  == printerid ).count().run() > 0, "No existe la impresora en tabla printers"
		cuantos = ptable.filter( rdb.row["user"] == usuario ).filter(rdb.row["printerid"] == printerid).count().run()
		print "cuantos ", cuantos
		if cuantos:
			print "updating"
			if copies == 0:
				
				ptable.filter(rdb.row["user"] == usuario ).filter(rdb.row["printerid"] == printerid).delete().run()
			else:
				ptable.filter(rdb.row["user"] == usuario ).filter(rdb.row["printerid"] == printerid).update(dict(copies = copies)).run()
		else:
			print "inserting"
			ptable.insert(dict(printerid = printerid , user = usuario, copies = copies)).run()
		return dict( success = "1")

	except:
		print_exc()
		return dict( success = "0")

def printers_info(user = None, id = None, match = True):
	try:
		ptable = rdb.db("printing").table("userprinter")
		printers = rdb.db("printing").table("printers")
		config = dict()
		emptyDic = dict()
		if user:
			print(paint.yellow("printers_info user is ... {}".format(user)))
			for x in ptable.filter( rdb.row["user"] == user.get("id") ).run():
				config[x.get("printerid")] = dict( copies = x.get("copies", 0), email = x.get("email",0))
		p = cloudSpooler.getPrinters()
		ps = []
		for i,x in enumerate(p,1):
			if "google" not in x:
				if match and printers.filter(dict(printerid = x)).count().run() == 0:
					continue
				else:
					if id and x <> id:
						continue
					y = p[x]
					status = cloudSpooler.getPrinterStatus( x )
					ps.append( dict(id = i, printerid = x, name = y.get('name',''), displayname = y.get('displayName',''), status = status, timestamp = datetime.now().isoformat(), copies = config.get(x, emptyDic).get("copies",0) ))
		return ps
	except:
		print_exc()
		return dict(error = "error")

@view_config( route_name = "foo", renderer= "json", request_method = "GET")
def foo(request):
    return dict( foo = "foo", bar = len(cached_results.dicAuthToken) )

@view_config( route_name = "routeauth", renderer="json", request_method="POST")
def routeauth(request):
    tname = "zen_token_hub"
    try:
        token = request.params.get("token", "")
        route = request.params.get("route", "")
    except:
        pass
    print token, route
    valid = "1"
    try:
        assert token != "", "Empty token"
        assert route != "", "Empty route"
        print cached_results.dicTokenUser.get(token)
        assert route in cached_results.dicTokenUser.get(token).get("routes"), "Route not valid"
    	print token, route
    except:
        print_exc()
        valid = "0"
    if valid == "1":
    	try:
        	rdb.db("iclar").table(tname).filter(rdb.row["token"] == token).update(dict( route = route )).run()
        	print "actualizando {}".format(route)
        except:
        	print_exc()
    return dict( access = valid )

@view_config( route_name = "revoke", renderer="json", request_method="POST")
def revoke( request ):
	print "revoking"
	tname = "zen_token_hub"

	#print request.params
	token = request.params.get(u"token","")
	print "el token es ", token
	try:
		if token:
			rdb.db("iclar").table(tname).filter(rdb.row["token"] == token).update(dict( active = False )).run()
			del cached_results.dicAuthToken[token]
			del cached_results.dicTokenUser[token]
		else:
			print "el token no existe"
	except:
		print_exc()


def get_google_token( code, refresh = False, source = None ):
    print( paint.blue("code google {}".format(code)))
    success = True
    db = rdb.db("printing")
    try:	
    
        ACCESSTOKEN_URL = "https://accounts.google.com/o/oauth2/token"
        REFRESHTOKEN_URL = "https://www.googleapis.com/oauth2/v3/token"
        d = dict()
        URL = ACCESSTOKEN_URL  
        if refresh:
            URL = REFRESHTOKEN_URL
            tk = redis_conn.get("google_refresh_token")
            print("tk {}".format(tk))

            d = dict(refresh_token = tk, client_id = CLIENT_ID, client_secret = CLIENT_KEY, grant_type = "refresh_token")
        else:
            d = dict( code = code, client_id = CLIENT_ID, client_secret = CLIENT_KEY, redirect_uri = ICLAR_REDIRECT_URI )
            d["grant_type"] = "authorization_code"
            #d["access_type"] = "offline"
              
        resul = requests.post( URL, data = d ).json()        
        print "resul from oauth is ,", resul
        if source:
            print(paint.blue("source is {}".format(source)))
        access_token = resul.get("access_token", "")
        assert access_token, "no hay token"
        refresh_token = resul.get("refresh_token","")
        #assert refresh_token, "no hay refresh token"
        if redis_conn.get("google_code") <> code:
            redis_conn.set("google_code", code)
            db.table("google_code").insert(dict( code = code )).run()
        redis_conn.set("google_token", access_token)
        db.table("google_token").insert(dict(token = access_token, active = True )).run()
        if refresh_token:
            redis_conn.set("google_refresh_token", refresh_token)
            db.table("google_refresh_token").insert(dict(token = refresh_token)).run()
    except:
        print_exc()
        success = False
    return dict( success = success )

@view_config( route_name = "oauth", renderer="json", request_method="GET")
def oauth( request ):
    rqp = request.params
    print "oauth content from google, ", rqp
    code = rqp.get("code","")
    try:
        assert code , "no charcha"
        return get_google_token( code )
    except AssertionError , e:
        print_exc()
        return dict( success = False )

@view_config( route_name = "refreshtoken", renderer="json", request_method="POST")
def refreshtoken( request ):
    #rqj = request.json_body
    #print request.params
    rqp = request.params
    passport = rqp.get("passport", "")
    print(paint.blue("refreshtoken {}".format(passport)))
    
    if passport:
        
        t = datetime.now()
        f = "{:04d}{:02d}{:02d}iclartoken".format(t.year, t.month, t.day)
        try:
        	assert sha1(f).hexdigest() == passport, "el passport no es correcto"
        	code = redis_conn.get("google_code")
        	assert code, "no hay google_code"
        	#return dict( success = True )
        	return get_google_token( code, True)
        except:
        	print_exc()
    return dict(success = False)


   
@view_config( route_name = "pruebapost", renderer="json", request_method="POST")
def pruebapost( request ):
    #rqj = request.json_body
    print request.params
    rqp = request.params

@view_config( route_name = "token", renderer="json", request_method="POST")
def token( request ):
    #rqj = request.json_body
    print request.params
    rqp = request.params
    email = str( rqp.get(u"username", ""))
    password = str( rqp.get(u"password", ""))
    print "cachando", email, password
    gravatar_email = str( rqp.get(u"gravatar_email", ""))
    usuarios = rdb.db("iclar").table("usuarios")
    gravatar = ""
    if True:
    	for x in usuarios.filter( rdb.row["usuario"] == email.upper()).run():
    		print "usuario ", email.upper(), "password", x["password"]
    		try:
    			gravatar = x["gravatar"]
    		except:
    			pass
    try:
        assert "" not in ( email, password), "Credenciales vacias"
        assert x["activo"] is True, "Usuario inactivo"
        print "usuario es activo"
    except AssertionError, e:
        request.response.status = 401
        return  dict( error = e.args[0] )
    try:
        assert rdb.db("iclar").table("usuarios").filter( rdb.row["usuario"] == email.upper()).filter( rdb.row["password"] == password).count().run() == 1, "Credenciales invalidas"

    except AssertionError, e:
        print_exc()
        request.response.status = 401
        return  dict( error = e.args[0] )

    for x in rdb.db("iclar").table("usuarios").filter( rdb.row["usuario"] == email.upper()).filter( rdb.row["password"] == password).run():
        id_user = int( x["appid"])
        domains = x["domains"]
        try:
            zen_profile = x["zen_profile"]
        except:
            zen_profile = ""
    try:
        assert zen_profile != "", "Perfil invalido"
    except AssertionError,e :
        print_exc()
        request.response.status = 401
        return  dict( error = e.args[0])

    routes = []
    for x in rdb.db("iclar").table("zen_profiles").filter(rdb.row["profile"] == zen_profile ).run():
        routes = x["routes"]
    gerente = 0
    try:
    	if zen_profile in "admin direccioncomercial subdireccioncomercial comercial" :
    		vendedor = 0
    	elif zen_profile == "vendedor":
    		for x in rdb.db("iclar").table("usuariosvendedores").filter( rdb.row["usuario"] == email.upper()).run():
    			vendedor = x["vendedor"]
    		v = DBSession.query( Vendedor ).get(int(vendedor))
    		gerente = v.gerente
    	elif zen_profile == "gerente":
    		for x in rdb.db("iclar").table("usuariosgerentes").filter( rdb.row["usuario"] == email.upper()).run():
    			gerente = int(x["gerente"])
    			vendedor = 0
    	else:
    		raise
    	

    except:
    	print_exc()
        request.response.status = 401
        return  dict( error = "error en definicion de perfil")

    if gravatar_email:
    	try:
    		gravatar = md5( gravatar_email ).hexdigest()
    		rdb.db("iclar").table("usuarios").filter(rdb.row["usuario"] == email.upper()).update( dict( gravatar = gravatar )).run()
    		print "gravatar actualizado"
    	except:
    		print "fallo actualizacion del gravatar"

    auth_token = request.session.new_csrf_token()
    request.session["auth_token"] = auth_token
    tname = "zen_token_hub"
    tname2 = "zen_track_casas_ofertas"
    tname3 = "zen_track_prospectos"
    try:
    	rdb.db("iclar").table_create(tname).run()
    except:
    	pass

    try:
    	ts = rdb.expr( datetime.now( rdb.make_timezone('00:00') ))
    	rdb.db("iclar").table(tname).insert(dict( usuario = email.upper(), token = auth_token, created = datetime.now().isoformat(), timestamp = ts, active = True)).run()
    except:
    	print_exc()
    
    try:	
    	rdb.db("iclar").table_create(tname2).run()
    except:
    	pass

    try:
    	rdb.db("iclar").table_create(tname3).run()
    except:
    	pass
    
    cached_results.dicAuthToken[auth_token] = True
    cached_results.dicTokenUser[auth_token] = dict( id = id_user, gravatar = gravatar, routes = routes, gerente = gerente, vendedor = vendedor, usuario = email, perfil = zen_profile )

    request.session["user_id"] = id_user
    #return dict( user_id = id_user ,  auth_token = auth_token  )
    return dict( access_token = auth_token )

def gcp():
    conf_file = "./zen/conf.json"
    params = json.load(open(conf_file))
    params["conf_file"] = conf_file
    #print "gcp() at {}".format( datetime.now().isoformat())
    #print params
    #print "is_luhn_valid 12345678903", Prospecto.is_luhn_valid("12345678903")
    #print "is_luhn_valid 12345678904", Prospecto.is_luhn_valid("12345678904")
    cloudSpooler = CloudSpooler(params["email"],params["password"],params["OAUTH"])
    return cloudSpooler

def print_pdf(file_to_print, printer_id):
    job = cloudSpooler.submitPdf( printer_id, file_to_print)
    job_id = job['job']['id']
    if job and job['success']:
    	print "job submitted"

class CachedResults(object):
    def __init__(self):
        self.dicAuthToken = ExpiringDict(max_len = 300, max_age_seconds = 3600 )
        self.dicTokenUser = dict()

cached_results = CachedResults()
cloudSpooler = gcp()
