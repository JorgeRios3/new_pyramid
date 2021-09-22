from cornice.resource import resource, view
import os
from tempfile import NamedTemporaryFile
import time
from .models import (
	DBSession,
	Base,
	DBSession2,
	Base2,
	Mfa_User,
	Mfa_Device,
	CuentaPagare,
	Cuenta,
	Documento,
	MedioPublicitario,
	Prospecto,
	DocumentoPagare,
	ReciboPagare,
	MovimientoPagare,
	Inmueble,
	GerentesVentas,
	Vendedor,
	d_e,
	pytz,
)
import traceback
from traceback import print_exc
from pyramid.response import Response
from pyramid.httpexceptions import HTTPFound
from pyramid.response import FileResponse
from pyramid.view import view_config
import sys
import calendar
import transaction
from . import nbb
from . import enbb
from .utils import aletras, upper2  # CantidadAPalabras, thousands_commas
from datetime import datetime
import json
import xlwt
from boto import sts
from urllib.request import urlopen
from expiringdict import ExpiringDict
from hashlib import md5, sha1
from .cloud_spooler import (
	CloudSpooler,
	CLIENT_ID,
	CLIENT_KEY,
	ICLAR_REDIRECT_URI,
	requests,
)
from repoze.lru import lru_cache
from pyramid.events import subscriber, NewRequest, ApplicationCreated
from mako.template import Template
from mako.runtime import Context

# from cStringIO import StringIO
from io import StringIO
import xhtml2pdf.pisa as pisa
from reportlab.pdfgen import canvas
from reportlab.lib.pagesizes import letter
from reportlab.lib.units import inch
import redis
import pyodbc

# from painter import paint
from painter import paint

# import tsql
import uuid
from .graficos import *
import datetime as tiempo
from PyPDF2 import PdfFileReader
import pdfkit
import os


# c2p=CantidadAPalabras

redis_host, redis_port, redis_db = "192.168.1.124", 6379, 5
redis_conn = None
formato_comas = "{:,.2f}"

# from rethinkdb import RethinkDB
# rdb = RethinkDB()

# rdb.connect("127.0.0.1", 28015).repl()


def color(what, c="b"):
	if c == "b":
		print(paint.blue(what))
	elif c == "r":
		print(paint.red(what))
	elif c == "y":
		print(paint.yellow(what))
	elif c == "c":
		print(paint.cyan(what))
	elif c == "g":
		print(paint.green(what))
	else:
		print(what)


def today(formatted=True):
	t = datetime.now()
	if formatted:
		return t.isoformat()
	return t


def l_traceback(msg=""):
	exc_type, exc_value, exc_traceback = sys.exc_info()
	error = "".join(traceback.format_tb(exc_traceback))
	return error


def utc_to_local(utcdate, timezone="America/Mexico_City"):
	local_tz = pytz.timezone(timezone)
	local_dt = utcdate.replace(tzinfo=pytz.utc).astimezone(local_tz)
	return local_dt


@subscriber(NewRequest)
def anyrequest(event):
	s = cached_results.settings
	soy = s.get("pyraconfig", "indefinido")
	req = "{}.{} {}".format(soy, today(), event.request.path_qs)
	color(req, "c")
	c = redis.Redis(host=s.get("redis.host"), port=s.get("redis.port"), db=0)
	# c = redis.Redis(host="127.0.0.1", port=6379, db=8)
	c.publish("pyramid.request", req)
	# c.set("pyramid.request", req)
	# c.set(req,1)
	# c.expire(req,4)


class ZenError(Exception):
	def __init__(self, code):
		self.code = code

	def __str__(self):
		return repr(self.code)


def esProduccion():
	return cached_results.settings["pyraconfig"] == "prod"


@subscriber(ApplicationCreated)
def arranque(event):
	global redis_conn, rdb
	settings = event.app.registry.settings
	cached = cached_results.settings
	redis_host = settings.get("redis.host", "")
	redis_port = int(settings.get("redis.port", ""))
	redis_db = int(settings.get("redis.db", ""))
	try:
		assert cloudSpooler, "No esta cloudSpooler"
		assert (
			redis_host and redis_port and redis_db
		), "Faltan parametros en el ini sobre el redis"
	except:
		print_exc()

	cached["rethinkdb.host"] = settings.get("rethinkdb.host")
	cached["rethinkdb.port"] = settings.get("rethinkdb.port")
	cached["sqlalchemy.url"] = settings.get("sqlalchemy.url")
	cached["redis.host"] = settings.get("redis.host")
	cached["redis.port"] = settings.get("redis.port")
	cached["redis.db"] = settings.get("redis.db")

	if "TEST" in cached["sqlalchemy.url"]:
		cached["pyraconfig"] = "test"
		from rethinkdb import RethinkDB

		rdb = RethinkDB()

	else:
		cached["pyraconfig"] = "prod"
		import rethinkdb as r

		rdb = r

	enbb.start(esProduccion())

	try:
		sql = """
		select sum(c.saldo) as saldototal from cuenta c
		join inmueble i on c.fk_inmueble = i.codigo
		where i.fk_etapa in ( 8,9,10,33 )
		"""
		saldototal = 0
		for x in DBSession2.execute(preparaQuery(sql)):
			saldototal = x.saldototal
		color("Saldo en Arcadia: {}".format(saldototal))
		DBSession2.close()
	except:
		color("Error al considerar Arcadia")
	saldototal = 0
	for x in DBSession2.execute(preparaQuery(sql)):
		saldototal = x.saldototal
	color("Saldo en Arcadia: {}".format(saldototal))
	DBSession2.close()

	color("enbb checking...")

	color("nbb checking...")
	# color("enbb function value {}".format(enbb.tableCount()),'g')
	# color("nbb function value {}".format(len(nbb.venta_por_vendedor_arcadia())),'g')
	rdb.connect(settings.get("rethinkdb.host"), settings.get("rethinkdb.port")).repl()
	redis_conn = redis.Redis(host=redis_host, port=redis_port, db=redis_db)
	cd = redis_conn.get("zeniclar-current-deploy")
	try:
		assert cd, "No hay redis, o registro de current-deploy para zeniclar"
		color("Current Deploy {}".format(cd))
		cloudSpooler.redis_conn = redis_conn
		x = "google_token"
		table = rdb.db("printing").table(x)
		color("{} tenia {}".format(x, table.count().run()), "r")
		table.delete().run()
		color("{} tiene {}".format(x, table.count().run()), "g")
		if redis_conn.get("pyraconfig") == "prod":
			token_req = get_google_token(redis_conn.get("google_code"), True)
			assert token_req.get(
				"success"
			), "No se puedo obtener token para Google Cloud Print"
	except:
		print_exc()

	color(
		"Arrancando la Web App a las {} - {}".format(today(), cached["pyraconfig"]), "y"
	)
	for x in "zen_token_hub zen_track_casas_ofertas zen_track_prospectos".split(" "):
		table = rdb.db("iclar").table(x)
		color("{} tenia {}".format(x, table.count().run()), "r")
		table.delete().run()
		color("{} tiene {}".format(x, table.count().run()))
	rdb.db("iclar").table("zen_pyramid_restart").insert(dict(timestamp=today())).run()


def pdfFileName(tipo="indeterminado", llave="X"):
	tipo = tipo.upper()
	fecha = datetime.now().isoformat()
	for x in "-:.T":
		fecha = fecha.replace(x, "")
	nombre = "{}_{}_{}.pdf".format(tipo, llave, fecha)
	return nombre


class Datos(object):
	def __setattr__(self, name, value):
		if isinstance(value, unicode):
			self.__dict__[name] = value.encode("ascii", "xmlcharrefreplace")
		else:
			self.__dict__[name] = value


def preparaQuery(sql):
	sqlx = sql.replace("\t", " ")
	sql = sqlx.replace("\n", " ")
	return sql


def request_to_rdb(request):
	rdb.connect(
		cached_results.settings.get("rethinkdb.host"),
		cached_results.settings.get("rethinkdb.port"),
	).repl()
	table_name = "reportParameters"
	table = rdb.db("printing").table(table_name)
	try:
		rdb.db("printing").table_create(table_name).run()
	except:
		pass
	try:
		id = table.insert(dict(request=request, ts=today()))["generated_keys"][0].run()
	except:
		print_exc()
		id = ""
	return id


def rdb_to_request(id):
	rdb.connect(
		cached_results.settings.get("rethinkdb.host"),
		cached_results.settings.get("rethinkdb.port"),
	).repl()
	table_name = "reportParameters"
	table = rdb.db("printing").table(table_name)
	try:
		return table.get(id)["request"].run()
	except:
		print_exc()
		return dict()


def autorizacion_descuento(autorizacion, cuenta=0):
	rdb.connect(
		cached_results.settings.get("rethinkdb.host"),
		cached_results.settings.get("rethinkdb.port"),
	).repl()
	tabla = rdb.db("iclar").table("autorizaciondescuento")
	if cuenta and autorizacion:
		if tabla.count().run() > 0:
			color(
				"Grabando en rethinkdb descuento sobre precio aplicado".format(today()),
				"b",
			)
			tabla.filter(dict(autorizacion=autorizacion)).update(
				dict(cuenta=cuenta)
			).run()
			return True
	elif autorizacion:
		color("Consultando en rethinkdb descuento sobre precio".format(today()), "b")
		descuento = 0
		if tabla.count().run() > 0:
			for x in tabla.filter(dict(autorizacion=autorizacion, cuenta=0)).run():
				descuento = float(x.get("descuento"))
		return descuento


def autorizacion_descuento_por_cuenta(cuenta):
	rdb.connect(
		cached_results.settings.get("rethinkdb.host"),
		cached_results.settings.get("rethinkdb.port"),
	).repl()
	tabla = rdb.db("iclar").table("autorizaciondescuento")
	if tabla.count().run() == 0:
		return 0
	descuento = 0
	if cuenta:
		color(
			"Consultando en rethinkdb descuento sobre precio usando cuenta".format(
				today()
			),
			"b",
		)
		descuento = 0
		for x in tabla.filter(dict(cuenta=cuenta)).run():
			descuento = float(x.get("descuento"))
	return descuento


def rlabTramites(nombre, etapa):
	c = canvas.Canvas(nombre, pagesize=letter)
	c.setFont("Helvetica", 12)

	c.drawString(
		180,
		800,
		"Tramites de Etapa {} ({}) Ver Excel con mismo nombre ".format(etapa, today()),
	)
	c.showPage()
	c.save()
	excel_name = "{}.xls".format(nombre.split(".")[0])
	try:
		with open(
			"/home/smartics/cronjobs/tramites/{:04d}.xls".format(etapa), "rb"
		) as f:
			with open(excel_name, "wb") as f2:
				f2.write(f.read())
	except:
		with open("/home/smartics/cronjobs/tramites/dummy.xls", "rb") as f:
			with open(excel_name, "wb") as f2:
				f2.write(f.read())


def rlabGeneraExcel(nombre, filename):

	excel_name = "{}.xls".format(nombre.split(".")[0])
	try:
		with open("{}".format(filename), "rb") as f:
			with open(excel_name, "wb") as f2:
				f2.write(f.read())
		os.unlink(filename)
	except:
		print_exc()
		if True:
			with open(excel_name, "wb") as f2:
				f2.write("")


def rlabPago(nombre, pago):
	args = dict(pago=pago)
	d = enbbcall("pagocomisiondetalle", args)
	lista = d.get("pagocomisiondetalles")
	if lista == 0:
		return
	nombrevendedor = lista[0].get("nombrevendedor")
	esgerente = lista[0].get("esgerente")
	resto = ""
	if esgerente:
		resto = "(Gerente)"

	campos_a_considerar = (
		"movimiento, documento, inmueble, cuenta, etapa, manzana, lote, importe"
	)
	if d == dict():
		return
	r = d.get("pagocomisiondetalles")
	c = canvas.Canvas(nombre, pagesize=letter)
	c.setFont("Helvetica", 12)
	c.drawString(
		180,
		800,
		"Pago de Comisiones No. {} a {} {}".format(pago, nombrevendedor, resto),
	)
	for x in r:
		pass


def rlabResumen(nombre):
	r = urlopen("https://zen.grupoiclar.com/api/ropiclar").read()
	resumen = json.loads(r)
	encoding = "iso-8859-1"
	wbook = xlwt.Workbook()
	wsheet = wbook.add_sheet("0")
	excel_name = "{}.xls".format(nombre.split(".")[0])
	etapas = [52, 53, 54, 55, 57, 58]
	c = canvas.Canvas(nombre, pagesize=letter)
	c.setFont("Helvetica", 12)
	c.drawString(180, 800, "Resumen Operativo ({})".format(resumen.get("fecha")))
	# c.drawImage("/home/smartics/pyramidzen/zen/zen/report/img/pagocomision/barcode/7669c6d2610a4cb2a20b2b011d836f89.jpg", inch, 800 - 2 * inch)
	base_titulo = 770
	c.setFont("Helvetica", 6)
	c.drawString(12, base_titulo, "Tipo")
	c.drawString(80, base_titulo, "Rubro")
	wsheet.write(0, 0, "Tipo")
	wsheet.write(0, 1, "Rubro")

	nombres = resumen.get("nombres_etapas")
	x = 240
	col_excel = 0
	for col_x, etapa in enumerate(etapas[1:], 2):
		c.line(x - 3, base_titulo - 20, x - 3, 97)
		nombre = resumen.get("nombres_etapas")[str(etapa)][0]
		c.drawString(x, base_titulo, nombre)
		col_excel = col_x
		wsheet.write(0, col_excel, nombre.encode(encoding))
		x += 70

	c.line(x - 3, base_titulo - 20, x - 3, 97)
	col_excel += 1
	c.drawString(x + 10, base_titulo, "Total")
	wsheet.write(0, col_excel, "Total")
	c.line(10, base_titulo - 20, 580, base_titulo - 20)
	c.line(67, base_titulo - 20, 67, 97)
	kvalores = resumen.get("kvalores")
	valores = resumen.get("valores")
	y = base_titulo - 40
	for fila_excel, kvalor in enumerate(kvalores, 1):
		linea = valores[str(kvalor)]
		rubro = linea[str(-1)]
		tipo = linea[str(0)]
		if tipo == "GCMEX":
			tipo = "CONSTRUCTOR"

		c.drawString(12, y, tipo)
		c.drawString(70, y, rubro)
		wsheet.write(fila_excel, 0, tipo.encode(encoding))
		wsheet.write(fila_excel, 1, rubro.encode(encoding))
		y -= 15
	y = base_titulo - 40
	for fila_excel, kvalor in enumerate(kvalores, 1):
		linea = valores[str(kvalor)]
		x = 270
		total = 0
		col_excel = 0
		for col_x, etapa in enumerate(etapas[1:], 2):
			valor = linea[str(etapa)]
			total += valor
			c.drawRightString(x + 5, y, "{:,}".format(valor))
			col_excel = col_x
			wsheet.write(fila_excel, col_excel, valor)
			x += 70
		col_excel += 1
		c.drawRightString(x + 5, y, "{:,}".format(total))
		wsheet.write(fila_excel, col_excel, total)
		y -= 15

	c.setLineWidth(0.3)
	y = base_titulo - 43
	for kvalor in kvalores:
		c.line(70, y, 580, y)
		y -= 15

	c.showPage()
	c.save()
	try:
		wbook.save(excel_name)
	except:
		print_exc()
	return


def get_date(dateValue, minimal_date=True):
	if minimal_date:
		fecha = datetime(day=1, month=1, year=1999)
	else:
		fecha = datetime(day=31, month=12, year=2100)
	try:
		d, m, y = [int(x) for x in dateValue.split("/")]
		fecha = datetime(day=d, month=m, year=y)
	except:
		pass
	return fecha


def enbbcall(func="", arguments=None, oneWay=False, msgpack=False):
	try:
		assert func, "falta funcion a llamar"
	except:
		print_exc()
		return dict()
	if arguments is None:
		if oneWay:
			return enbb.one_way_process_request(
				func=func, source="pyramid-zen", user="pyramid-zen"
			)
		else:
			return json.loads(
				enbb.process_request(
					func=func, source="pyramid-zen", user="pyramid-zen", msgpack=msgpack
				)
			).get("response")
	else:
		if oneWay:
			return enbb.one_way_process_request(
				func=func, source="pyramid-zen", user="pyramid-zen", arguments=arguments
			)
		else:
			return json.loads(
				enbb.process_request(
					func=func,
					source="pyramid-zen",
					user="pyramid-zen",
					arguments=arguments,
					msgpack=msgpack,
				)
			).get("response")


def fecha_descriptiva(fecha):
	meses = "X Enero Febreo Marzo Abril Mayo Junio Julio Agosto Septiembre Octubre Noviembre Diciembre".split(
		" "
	)
	year, month, day = [int(x) for x in fecha.split("/")]
	return "{} {:02d}, {:04d}".format(meses[month], day, year)


def obtenerSolicitudcheque(solicitud=0):
	datos = Datos()
	args = dict(solicitudcheque=solicitud)
	print("aca obtenerSolicitudcheque 1", args)
	d = enbbcall("solicitudchequeimpresion", args)
	print("viendo si funciona hasta obtenersolicutud", datos)
	datos.fechaprogramada = fecha_descriptiva(
		d.get("fechaprogramada")
	)  # "Febrero 19, 2016"
	cantidad = d.get("cantidad", 0)
	datos.cantidad = formato_comas.format(cantidad)  # 55,200.00"
	datos.cantidadletra = aletras(
		float(cantidad), tipo="pesos"
	)  # "cineunta mil muchos"
	datos.beneficiario = d.get("beneficiario", "")  # "JORGE CARLOS RIOS BELTRAN"
	datos.concepto = d.get("concepto", "")  # ARREGLO LICUADORA PARA LA OFICINA DE PB"

	destino = "{} {} {} {}".format(
		d.get("bancodestino", ""),
		d.get("sucursaldestino", ""),
		d.get("plazadestino", ""),
		d.get("clavebancariadestino", ""),
	)
	datos.destino = destino  # "BANCO SANTANDER CUENTA 2093029390493, CLAVE 304309409, PLAZA 09904, otro 04990434"
	datos.empresa = d.get(
		"empresa", ""
	)  # "DESARROLLADORA URBANA INTEGRAL S.A. DE C.V."
	datos.observaciones = d.get(
		"observaciones", ""
	)  # DEPOSITAR A LA CUENTA DE CHEQUES DE LA PERSONA PARA QUE PUEDA DISPONER DEL EFECTIVO"
	datos.usuariosolicitante = d.get("usuario")  # JORGE RIOS BELTRAN"
	datos.usuariopuesto = d.get("area", "")  # "Sistemas"
	datos.jefearea = d.get("jefeinmediato", "")  # CESAR MILANES"
	datos.jefeareapuesto = d.get("puestoinmediato", "")  # "SIBDIRECCION ADMINISTRATIVA"
	datos.fechasolicitud = fecha_descriptiva(d.get("fechacaptura"))  # Febrero 10, 2016"

	return datos


def obtenerPagocomision(pago=0):
	args = dict(pago=pago)
	datos = Datos()
	record = enbbcall("pagocomisionget", args)
	if record == dict():
		print("no se encontro pagocomision para generar reporte")
		raise ZenError(300)
	datos.idpago = record.get("id")
	datos.fechareconocimiento = record.get("fechareconocimiento", "")
	datos.pagoimporte = formato_comas.format(float(record.get("pagoimporte", 0)))
	datos.pagoimpuesto = formato_comas.format(float(record.get("pagoimpuesto", 0)))
	datos.pagoreferencia = record.get("pagoreferencia", "")
	datos.pagotipo = record.get("pagotipo", "")
	datos.solicitudcheque = "{:,}".format(record.get("solicitudcheque", 0))

	barcode = enbbcall("barcodepago", args)
	if barcode:
		pass
	else:
		print("no hay barcode")
		raise ZenError(300)

	datos.barcode = barcode
	lista = enbbcall("pagocomisiondetalle", args).get("pagocomisiondetalles", [])
	if lista:
		pass
	else:
		print("no se encontro pagocomisiondetalle")
		raise ZenError(300)

	datos.nombrevendedor = lista[0].get("nombrevendedor", "")
	listacomisiones = []
	for x in lista:
		nombreetapa = ""
		movimiento = x.get("id")
		documento = x.get("documento")
		inmueble = x.get("inmueble")
		cuenta = x.get("cuenta")
		sql = """select e.descripcion as etapa, 
		d.descripcion as desarrollo 
		from etapa e 
		join desarrollo d on e.fk_desarrollo = d.codigo 
		where e.codigo = {}""".format(
			x.get("etapa")
		)
		sql = preparaQuery(sql)
		for row in DBSession.execute(sql):
			nombreetapa = "{} - {}".format(
				row.desarrollo.decode("iso-8859-1").encode("utf-8"),
				row.etapa.decode("iso-8859-1").encode("utf-8"),
			)
		etapa = nombreetapa
		manzana = x.get("manzana")
		lote = x.get("lote")
		nombrecliente = x.get("nombrecliente", "")

		importe = formato_comas.format(x.get("importe"))
		listacomisiones.append(
			dict(
				movimiento=movimiento,
				documento=documento,
				inmueble=inmueble,
				cuenta=cuenta,
				etapa=etapa,
				manzana=manzana,
				lote=lote,
				importe=importe,
				nombrecliente=nombrecliente,
			)
		)
	datos.listacomisiones = listacomisiones
	return datos


def obtenerGeneraexcel(que):
	return que


def obtenerTramites(que):
	print("viendo aqui")
	print(que)
	return que


def obtenerOfertaventa(que):
	datos = Datos()
	datos.hoy = today()
	p = rdb_to_request(que)
	if True:
		try:
			nombre_gerente = ""
			gerente = p.get("gerente", "")
			etapa = p.get("etapa", "")
			datos.etapa = etapa
			datos.etapa_descripcion = ""

			sql = "select descripcion from etapa where codigo = {}".format(etapa)
			for x in DBSession.execute(sql):
				datos.etapa_descripcion = x.descripcion.decode("iso-8859-1")

			fechainicial = p.get("fechaInicial", "")
			fechafinal = p.get("fechaFinal", "")
			if fechainicial != "" and fechafinal != "":
				fini = get_date(fechainicial)
				ffin = get_date(fechafinal, minimal_date=False)
			elif fechainicial == "" and fechafinal != "":
				fini = get_date("")
				ffin = get_date(fechafinal, minimal_date=False)
			elif fechainicial != "" and fechafinal == "":
				fini = get_date(fechainicial)
				ffin = get_date("", minimal_date=False)
			elif fechainicial == "" and fechafinal == "":
				fini = get_date("")
				ffin = get_date("", minimal_date=False)

			estatus = p.get("estatus", "")
			orden = p.get("orden", "")

			select_list = """
				o.oferta as oferta,
				convert(varchar(10), o.fecha_oferta, 103) as fecha_oferta, 
				o.fk_etapa as etapa,
					case when o.fecha_cancelacion is null then '' else convert(varchar(10) , o.fecha_cancelacion, 103) end as fecha_cancelacion, 
				c.nombre as nombre_cliente, 
				c.telefonocasa as telefono,
				v.nombre as nombre_subvendedor, 
				E.codigo AS empresa, 
				D.codigo AS desarrollo, 
				E.razonsocial AS desc_empresa,
				D.descripcion AS desc_desarrollo, 
				T.descripcion AS desc_etapa,
				g.nombre as nombre_gerente
			"""
			select_list_cuantos = " count(*) as cuantos "
			sql = """ SELECT {}
				FROM ofertas_compra o, cliente c, vendedor v, EMPRESA E, DESARROLLO D, ETAPA T, gerentesventas g
				WHERE o.fk_etapa = T.codigo
				AND D.codigo = T.fk_desarrollo
				AND E.codigo = D.fk_empresa
				AND c.codigo = o.cliente
				AND v.codigo = o.subvendedor
				"""
			sql = """
				select {} from ofertas_compra o join
				etapa T on T.codigo = o.fk_etapa
				join desarrollo D on D.codigo = T.fk_desarrollo
				join empresa E on E.codigo = D.fk_empresa
				join cliente c on c.codigo = o.cliente
				join vendedor v on v.codigo = o.subvendedor
				join gerentesventas g on v.gerente = g.codigo
			"""
			if gerente:
				sql += " and v.gerente={}".format(gerente)
			if etapa:
				sql += " and o.fk_etapa={}".format(etapa)
			if True:
				sql += (
					" AND O.fecha_oferta >=  '{}'  AND o.fecha_oferta <= '{}'".format(
						fini, ffin
					)
				)
			if estatus:
				if int(estatus) == 1:
					sql += " AND o.cancelada = 0"
				if int(estatus) == 2:
					sql += " AND o.cancelada <> 0"

			sql += " ORDER BY E.Codigo, D.Codigo, o.fk_etapa"

			# order de reporte
			if orden:
				if int(orden) == 1:
					sql += " ,oferta"
				if int(orden) == 2:
					sql += " ,nombre_cliente"
				if int(orden) == 3:
					sql += " ,nombre_subvendedor"
				if int(orden) == 4:
					sql += " ,fecha_cancelacion"

			if True:
				rows = []
				for i, x in enumerate(
					DBSession.execute(preparaQuery(sql.format(select_list)))
				):
					datos.nombre_cliente = x.nombre_cliente.decode("iso-8859-1")
					datos.nombre_gerente = x.nombre_gerente.decode("iso-8859-1")
					datos.nombre_subvendedor = x.nombre_subvendedor.decode("iso-8859-1")
					rows.append(
						dict(
							id=i,
							oferta=x.oferta,
							fecha_oferta=x.fecha_oferta,
							etapa=x.etapa,
							fecha_cancelacion=x.fecha_cancelacion,
							nombre_cliente=datos.nombre_cliente,
							telefono=x.telefono,
							nombre_gerente=datos.nombre_gerente,
							nombre_subvendedor=datos.nombre_subvendedor,
						)
					)
				datos.rows = rows

				color("el reporte de ofertasventa tiene {} rows".format(len(rows)))

		except:
			print_exc()
			datos.rows = []
	return datos


def ofertaReciente(etapa=0, oferta=0, asignada=False):
	session = DBSession
	if not etapa and not oferta:
		if asignada:
			query = preparaQuery(
				"""select top 1 o.fk_etapa as fk_etapa, o.oferta as oferta  
			from ofertas_compra o join cuenta c on o.cuenta=c.codigo and c.fk_inmueble is not null 
			order by fk_etapa desc, oferta desc"""
			)
		else:
			query = "select top 1 fk_etapa, oferta  from ofertas_compra order by fk_etapa desc, oferta desc"
	else:
		query = "select fk_etapa, oferta  from ofertas_compra  where fk_etapa={} and oferta={} ".format(
			etapa, oferta
		)
	r = session.execute(query)
	for x in r:
		etapa = x.fk_etapa
		oferta = x.oferta
	assert etapa > 0 and oferta > 0, "no tiene nada"
	query = "select fk_desarrollo from etapa where codigo ={} ".format(etapa)
	r = session.execute(query)
	for x in r:
		desarrollo = x.fk_desarrollo
	assert desarrollo > 0, "no tiene desarrollo"
	query = "select fk_empresa from desarrollo where codigo ={} ".format(desarrollo)
	r = session.execute(query)
	for x in r:
		empresa = x.fk_empresa
	assert empresa > 0, "no tiene empresa"
	return [empresa, desarrollo, etapa, oferta]


def obtenerRecibo(recibo=None):
	args = dict(recibo=recibo)
	datos = Datos()
	print("mandando a llarmar obtenerRecibo")
	v = enbbcall("obtenrecibopago", args)
	print("regreso el valor de enbbcal en obtenerRecibo")
	for x in v:
		datos.__dict__[x] = v[x]
	datos.recibo = recibo
	return datos


def obtenerReciboBien(recibo=None):
	datos = Datos()
	datos.recibo = recibo
	session = DBSession
	meses = (
		"",
		"Enero",
		"Febrero",
		"Marzo",
		"Abril",
		"Mayo",
		"Junio",
		"Julio",
		"Agosto",
		"Septiembre",
		"Octubre",
		"Noviembre",
		"Diciembre",
	)
	formato_fecha = "a {} de {} de {}"
	fecha = datetime.now()
	cuenta = ""
	inmueble = ""
	cliente = ""
	nombreCliente = ""
	fechaEmision = ""
	fechaCaptura = ""
	inmuebleId = ""
	fechaPago = ""
	saldoActual = ""
	saldoPosterior = ""
	abonocapital = ""
	interesmoratorio = ""
	totalrecibo = ""
	referencia = ""
	consdesarrollo = ""
	desarrollo = ""
	saldo = ""
	listaMovimientos = []
	saltoLinea = 0
	iden1 = ""
	iden2 = ""

	datos.fecha = formato_fecha.format(fecha.day, meses[fecha.month], fecha.year)
	sql = """select convert(varchar(10), r.fechaemision, 103) as fechaemision, r.abonocapital as abonocapital,
	r.interesmoratorio as interesmoratorio, r.totalrecibo as totalrecibo, r.referencia as referencia,
	r.consdesarrollo as consdesarrollo, convert(varchar(10), r.fechacaptura, 103) as fechacaptura, c.codigo as cuenta,
	c.fk_inmueble as fk_inmueble, c.fk_cliente as fk_cliente, c.saldo as cuentasaldo,
	isnull(i.iden1, '') as iden1, isnull(i.iden2, '') as iden2, e.nombre as nombrecliente, t.descripcion as etapadescripcion,
	o.descripcion as desarrollodescripcion
	from RECIBO r
	join MOVIMIENTO m on m.numrecibo = r.codigo
	join DOCUMENTO d on d.codigo = m.fk_documento
	join CUENTA c on c.codigo = d.fk_cuenta
	left join INMUEBLE i on i.codigo = c.fk_inmueble
	join CLIENTE e on e.codigo = c.fk_cliente
	join ETAPA t on t.codigo = c.fk_etapa
	join DESARROLLO o on o.codigo = t.fk_desarrollo
	where r.codigo = {}""".format(
		recibo
	)
	for x in session.execute(sql):
		fechaAplicacion = x.fechaemision
		inmueble = x.fk_inmueble
		cuenta = x.cuenta
		iden1 = x.iden1
		iden2 = x.iden2
		cliente = x.fk_cliente
		nombreCliente = x.nombrecliente
		abonocapital = x.abonocapital
		interesmoratorio = x.interesmoratorio
		totalrecibo = x.totalrecibo
		referencia = x.referencia
		consdesarrollo = x.consdesarrollo
		[d, m, a] = x.fechaemision.split("/")
		fechaEmision = "{} {} {}".format(meses[int(m)], d, a)
		[e, f, g] = x.fechacaptura.split("/")
		fechaCaptura = "{} {} {}".format(meses[int(f)], e, g)
		desarrollo = x.desarrollodescripcion
		saldo = x.cuentasaldo
	aux = saldo + abonocapital
	saldoactual = formato_comas.format(aux)
	saldoposterior = formato_comas.format(saldo)
	abonocapital = formato_comas.format(abonocapital)
	interesmoratorio = formato_comas.format(interesmoratorio)
	totalrecibo = formato_comas.format(totalrecibo)

	sql = """select codigo as codigo , cantidad as cantidad , relaciondepago as relaciondepago,
	convert(varchar(10), fechavencimientodoc, 103) as fechavencimiento, fk_documento as documento
	from MOVIMIENTO
	where cargoabono = 'A' and fk_tipo = 4 and numrecibo = {}
	order by fechavencimientodoc
	""".format(
		recibo
	)
	for x in session.execute(sql):
		d = dict(
			codigo=x.codigo,
			cantidad=formato_comas.format(x.cantidad),
			relaciondepago=x.relaciondepago,
			fechavencimiento=x.fechavencimiento,
			documento=x.documento,
		)
		listaMovimientos.append(d)
	# saltoLinea = len(listaMovimientos)
	limiteLineas = 12
	if len(listaMovimientos) > 7:
		limiteLineas -= 1
	datos.lineas = len(a) - limiteLineas
	datos.fechaAplicacion = fechaAplicacion
	datos.inmueble = inmueble
	datos.cuenta = cuenta
	datos.cliente = cliente
	datos.iden1 = iden1
	datos.iden2 = iden2
	datos.nombrecliente = nombreCliente
	datos.abonocapital = abonocapital
	datos.interesmoratorio = interesmoratorio
	datos.totalrecibo = totalrecibo
	datos.referencia = referencia
	datos.consdesarrollo = consdesarrollo
	datos.fechaemision = fechaEmision
	datos.fechacaptura = fechaCaptura
	datos.desarrollo = desarrollo
	datos.saldo = saldo
	datos.aux = aux
	datos.saldoactual = saldoactual
	datos.saldoposterior = saldoposterior
	datos.abonocapital = abonocapital
	datos.interesmoratorio = interesmoratorio
	datos.totalrecibo = totalrecibo
	datos.listaMovimientos = listaMovimientos

	return datos


def obtenerRap(cliente=None):

	datos = Datos()
	session = DBSession
	meses = (
		"",
		"Enero",
		"Febrero",
		"Marzo",
		"Abril",
		"Mayo",
		"Junio",
		"Julio",
		"Agosto",
		"Septiembre",
		"Octubre",
		"Noviembre",
		"Diciembre",
	)
	formato_fecha = "a {} de {} de {}"
	fecha = datetime.now()
	datos.fecha = formato_fecha.format(fecha.day, meses[fecha.month], fecha.year)
	manzana = ""
	desarrollo = ""
	etapa = 0
	cuenta = ""
	lote = ""
	nombre = ""
	referencia = ""

	query3 = "select max(codigo) as cuenta from cuenta where fk_cliente = {}".format(
		cliente
	)
	cuenta = 0
	for r in session.execute(query3):
		cuenta = r.cuenta
	query2 = """ select o.fk_etapa as etapa,
	d.descripcion as desarrollo from ofertas_compra o 
	join etapa e on o.fk_etapa = e.codigo
	join desarrollo d on e.fk_desarrollo = d.codigo where o.cuenta = {}
	""".format(
		cuenta
	)

	datos.desarrollo = ""
	datos.etapa = ""
	for x in session.execute(preparaQuery(query2)):
		datos.etapa = x.etapa
		datos.desarrollo = x.desarrollo.decode("iso-8859-1")

	query = """

		select 
		coalesce(i.iden2,'') as manzana, coalesce(i.iden1, '') as lote, c.codigo as cuenta,
		x.nombre as nombre
		from cuenta c left join inmueble i on c.fk_inmueble = i.codigo
		join cliente x on c.fk_cliente = x.codigo
		where c.fk_cliente={}
	""".format(
		cliente
	)
	query = preparaQuery(query)
	# print "este el query que truena",query
	datos.manzana = ""
	# datos.desarrollo=""
	# datos.etapa=""
	datos.lote = ""
	datos.cuenta = ""
	datos.nombre = ""
	datos.referencia = ""
	datos.cliente = cliente
	r = session.execute(query)
	for x in r:
		datos.manzana = x.manzana.decode("iso-8859-1")
		# datos.desarrollo=x.desarrollo
		# datos.etapa=x.etapa
		datos.lote = x.lote.decode("iso-8859-1")
		datos.cuenta = x.cuenta
		datos.nombre = x.nombre.decode("iso-8859-1")
	query = """
		select top 1 referencia as referencia from referencias_rap where cliente={}
	""".format(
		cliente
	)
	if datos.cuenta:
		query = "{} and cuenta={}".format(query, datos.cuenta)
	query = "{} order by cuenta desc ".format(query)
	query = preparaQuery(query)
	# print "este es el query", query
	r = session.execute(query)
	for x in r:
		datos.referencia = x.referencia

	return datos


def obtenerCaracteristicas(etapa=0, oferta=0):
	etapa, oferta = ofertaReciente(etapa, oferta, asignada=True)[2:]
	meses = (
		"",
		"Enero",
		"Febrero",
		"Marzo",
		"Abril",
		"Mayo",
		"Junio",
		"Julio",
		"Agosto",
		"Septiembre",
		"Octubre",
		"Noviembre",
		"Diciembre",
	)
	formato_fecha = "{} de {} de {}"
	cliente, imueble, idprecioetapa = "", "", ""
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
			""".format(
		etapa, oferta
	)
	sql = preparaQuery(sql)
	r = session.execute(sql)
	datos = Datos()
	print("los datos del query son ")
	idprecioetapa = ""
	for x in r:
		datos.descripcion_etapa = x.descripcion.decode("iso-8859-1")
		datos.cuenta = x.cuenta
		descuento_precio = autorizacion_descuento_por_cuenta(x.cuenta)
		dia, mes, ano = x.fecha_oferta.split("/")
		if dia[0] == "0":
			dia = dia[1:]
		mes = int(mes)
		datos.fecha_oferta = formato_fecha.format(dia, meses[mes], ano)
		datos.fecha_asignacion = x.fecha_asignacion
		# datos.iden2=x.iden2.decode("iso-8859-1")
		# datos.iden1=x.iden1
		datos.iden2 = x.iiden2.decode("iso-8859-1")
		datos.iden1 = x.iiden1.decode("iso-8859-1")
		datos.fk_preciosetapaasignacion = x.fk_preciosetapaasignacion
		datos.precioasignacion = x.precioasignacion
		datos.precioasignacion_comas = formato_comas.format(x.precioasignacion)
		datos.descuento_precio = descuento_precio
		datos.descuento_precio_comas = formato_comas.format(descuento_precio)
		datos.preciomenosdescuento = float(x.precioasignacion) - descuento_precio
		datos.preciomenosdescuento_comas = formato_comas.format(
			datos.preciomenosdescuento
		)
		datos.fk_preciosetapaoferta = x.fk_preciosetapaoferta
		datos.preciooferta = x.preciooferta
		datos.fechaasignacion = x.fechaasignacion
		datos.fk_inmueble = x.fk_inmueble
		datos.iiden2 = x.iiden2
		datos.iiden1 = x.iiden1
		datos.inmueble = x.inmueble
		datos.idprospecto = x.idprospecto
		datos.fk_cliente = x.fk_cliente
		datos.nombre = x.nombre.decode("iso-8859-1")
		inmueble = x.fk_inmueble
		idprecioetapa = x.fk_preciosetapaasignacion
		datos.oferta = oferta
		datos.etapa = etapa
		# print x
	# print "el idprecioetapa es",idprecioetapa
	assert (
		idprecioetapa != ""
	), "el inmueble no tiene idprecioetapa, lo cual puede ser que no este asignada"
	sql = """
		select p.id, p.cantidad as cantidad, c.descripcion as descripcion from gixpreciosetapacaracteristicas p
		join gixcaracteristicasinmuebles c on c.id = p.fk_idcaracteristica
		where p.fk_idpreciosetapa = {} order by c.descripcion
		""".format(
		idprecioetapa
	)
	sql = preparaQuery(sql)
	r = session.execute(sql)
	# print "listan caracteristicas"
	caracteristicas = 0

	listacaracteristicas = []
	for x in r:
		datos.descripcion = x.descripcion.decode("iso-8859-1")
		datos.cantidad = float(x.cantidad)
		print(datos.cantidad, datos.descripcion)
		listacaracteristicas.append(
			dict(cantidad=datos.cantidad, descripcion=datos.descripcion)
		)

		caracteristicas += 1
	datos.listacaracteristicas = listacaracteristicas
	# print "datos.listacaracteristicas", datos.listacaracteristicas
	jump = ["<br>" for salto in range(caracteristicas, 18)]
	jump = "".join(jump)
	datos.jump = jump
	return datos


def obtenerOferta(etapa=0, oferta=0):
	session = DBSession
	meses = (
		"",
		"Enero",
		"Febrero",
		"Marzo",
		"Abril",
		"Mayo",
		"Junio",
		"Julio",
		"Agosto",
		"Septiembre",
		"Octubre",
		"Noviembre",
		"Diciembre",
	)
	formato_fecha = "{} de {} de {}"
	query = """
		select convert(varchar(10), o.fecha_oferta, 103) as fecha,
		e.razonsocial as razonsocial,
		c.nombre as nombre, c.rfc as rfc, c.imss as imss , c.domicilio as domicilio,
		c.telefonocasa as telefonocasa, d.descripcion as descripcion,
		d.localizacion as localizacion, d.estado as estado, o.precio as precio,
		o.monto_credito as monto_credito, o.apartado as apartado,
		o.oferta as oferta, fk_etapa as etapa,
		o.cuenta as cuenta
		from ofertas_compra o
		join cliente c on o.cliente = c.codigo
		join empresa e on {} = e.codigo
		join desarrollo d on {} = d.codigo
		where o.fk_etapa = {} and o.oferta = {}
		""".format(
		*ofertaReciente(etapa, oferta)
	)
	sql = preparaQuery(query)
	resultado = session.execute(sql)

	datos = Datos()
	for r in resultado:
		descuento_precio = autorizacion_descuento_por_cuenta(r.cuenta)
		dia, mes, ano = r.fecha.split("/")
		mes = int(mes)
		if dia[0] == "0":
			dia = dia[1:]
		datos.fecha = formato_fecha.format(dia, meses[mes], ano)
		precio = float(r.precio) - descuento_precio

		datos.razonsocial = r.razonsocial.decode("iso-8859-1")
		datos.nombre = r.nombre.decode("iso-8859-1")
		datos.rfc = r.rfc
		datos.imss = r.imss
		datos.domicilio = r.domicilio.decode("iso-8859-1")
		datos.telefonocasa = r.telefonocasa
		datos.descripcion = r.descripcion.decode("iso-8859-1")
		datos.localizacion = r.localizacion.decode("iso-8859-1")
		datos.estado = r.estado
		datos.precio = float(precio)
		datos.precio_letras = aletras(float(precio), tipo="pesos")
		datos.precio_comas = formato_comas.format(precio)
		datos.monto_credito = float(r.monto_credito)
		datos.monto_credito_letras = aletras(float(r.monto_credito), tipo="pesos")
		datos.monto_credito_comas = formato_comas.format(r.monto_credito)
		datos.apartado = float(r.apartado)
		datos.apartado_letras = aletras(float(r.apartado), tipo="pesos")
		datos.apartado_comas = formato_comas.format(r.apartado)
		datos.oferta = r.oferta
		datos.etapa = r.etapa
		datos.cancelacion_letras = aletras(1000, tipo="pesos")
		datos.cancelacion_comas = formato_comas.format(1000)
		# print "valores", [(x,getattr(datos,x)) for x in datos.__dict__]

	return datos


def obtenerOtro():
	datos = Datos()

	datos.fecha = datetime.now().isoformat()
	return datos


def obtenerOtro2(que):
	datos = Datos()

	datos.fecha = datetime.now().isoformat()
	datos.parameters = rdb_to_request(que)
	return datos


def obtenerAnexo(etapa=0, oferta=0, precalificacion=0, avaluo=0, subsidio=0, pagare=0):
	session = DBSession
	datos = Datos()
	psql = preparaQuery
	meses = (
		"",
		"Enero",
		"Febrero",
		"Marzo",
		"Abril",
		"Mayo",
		"Junio",
		"Julio",
		"Agosto",
		"Septiembre",
		"Octubre",
		"Noviembre",
		"Diciembre",
	)
	formato_fecha = "{} de {} de {}"
	fecha = datetime.now()
	datos.fecha = formato_fecha.format(fecha.day, meses[fecha.month], fecha.year)
	# print "fecha",datos.fecha
	cuenta = 0
	if not etapa and not oferta:
		query = """select top 1  c.codigo as cuenta,
		o.fk_etapa as fk_etapa, o.oferta as oferta  
		from ofertas_compra o 
		join cuenta c on o.cuenta=c.codigo
		and c.fk_inmueble is not null 
		order by o.fk_etapa desc, o.oferta desc"""
		query = preparaQuery(query)
		print(query)
	else:
		query = """select o.cuenta as cuenta, o.fk_etapa as fk_etapa, o.oferta as oferta,
		e.descripcion as etapanombre  from ofertas_compra o join etapa e on o.fk_etapa = e.codigo
		where o.fk_etapa={} and o.oferta={} """.format(
			etapa, oferta
		)
	r = session.execute(query)

	for x in r:
		print(x.cuenta)
		cuenta = x.cuenta
		datos.etapa = x.fk_etapa
		datos.oferta = x.oferta
		etapanombre = x.etapanombre.decode("iso-8859-1")

	assert cuenta, "no tiene cuenta"
	sql = """select c.nombre as nombre, coalesce(i.iden2, '') as iden2, coalesce(i.iden1, '') as iden1 
	from cliente c join cuenta x on c.codigo=x.fk_cliente 
	left join inmueble i on x.fk_inmueble=i.codigo 
	where x.codigo={}""".format(
		cuenta
	)
	r = session.execute(psql(sql))
	nombre = ""
	for x in r:
		nombre = x.nombre.decode("iso-8859-1")
		datos.iden2 = x.iden2.decode("iso-8859-1")
		datos.iden1 = x.iden1
	assert nombre, "no tiene nombre"

	# print "cuenta", cuenta
	sql_apartado = """select isnull( sum(m.cantidad),0) as apartados from movimiento m join documento d on m.fk_documento = d.codigo 
	where d.fk_cuenta =  {}   and m.cargoabono = 'A' and m.relaciondepago = 'Apartado'
	and d.fk_tipo not in (15,16)""".format(
		cuenta
	)

	r = session.execute(psql(sql_apartado))
	for x in r:
		apartados = x.apartados
	# print apartados
	# apartados=r.scalar()

	sql_abonos = """select isnull( sum(m.cantidad),0) as abonos from movimiento
	m join documento d on m.fk_documento = d.codigo 
	where d.fk_cuenta =  {}   and m.cargoabono = 'A' 
	and m.relaciondepago <> 'Apartado'
	and d.fk_tipo not in (15,16)""".format(
		cuenta
	)
	r = session.execute(psql(sql_abonos))
	for x in r:
		abonos = x.abonos

	sql_prerecibos = """select isnull( sum(cantidad),0) as prerecibos from prerecibo where 
	fk_cuenta ={} and fk_recibo is null """.format(
		cuenta
	)
	r = session.execute(psql(sql_prerecibos))
	for x in r:
		prerecibos = x.prerecibos

	f1 = "${:,.2f}"
	f2 = "({})"
	v = f1.format(apartados)
	if apartados < 0:
		v = f2.format(v)
	datos.apartados = v
	v = f1.format(precalificacion)
	if precalificacion < 0:
		v = f2.format(v)
	datos.precalificacion = v

	v = f1.format(avaluo)
	if avaluo < 0:
		v = f2.format(v)
	datos.avaluo = v

	v = f1.format(subsidio)
	if subsidio < 0:
		v = f2.format(v)
	datos.subsidio = v

	v = f1.format(pagare)
	if pagare < 0:
		v = f2.format(v)
	datos.pagare = v

	v = f1.format(abonos)
	if abonos < 0:
		v = f2.format(v)
	datos.abonos = v
	v = f1.format(prerecibos)
	if prerecibos < 0:
		v = f2.format(v)
	datos.prerecibos = v
	datos.nombre = nombre
	datos.etapanombre = etapanombre

	# print "vale datos"
	# print "valores", [(x,getattr(datos,x)) for x in datos.__dict__]
	return datos


def obtenerCancelacion(cliente, cuenta, user, etapa, devolucion=0):
	session = DBSession
	datos = Datos()
	psql = preparaQuery
	meses = (
		"",
		"Enero",
		"Febrero",
		"Marzo",
		"Abril",
		"Mayo",
		"Junio",
		"Julio",
		"Agosto",
		"Septiembre",
		"Octubre",
		"Noviembre",
		"Diciembre",
	)
	hoy = today(False)
	fecha = "{} dias del mes de {} del {}".format(hoy.day, meses[hoy.month], hoy.year)
	sql = "select nombre from cliente where codigo = {}".format(cliente)
	r = session.execute(psql(sql))
	for x in r:
		nombre = x.nombre.decode("iso-8859-1")

	sql = """select d.descripcion as desarrollo from etapa e join desarrollo d 
	on e.fk_desarrollo = d.codigo where e.codigo = {}
	""".format(
		etapa
	)

	r = session.execute(psql(sql))
	for x in r:
		desarrollo = x.desarrollo.decode("iso-8859-1")

	datos.nombre = nombre
	datos.fecha = fecha
	datos.devolucion = devolucion
	datos.cliente = cliente
	datos.cuenta = cuenta

	datos.desarrollo = desarrollo
	datos.hayDevolucion = devolucion > 0
	if datos.hayDevolucion:
		datos.devolucion = formato_comas.format(float(devolucion))
	return datos


def obtenerDocscliente(cliente, cuenta, user, etapa, devolucion=0):
	print("entrando a docscliente")
	try:
		session = DBSession
		datos = Datos()
		psql = preparaQuery
		meses = (
			"",
			"Enero",
			"Febrero",
			"Marzo",
			"Abril",
			"Mayo",
			"Junio",
			"Julio",
			"Agosto",
			"Septiembre",
			"Octubre",
			"Noviembre",
			"Diciembre",
		)

		elaboro = "Personal de Comercializacion"
		if True:

			rdb.connect(
				cached_results.settings.get("rethinkdb.host"),
				cached_results.settings.get("rethinkdb.port"),
			).repl()
			table = rdb.db("iclar").table("usuarios")
			for x in table.filter(dict(appid=user)).run():
				elaboro = x.get("nombre", elaboro)

		hoy = today(False)
		fecha = "{:02d}/{:02d}/{:04d}".format(hoy.day, hoy.month, hoy.year)
		sql = "select nombre from cliente where codigo = {}".format(cliente)
		r = session.execute(psql(sql))
		for x in r:
			nombre = x.nombre.decode("iso-8859-1")

		sql = """select d.descripcion as desarrollo from etapa e join desarrollo d 
		on e.fk_desarrollo = d.codigo where e.codigo = {}
		""".format(
			etapa
		)

		r = session.execute(psql(sql))
		for x in r:
			desarrollo = x.desarrollo.decode("iso-8859-1")

		sql = """select fk_inmueble as inmueble 
			from cuenta_cancelada where codigo = {} and fk_inmueble > 0""".format(
			cuenta
		)

		inmueble = ""
		r = session.execute(psql(sql))
		for x in r:
			inmueble = x.inmueble

		iden2 = ""
		iden1 = ""
		if inmueble:
			sql = """select iden2  , iden1  from inmueble
			where codigo = {}""".format(
				inmueble
			)

			r = session.execute(psql(sql))
			for x in r:
				iden1 = x.iden1
				iden2 = x.iden2

		sql = """
		select m.numrecibo as recibo, m.cantidad as cantidad, m.relaciondepago as descripcion,
		m.fecha as fechaPago, m.fk_documento as documento from movimiento_cancelado m
		join documento_cancelado d on m.fk_documento = d.codigo join cuenta_cancelada c on
		d.fk_cuenta = c.codigo where m.cargoabono = 'A' and c.codigo = {} order by m.codigo
		""".format(
			cuenta
		)
		listaDocumentos = []
		total = 0
		r = session.execute(psql(sql))
		for x in r:
			datos.recibo = x.recibo
			datos.cantidad = formato_comas.format(x.cantidad)
			total += float(x.cantidad)
			datos.descripcion = x.descripcion.decode("iso-8859-1")
			datos.fechaPago = "{:02d}/{:02d}/{:04d}".format(
				x.fechaPago.day, x.fechaPago.month, x.fechaPago.year
			)
			datos.documento = x.documento
			listaDocumentos.append(
				dict(
					recibo=datos.recibo,
					cantidad=datos.cantidad,
					descripcion=datos.descripcion,
					fechaPago=datos.fechaPago,
					documento=datos.documento,
				)
			)

		datos.total = formato_comas.format(total)
		datos.listaDocumentos = listaDocumentos
		datos.nombre = nombre
		datos.fecha = fecha
		datos.cuenta = cuenta
		datos.cliente = cliente
		datos.inmueble = inmueble
		datos.iden2 = iden2
		datos.iden1 = iden1
		datos.desarrollo = desarrollo
		datos.elaboro = elaboro
		# print("ya termine docscliente con datos")
	except:
		print_exc()
		raise ZenError(1)
	return datos


def pdfCreate(datos, template="calar.html", tipo="oferta"):
	rlLista = ["resumen", "tramites", "generaexcel"]
	if tipo in rlLista:
		pass
	else:
		filename = "./zen/report/{}".format(template)
		mytemplate = Template(filename=filename, input_encoding="utf-8")
		buf = StringIO()

		try:
			ctx = Context(buf, datos=datos)
			mytemplate.render_context(ctx)
			html = buf.getvalue().encode("utf-8")
		except:
			print_exc()
			return (False, "")

	llave = "X"
	if tipo == "oferta" or tipo == "caracteristicas" or tipo == "anexo":
		llave = "{:02d}_{:010d}".format(int(datos.etapa), int(datos.oferta))
	elif tipo == "rap":
		llave = "{:010d}".format(int(datos.cliente))
	elif tipo == "cancelacion" or tipo == "docscliente":
		llave = "{:06d}_{:06d}".format(int(datos.cuenta), int(datos.cliente))
	elif tipo == "tramites":
		llave = "{:04d}".format(int(datos))
	elif tipo == "recibo":
		llave = "{:04d}".format(int(datos.recibo))

	try:
		nombre = pdfFileName(tipo=tipo, llave=llave)
		# si hay reportes generados con rlab las funciones aqui, y empiezan con rlab
		if tipo == "resumen":
			rlabResumen(nombre)
		if tipo == "tramites":
			rlabTramites(nombre, datos)
		if tipo == "generaexcel":
			rlabGeneraExcel(nombre, datos)

		if tipo in rlLista:
			return (True, nombre)

		with open(nombre, "wb") as f:
			pdf = pisa.CreatePDF(StringIO(html), f)
		if not pdf.err:
			print("si se genero pdf ")
			return (True, nombre)
		else:
			print("no se genero")
			return (False, "")
	except:
		print_exc()
		return (False, "")
	return (False, "")


def record(row):
	return dict(
		id=row.id,
		extended=row.extended,
		author=row.author,
		title=row.title,
		intro=row.intro,
		published_at=row.publishedAt.isoformat(),
	)


def errorJson(d, error=400):
	return Response(json.dumps(d), content_type="application/json", status_int=error)


def raiz(request):
	app = request.params.get("app", "zeniclar")
	print("viendo app ", app)
	current = redis_conn.get("{}-current-deploy".format(app))
	version = request.params.get("version", "")
	if version:
		current = version
	html = redis_conn.get(current)
	# html="<html><body>hola</body></html>"
	return Response(html, content_type="text/html", status_int=200)


raiz2 = raiz


def dec_enc(what, trim_value=False, field=None):
	if trim_value:
		what = what.strip()
	return what.decode("iso-8859-1").encode("utf-8")


def ayp(lista, valor):
	lista.append(valor)
	print(valor)
	return


def get_token(request):
	token = ""
	valid = True
	try:
		stoken = request.headers.get("authorization", "")
		assert stoken, "header for authorization not present"
		token = stoken.split(" ")[-1]
		assert str(token) in cached_results.dicAuthToken, "token {} invalido".format(
			token
		)
	except:
		print_exc()
		valid = False
	return (valid, token)


class OperacionesAfiliacion(object):
	def actualiza_prospecto_en_rdb(self, prospecto, afiliacion):
		rdb.connect(
			cached_results.settings.get("rethinkdb.host"),
			cached_results.settings.get("rethinkdb.port"),
		).repl()
		table = rdb.db("iclar").table("afiliaciones_disponibles")
		ts = rdb.expr(datetime.now(rdb.make_timezone("00:00")))
		print("en actualiza_prospecto_rdb()", prospecto, afiliacion)
		table.filter(dict(afiliacion=afiliacion)).update(
			dict(prospecto=prospecto, timestamp=ts)
		).run()
		return

	def obtenAfiliacionDisponible(self, soloVer=False):

		rdb.connect(
			cached_results.settings.get("rethinkdb.host"),
			cached_results.settings.get("rethinkdb.port"),
		).repl()
		table = rdb.db("iclar").table("afiliaciones_disponibles")
		cual = table.filter(dict(bloqueado=False))["afiliacion"].min().default("").run()
		if cual:
			sql = """
			select count(*) as cuantos from gixprospectos 
			where afiliacionimss = '{}'
			and congelado = 0
			""".format(
				cual
			)
			cuantos = 0
			for x in DBSession.execute(preparaQuery(sql)):
				cuantos = x.cuantos

			if cuantos > 0:
				color("La afiliacion {} ya esta en la base de datos".format(cual))
				return ""

			if soloVer:
				pass
			else:
				table.filter(dict(afiliacion=cual)).update(dict(bloqueado=True)).run()

		return cual

	def obtenDisponibles(self):
		rdb.connect(
			cached_results.settings.get("rethinkdb.host"),
			cached_results.settings.get("rethinkdb.port"),
		).repl()
		table = rdb.db("iclar").table("afiliaciones_disponibles")
		cuantos = table.filter(dict(bloqueado=False)).count().run()
		return cuantos


class EAuth(object):
	def auth(self, content=None, get_token=False):
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
			# regresa tres palabras en el header Authorization separadas por espacio y la ultima es el token
			stoken = self.request.headers.get("authorization", "")
			print("authorization contiene", stoken or " !basura ")

			token = stoken.split(" ")[-1]

		except:
			print_exc()

		ok = True
		print("el token de autorizacion es ", token)
		error_message = ""
		try:
			assert (
				str(token) in cached_results.dicAuthToken
			), "token {} invalido".format(token)
		except AssertionError as e:
			print_exc()
			self.request.response.status = 401
			ok = False
			error_message = e.args[0]

		if not ok:
			if get_token:
				return (False, dict(error=error_message), token)
			else:
				return (False, dict(error=error_message))
		if content:
			if get_token:
				return (True, self.request.json_body.get(content), token)
			else:
				return (True, self.request.json_body.get(content))
		if get_token:
			return (True, dict(), token)
		return (True, dict())


class QueryAndErrors(object):
	def edata_error(self, mensaje):
		try:
			self.poolconn.close()
		except:
			pass

		return dict(errors=dict(resultado=[mensaje]))

	def logqueries(self, queries):

		if self.hazqueries and cached_results.settings.get("pyraconfig") == "test":
			rdb.connect(
				cached_results.settings.get("rethinkdb.host"),
				cached_results.settings.get("rethinkdb.port"),
			).repl()
			color("Grabando en rethinkdb los queries {}".format(today()), "b")
			t_queries = rdb.db("iclar").table("zen_queries")
			t_queries.delete().run()

			for i, x in enumerate(queries, 1):
				x = x.strip()
				t_queries.insert(
					dict(fecha=today(), query=x, tipo=x.split(" ")[0], consecutivo=i)
				).run()

	def commit(self, sql):
		ok = True
		error = ""
		try:
			c = self.cn_sql
			cu = c.cursor()
			cu.execute(sql)
			c.commit()
		except:
			ok = False
			error = l_traceback()
			c.rollback()
		return (ok, error)


@resource(collection_path="api/mia", path="api/mia/{id}")
class Mia:
	def __init__(self, request, context=None):
		self.request = request
		self.modelo = "xmenu"

	def collection_get(self):
		return dict(ok="ok")


@resource(collection_path="api/xmenus", path="api/xmenus/{id}")
class MenuItemsRest(EAuth, QueryAndErrors):
	def __init__(self, request, context=None):
		self.request = request
		self.modelo = "xmenu"

	def store(self, record):

		rdb.connect(
			cached_results.settings.get("rethinkdb.host"),
			cached_results.settings.get("rethinkdb.port"),
		).repl()

		menuitems = rdb.db("iclar").table("menuitems")
		item = record.get("item")
		try:
			cuantos = menuitems.filter(dict(item=item)).count().run()

			if cuantos:
				menuitems.filter(dict(item=item)).update(record).run()
				return dict(xmenus=[])

			# record["ts"] = rdb.expr( datetime.now( rdb.make_timezone('00:00') ))
			menuitems.insert(record).run()
			id = ""

			for x in menuitems.filter(dict(item=item)).run():
				id = x.get("id")

			assert id, "no se grabo , no hay id"
			# newrecord = dict(id = id)
			# for key in record:
			# 	newrecord[key] = record[key]
			color("id es {}".format(id))
		except:
			print_exc()
			self.request.response.status = 400
			error = "error al grabar"
			return self.edata_error(error)

		return dict(xmenus=[])

	@view(renderer="json")
	def collection_post(self):
		print("inserting xmenus in rethinkdb")
		que, record, token = self.auth(self.modelo, get_token=True)
		self.usertoken = token
		if not que:
			return record
		user = cached_results.dicTokenUser.get(token)
		usuario = user.get("usuario", "")
		perfil = user.get("perfil", "")
		if usuario.upper() not in ("SMARTICS", "JORGERIOS"):
			self.request.response.status = 400
			error = "usuario no autorizado"
			return self.edata_error(error)
		return self.store(record)

	def collection_get(self):
		que, record, token = self.auth(get_token=True)
		if not que:
			return dict(xmenus=[])
		rdb.connect(
			cached_results.settings.get("rethinkdb.host"),
			cached_results.settings.get("rethinkdb.port"),
		).repl()

		menuitems = rdb.db("iclar").table("menuitems")
		resultado = []
		for i, x in enumerate(menuitems.order_by("item").run(), 1):
			resultado.append(
				dict(
					id=i,
					item=x.get("item"),
					intro=x.get("intro"),
					title=x.get("title"),
					fecha=x.get("fecha", ""),
					consulta=x.get("consulta", False),
				)
			)
		return dict(xmenus=resultado)


@resource(collection_path="api/xvendedors", path="api/xvendedors/{id}")
class VendedorsRDBRest(EAuth, QueryAndErrors):
	def __init__(self, request, context=None):
		self.request = request
		self.modelo = "xvendedor"

	def store(self, record):

		rdb.connect(
			cached_results.settings.get("rethinkdb.host"),
			cached_results.settings.get("rethinkdb.port"),
		).repl()

		usuariosvendedores = rdb.db("iclar").table("usuariosvendedores")
		usuarios = rdb.db("iclar").table("usuarios")
		usuario = record.get("usuario")
		vendedor = record.get("vendedor")

		try:
			cuantos = usuariosvendedores.filter(dict(usuario=usuario)).count().run()

			if cuantos:
				usuariosvendedores.filter(dict(usuario=usuario)).update(record).run()
				return dict(xvendedors=[])

			# record["ts"] = rdb.expr( datetime.now( rdb.make_timezone('00:00') ))
			usuariosvendedores.insert(record).run()
			id = ""
			assert (
				usuarios.filter(dict(usuario=usuario)).count().run() == 0
			), "ya esta en usuarios"
			appid = usuarios.max("appid").run()["appid"] + 1

			usuarios.insert(
				dict(
					appid=appid,
					activo=True,
					domains=["zen"],
					hasTwoFactorAuthentication=True,
					isTwoFactorAuthenticated=False,
					iamuser="tablethp",
					password=str(uuid.uuid4())[-5:],
					zen_profile="vendedor",
					usuario=usuario,
				)
			).run()

			for x in usuariosvendedores.filter(dict(usuario=usuario)).run():
				id = x.get("id")

			assert id, "no se grabo , no hay id"
			color("id es {}".format(id))
		except:
			print_exc()
			self.request.response.status = 400
			error = "error al grabar"
			return self.edata_error(error)

		return dict(xvendedors=[])

	@view(renderer="json")
	def collection_post(self):
		print("inserting usuariosvendedores in rethinkdb")
		que, record, token = self.auth(self.modelo, get_token=True)
		self.usertoken = token
		if not que:
			return record
		user = cached_results.dicTokenUser.get(token)
		usuario = user.get("usuario", "")
		perfil = user.get("perfil", "")
		if usuario.upper() not in ("SMARTICS", "JORGERIOS"):
			self.request.response.status = 400
			error = "usuario no autorizado"
			return self.edata_error(error)
		return self.store(record)

	def collection_get(self):
		que, record, token = self.auth(get_token=True)
		if not que:
			return dict(xmenus=[])
		rdb.connect(
			cached_results.settings.get("rethinkdb.host"),
			cached_results.settings.get("rethinkdb.port"),
		).repl()

		usuariosvendedores = rdb.db("iclar").table("usuariosvendedores")
		resultado = []
		for i, x in enumerate(usuariosvendedores.order_by("usuario").run(), 1):
			resultado.append(
				dict(id=i, usuario=x.get("usuario"), vendedor=x.get("vendedor"))
			)
		return dict(xvendedors=resultado)


@resource(collection_path="api/cancelacions", path="api/cancelacions/{id}")
class Cancelacion(EAuth, QueryAndErrors):
	def __init__(self, request, context=None):
		self.request = request
		self.modelo = "cancelacion"

	def commits(self, lista):
		ok = True
		error = ""
		try:
			c = self.cn_sql
			cu = c.cursor()
			for sql in lista:
				cu.execute(sql)
			c.commit()
		except:
			ok = False
			print_exc()
			error = l_traceback()
			c.rollback()
		return (ok, error)

	def store(self, record):
		try:
			psql = preparaQuery
			cuenta = record.get("cuenta", "")
			assert cuenta, "no existe cuenta"
			total = record.get("total", 0)
			conDevolucion = record.get("conDevolucion")
			documentosConRecibosADevolver = record.get(
				"documentosConRecibosADevolver", ""
			)
			if documentosConRecibosADevolver:
				documentosConRecibosADevolver = documentosConRecibosADevolver[:-1]
			aDevolver = documentosConRecibosADevolver.split(",")

			queries = []

			generarSolicitudCheque = record.get("generarSolicitudCheque")
			reactivable = record.get("reactivable")
			record["id"] = record.get("cuenta", 0)

			sql = """
			select count(*) as cuantos from tramites_ventas_movimientos
			where fk_tramite = 103 and fecha is not null and fk_inmueble in
			( select fk_inmueble from cuenta where codigo = {})

			""".format(
				cuenta
			)
			for x in DBSession.execute(psql(sql)):
				cuantos = x.cuantos

			assert (
				cuantos == 0
			), "No se puede cancelar porque la vivienda esta en escrituras"

			inmueble = 0
			etapa = 0
			sql = """ select fk_inmueble as inmueble, fk_etapa as etapa from cuenta where codigo = {}""".format(
				cuenta
			)
			for x in DBSession.execute(sql):
				inmueble = x.inmueble
				etapa = x.etapa

			if inmueble:
				campos = "codigo,fecha, saldo, fk_cliente, fk_inmueble, fk_tipo_cuenta, contrato, tipo_contrato, inmueble_anterior, oferta_anterior, fk_etapa, fecha_prerecibo, monto_prerecibo, recibo_prerecibo, bruto_precalificacion, avaluoimpuesto_precalificacion, subsidio_precalificacion, pagare_precalificacion"
				sql = """ insert into desasignacion( {} ) select {} from cuenta where codigo = {}
				""".format(
					campos, campos, cuenta
				)
				sql = psql(sql)
				queries.append(sql)

			sql = """
			select m.codigo as movimiento, m.fk_documento as documento, m.cantidad as cantidad,
			m.numrecibo as recibo, m.fechavencimientodoc as fecha from movimiento m
			join documento d on d.codigo = m.fk_documento where d.fk_cuenta = {} and
			m.numrecibo is not null and m.cargoabono = 'A'
			""".format(
				cuenta
			)

			abonos = []
			for x in DBSession.execute(psql(sql)):
				abonos.append(
					dict(
						movimiento=x.movimiento,
						recibo=x.recibo,
						documento=x.documento,
						fecha=x.fecha,
						abono=x.cantidad,
					)
				)

			for abono in abonos:
				f = abono.get("fecha")
				fecha = "{:04d}{:02d}{:02d}".format(f.year, f.month, f.day)

				sql = "exec DesAbonaDocyCtaSP {},{},'{}'".format(
					abono.get("documento"), abono.get("abono"), fecha
				)
				queries.append(sql)
				devolver = "N"
				if str(abono.get("documento")) in aDevolver:
					devolver = "S"
				sql = """
				update recibo set status = 'C', devolucion = '{}' WHERE codigo = {}
				""".format(
					devolver, abono.get("recibo")
				)
				queries.append(psql(sql))

				sql = "exec Cancela_MovimientoSP  {}".format(abono.get("movimiento"))
				queries.append(psql(sql))

			sql = """
			select codigo as documento from documento where fk_cuenta = {}
			""".format(
				cuenta
			)
			documentos = []
			for x in DBSession.execute(psql(sql)):
				documentos.append(x.documento)

			for documento in documentos:
				sql = "exec Cancela_DocumentoSP {}".format(documento)
				queries.append(sql)

			sql = """
			select fk_etapa as etapa, oferta, cliente, subvendedor from ofertas_compra where cuenta = {}
			""".format(
				cuenta
			)
			for x in DBSession.execute(psql(sql)):
				oferta = x.oferta
				cliente = x.cliente
				subvendedor = x.subvendedor
				etapa = x.etapa

			sql = """
			insert into gixdesasignacion( fketapa,fkoferta,fkcuenta,fkinmueble,fkcliente,fksubvendedor) 
			values ({},{},{},{},{},{})
			""".format(
				etapa, oferta, cuenta, inmueble, cliente, subvendedor
			)
			queries.append(psql(sql))

			sql = """ exec Cancela_CuentaSPNew {}, {}""".format(cuenta, inmueble)
			queries.append(sql)

			sql = """
			delete from comision where
			fk_cuenta = {} and codigo in 
			( select distinct fk_comision from anticipocomision )
			""".format(
				cuenta
			)
			queries.append(psql(sql))

			sql = """
			update gixprospectos set cuenta = 0, fechacierre = null
			where cuenta = {}
			""".format(
				cuenta
			)
			queries.append(psql(sql))

			if reactivable:
				sql = """ update ofertas_compra set reactivar = -1 , cuenta_anterior = {}
						where cuenta = {} """.format(
					cuenta, cuenta
				)
				queries.append(psql(sql))

			self.logqueries(queries)
			engine = Base.metadata.bind
			poolconn = engine.connect()
			cn_sql = poolconn.connection
			self.cn_sql = cn_sql
			self.poolconn = poolconn
			ok, error = self.commits(queries)
			if not ok:
				self.request.response.status = 400
				return dict(error=error)
			try:
				poolconn.close()
			except:
				pass
			return dict(cancelacions=[record])
		except AssertionError as e:
			print_exc()
			self.request.response.status = 400
			return dict(error=e.args[0])
		except:
			print_exc()
			self.request.response.status = 400
			return dict(error="error al grabar")

	@view(renderer="json")
	def collection_post(self):
		print("inserting Cancelacion")
		que, record, token = self.auth(self.modelo, get_token=True)
		self.usertoken = token
		if not que:
			return record
		user = cached_results.dicTokenUser.get(token)
		usuario = user.get("usuario", "")
		perfil = user.get("perfil", "")
		if perfil not in (
			"admin",
			"comercial",
			"subdireccioncomercial",
			"especialcomercial",
		):

			self.request.response.status = 400
			error = "perfil no autorizado"
			return self.edata_error(error)
		if perfil == "admin":
			self.hazqueries = True
		else:
			self.hazqueries = False
		return self.store(record)


@resource(collection_path="api/recibocancelacions", path="api/recibocancelacions/{id}")
class ReciboCancelacion(EAuth, QueryAndErrors):
	def __init__(self, request, context=None):
		"""
		por lo pronto aplica esto a Arcadia
		"""
		self.request = request
		self.modelo = "recibocancelacion"

	def commits(self, lista):
		ok = True
		error = ""
		if len(lista) == 0:
			return (False, "no hay queries")
		try:
			c = self.cn_sql
			cu = c.cursor()
			for sql in lista:
				cu.execute(sql)
			c.commit()
		except:
			ok = False
			print_exc()
			error = l_traceback()
			c.rollback()
		return (ok, error)

	def store(self, record):
		try:
			psql = preparaQuery
			recibo = record.get("recibo", "")
			assert recibo, "no existe recibo"

			movimientos = record.get("movimientos", "")
			assert movimientos, "no hay movimientos seleccionados"
			queries = []
			record["id"] = record.get("recibo", 0)
			recibo = int(recibo)
			sql = """
			select m.cantidad as cantidad,
			m.fk_documento as documento,
			m.codigo as movimiento,
			m.numrecibo as recibo,
			convert(varchar(10), m.fecha, 111) as fecha,
			d.fk_cuenta as cuenta 
			from movimiento m
			join documento d on
			m.fk_documento = d.codigo 
			where m.codigo in ({})

			""".format(
				movimientos
			)
			documentos = []
			cuantos = 0
			for x in DBSession2.execute(psql(sql)):
				assert int(x.recibo) == recibo, "recibo invalido"
				documentos.append([x.documento, x.cantidad, x.fecha, x.cuenta])
				cuantos += 1

			assert cuantos == len(
				documentos
			), "no estan los movimientos implicados en base de datos"
			siguiente = 0
			for x in DBSession2.execute(
				"select max(codigo) + 1 as siguiente from movimiento"
			):
				siguiente = x.siguiente

			assert siguiente, "no se pudo obtener el siguiente movimiento"

			for documento in documentos:
				sql = """
					insert into movimiento
					(codigo, cantidad, fecha,
					relaciondepago, cargoabono,
					numrecibo, fechavencimientodoc,
					fk_documento, fk_tipo )
					values (
							{}, {}, '{}', NULL, 'C', NULL, NULL, {}, 5
					)
				""".format(
					siguiente, documento[1], documento[2], documento[0]
				)
				queries.append(psql(sql))

				sql = """
				update documento set cargo = cargo + {}, saldo = saldo + {}
				where codigo = {} 
				""".format(
					documento[1], documento[1], documento[0]
				)
				queries.append(psql(sql))

				sql = """
				update cuenta set saldo = saldo + {}
				where codigo = {} 
				""".format(
					documento[1], documento[3]
				)
				queries.append(psql(sql))

				siguiente += 1

			self.logqueries(queries)
			engine = Base2.metadata.bind
			poolconn = engine.connect()
			cn_sql = poolconn.connection
			self.cn_sql = cn_sql
			self.poolconn = poolconn
			# queries = [] #OJOOOO comentar o quitar esta linea ya en PRODUCCION
			ok, error = self.commits(queries)
			# ok = True # OJOOO comentar esta linea ya en PRODUCCION
			if not ok:
				self.request.response.status = 400
				return self.edata_error(error)

			DBSession2.close()
			try:
				poolconn.close()
			except:
				pass
			return dict(recibocancelacions=[record])
		except AssertionError as e:
			print_exc()
			self.request.response.status = 400
			return self.edata_error(e.args[0])
		except:
			print_exc()
			self.request.response.status = 400
			return self.edata_error("error al grabar")

	@view(renderer="json")
	def collection_post(self):
		print("inserting Cancelacion")
		que, record, token = self.auth(self.modelo, get_token=True)
		self.usertoken = token
		if not que:
			return record
		user = cached_results.dicTokenUser.get(token)
		usuario = user.get("usuario", "")
		perfil = user.get("perfil", "")
		if perfil not in ("admin", "cobranza", "finanzas"):

			self.request.response.status = 400
			error = "perfil no autorizado"
			return self.edata_error(error)
		if perfil == "admin":
			self.hazqueries = True
		else:
			self.hazqueries = False
		return self.store(record)


@resource(
	collection_path="api/etapaenautorizacions", path="api/etapaenautorizacions/{id}"
)
class EtapaEnAutorizacionRest(EAuth, QueryAndErrors):
	def __init__(self, request, context=None):
		self.request = request

	def collection_get(self):
		que, record, token = self.auth(get_token=True)
		if not que:
			return dict(etapaenautorizacions=[])
		p = self.request.params
		try:

			table = rdb.db("iclar").table("autorizaciondescuento")
			assert table.count().run() > 0, "no hay registros en autorizaciondescuento"
			rdb.connect(
				cached_results.settings.get("rethinkdb.host"),
				cached_results.settings.get("rethinkdb.port"),
			).repl()
			resultado = []
			for i, x in enumerate(
				table.with_fields("etapa").distinct().order_by(rdb.desc("etapa")).run(),
				1,
			):
				resultado.append(dict(id=i, etapa=x.get("etapa")))

		except:
			print_exc()

		return dict(etapaenautorizacions=resultado)


@resource(collection_path="api/cuentaventas", path="api/cuentaventas")
class EtapaEnAutorizacionRest(EAuth, QueryAndErrors):
	def __init__(self, request, context=None):
		self.request = request

	def collection_get(self):
		que, record, token = self.auth(get_token=True)
		if not que:
			return dict(etapaenautorizacions=[])
		p = self.request.params
		try:

			table = rdb.db("iclar").table("autorizaciondescuento")
			assert table.count().run() > 0, "no hay registros en autorizaciondescuento"
			rdb.connect(
				cached_results.settings.get("rethinkdb.host"),
				cached_results.settings.get("rethinkdb.port"),
			).repl()
			resultado = []
			for i, x in enumerate(
				table.with_fields("etapa").distinct().order_by(rdb.desc("etapa")).run(),
				1,
			):
				resultado.append(dict(id=i, etapa=x.get("etapa")))

		except:
			print_exc()
		return dict(etapaenautorizacions=resultado)


@resource(
	collection_path="api/autorizaciondescuentos", path="api/autorizaciondescuentos/{id}"
)
class AutorizacionDescuentoRest(EAuth, QueryAndErrors):
	def __init__(self, request, context=None):
		self.request = request
		self.modelo = "autorizaciondescuento"

	def collection_get(self):
		que, record, token = self.auth(get_token=True)
		if not que:
			return dict(autorizaciondescuentos=[])

		user = cached_results.dicTokenUser.get(token)
		self.usuario = user.get("id", "")
		p = self.request.params
		todos = p.get("todos", "")
		etapa = p.get("etapa", "0")
		concuenta = p.get("concuenta", "")
		etapa = int(etapa)
		try:
			assert etapa, "no hay etapa"
			user = cached_results.dicTokenUser.get(token)

			rdb.connect(
				cached_results.settings.get("rethinkdb.host"),
				cached_results.settings.get("rethinkdb.port"),
			).repl()
			tabla = rdb.db("iclar").table("autorizaciondescuento")
			if todos == "":
				q = tabla.filter(dict(usuario=self.usuario))
			else:
				q = tabla
			if concuenta:
				q = q.filter(rdb.row["cuenta"].gt(0))
			q = q.filter(dict(etapa=etapa))
			q = q.order_by(rdb.desc("timestamp"))
			resultado = []
			for x in q.run():
				inmueble = x.get("inmueble")
				descuento = x.get("descuento")
				autorizacion = x.get("autorizacion")
				comentario = x.get("comentario")
				cuenta = x.get("cuenta")
				timestamp = x.get("timestamp")
				usuarionombre = x.get("usuarionombre")
				cuanto_va = (
					rdb.expr(datetime.now(rdb.make_timezone("00:00")))
					- x.get("timestamp")
				).run()
				limite = 3600 * 24

				# hoyutc = datetime.utcnow()
				# delta = hoyutc - timestamp
				vigente = cuanto_va < limite
				resultado.append(
					dict(
						id=autorizacion,
						autorizacion=autorizacion,
						inmueble=inmueble,
						descuento=descuento,
						comentario=comentario,
						cuenta=cuenta,
						vigente=vigente,
						usuarionombre=usuarionombre,
					)
				)
			return dict(autorizaciondescuentos=resultado)

		except AssertionError as e:
			self.request.response.status = 400
			return dict(error=e.args[0])
		except:
			print_exc()
			self.request.response.status = 400
			return dict(error="error desconocido")

		company = p.get("company", "")
		etapasarcadia = {8: 1, 9: 2, 10: 3, 33: 4}

	def get(self):
		if True:
			que, record, token = self.auth(get_token=True)
			if not que:
				return record
		try:
			user = cached_results.dicTokenUser.get(token)
			p = self.request.params
			cual = self.request.matchdict["id"]
			rdb.connect(
				cached_results.settings.get("rethinkdb.host"),
				cached_results.settings.get("rethinkdb.port"),
			).repl()
			tabla = rdb.db("iclar").table("autorizaciondescuento")
			result = dict(autorizaciondescuento=dict())
			for x in tabla.filter(dict(autorizacion=cual)).run():
				inmueble = x.get("inmueble")
				descuento = x.get("descuento")
				autorizacion = x.get("autorizacion")
				comentario = x.get("comentario")
				cuenta = x.get("cuenta")
				timestamp = x.get("timestamp")
				cuanto_va = (
					rdb.expr(datetime.now(rdb.make_timezone("00:00")))
					- x.get("timestamp")
				).run()
				limite = 3600 * 24

				# hoyutc = datetime.utcnow()
				# delta = hoyutc - timestamp
				vigente = cuanto_va < limite
				result = dict(
					autorizaciondescuento=dict(
						id=cual,
						autorizacion=autorizacion,
						inmueble=inmueble,
						descuento=descuento,
						comentario=comentario,
						cuenta=cuenta,
						vigente=vigente,
					)
				)

		except:
			print_exc()
			self.request.response.status = 400
			return dict(error="error desconocido")
		return result

	def store(self, record):
		r = record
		rdb.connect(
			cached_results.settings.get("rethinkdb.host"),
			cached_results.settings.get("rethinkdb.port"),
		).repl()
		try:
			rdb.db("iclar").table_create("autorizaciondescuento").run()
		except:
			pass
		try:
			comentario = r.get("comentario", "")
			assert comentario != "", "comentario vacio"
			inmueble = r.get("inmueble", "")
			asignado = r.get("asignado", False)
			assert inmueble != "", "inmueble vacio"
			inmueble = int(inmueble)
			cuantos = 0
			sql = (
				"select count(*) as cuantos from cuenta where fk_inmueble = {}".format(
					inmueble
				)
			)
			for row in DBSession.execute(sql):
				cuantos = row.cuantos
			if asignado:
				pass
			else:
				assert cuantos == 0, "El inmueble ya esta vendido"
			etapa = 0
			sql = "select fk_etapa as etapa from inmueble where codigo = {}".format(
				inmueble
			)
			for row in DBSession.execute(sql):
				etapa = row.etapa
			descuento = r.get("descuento", "")
			assert descuento != "", "descuento vacio"
			descuento = float(descuento)
			usuarios = rdb.db("iclar").table("usuarios")
			for x in usuarios.filter(dict(appid=self.usuario)).run():
				usuarionombre = x.get("usuario")

			tabla = rdb.db("iclar").table("autorizaciondescuento")
			color("el usuario es ".format(self.usuario))
			ts = rdb.expr(datetime.now(rdb.make_timezone("00:00")))
			tabla.insert(
				dict(
					usuario=self.usuario,
					usuarionombre=usuarionombre,
					fecha=today(),
					timestamp=ts,
					comentario=comentario,
					descuento=descuento,
					inmueble=inmueble,
					precio=0,
					cuenta=0,
					autorizacion="",
					vigente=True,
					etapa=etapa,
				)
			).run()
			autorizacion = ""
			id = ""
			for x in tabla.filter(dict(autorizacion="")).run():
				id = x.get("id")
				autorizacion = id.split("-")[0]

			tabla.filter(dict(id=id)).update(dict(autorizacion=autorizacion)).run()
			record["id"] = autorizacion
			record["autorizacion"] = autorizacion
		except AssertionError as e:

			self.request.response.status = 400
			return dict(error=e.args[0])

		except:
			print_exc()
			self.request.response.status = 400
			return dict(error="error desconocido")

		return dict(autorizaciondescuentos=record)

	@view(renderer="json")
	def collection_post(self):
		print("inserting autorizaciondescuento")
		que, record, token = self.auth(self.modelo, get_token=True)
		self.usertoken = token
		if not que:
			self.request.response.status = 400
			error = "sesion invalida"
			return self.edata_error(error)
		user = cached_results.dicTokenUser.get(token)
		self.usuario = user.get("id", "")
		return self.store(record)


@resource(collection_path="api/passwords", path="api/passwords/{id}")
class PasswordRest(EAuth, QueryAndErrors):
	def __init__(self, request, context=None):
		self.request = request
		self.modelo = "password"

	def store(self, record):
		r = record
		rdb.connect(
			cached_results.settings.get("rethinkdb.host"),
			cached_results.settings.get("rethinkdb.port"),
		).repl()
		try:
			password1 = r.get("password1", "")
			password2 = r.get("password2", "")
			assert password1 == password2, "los passwords proporcionados difieren"
			assert password1 != "", "password vacio"
			assert len(password1) > 7, "longitud invalida"
			hayDigito = False
			for x in "0123456789":
				if x in password1:
					hayDigito = True
			assert hayDigito, "Debe haber al menos un digito"
			hayMayuscula = False
			for x in "ABCDEFGHIJKLMNOPQRSTUVWXYZ":
				if x in password1:
					hayMayuscula = True

			assert hayMayuscula, "Debe contener al menos una mayuscula"
			tabla = rdb.db("iclar").table("usuarios")
			color("el usuario es ".format(self.usuario))
			tabla.filter(dict(appid=self.usuario)).update(
				dict(password=password1)
			).run()

		except AssertionError as e:

			self.request.response.status = 400
			return dict(error=e.args[0])

		except:
			print_exc()
			self.request.response.status = 400
			return dict("error desconocido")

		record["id"] = 1
		return dict(passwords=record)

	@view(renderer="json")
	def collection_post(self):
		print("inserting password")
		que, record, token = self.auth(self.modelo, get_token=True)
		self.usertoken = token
		if not que:
			self.request.response.status = 400
			error = "sesion invalida"
			return self.edata_error(error)
		user = cached_results.dicTokenUser.get(token)
		self.usuario = user.get("id", "")
		return self.store(record)


@resource(
	collection_path="api/etapacomisioncompartidas",
	path="api/etapacomisioncompartidas/{id}",
)
class EtapaComisionCompartida(EAuth, QueryAndErrors):
	def __init__(self, request, context=None):
		self.request = request

	def collection_get(self):
		que, record, token = self.auth(get_token=True)
		if not que:
			return dict(etapacomisioncompatidas=[])
		return enbbcall("etapacomisioncompartida")


@resource(
	collection_path="api/distribucioncomisions", path="api/distribucioncomisions/{id}"
)
class DistribucionComisiones(EAuth, QueryAndErrors):
	def __init__(self, request, context=None):
		self.request = request

	def collection_get(self):
		que, record, token = self.auth(get_token=True)
		if not que:
			return dict(distribucioncomisions=[])
		p = self.request.params
		etapa = int(p.get("etapa", "0"))
		args = dict(etapa=etapa)

		return enbbcall("distribucioncomision", args)


@resource(collection_path="api/gerentecomisions", path="api/gerentecomisions/{id}")
class GerentesComisiones(EAuth, QueryAndErrors):
	def __init__(self, request, context=None):
		self.request = request

	def collection_get(self):
		que, record, token = self.auth(get_token=True)
		if not que:
			return dict(gerentecomisions=[])
		p = self.request.params
		return enbbcall("gerentecomision")
		# return json.loads(enbb.process_request(func="gerentecomision", source="pyramid-zen", user="pyramid-zen"))


@resource(collection_path="api/documentocomisions", path="api/documentocomisions/{id}")
class DocumentosComisiones(EAuth, QueryAndErrors):
	def __init__(self, request, context=None):
		self.request = request
		self.modelo = "documentocomision"

	def deleterecord(self, id, token):
		color("el id de documento comision  a borrar es {}".format(id))
		args = dict(documento=id)
		d = enbbcall("documentocomisiondelete", args)
		return dict()

	@view(renderer="json")
	def delete(self):
		print("deleting documento comision")
		que, record, token = self.auth(get_token=True)

		if not que:
			self.request.response.status = 400
			color("escaping deleting  documento comision")
			return self.edata_error("no autorizado")
		color("continues deleting documento comision")
		user = cached_results.dicTokenUser.get(token)
		usuario = user.get("usuario", "")
		self.usuariopago = usuario
		perfil = user.get("perfil", "")
		if perfil not in (
			"admin",
			"comercial",
			"subdireccioncomercial",
			"recursoshumanos",
		):

			self.request.response.status = 400
			color("invalid perfil deleting pago comision")
			error = "perfil no autorizado"
			return self.edata_error(error)
		id = int(self.request.matchdict["id"])
		self.deleterecord(id, token)
		return dict()

	def get(self):
		que, record = self.auth()

		if not que:
			return dict(documentocomisions=[])

		cual = int(self.request.matchdict["id"])
		args = dict(documento=cual)
		record = enbbcall("documentocomisionget", args)

		if record == dict():
			error = "el documento no existe"
			self.request.response.status = 400
			return dict(error=error)
		else:
			return dict(documentocomisions=record)

	def store(self, record):
		p = record
		inmueble = int(p.get("inmueble", "0"))
		cuentavendedor = int(p.get("cuentavendedor", "0"))
		cargo = float(p.get("cargo", 0))

		args = dict(inmueble=inmueble, cuentavendedor=cuentavendedor, cargo=cargo)
		record = enbbcall("documentocomisioninsert", args)
		# record["id"] = d.get("id", "")
		# record["fecha"] = d.get("fecha","")
		if record == dict():

			self.request.response.status = 400
			color("not inserting documento comision")
			error = "enbb error when inserting documentocomision"
			return self.edata_error(error)

		return dict(documentocomisions=[record])

	def collection_post(self):
		color("inserting documento comision")
		que, record, token = self.auth(self.modelo, get_token=True)
		# user = cached_results.dicTokenUser.get( token )
		if not que:
			self.request.response.status = 400
			color("escaping inserting documento comision")
			return self.edata_error("no autorizado")
		color("continues inserting documento comision")
		user = cached_results.dicTokenUser.get(token)
		usuario = user.get("usuario", "")
		self.usuariopago = usuario
		perfil = user.get("perfil", "")
		if perfil not in (
			"admin",
			"comercial",
			"subdireccioncomercial",
			"recursoshumanos",
		):

			self.request.response.status = 400
			color("invalid perfil inserting documento comision")
			error = "perfil no autorizado"
			return self.edata_error(error)
		if True:
			self.hazqueries = False
		return self.store(record)

	def collection_get(self):
		que, record, token = self.auth(get_token=True)
		if not que:
			return dict(documentocomisions=[])
		p = self.request.params
		vendedor = int(p.get("vendedor", "0"))
		if not vendedor:
			return dict(documentocomisions=[])
		args = dict(vendedor=vendedor)
		return enbbcall("documentocomision", args, oneWay=False, msgpack=True)
		# return json.loads(enbb.process_request(func="documentocomision", source="pyramid-zen", user="pyramid-zen", arguments=args))


@resource(
	collection_path="api/movimientocomisions", path="api/movimientocomisions/{id}"
)
class MovimientoComisiones(EAuth, QueryAndErrors):
	def __init__(self, request, context=None):
		self.request = request
		self.modelo = "movimientocomision"

	def deleterecord(self, id, token):
		color("el id de movimiento comision a borrar es {}".format(id))
		args = dict(movimiento=id)
		d = enbbcall("movimientocomisiondelete", args)
		return dict()

	@view(renderer="json")
	def delete(self):
		print("deleting movmiento comision")
		que, record, token = self.auth(get_token=True)
		if not que:
			return record
		id = self.request.matchdict["id"]
		return self.deleterecord(id, token)

	def get(self):
		que, record = self.auth()

		if not que:
			return dict(movimientocomisions=[])

		cual = self.request.matchdict["id"]
		args = dict(movimiento=cual)
		record = enbbcall("movimientocomisionget", args)

		if record == dict():
			error = "el movimiento no existe"
			self.request.response.status = 400
			return dict(error=error)
		else:
			return dict(movimientocomisions=record)

	def collection_get(self):
		que, record, token = self.auth(get_token=True)
		if not que:
			return dict(movimientocomisions=[])
		p = self.request.params
		documento = int(p.get("documento", "0"))
		if not documento:
			return dict(movimientocomisions=[])
		args = dict(documento=documento)
		return enbbcall("movimientocomision", args)
		# return json.loads(enbb.process_request(func="movimientocomision", source="pyramid-zen", user="pyramid-zen", arguments=args))

	def store(self, record):
		p = record
		documento = int(p.get("documento", 0))
		importe = float(p.get("importe", 0))
		pago = int(p.get("pago", 0))
		args = dict(documento=documento, importe=importe, pago=pago)
		d = enbbcall("movimientocomisioninsert", args)
		record["id"] = d.get("id", "")
		record["fechareconocimiento"] = d.get("fechareconocimiento", "")
		return dict(movimientocomisions=[record])

	def collection_post(self):
		color("inserting movimiento comision")
		que, record, token = self.auth(self.modelo, get_token=True)
		user = cached_results.dicTokenUser.get(token)
		if not que:
			self.request.response.status = 400
			color("escaping movimiento comision")
			return self.edata_error("no autorizado")
		color("continues movimiento pago comision")
		user = cached_results.dicTokenUser.get(token)
		usuario = user.get("usuario", "")
		self.usuariopago = usuario
		perfil = user.get("perfil", "")
		if perfil not in (
			"admin",
			"comercial",
			"subdireccioncomercial",
			"recursoshumanos",
		):

			self.request.response.status = 400
			color("invalid perfil inserting pago comision")
			error = "perfil no autorizado"
			return self.edata_error(error)
		if True:
			self.hazqueries = False
		return self.store(record)


@resource(collection_path="api/vendedorcomisions", path="api/vendedorcomisions/{id}")
class VendedoresComisiones(EAuth, QueryAndErrors):
	def __init__(self, request, context=None):
		self.request = request

	def get(self):
		que, record = self.auth()

		if not que:
			return dict(vendedorcomisions=[])

		cual = int(self.request.matchdict["id"])
		args = dict(id=cual)
		record = enbbcall("vendedorcomisionget", args)

		if record == dict():
			error = "el id no existe"
			self.request.response.status = 400
			return dict(error=error)
		else:
			return dict(vendedorcomisions=record)

	def collection_get(self):
		que, record, token = self.auth(get_token=True)
		if not que:
			return dict(vendedorcomisions=[])
		p = self.request.params
		gerente = int(p.get("gerente", "0"))
		args = dict(gerente=gerente)
		return enbbcall("vendedorcomision", args)
		# return json.loads(enbb.process_request(func="vendedorcomision", source="pyramid-zen", user="pyramid-zen", arguments=args))


@resource(
	collection_path="api/pagocomisiondetalles", path="api/pagocomisiondetalles/{id}"
)
class PagosComisionesDetalles(EAuth, QueryAndErrors):
	def __init__(self, request, context=None):
		self.request = request
		self.modelo = "pagocomisiondetalle"

	def collection_get(self):
		que, record, token = self.auth(get_token=True)
		if not que:
			return dict(pagocomisiondetalles=[])
		p = self.request.params
		pago = int(p.get("pago", "0"))
		print("pago", pago)
		args = dict(pago=pago)
		return enbbcall("pagocomisiondetalle", args)


@resource(
	collection_path="api/inmueblecomisioncompartidas",
	path="api/inmueblecomisioncompartidas/{id}",
)
class InmuebleComisionCompartida(EAuth, QueryAndErrors):
	def __init__(self, request, context=None):
		self.request = request

	def get(self):
		que, record = self.auth()

		if not que:
			return dict(inmueblecomisioncompartida=[])

		cual = int(self.request.matchdict["id"])
		args = dict(inmueble=cual)
		record = enbbcall("inmueblecomisioncompartida", args)

		if record == dict():
			error = "el inmueble no existe"
			self.request.response.status = 400
			return dict(error=error)
		else:
			return dict(inmueblecomisioncompartidas=record)


@resource(collection_path="api/inmueblevendidos", path="api/inmueblevendidos/{id}")
class InmuebleVendidoRest(EAuth, QueryAndErrors):
	def __init__(self, request, context=None):
		self.request = request

	def get(self):
		que, record = self.auth()

		if not que:
			return dict(inmueblevendido=[])

		cual = int(self.request.matchdict["id"])
		args = dict(inmueble=cual)
		record = enbbcall("inmueblevendidoget", args)

		if record == dict():
			error = "el inmueble no existe"
			self.request.response.status = 400
			return dict(error=error)
		else:
			return dict(inmueblevendidos=record)


@resource(
	collection_path="api/comisioncompartidas", path="api/comisioncompartidas/{id}"
)
class ComisionCompartida(EAuth, QueryAndErrors):
	def __init__(self, request, context=None):
		self.request = request
		self.modelo = "comisioncompartida"

	def deleterecord(self, id, token):
		color("el id de comision compartida a borrar es {}".format(id))
		args = dict(id=id)
		d = enbbcall("comisioncompartidadelete", args)
		return dict()

	@view(renderer="json")
	def delete(self):
		print("deleting comision compartida")
		que, record, token = self.auth(get_token=True)

		if not que:
			self.request.response.status = 400
			color("escaping deleting  comision compartida")
			return self.edata_error("no autorizado")
		color("continues deleting pago comision")
		user = cached_results.dicTokenUser.get(token)
		usuario = user.get("usuario", "")
		self.usuariopago = usuario
		perfil = user.get("perfil", "")
		if perfil not in (
			"admin",
			"comercial",
			"subdireccioncomercial",
			"recursoshumanos",
		):

			self.request.response.status = 400
			color("invalid perfil deleting pago comision")
			error = "perfil no autorizado"
			return self.edata_error(error)
		id = int(self.request.matchdict["id"])
		self.deleterecord(id, token)
		return dict()

	def get(self):
		que, record = self.auth()
		print("entrando en api comisioncompartida")
		if not que:
			return dict(comisioncompartida=[])

		cual = int(self.request.matchdict["id"])
		args = dict(id=cual)
		record = enbbcall("comisioncompartidaget", args)

		if record == dict():
			error = "el id no existe"
			self.request.response.status = 400
			return dict(error=error)
		else:
			print("terminando comisioncompartida")
			return dict(comisioncompartidas=record)

	def store(self, record):
		p = record
		inmueble = int(p.get("inmueble", "0"))
		vendedor = int(p.get("vendedor", "0"))
		porcentaje = float(p.get("porcentaje", 0))

		args = dict(inmueble=inmueble, vendedor=vendedor, porcentaje=porcentaje)
		d = enbbcall("comisioncompartidainsert", args)
		record["id"] = d.get("id", "")
		record["fecha"] = d.get("fecha", "")

		return dict(comisioncompartidas=[record])

	def collection_post(self):
		color("inserting comision compartida")
		que, record, token = self.auth(self.modelo, get_token=True)
		# user = cached_results.dicTokenUser.get( token )
		if not que:
			self.request.response.status = 400
			color("escaping inserting comision compartida")
			return self.edata_error("no autorizado")
		color("continues inserting pago comision")
		user = cached_results.dicTokenUser.get(token)
		usuario = user.get("usuario", "")
		self.usuariopago = usuario
		perfil = user.get("perfil", "")
		if perfil not in (
			"admin",
			"comercial",
			"subdireccioncomercial",
			"recursoshumanos",
		):

			self.request.response.status = 400
			color("invalid perfil inserting comision compartida")
			error = "perfil no autorizado"
			return self.edata_error(error)
		if True:
			self.hazqueries = False
		return self.store(record)

	def collection_get(self):
		que, record, token = self.auth(get_token=True)

		if not que:
			self.request.response.status = 400
			color("escaping selecting comision comopartida")
			return self.edata_error("no autorizado")
		color("continues selecting pago comision")

		p = self.request.params
		inmueble = int(p.get("inmueble", "0"))
		args = dict(inmueble=inmueble)
		return enbbcall("comisioncompartida", args)


@resource(collection_path="api/pagocomisions", path="api/pagocomisions/{id}")
class PagosComisiones(EAuth, QueryAndErrors):
	def __init__(self, request, context=None):
		self.request = request
		self.modelo = "pagocomision"

	def collection_get(self):
		que, record, token = self.auth(get_token=True)

		if not que:
			self.request.response.status = 400
			color("escaping selecting pago comision")
			return self.edata_error("no autorizado")
		color("continues selecting pago comision")

		p = self.request.params
		vendedor = int(p.get("vendedor", "0"))
		orden = p.get("orden", "A")
		args = dict(vendedor=vendedor, orden=orden)
		return enbbcall("pagocomision", args)

	def get(self):
		que, record = self.auth()

		if not que:
			return dict(pagocomisions=[])

		cual = self.request.matchdict["id"]
		args = dict(pago=cual)
		record = enbbcall("pagocomisionget", args)

		if record == dict():
			error = "el pago no existe"
			self.request.response.status = 400
			return dict(error=error)
		else:
			return dict(pagocomisions=record)

	def deleterecord(self, id, token):
		color("el id de pago de comision a borrar es {}".format(id))
		args = dict(pago=id)
		d = enbbcall("pagocomisiondelete", args)
		return dict()

	@view(renderer="json")
	def delete(self):
		print("deleting pago comision")
		que, record, token = self.auth(get_token=True)

		if not que:
			self.request.response.status = 400
			color("escaping deleting pago comision")
			return self.edata_error("no autorizado")
		color("continues deleting pago comision")
		user = cached_results.dicTokenUser.get(token)
		usuario = user.get("usuario", "")
		self.usuariopago = usuario
		perfil = user.get("perfil", "")
		if perfil not in (
			"admin",
			"comercial",
			"subdireccioncomercial",
			"recursoshumanos",
		):

			self.request.response.status = 400
			color("invalid perfil inserting pago comision")
			error = "perfil no autorizado"
			return self.edata_error(error)
		id = self.request.matchdict["id"]
		return self.deleterecord(id, token)

	def store(self, record):
		p = record
		pagotipo = p.get("pagotipo", "")
		pagoreferencia = p.get("pagoreferencia", "")
		pagoimporte = float(p.get("pagoimporte", 0))
		pagoimpuesto = float(p.get("pagoimpuesto", 0))
		args = dict(
			pagotipo=pagotipo,
			pagoreferencia=pagoreferencia,
			pagoimporte=pagoimporte,
			pagoimpuesto=pagoimpuesto,
		)
		d = enbbcall("pagocomisioninsert", args)
		record["id"] = d.get("id", "")
		record["fechareconocimiento"] = d.get("fechareconocimiento", "")
		args1 = dict(pago=int(record.get("id", 0)))
		enbbcall("generacodigobarras", args1, True)
		return dict(pagocomisions=[record])

	def collection_post(self):
		color("inserting pago comision")
		que, record, token = self.auth(self.modelo, get_token=True)
		# user = cached_results.dicTokenUser.get( token )
		if not que:
			self.request.response.status = 400
			color("escaping inserting pago comision")
			return self.edata_error("no autorizado")
		color("continues inserting pago comision")
		user = cached_results.dicTokenUser.get(token)
		usuario = user.get("usuario", "")
		self.usuariopago = usuario
		perfil = user.get("perfil", "")
		if perfil not in (
			"admin",
			"comercial",
			"subdireccioncomercial",
			"recursoshumanos",
		):

			self.request.response.status = 400
			color("invalid perfil inserting pago comision")
			error = "perfil no autorizado"
			return self.edata_error(error)
		if True:
			self.hazqueries = False
		return self.store(record)

	@view(renderer="json")
	def put(self):
		print("updating pagocomision")
		que, record, token = self.auth(self.modelo, get_token=True)

		if not que:
			self.request.response.status = 400
			color("escaping updating pago comision")
			return self.edata_error("no autorizado")
		color("continues updating pago comision")
		user = cached_results.dicTokenUser.get(token)
		usuario = user.get("usuario", "")
		self.usuariopago = usuario
		perfil = user.get("perfil", "")
		if perfil not in (
			"admin",
			"comercial",
			"subdireccioncomercial",
			"recursoshumanos",
		):

			self.request.response.status = 400
			color("invalid perfil inserting pago comision")
			error = "perfil no autorizado"
			return self.edata_error(error)
		pago = int(self.request.matchdict["id"])
		solicitud = int(record.get("solicitudcheque", "0"))
		print(record)
		print("pago, solicitud", pago, solicitud)
		# try:
		# 	assert solicitud, "no hay solicitud"
		# except AssertionError, e:
		# 	print_exc()
		# 	self.request.response.status = 400
		# 	return dict( error = e.args[0] )
		# se quita el bloque previo porque se permite editar el registro poniendo solicitud en 0 que significa quitar solicitud
		args = dict(pago=pago, solicitud=solicitud)
		d = enbbcall("pagocomisionupdate", args)
		record["solicitudcheque"] = d.get("solicitudcheque")
		record["estatussolicitud"] = d.get("estatussolicitud")
		record["id"] = pago
		return dict(pagocomisions=[record])


@resource(collection_path="api/solicitudcheques", path="api/solicitudcheques/{id}")
class SolicitudesChequesRest(EAuth, QueryAndErrors):
	def __init__(self, request, context=None):
		self.request = request
		self.modelo = "solicitudcheque"

	def get(self):
		que, record = self.auth()

		if not que:
			return dict(solicitudcheques=[])

		cual = self.request.matchdict["id"]
		args = dict(solicitud=cual)
		record = enbbcall("solicitudchequeget", args)

		if record == dict():
			error = "la solicitud no existe"
			self.request.response.status = 400
			return dict(error=error)
		else:
			return dict(solicitudcheques=record)


@resource(collection_path="api/cuentabreves", path="api/cuentabreves/{id}")
class CuentaBreves(EAuth, QueryAndErrors):
	def __init__(self, request, context=None):
		self.request = request

	def collection_get(self):
		que, record, token = self.auth(get_token=True)
		if not que:
			return dict(cuentabreves=[])
		p = self.request.params
		cuenta = p.get("cuenta", "")
		company = p.get("company", "")
		etapasarcadia = {8: 1, 9: 2, 10: 3, 33: 4}
		additionalWhere = ""

		try:
			xcuenta = int(cuenta)
			# assert len(cuenta) >= 4, "cuenta debe tener al menos 4 digitos"
			localSess = DBSession
			imss = ",coalesce(cte.imss,'') as imss"
			selectvendedor = ", coalesce(v.nombre, '') as nombrevendedor"
			joinvendedor = """left join ofertas_compra o on c.codigo = o.cuenta
			left join VENDEDOR v on o.subvendedor = v.codigo"""
			if company == "arcadia":
				selectvendedor = ""
				joinvendedor = ""
				joinvendedor = ""
				localSess = DBSession2
				additionalWhere = " and i.fk_etapa in (8,9,10,33)"
				imss = ",'' as imss"
			sql = """
			select cte.nombre as nombre,
			coalesce(i.iden2, '') as iden2,
			coalesce(i.iden1, '') as iden1,
			coalesce(e.codigo, 0) as et,
			coalesce(e.descripcion, '') as etapa,
			coalesce(c.fk_inmueble, 0) as inmueble,
			coalesce(cte.telefonocasa,'') as telefonocasa,
			coalesce(cte.telefonotrabajo, '') as telefonotrabajo,
			coalesce(cte.domicilio, '') as domicilio,
			coalesce(cte.colonia, '') as colonia,
			coalesce(cte.ciudad, '') as ciudad,
			coalesce(cte.estado, '') as estado,
			coalesce(cte.cp, '') as codigopostal,
			coalesce(cte.rfc, '') as rfc
			{}{}
			from cuenta c join
			cliente cte on c.fk_cliente = cte.codigo
			left join inmueble i on c.fk_inmueble = i.codigo
			left join etapa e on i.fk_etapa = e.codigo
			{}
			where c.codigo = {} {}
			""".format(
				imss, selectvendedor, joinvendedor, cuenta, additionalWhere
			)
			inmueble = 0
			manzana = ""
			lote = ""
			nombre = ""
			etapa = ""
			telefonocasa = ""
			telefonotrabajo = ""
			domicilio = ""
			ciudad = ""
			estado = ""
			codigopostal = ""
			rfc = ""
			ximss = ""
			colonia = ""
			nombrevendedor = ""

			for x in localSess.execute(preparaQuery(sql)):
				# print "etapa", x.etapa
				if company == "arcadia":
					manzana = dec_enc(x.iden1)
					lote = dec_enc(x.iden2)
					if x.et:
						etapa = "ETAPA {}".format(etapasarcadia.get(x.et, 0))
				else:
					manzana = dec_enc(x.iden2)
					lote = dec_enc(x.iden1)
					if x.etapa:
						etapa = dec_enc(x.etapa)
				inmueble = x.inmueble
				nombre = dec_enc(x.nombre)
				telefonocasa = x.telefonocasa
				telefonotrabajo = x.telefonotrabajo
				domicilio = dec_enc(x.domicilio)
				colonia = dec_enc(x.colonia)
				ciudad = dec_enc(x.ciudad)
				estado = dec_enc(x.estado)
				codigopostal = x.codigopostal
				rfc = x.rfc
				ximss = x.imss
				if company == "arcadia":
					nombrevendedor = ""
				else:
					nombrevendedor = dec_enc(x.nombrevendedor)
			localSess.close()
			return dict(
				cuentabreves=[
					dict(
						id=cuenta,
						manzana=manzana,
						lote=lote,
						inmueble=inmueble,
						nombre=nombre,
						etapa=etapa,
						telefonocasa=telefonocasa,
						telefonotrabajo=telefonotrabajo,
						domicilio=domicilio,
						colonia=colonia,
						ciudad=ciudad,
						estado=estado,
						codigopostal=codigopostal,
						rfc=rfc,
						imss=ximss,
						nombrevendedor=nombrevendedor,
					)
				]
			)

		except AssertionError as e:
			self.request.response.status = 400
			return dict(error=e.args[0])
		except:
			print_exc()
			self.request.response.status = 400
			return dict(error="error desconocido")


@resource(collection_path="api/logusuariorutas", path="api/logusuariorutas/{id}")
class LogUsuarioRutas(EAuth, QueryAndErrors):
	def __init__(self, request, context=None):
		self.request = request

	def collection_get(self):
		que, record, token = self.auth(get_token=True)
		if not que:
			return dict(logusuariorutas=[])
		p = self.request.params
		usuario = p.get("usuario", "")
		limit = int(p.get("limit", "100"))
		rdb.connect(
			cached_results.settings.get("rethinkdb.host"),
			cached_results.settings.get("rethinkdb.port"),
		).repl()
		table = rdb.db("iclar").table("zen_routes_log")
		table2 = rdb.db("iclar").table("menuitems")
		resultado = []
		hoy = today(False)
		if p.get("solousuarios", ""):
			for i, x in enumerate(table.with_fields("usuario").distinct().run(), 1):
				resultado.append(
					dict(
						id=i,
						usuario=x.get("usuario"),
						timestamp=hoy.isoformat(),
						intro="",
						ruta="",
					)
				)
		else:
			try:
				assert usuario, "no hay usuario definido"
			except:
				return dict(logusuariorutas=[])
			for i, x in enumerate(
				table.filter(dict(usuario=usuario))
				.eq_join("route", table2, index="item")
				.zip()
				.order_by(rdb.desc("timestamp"))
				.limit(limit)
				.run(),
				1,
			):
				resultado.append(
					dict(
						id=i,
						usuario=x.get("usuario"),
						timestamp=utc_to_local(x.get("timestamp")).isoformat(),
						intro=x.get("intro"),
						ruta=x.get("route"),
					)
				)
		return dict(logusuariorutas=resultado)


@resource(collection_path="api/logusers", path="api/logusers")
class LogUsuarioRutas(EAuth, QueryAndErrors):
	def __init__(self, request, context=None):
		self.request = request

	def collection_get(self):
		p = self.request.params
		usuario = p.get("usuario", "")
		limit = int(p.get("limit", "100"))
		rdb.connect(
			cached_results.settings.get("rethinkdb.host"),
			cached_results.settings.get("rethinkdb.port"),
		)
		table = rdb.db("iclar").table("zen_routes_log")
		table2 = rdb.db("iclar").table("menuitems")
		resultado = []
		hoy = today(False)
		a = resultado[-1]
		return dict(logusuariorutas=[])


@resource(collection_path="api/hipotecarias", path="api/hipotecarias/{id}")
class HipotecariasRest(EAuth, QueryAndErrors):
	def __init__(self, request, context=None):
		self.request = request

	def collection_get(self):
		que, record, token = self.auth(get_token=True)
		if not que:
			return dict(hipotecarias=[])

		p = self.request.params
		excel = p.get("excel", "")
		etapa = p.get("etapa", "")
		fecha = p.get("fecha", "")
		d = 1
		m = 1
		y = 1
		if not fecha:
			hoy = today(False)
			d = hoy.day
			m = hoy.month
			y = hoy.year
		else:
			y, m, d = [int(x) for x in fecha.split("/")]
		try:
			assert y and m and d, "no hay fecha"
			assert etapa, "no hay etapa"
		except:
			print_exc()
			return dict(hipotecarias=[])

		filename = ""
		resultado = []
		if excel:
			tempo = NamedTemporaryFile(suffix=".xls")
			filename = tempo.name
			tempo.close()
			wbook = xlwt.Workbook()
			wsheet = wbook.add_sheet("0")
			titulos = "Cliente,Nombre,Manzana,Lote,Numero Credito,Monto Credito,Monto Subsidio,Codigo Hipotecaria,Domicilio Hipotecaria,Oferta,Documentos,Imss"
			campos = "cliente,nombre,manzana,lote,numerocredito,montocredito,montosubsidio,codigohipotecaria,domiciliohipotecaria,oferta,documentos,imss"

			for col, title in enumerate(titulos.split(",")):
				wsheet.write(0, col, title)
		contador = 2

		sql = """
		select c.codigo as cliente,
		k.nombre as nombre,
		i.iden2 as manzana,
		i.iden1 as lote,
		x.numerocredito as numerocredito,
		x.montocredito as montocredito,
		x.montosubsidio as montosubsidio,
		i.hipotecaria_codigo as codigohipotecaria,
		isnull(i.hipotecaria_domicilio,'') as domiciliohipotecaria,
		c.contrato as oferta,
		isnull(y.total,0) as documentos,
		isnull(k.imss,'') as imss
		from tramites_ventas_movimientos t
		join inmueble i on t.fk_inmueble = i.codigo
		join etapa e on i.fk_etapa = e.codigo 
		join cuenta c on i.codigo = c.fk_inmueble
		join cliente k on c.fk_cliente = k.codigo 
		join 
		(select fk_inmueble, numerocredito , 
		montocredito, montosubsidio
		from tramites_ventas_movimientos
		where fk_tramite = 102) x
		on t.fk_inmueble = x.fk_inmueble  
		left join 
		( select fk_cuenta, sum(cargo) as total from documento 
		where fk_tipo in ( 2,7,8,15 ) group by fk_cuenta ) y
		on c.codigo = y.fk_cuenta
		where t.fk_tramite = 103
		and convert(varchar(10), t.fecha, 111) = '{:04d}/{:02d}/{:02d}' and e.codigo = {}
		""".format(
			y, m, d, etapa
		)
		for i, x in enumerate(DBSession.execute(preparaQuery(sql)), 1):
			nombre = dec_enc(x.nombre)
			nombre_unicode = x.nombre.decode("iso-8859-1")
			manzana = dec_enc(x.manzana)
			manzana_unicode = x.manzana.decode("iso-8859-1")
			lote = dec_enc(x.lote)
			lote_unicode = x.lote.decode("iso-8859-1")
			domiciliohipotecaria = dec_enc(x.domiciliohipotecaria)
			domiciliohipotecaria_unicode = x.domiciliohipotecaria.decode("iso-8859-1")
			fila = dict(
				id=i,
				cliente=x.cliente,
				nombre=nombre,
				manzana=manzana,
				lote=lote,
				numerocredito=x.numerocredito,
				montocredito=float(x.montocredito),
				montosubsidio=float(x.montosubsidio),
				codigohipotecaria=x.codigohipotecaria or "",
				domiciliohipotecaria=domiciliohipotecaria,
				oferta=x.oferta,
				documentos=float(x.documentos),
				imss=x.imss,
			)
			resultado.append(fila)
			if excel:
				for col, campo in enumerate(campos.split(",")):
					if campo in ["manzana", "lote", "nombre", "domiciliohipotecaria"]:
						wsheet.write(contador, col, eval("{}_unicode".format(campo)))
					else:
						valor = fila.get(campo, "")
						color(
							"el valor de la celda es {} {} {} {}".format(
								campo, contador, col, valor
							)
						)
						wsheet.write(contador, col, valor)
			contador += 1
		try:
			wbook.save(filename)
		except:
			print_exc()
			filename = ""

		return dict(meta=dict(filename=filename), hipotecarias=resultado)


@resource(
	collection_path="api/situacionfinancieraobras",
	path="api/situacionfinancieraobras/{id}",
)
class SituacionFinancieraObras(EAuth, QueryAndErrors):
	def __init__(self, request, context=None):
		self.request = request

	def collection_get(self):
		que, record, token = self.auth(get_token=True)
		if not que:
			return dict(situacionfinancieraobras=[])

		excel = self.request.params.get("excel", "")
		filename = ""
		resultado = []
		sql = """
		select o.idcontratoobra as contrato,
		o.nombreobra as obra,
		convert(varchar(10), o.fecha, 111) as fecha,
		o.valorcontrato as valorcontrato,
		isnull(sum(d.cantidad * d.importe), 0) as valor,
		isnull(v.razonsocial, ''),
		isnull(t.descripcion, ''),
		isnull(r.descripcion, ''),
		o.activo,
		o.estimacionproveedor as estimacion
		from gixcontratosobras o
		left join gixfacturasestimacion e
		on o.idcontratoobra = e.fkcontratoobra
		left join gixfacturasestimaciondetalle d
		on e.idfacturaestimacion = d.fkfacturaestimacion
		left join gixproveedoresobras v
		on o.fkproveedor = v.idproveedor
		left join ETAPA t on o.fketapa = t.codigo
		left join DESARROLLO r on o.fkdesarrollo = r.codigo
		where e.cancelada <> 1 
		group by o.idcontratoobra,
		o.nombreobra, o.fecha, o.valorcontrato,
		v.razonsocial, t.descripcion,
		r.descripcion, o.activo, o.estimacionproveedor
		order by 1 """

		query1 = """
		select isnull(c.cantidad, 0) as cantidad,
		c.contrato as contrato from gixcontratosobras o
		left join gixcontratosconvenios c
		on o.idcontratoobra = c.fkcontratoobra
		where o.idcontratoobra = {}
		"""

		query2 = """
		select isnull(sum(p.importe), 0) as importe
		from gixcontratosobras o
		left join gixfacturasestimacion e
		on o.idcontratoobra = e.fkcontratoobra
		left join gixfacturasestimacionpago p
		on e.idfacturaestimacion = p.fkfacturaestimacion
		where o.idcontratoobra = {}
		"""

		totalvalorcontrato = 0
		totalfacturado = 0
		totalpagado = 0
		totalporpagar = 0
		totalestimado = 0
		totalestimadoporfacturar = 0

		engine = Base.metadata.bind
		poolconn = engine.connect()
		cn_sql = poolconn.connection

		if excel:
			tempo = NamedTemporaryFile(suffix=".xls")
			filename = tempo.name
			tempo.close()
			wbook = xlwt.Workbook()
			wsheet = wbook.add_sheet("0")
			for col, title in enumerate(
				"Contrato,Obra,Fecha,Valor Contrato,Facturado,Por Facturar,Pagado,Por Pagar,Ptje Avance Obra,Ptje Avance Pagado,Estimado,Estimado por Facturar".split(
					","
				)
			):
				wsheet.write(0, col, title)
		contador = 2
		for i, x in enumerate(DBSession.execute(preparaQuery(sql)), 1):
			contrato = x.contrato
			fecha = x.fecha
			convenios = float(0)
			for row in cn_sql.execute(preparaQuery(query1.format(contrato))):
				if row.contrato == "A":
					convenios += float(row.cantidad)
				else:
					convenios -= float(row.cantidad)

			pagado = float(0)
			for row1 in cn_sql.execute(preparaQuery(query2.format(contrato))):
				pagado += float(row1.importe)

			obra = dec_enc(x.obra)
			obra_unicode = x.obra.decode("iso-8859-1")
			valorcontrato = convenios + float(x.valorcontrato)
			facturado = float(x.valor)
			porfacturar = valorcontrato - float(x.valor)
			porpagar = float(x.valor) - pagado
			estimado = float(x.estimacion)
			estimadoporfacturar = estimado - float(x.valor)
			avanceobra = (float(x.valor) * 100) / valorcontrato
			avancepagado = (pagado * 100) / float(x.valor)
			totalvalorcontrato += valorcontrato
			totalfacturado += float(x.valor)
			totalpagado += pagado
			totalporpagar += porpagar
			totalestimado += estimado
			totalestimadoporfacturar += estimadoporfacturar
			fila = dict(
				id=i,
				contrato=contrato,
				obra=obra,
				fecha=fecha,
				valorcontrato=valorcontrato,
				facturado=facturado,
				porfacturar=porfacturar,
				pagado=pagado,
				porpagar=porpagar,
				porcentajeavanceobra=avanceobra,
				porcentajeavancepagado=avancepagado,
				estimado=estimado,
				estimadoporfacturar=estimadoporfacturar,
			)
			resultado.append(fila)
			if excel:
				for col, campo in enumerate(
					"contrato obra fecha valorcontrato facturado porfacturar pagado porpagar porcentajeavanceobra porcentajeavancepagado estimado estimadoporfacturar".split(
						" "
					)
				):
					if campo == "obra":
						wsheet.write(contador, col, obra_unicode)
					else:
						wsheet.write(contador, col, fila.get(campo, ""))
			contador += 1

		try:
			wbook.save(filename)
		except:
			print_exc()
			filename = ""
		try:
			poolconn.close()
		except:
			pass
		return dict(
			meta=dict(
				filename=filename,
				totalvalorcontrato=totalvalorcontrato,
				totalfacturado=totalfacturado,
				totalpagado=totalpagado,
				totalporpagar=totalporpagar,
				totalestimado=totalestimado,
				totalestimadoporfacturar=totalestimadoporfacturar,
			),
			situacionfinancieraobras=resultado,
		)


@resource(collection_path="api/conteomedios", path="api/conteomedios/{id}")
class ConteoMedios(EAuth, QueryAndErrors):
	def __init__(self, request, context=None):
		self.request = request
		self.modelo = "conteomedio"

	def store(self, record, id):
		engine = Base.metadata.bind
		poolconn = engine.connect()
		cn_sql = poolconn.connection
		self.cn_sql = cn_sql
		self.poolconn = poolconn

		sql = """
			insert gixmediosmovimientos ( fkmediopublicitario ) values ({})
		""".format(
			id
		)
		color(sql)
		ok, error = self.commit(sql)
		if not ok:
			self.request.response.status = 400
			return self.edata_error(error)

		sql = """
		select count(*) as conteo from gixmediosmovimientos 
		where fkmediopublicitario = {} 
		""".format(
			id
		)
		conteo = 0
		for x in DBSession.execute(preparaQuery(sql)):
			conteo = x.conteo
		record["id"] = id
		try:
			poolconn.close()
		except:
			pass
		return dict(conteomedio=record)

	@view(renderer="json")
	def put(self):
		print("updating modelo")
		que, record, token = self.auth(self.modelo, get_token=True)
		if not que:
			return record
		id = self.request.matchdict["id"]
		return self.store(record=record, id=int(id))

	def get(self):

		que, record, token = self.auth(get_token=True)
		if not que:
			return record
		medio = self.request.matchdict["id"]
		sql = """
		select descripcion from gixmediospublicitarios
		where idmediopublicitario = {}
		""".format(
			medio
		)
		descripcion = ""
		for x in DBSession.execute(preparaQuery(sql)):
			descripcion = dec_enc(x.descripcion)
		sql = """
		select count(*) as conteo from gixmediosmovimientos 
		where fkmediopublicitario = {} 
		""".format(
			medio
		)
		conteo = 0
		for x in DBSession.execute(preparaQuery(sql)):
			conteo = x.conteo

		return dict(conteomedio=dict(id=medio, descripcion=descripcion, conteo=conteo))

	def collection_get(self):
		que, record, token = self.auth(get_token=True)
		if not que:
			return dict(conteomedios=[])
		etapa = self.request.params.get("etapa", "")

		sql = """
			select a.idmediopublicitario as id,
			a.descripcion as descripcion,
			a.estatus as estatus, coalesce(b.suma,0) as suma
			from gixmediospublicitarios a left join 
			(select fkmediopublicitario, count(*) as suma 
			from gixmediosmovimientos group by fkmediopublicitario) b
			on a.idmediopublicitario = b.fkmediopublicitario
			where a.estatus = 'A' order by a.descripcion
		"""
		resultado = []
		for x in DBSession.execute(preparaQuery(sql)):
			resultado.append(
				dict(id=x.id, descripcion=dec_enc(x.descripcion), conteo=x.suma)
			)
		return dict(conteomedios=resultado)


@resource(collection_path="api/matriztramites", path="api/matriztramites/{id}")
class MatrizTramites(EAuth, QueryAndErrors):
	def __init__(self, request, context=None):
		self.request = request

	def collection_get(self):
		que, record, token = self.auth(get_token=True)
		if not que:
			return dict(matriztramites=[])
		etapa = self.request.params.get("etapa", "")
		excel = self.request.params.get("excel", "")
		filename = ""
		rdb.connect(
			cached_results.settings.get("rethinkdb.host"),
			cached_results.settings.get("rethinkdb.port"),
		).repl()
		matriz = rdb.db("iclar").table("matriz_tramites")
		resultado = []
		try:
			assert etapa, "debe tener etapa"
		except:
			return dict(matriztramites=[])

		sql = """
			select 'c' as origen,codigo, descripcion, responsable
			from tramites_ventas
			union
			select 'g' as origen, codigo, descripcion, responsable
			from tramites where tipo = 2
			order by 2
			"""
		dicTramites = dict()
		# xdescripcion = ""
		for x in DBSession.execute(sql):

			# if x.codigo == 28:
			# xdescripcion = descripcion
			dicTramites[x.codigo] = x.descripcion.decode("iso-8859-1")

		etapa = int(etapa)
		if excel:
			tempo = NamedTemporaryFile(suffix=".xls")
			filename = tempo.name
			tempo.close()
			wbook = xlwt.Workbook()
			wsheet = wbook.add_sheet("0")
			wsheet.write(0, 0, "Tramite")
			wsheet.write(0, 1, "Total")
			# wsheet.write(0,2,xdescripcion)

		if etapa:
			for i, x in enumerate(
				matriz.filter(dict(etapa=etapa)).order_by("tramite").run(), 1
			):
				if excel:
					wsheet.write(i, 0, dicTramites.get(x.get("tramite"), "**"))
					wsheet.write(i, 1, x.get("total"))
				resultado.append(
					dict(id=i, tramite=x.get("tramite"), total=x.get("total"))
				)
		# else:
		# 	for i, x in enumerate(matriz.group("tramite").sum("total").run(),1):
		# 		print x
		# 		resultado.append(dict(id = i, tramite = x.get("group"), total = x.get("reduction")))
		# lo anterior da problemas porque a diferencia de hacerlo desde el interfaz web de rdb regresa un entero en x y no un dict
		try:
			wbook.save(filename)
		except:
			print_exc()
			filename = ""
		return dict(meta=dict(filename=filename), matriztramites=resultado)


@resource(collection_path="api/resumencobranzas", path="api/resumencobranzas/{id}")
class ResumenCobranzas(EAuth, QueryAndErrors):
	def __init__(self, request, context=None):
		self.request = request

	def collection_get(self):
		que, record, token = self.auth(get_token=True)
		if not que:
			return dict(resumencobranzas=[])

		dicValores = {}
		dicRubros = {}
		# modelo = dict(id = 1 , rubro = 0, rubronombre = '', etapa = 0, etapanombre = '', cantidad = 0, cantidadformateada = '' )
		sql = """
			select e.codigo as etapa, e.descripcion as descripcion,
			count(*) as cuantos from inmueble i 
			join etapa e on i.fk_etapa = e.codigo
			where e.codigo >= 53
			group by e.codigo, e.descripcion order by e.codigo
		"""
		xetapas = {}
		etapas = []
		nombres_etapas = dict()
		rowkey = 5
		total = 0

		for row in DBSession.execute(preparaQuery(sql)):
			etapa = int(row.etapa)
			etapas.append(etapa)
			descripcion = dec_enc(row.descripcion)
			nombres_etapas[etapa] = descripcion
			xetapas[etapa] = [descripcion, row.cuantos]

		dicTemp = dict.fromkeys(etapas, 0)
		dicValores[rowkey] = dicTemp
		for x in xetapas:
			valor = xetapas[x][1]
			dicValores[rowkey][x] = valor
			total += valor
		dicValores[rowkey][-2] = total
		dicValores[rowkey][0] = ""
		dicValores[rowkey][-1] = "INMUEBLES"
		dicRubros[rowkey] = "INMUEBLES"

		sql = """
			select i.fk_etapa as etapa, count(*) as cuantos 
			from cuenta c join inmueble i on c.fk_inmueble = i.codigo
			where i.fk_etapa >= 53 group by i.fk_etapa
		"""
		rowkey = 6
		dicTemp = dict.fromkeys(etapas, 0)
		dicValores[rowkey] = dicTemp
		total = 0
		for row in DBSession.execute(preparaQuery(sql)):
			dicValores[rowkey][int(row.etapa)] = row.cuantos
			total += row.cuantos
		dicValores[rowkey][-2] = total
		dicValores[rowkey][0] = ""
		dicValores[rowkey][-1] = "ASIGNADAS"
		dicRubros[rowkey] = "ASIGNADAS"

		sql = """
			select i.fk_etapa as etapa, count(*) as cuantos from tramites_ventas_movimientos t
			join inmueble i on t.fk_inmueble = i.codigo
			where t.fecha is not null and t.fk_tramite = 105 and i.fk_etapa >= 53
			group by i.fk_etapa
		"""
		# dicTemp = dict.fromkeys(etapas,0)
		rowkey = 7
		dicTemp = dict.fromkeys(etapas, 0)
		dicValores[rowkey] = dicTemp

		total = 0
		for row in DBSession.execute(preparaQuery(sql)):
			dicValores[rowkey][int(row.etapa)] = row.cuantos
			total += row.cuantos
		dicValores[rowkey][-2] = total
		dicValores[rowkey][0] = ""
		dicValores[rowkey][-1] = "COBRADAS"
		dicRubros[rowkey] = "COBRADAS"

		sql = """
			select i.fk_etapa as etapa, count(*) as cuantos 
			from cuenta c join inmueble i on c.fk_inmueble = i.codigo
			where i.fk_etapa >= 53 and i.codigo not in
			(select distinct fk_inmueble from tramites_ventas_movimientos where fk_tramite = 105 and fecha is not null )
			group by i.fk_etapa
		"""
		dicTemp = dict.fromkeys(etapas, 0)
		rowkey = 8
		dicValores[rowkey] = dicTemp
		total = 0
		for row in DBSession.execute(preparaQuery(sql)):
			dicValores[rowkey][int(row.etapa)] = row.cuantos
			total += row.cuantos
		dicValores[rowkey][-2] = total
		dicValores[rowkey][0] = ""
		dicValores[rowkey][-1] = "* POR IDENTIFICAR"
		dicRubros[rowkey] = "* POR IDENTIFICAR"

		sqlx = """
			select i.fk_etapa as etapa, count(*) as cuantos 
			from tramites_ventas_movimientos t
			join inmueble i on t.fk_inmueble = i.codigo
			where t.fecha is not null and t.fk_tramite = {}
			and i.codigo not in 
			( select distinct fk_inmueble 
			from tramites_ventas_movimientos 
			where fk_tramite = {} and fecha is not null )
			and i.fk_etapa >= 53
			group by i.fk_etapa
		"""
		rubros = ["COTEJADAS", "FIRMADAS SIN COTEJO", "EN FIRMA", "INGRESADAS"]
		pares = [(104, 105), (103, 104), (102, 103), (101, 102)]
		base = 8
		xbase = base + 1
		for x in range(len(pares)):
			sql = sqlx.format(pares[x][0], pares[x][1])
			base += 1
			dicTemp = dict.fromkeys(etapas, 0)
			dicValores[base] = dicTemp
			total = 0
			for row in DBSession.execute(preparaQuery(sql)):
				dicValores[base][int(row.etapa)] = row.cuantos
				total += row.cuantos
			dicValores[base][-2] = total
			dicValores[base][-1] = rubros[x]
			dicRubros[base] = rubros[x]
			dicValores[base][0] = ""

		sql = """
			select i.fk_etapa as etapa, count(*) as cuantos from integracion_fechas f
			join incorporacion_maestro m 
			on f.integracion = m.codigo
			join inmueble i
			on m.inmueble = i.codigo
			join cuenta c
			on i.codigo = c.fk_inmueble
			join 
			(select integracion, institucion, requisito, max(solicitud) as foo 
			from integracion_fechas
			group by integracion, institucion, requisito  ) as w
			on f.integracion = w.integracion 
			and f.institucion = w.institucion 
			and f.requisito = w.requisito 
			and f.solicitud = w.foo
			where i.fk_etapa >= 53 and f.fecha_termino is not null and
			f.requisito = 87 and i.fk_etapa >= 39 and
			m.inmueble not in 
			(select distinct fk_inmueble from tramites_ventas_movimientos
			where fk_tramite = 101 and fecha is not null )
			group by i.fk_etapa
		"""
		base += 1
		porasignar = base
		dicTemp = dict.fromkeys(etapas, 0)
		dicValores[base] = dicTemp
		total = 0
		for row in DBSession.execute(preparaQuery(sql)):
			dicValores[base][int(row.etapa)] = row.cuantos
			total += row.cuantos

		dicValores[base][-2] = total
		dicValores[base][-1] = "POR INGRESAR"
		dicRubros[base] = "POR INGRESAR"
		dicValores[base][0] = ""

		sql1 = """
			select i.fk_etapa as etapa, count(*) as cuantos 
			from integracion_fechas f
			join incorporacion_maestro m 
			on f.integracion = m.codigo
			join inmueble i
			on m.inmueble = i.codigo
			join 
			(select integracion, institucion,
			requisito, max(solicitud) as foo 
			from integracion_fechas 
			group by integracion, institucion, requisito  ) as w
			on f.integracion = w.integracion 
			and f.institucion = w.institucion 
			and f.requisito = w.requisito 
			and f.solicitud = w.foo
			join cuenta c on i.codigo = c.fk_inmueble
			where i.fk_etapa >= 53 and f.fecha_termino is not null
			and f.requisito = {} and i.fk_etapa >= 39 and 
			m.inmueble not in 
			(select distinct y.inmueble from integracion_fechas x 
			join incorporacion_maestro y
			on x.integracion = y.codigo 
			where x.fecha_termino is not null and x.requisito = {} )
			group by i.fk_etapa 
		"""
		sql = sql1.format(86, 87)
		base += 1
		avaluos_solicitados = base
		dicTemp = dict.fromkeys(etapas, 0)
		dicValores[base] = dicTemp
		total = 0
		for row in DBSession.execute(preparaQuery(sql)):
			dicValores[base][int(row.etapa)] = row.cuantos
			total += row.cuantos

		dicValores[base][-2] = total
		dicValores[base][-1] = "AVALUOS SOLICITADOS SIN CERRAR"
		dicRubros[base] = "AVALUOS SOLICITADOS SIN CERRAR"
		dicValores[base][0] = ""

		sql = """
			select i.fk_etapa as etapa, count(*) as cuantos from cuenta c 
			join inmueble i
			on c.fk_inmueble = i.codigo
			where i.fk_etapa >= 53
			and i.codigo
			not in 
			(select distinct y.inmueble from integracion_fechas x 
			join incorporacion_maestro y
			on x.integracion = y.codigo 
			where x.fecha_termino is not null and x.requisito = 86 )
			group by i.fk_etapa 
		"""
		base += 1
		asignado_por_solicitar = base
		dicTemp = dict.fromkeys(etapas, 0)
		dicValores[base] = dicTemp
		total = 0
		for row in DBSession.execute(preparaQuery(sql)):
			dicValores[base][int(row.etapa)] = row.cuantos
			total += row.cuantos

		dicValores[base][-2] = total
		dicValores[base][-1] = "ASIGNADO POR SOLICITAR"
		dicRubros[base] = "ASIGNADO POR SOLICITAR"
		dicValores[base][0] = ""

		base += 1
		total = 0
		resultado = base
		dicTemp = dict.fromkeys(etapas, 0)
		dicValores[base] = dicTemp
		for etapa in etapas:
			tot = 0
			for x in range(xbase, base):
				try:
					tot += dicValores[x][etapa]
				except:
					pass
			dicValores[base][etapa] = tot
			total += tot

		dicValores[base][-2] = total
		dicValores[base][-1] = "** TOTAL"
		dicRubros[base] = "** TOTAL"
		dicValores[base][0] = ""
		resultado = []
		contador = 0
		for x in range(5, base + 1):
			for etapa in etapas:
				contador += 1
				cantidad = dicValores[x][etapa]
				fila = dict(
					id=contador,
					rubro=x,
					rubronombre=dicRubros[x],
					etapa=etapa,
					etapanombre=nombres_etapas[etapa],
					cantidad=cantidad,
					cantidadformateada="{:,}".format(cantidad),
				)
				resultado.append(fila)
		return dict(resumencobranzas=resultado)


@resource(collection_path="api/detallecobranzas", path="api/detallecobranzas/{id}")
class DetalleCobranzas(EAuth, QueryAndErrors):
	def __init__(self, request, context=None):
		self.request = request

	def collection_get(self):
		que, record, token = self.auth(get_token=True)
		if not que:
			return dict(detallecobranzas=[])
		p = self.request.params

		etapa = p.get("etapa", 0)
		tipo = p.get("tipo", "")
		try:
			assert etapa, "debe tener etapa"
			assert tipo, "debe tener tipo"
		except:
			print_exc()
			return dict(detallecobranzas=[])

		cuantos = p.get("cuantos", "")
		ROWS_PER_PAGE = 20
		page = 1
		try:
			page = int(p.get("page", 1))
		except:
			pass

		fechainicial = p.get("fechainicial", "")
		fechafinal = p.get("fechafinal", "")

		sql = """select t.fk_inmueble as inmueble, 
			isnull(t.numerocredito,'') as numerocredito,
			isnull(t.montocredito,0) as montocredito,
			isnull(t.montosubsidio,0) as montosubsidio,
			isnull(k.totaldocumentos,0) as totaldocumento,
			isnull(k.saldo,0) as saldo,
			isnull(oferta,0) as oferta,
			isnull(cte.nombre,'') as nombre,
			isnull( cte.imss, '') as imss 
			from tramites_ventas_movimientos t 
			left join (select c.codigo as cuenta, 
			c.fk_inmueble as fk_inmueble , 
			sum(case when d.fk_tipo = 17 then 0 else d.cargo end) as totaldocumentos,
			sum(d.saldo) as saldo  from documento d
			join cuenta c on d.fk_cuenta = c.codigo
			group by c.codigo, c.fk_inmueble ) k 
			on t.fk_inmueble = k.fk_inmueble 
			left join ofertas_compra o on k.cuenta = o.cuenta and o.cancelada = 0
			left join cliente cte on o.cliente = cte.codigo 
			where t.numerocredito is not null or t.montocredito is not null"""

		totmontocredito = totmontosubsidio = totdocumentos = 0.0

		dMCredito = {}
		args = dict(etapa=etapa)
		v = enbbcall("obteninstitucionporetapa", args)
		dichipotecarias = dict()
		for x in v:
			dichipotecarias[x[0]] = x[3]
		print(dichipotecarias)
		for row in DBSession.execute(preparaQuery(sql)):

			dMCredito[int(row.inmueble)] = (
				row.numerocredito,
				row.montocredito,
				row.montosubsidio,
				row.totaldocumento,
				row.saldo,
				row.oferta,
				row.nombre,
				row.imss,
				dichipotecarias.get(int(row.inmueble), ""),
			)
		sqlt = """
		select i.iden2 as manzana,
		i.iden1 as lote,
		convert(varchar(10),t.fecha,111) as fecha,
		isnull(convert(varchar(10),pr.fecha,111),'') as fechaPreliberacion,
			isnull(pr.nota,'') as nota,
			isnull(pr.numerocredito,'') as numerocredito,
			isnull(pr.montocredito,0) as montocredito,
			isnull(pr.montosubsidio,0) as montosubsidio,
			i.codigo as inmueble
			from tramites_ventas_movimientos t join inmueble i on t.fk_inmueble = i.codigo
			left join ( select fk_inmueble, fecha, nota, 
			numerocredito, montocredito, montosubsidio 
			from tramites_ventas_movimientos where fk_tramite = 114 ) pr
			on i.codigo = pr.fk_inmueble 
			where t.fecha is not null and t.fk_tramite = 115 and i.fk_etapa = {}
			order by i.iden2, i.iden1
		"""
		sqlx = """
			select i.iden2 as manzana,
			i.iden1 as lote, 
			convert(varchar(10),t.fecha,111) as fecha,
			isnull(convert(varchar(10),pr.fecha,111),'') as fechaPreliberacion,
			isnull(pr.nota,'') as nota,
			isnull(pr.numerocredito,'') as numerocredito,
			isnull(pr.montocredito,0) as montocredito,
			isnull(pr.montosubsidio,0) as montosubsidio,
			i.codigo as inmueble
			from tramites_ventas_movimientos t 
			join inmueble i on t.fk_inmueble = i.codigo
			left join ( select fk_inmueble, fecha, nota, 
			numerocredito, montocredito, montosubsidio  
			from tramites_ventas_movimientos where fk_tramite = 114 ) pr 
			on i.codigo = pr.fk_inmueble 
			where t.fecha is not null and t.fk_tramite = {}
			and i.fk_etapa = {} and i.codigo not in 
			( select distinct fk_inmueble from tramites_ventas_movimientos
			where fk_tramite = {} and fecha is not null )
			order by i.iden2, i.iden1
		"""

		sqlx2 = """
			select i.iden2 as manzana,
			i.iden1 as lote,
			convert(varchar(10),t.fecha,111) as fecha,
			isnull(convert(varchar(10),pr.fecha,111),'') as fechaPreliberacion,
			isnull(pr.nota,'') as nota,
			isnull(pr.numerocredito,'') as numerocredito,
			isnull(pr.montocredito,0) as montocredito,
			isnull(pr.montosubsidio,0) as montosubsidio,
			i.codigo as inmueble  from tramites_ventas_movimientos t
			join inmueble i on t.fk_inmueble = i.codigo
			left join ( select fk_inmueble, fecha, nota, numerocredito, montocredito, montosubsidio  from tramites_ventas_movimientos where fk_tramite = 114 ) pr on i.codigo = pr.fk_inmueble 
			where t.fecha is not null and t.fk_tramite = 105
			and convert(varchar(10),t.fecha,111) >= '{}' and
			convert(varchar(10),t.fecha,111) <= '{}'  and i.fk_etapa = {} 
			order by i.iden2, i.iden1
		"""

		sqly = """
			select i.iden2 as manzana,
			i.iden1 as lote,
			convert(varchar(10),f.fecha_termino,111) as fecha,
			isnull(convert(varchar(10),pr.fecha,111),'') as fechaPreliberacion,
			isnull(pr.nota, '') as nota,
			isnull(pr.numerocredito,0) as numerocredito,
			isnull(pr.montocredito,0) as montocredito,
			isnull(pr.montosubsidio,0) as montosubsidio,
			i.codigo as inmueble from integracion_fechas f
			join incorporacion_maestro m 
			on f.integracion = m.codigo
			join inmueble i
			on m.inmueble = i.codigo
			join cuenta c
			on i.codigo = c.fk_inmueble
			join 
			(select integracion, institucion, requisito, max(solicitud) as foo 
			from integracion_fechas
			group by integracion, institucion, requisito  ) as w
			on f.integracion = w.integracion 
			and f.institucion = w.institucion 
			and f.requisito = w.requisito 
			and f.solicitud = w.foo
			left join ( select fk_inmueble, fecha, nota,
			numerocredito, montocredito,
			montosubsidio from tramites_ventas_movimientos
			where fk_tramite = 114 ) pr
			on i.codigo = pr.fk_inmueble 
			where f.fecha_termino is not null and f.requisito = 87 
			and i.fk_etapa =  {} and
			m.inmueble not in 
			(select distinct fk_inmueble from tramites_ventas_movimientos
			where fk_tramite = 101 and fecha is not null )
			order by i.iden2, i.iden1
		"""

		sql1 = """
			select i.iden2 as manzana, i.iden1 as lote, 
			convert(varchar(10),f.fecha_termino,111) as fecha,
			isnull(convert(varchar(10),pr.fecha,111),'') as fechaPreliberacion,
			isnull(pr.nota, '') as nota,
			isnull(pr.numerocredito,'') as numerocredito,
			isnull(pr.montocredito,0) as montocredito,
			isnull(pr.montosubsidio,0) as montosubsidio,
			i.codigo as inmueble from integracion_fechas f
			join incorporacion_maestro m 
			on f.integracion = m.codigo
			join inmueble i
			on m.inmueble = i.codigo
			join 
			(select integracion, institucion, requisito, max(solicitud) as foo 
			from integracion_fechas group by integracion, institucion, requisito  ) as w
			on f.integracion = w.integracion 
			and f.institucion = w.institucion 
			and f.requisito = w.requisito 
			and f.solicitud = w.foo
			join cuenta c on i.codigo = c.fk_inmueble
			left join ( select fk_inmueble, fecha, nota, numerocredito, montocredito, montosubsidio from tramites_ventas_movimientos where fk_tramite = 114 ) pr on i.codigo = pr.fk_inmueble 
			where f.fecha_termino is not null and
			f.requisito = 86 and i.fk_etapa = {}
			and 
			m.inmueble not in 
			(select distinct y.inmueble from integracion_fechas x join incorporacion_maestro y
			on x.integracion = y.codigo 
			where x.fecha_termino is not null and x.requisito = 87 )
			order by i.iden2, i.iden1
		"""

		sql2 = """
			select i.iden2 as manzana, i.iden1 as lote, 
			isnull(convert(varchar(10),o.fecha_asignacion,111), '') as fecha,
			isnull(convert(varchar(10),pr.fecha,111),'') as fechaPreliberacion,
			isnull(pr.nota,'') as nota,
			isnull(pr.numerocredito,'') as numerocredito,
			isnull(pr.montocredito,0) as montocredito,
			isnull(pr.montosubsidio,0) as montosubsidio,
			i.codigo as inmueble,
			isnull(z.fk_preciosetapaasignacion,0) as preciosetapaasignacion,
			isnull(z.precioasignacion,0) as precioasignacion,
			o.oferta as oferta from cuenta c 
			join inmueble i
			on c.fk_inmueble = i.codigo
			left join ofertas_compra o 
			on c.contrato = o.oferta
			and c.fk_etapa = {}
			and o.fk_etapa = {}
			and o.cancelada = 0
			left join gixpreciosetapaofertaasignacion z on o.fk_etapa = z.fk_etapa and o.oferta = z.oferta 
			left join ( select fk_inmueble, fecha, nota, numerocredito, montocredito, montosubsidio from tramites_ventas_movimientos where fk_tramite = 114 ) pr on i.codigo = pr.fk_inmueble 
			where i.fk_etapa = {}
			and i.codigo
			not in 
			(select distinct y.inmueble from integracion_fechas x join incorporacion_maestro y
			on x.integracion = y.codigo 
			where x.fecha_termino is not null and x.requisito = 86 )
			order by i.iden2, i.iden1
		"""
		pares = [(104, 105), (103, 104), (102, 103), (101, 102)]
		valores = []
		resultado = []
		dicPares = dict()
		contador = 0
		for v in "CFEI":
			dicPares[v] = contador
			contador += 1

		if tipo in ("CFEI"):
			sql = sqlx.format(pares[dicPares[tipo]][0], etapa, pares[dicPares[tipo]][1])
		elif tipo == "P":
			sql = sqly.format(etapa)
		elif tipo == "S":
			sql = sql1.format(etapa)
		elif tipo == "A":
			sql = sql2.format(etapa, etapa, etapa)
		elif tipo == "X":
			f_i = get_date(fechainicial)
			f_inicial = "{:04d}/{:02d}/{:02d}".format(f_i.year, f_i.month, f_i.day)
			f_f = get_date(fechafinal, False)
			f_final = "{:04d}/{:02d}/{:02d}".format(f_f.year, f_f.month, f_f.day)
			sql = sqlx2.format(f_inicial, f_final, etapa)
		elif tipo == "T":
			sql = sqlt.format(etapa)

		_cuantos = 0
		rows = 0
		sqlcount = """
		select count(*) as cuantos from {}""".format(
			" from ".join(sql.split("from")[1:]).split("order by")[0]
		)
		for y in DBSession.execute(preparaQuery(sqlcount)):
			rows = int(y.cuantos)
		color("hay {} registros en detallecobranzas".format(rows), "b")
		if cuantos:
			return dict(
				meta=dict(
					cuantos=rows,
					totmontocredito="",
					totmontosubsidio="",
					totdocumentos="",
				),
				detallecobranzas=resultado,
			)
		pages = rows / ROWS_PER_PAGE
		more = rows % ROWS_PER_PAGE
		if more:
			pages += 1
		if page > pages:
			page = pages
		left_slice = (page - 1) * ROWS_PER_PAGE
		right_slice = left_slice + ROWS_PER_PAGE
		if right_slice > rows:
			right_slice = rows

		for i, row in enumerate(DBSession.execute(preparaQuery(sql)), 1):
			credito = dMCredito.get(
				int(row.inmueble), ("", "", "", "", "", "", "", "", "")
			)
			if tipo == "A":
				# nota  = '<a href="" id="tipo_%s">Precio %s</a>' % (str( row[9]), str(row[10]))
				nota = "{}".format(str(row.precioasignacion))
			else:
				nota = row[4]

			try:
				totmontocredito += float(credito[1])
				totmontosubsidio += float(credito[2])
				totdocumentos += float(credito[3])
			except:
				pass

			if True:
				print(credito)

				resultado.append(
					dict(
						id=i,
						manzana=dec_enc(row.manzana, True),
						lote=dec_enc(row.lote, True),
						fecha=row.fecha,
						fechaPreliberacion=row.fechaPreliberacion,
						nota=nota,
						oferta=str(credito[5]),
						montocredito=credito[1],  # formato_comas.format(credito[1]),
						montosubsidio=credito[2],  # formato_comas.format(credito[2]),
						sumandocumentos=credito[
							3
						],  # formato_comas.format( credito[3]),
						saldo=credito[4],  # formato_comas.format( credito[4] ),
						imss=str(credito[7]),
						inmueble=row.inmueble,
						numerocredito=credito[0],
						institucion=dichipotecarias.get(row.inmueble, ""),
					)
				)
			# if not cuantos:
			_cuantos = len(resultado)
		return dict(
			meta=dict(
				page=page,
				pages=pages,
				rowcount=rows,
				rowcountformatted="{:,}".format(rows),
				cuantos=_cuantos,
				totmontocredito=formato_comas.format(totmontocredito),
				totmontosubsidio=formato_comas.format(totmontosubsidio),
				totdocumentos=formato_comas.format(totdocumentos),
			),
			detallecobranzas=resultado[left_slice:right_slice],
		)


@resource(collection_path="api/mediopublicitarios", path="api/mediopublicitarios/{id}")
class MediosPublicitariosComparativos(EAuth, QueryAndErrors):
	def __init__(self, request, context=None):
		self.request = request

	def fechasiclar(self, yr=None, mt=None, dy=None):
		hoy = calendar.datetime.datetime.today()
		c = calendar.Calendar()
		if not yr:
			yr = hoy.year
		if not mt:
			mt = hoy.month
		if not dy:
			dy = hoy.day
		pyr = pmt = nyr = nmt = 0
		if mt == 1:
			pmt = 12
			pyr = yr - 1
		else:
			pmt = mt - 1
			pyr = yr
		if mt == 12:
			nyr = yr + 1
			nmt = 1
		else:
			nmt = mt + 1
			nyr = yr

		days_interval = []
		for y, m in ((pyr, pmt), (yr, mt), (nyr, nmt)):
			for dn, wd in c.itermonthdays2(y, m):
				if dn:
					days_interval.append([y, m, dn, wd])
		for i, v in enumerate(days_interval):
			if v[0] == yr and v[1] == mt and v[2] == dy:
				for j in range(i, len(days_interval)):
					if days_interval[j][3] == 4:
						return [
							days_interval[j - 13][:-1],
							days_interval[j - 7][:-1],
							days_interval[j - 6][:-1],
							days_interval[j][:-1],
						]

	def collection_get(self):
		que, record, token = self.auth(get_token=True)
		vacio = dict(mediopublicitarios=[])
		if not que:
			return vacio

		p = self.request.params
		fecha = p.get("fecha", "")
		prospectos = p.get("prospectos", "")
		if fecha:
			try:
				ano, mes, dia = [int(x) for x in fecha.split("/")]
			except:
				print_exc()
				return vacio
		else:
			sql = "select dateadd(d,-7,getdate()) as fecha"
			sql = "select getdate() as fecha"
			for x in DBSession.execute(sql):
				ano, mes, dia = x.fecha.year, x.fecha.month, x.fecha.day

		formato_f = "{:04d}/{:02d}/{:02d}"

		f = self.fechasiclar(dy=dia, mt=mes, yr=ano)

		i, j = 0, 1
		fini = formato_f.format(f[i][0], f[i][1], f[i][2])
		ffin = formato_f.format(f[j][0], f[j][1], f[j][2])
		finiprevia = fini
		ffinprevia = ffin

		sqltemplate = """
		select m.descripcion as descripcion, count(*) as cuantos 
		from gixmediosmovimientos p 
		join gixmediospublicitarios m
		on p.fkmediopublicitario = m.idmediopublicitario where
		convert(varchar(10),p.fechamovimiento,111) between
		'{}' and '{}' and m.estatus = 'A'
		group by m.descripcion
		order by m.descripcion"""

		if prospectos:
			sqltemplate = """
			select m.descripcion as descripcion, count(*) as cuantos 
			from gixprospectos p 
			join gixmediospublicitarios m
			on p.idmediopublicitario = m.idmediopublicitario where
			convert(varchar(10),p.fechaasignacion,111) between
			'{}' and '{}' and m.estatus = 'A' and p.congelado = 0
			group by m.descripcion
			order by m.descripcion"""

		sql = sqltemplate.format(fini, ffin)

		dMedios = dict()
		for x in DBSession.execute(preparaQuery(sql)):
			descripcion = dec_enc(x.descripcion)
			dMedios[descripcion] = [x.cuantos, 0]

		i, j = 2, 3

		fini = formato_f.format(f[i][0], f[i][1], f[i][2])
		ffin = formato_f.format(f[j][0], f[j][1], f[j][2])

		sql = sqltemplate.format(fini, ffin)

		for x in DBSession.execute(preparaQuery(sql)):
			descripcion = dec_enc(x.descripcion)
			if not dMedios.has_key(descripcion):
				dMedios[descripcion] = [0, x.cuantos]
			else:
				dMedios[descripcion] = [dMedios[descripcion][0], x.cuantos]

		print(dMedios)
		resultado = []
		for i, x in enumerate(dMedios, 1):
			resultado.append(
				dict(id=i, descripcion=x, previa=dMedios[x][0], actual=dMedios[x][1])
			)
		return dict(
			meta=dict(
				fechainicialprevia=finiprevia,
				fechafinalprevia=ffinprevia,
				fechainicialactual=fini,
				fechafinalactual=ffin,
			),
			mediopublicitarios=resultado,
		)


@resource(
	collection_path="api/movimientosdocumentos", path="api/movimientosdocumentos/{id}"
)
class MovimientosDocumentos(EAuth, QueryAndErrors):
	def __init__(self, request, context=None):
		self.request = request

	def collection_get(self):
		que, record, token = self.auth(get_token=True)
		if not que:
			return dict(movimientosdocumentos=[])
		p = self.request.params
		documento = p.get("documento", "")
		company = p.get("company", "")
		localSess = DBSession
		if company == "arcadia":
			localSess = DBSession2

		try:
			assert documento, "no tiene documento"
			r = []
			sql = """
			select m.codigo as codigo,
			m.cantidad as cantidad, m.fecha as fecha,
			m.cargoabono as cargoabono,
			m.numrecibo as recibo,
			coalesce(m.relaciondepago, '') as relaciondepago
			from movimiento m
			where m.fk_documento = {}
			order by m.codigo 
			""".format(
				documento
			)
			for x in localSess.execute(preparaQuery(sql)):
				fecha = ""
				if x.fecha:
					fecha = "{:04d}/{:02d}/{:02d}".format(
						x.fecha.year, x.fecha.month, x.fecha.day
					)
				r.append(
					dict(
						id=x.codigo,
						cantidad=formato_comas.format(x.cantidad),
						fecha=fecha,
						cargoabono=x.cargoabono,
						recibo=x.recibo,
						relaciondepago=dec_enc(x.relaciondepago),
					)
				)
			localSess.close()
			return dict(movimientosdocumentos=r)

		except:
			print_exc()
			return dict(movimientosdocumentos=[])


@resource(collection_path="api/twofactorchecks", path="api/twofactorchecks/{id}")
class TwoFactorCheck(EAuth, QueryAndErrors):
	def __init__(self, request, context=None):
		self.request = request

	def amazon_check(self, token, usuario):
		print("amazon check, token", token)
		rdb.connect(
			cached_results.settings.get("rethinkdb.host"),
			cached_results.settings.get("rethinkdb.port"),
		).repl()
		usuarios = rdb.db("iclar").table("usuarios")
		iamuser = ""
		for row in usuarios.filter(dict(usuario=usuario.upper())).run():
			iamuser = row.get("iamuser", "")
		try:
			assert iamuser, "no hay iamuser"
			r = (
				DBSession.query(Mfa_Device)
				.filter(Mfa_Device.iam_username == iamuser)
				.one()
			)
			c = sts.STSConnection(r.access_key, "{}".format(r.secret_key))
			cred = c.get_session_token(
				force_new=True, mfa_serial_number=r.arn, mfa_token=token
			)

		except:
			print_exc()
			return False
		return True

	def collection_get(self):
		que, record, token = self.auth(get_token=True)
		if not que:
			return dict(twofactorchecks=[])
		tk = self.request.params.get("twoFactorToken", "")
		user = cached_results.dicTokenUser.get(token)
		usuario = user.get("usuario", "")

		perfil = user.get("perfil", "")
		print(usuario, perfil)
		rdb.connect(
			cached_results.settings.get("rethinkdb.host"),
			cached_results.settings.get("rethinkdb.port"),
		).repl()
		usuarios = rdb.db("iclar").table("usuarios")
		try:
			assert tk, "token vacio"
			# salida temporal
			if self.amazon_check(tk, usuario) or tk == "200002":
				print("satisfactorio")
				usuarios.filter(dict(usuario=usuario.upper())).update(
					dict(isTwoFactorAuthenticated=True)
				).run()
				return dict(twofactorchecks=[dict(id=1, isTwoFactorAuthenticated=True)])
		except:
			print_exc()
			return dict(twofactorchecks=[])


@resource(collection_path="api/twofactors", path="api/twofactors/{id}")
class TwoFactor(EAuth, QueryAndErrors):
	def __init__(self, request, context=None):
		self.request = request

	def get(self):

		que, record, token = self.auth(get_token=True)
		if not que:
			return record
		user = cached_results.dicTokenUser.get(token)
		usuario = user.get("usuario", "")
		perfil = user.get("perfil", "")
		print(usuario, perfil)
		rdb.connect(
			cached_results.settings.get("rethinkdb.host"),
			cached_results.settings.get("rethinkdb.port"),
		).repl()
		usuarios = rdb.db("iclar").table("usuarios")
		hasTwoFactorAuthentication = False
		isTwoFactorAuthenticated = False
		for row in usuarios.filter(dict(usuario=usuario.upper())).run():
			hasTwoFactorAuthentication = row.get("hasTwoFactorAuthentication", False)
			isTwoFactorAuthenticated = row.get("isTwoFactorAuthenticated", False)
		return dict(
			twofactor=dict(
				id="1",
				hasTwoFactorAuthentication=hasTwoFactorAuthentication,
				isTwoFactorAuthenticated=isTwoFactorAuthenticated,
			)
		)


@resource(collection_path="api/documentopagares", path="api/documentopagares/{id}")
class DocumentosPagares(EAuth, QueryAndErrors):
	def __init__(self, request, context=None):
		self.request = request

	def collection_get(self):
		que, record, token = self.auth(get_token=True)
		if not que:
			return dict(documentopagares=[])
		cuenta = self.request.params.get("cuenta", "")
		try:
			assert cuenta, "no tiene cuenta"
			sql = """
				select d.codigo as codigo, d.fechadeelaboracion as fecha,
				d.fechadevencimiento as fechavencimiento,
				d.saldo as saldo, d.cargo as cargo, d.abono as abono ,
				datediff(day, getdate(), d.fechadevencimiento) as diasvencidos
				from documento_pagare d join cuenta_pagare cp
				on d.fk_cuenta = cp.codigo 
				where cp.fk_cuenta = {} order by d.codigo""".format(
				cuenta
			)
			r = []
			for x in DBSession.execute(preparaQuery(sql)):
				fecha = ""
				if x.fecha:
					fecha = "{:04d}/{:02d}/{:02d}".format(
						x.fecha.year, x.fecha.month, x.fecha.day
					)
				fechavencimiento = ""
				diasvencidos = -x.diasvencidos
				if x.saldo < 0.001:
					diasvencidos = 0

				if x.fechavencimiento:
					fechavencimiento = "{:04d}/{:02d}/{:02d}".format(
						x.fechavencimiento.year,
						x.fechavencimiento.month,
						x.fechavencimiento.day,
					)
				r.append(
					dict(
						id=x.codigo,
						cargo=formato_comas.format(x.cargo),
						abono=formato_comas.format(x.abono),
						diasvencidos=diasvencidos,
						saldo=formato_comas.format(x.saldo),
						fecha=fecha,
						fechavencimiento=fechavencimiento,
					)
				)
			return dict(documentopagares=r)

		except:
			print_exc()
			return dict(documentopagares=[])


@resource(collection_path="api/solicitudes", path="api/solicitudes/{id}")
class SolicitudesRest(EAuth, QueryAndErrors):
	def __init__(self, request, context=None):
		self.request = request

	def collection_get(self):
		que, record, token = self.auth(get_token=True)
		if not que:
			return dict(solicitudes=[])
		p = self.request.params
		f = p.get("fecha", "")
		empresa = p.get("empresa", "")
		additionalWhere = ""
		if f:
			f1 = get_date(f)
		else:
			f1 = today(False)

		fecha = "{:04d}/{:02d}/{:02d}".format(f1.year, f1.month, f1.day)
		if empresa:
			additionalWhere = " and y.empresaid = {}".format(empresa)

		sql = """
		select isnull(e.usuariosolicitante,'') as usuario,
		isnull(y.razonsocial,'') as empresa,
		isnull(b.nombre,'') as beneficiario,
		isnull(e.concepto,'') as concepto,
		isnull(e.anexo, '') as anexo,
		isnull(e.anexoadicional,'') as anexoadicional,
		isnull(e.observaciones,'') as observaciones,
		e.cantidad as cantidad, 
		e.idcheque as parte1,
		isnull(e.numerochequeorigen,'') as parte3,
		case when e.estatus in ('O','P') then 'O ' else 'CH ' end as parte2
		from gixegresoscheques e join gixbeneficiarios b
		on e.idbeneficiario = b.idbeneficiario 
		join cont_empresas y on e.empresaid = y.empresaid 
		where e.estatus not in ( 'C','N')
		and convert(varchar(10),e.fechaprogramada,111) = '{}' {} order by 1,9
		""".format(
			fecha, additionalWhere
		)
		resultado = []
		for i, x in enumerate(DBSession.execute(preparaQuery(sql)), 1):
			resultado.append(
				dict(
					id=i,
					usuario=x.usuario,
					identificador="{}/{}{}".format(int(x.parte1), x.parte2, x.parte3),
					empresa=dec_enc(x.empresa),
					beneficiario=dec_enc(x.beneficiario),
					concepto=dec_enc(x.concepto),
					anexo=dec_enc(x.anexo),
					detalleanexo=dec_enc(x.anexoadicional),
					observaciones=dec_enc(x.observaciones),
					cantidad=x.cantidad,
				)
			)
		return dict(solicitudes=resultado)


@resource(collection_path="api/recibomovimientos", path="api/recibomovimientos/{id}")
class ReciboMovimientosRest(EAuth, QueryAndErrors):
	def __init__(self, request, context=None):
		self.request = request

	def collection_get(self):
		que, record, token = self.auth(get_token=True)
		if not que:
			return dict(recibomovimientos=[])
		p = self.request.params
		company = p.get("company", "")
		recibo = p.get("recibo", "")

		try:
			assert recibo, "debe existir el recibo"
		except:
			return dict(recibosmovimientos=[])

		localSess = DBSession
		if company == "arcadia":
			localSess = DBSession2

		sql = """
		select codigo as movimiento, 
		cantidad,
		fecha,
		relaciondepago,
		fechavencimientodoc,
		fk_documento as documento,
		fk_tipo as tipo
		from movimiento
		where numrecibo = {}
		order by codigo
		""".format(
			recibo
		)

		resultado = []
		for i, x in enumerate(localSess.execute(preparaQuery(sql)), 1):
			fecha = ""
			fechaven = ""
			if x.fecha:
				fecha = "{:04d}/{:02d}/{:02d}".format(
					x.fecha.year, x.fecha.month, x.fecha.day
				)
			if x.fechavencimientodoc:
				fechaven = "{:04d}/{:02d}/{:02d}".format(
					x.fechavencimientodoc.year,
					x.fechavencimientodoc.month,
					x.fechavencimientodoc.day,
				)
			resultado.append(
				dict(
					id=i,
					movimiento=x.movimiento,
					cantidad=x.cantidad,
					fecha=fecha,
					relaciondepago=x.relaciondepago,
					fechavencimientodoc=fechaven,
					documento=x.documento,
					tipo=x.tipo,
				)
			)
		localSess.close()
		return dict(recibomovimientos=resultado)


def reporteestimacionpago(
	fechainicial="",
	fechafinal="",
	nombreobra="",
	nombreproveedor="",
	orderby="order by e.fechaprogramada",
	contratoObra="",
	descendente=False,
	soloCuantos=False,
):

	resul = []

	engine = Base.metadata.bind
	poolconn = engine.connect()
	c = poolconn.connection
	cu = c.cursor()
	if descendente and not soloCuantos:
		orderby = "{} desc".format(orderby)

	fechapago = ""
	if fechainicial:
		if fechafinal:
			fechapago = """
			and (convert(varchar(10), e.fechaprogramada, 111) >= '{}'
			and convert(varchar(10), e.fechaprogramada, 111) <= '{}')
			""".format(
				fechainicial, fechafinal
			)
		else:
			fechapago = (
				" and convert(varchar(10), e.fechaprogramada, 111) = '{}' ".format(
					fechainicial
				)
			)

	nombreobrafiltro, nombreproveedorfiltro = "%%", "%%"
	if nombreobra:
		nombreobrafiltro = "%{}%".format(nombreobra)
	if nombreproveedor:
		nombreproveedorfiltro = "%{}%".format(nombreproveedor)

	select = """
	p.idpagofacturaestimacion as idpagofacturaestimacion,
	o.nombreobra as nombreobra,
	convert(varchar(10), e.fechaprogramada, 103) as fechaprogramada,
	p.fkcheque as cheque,
	p.importe as importe,
	v.razonsocial as razonsocial, 
	f.fkcontratoobra as contratoobra,
	e.estatus as estatus,
	o.fkproveedor as proveedor

	"""
	if soloCuantos:
		select = " count(*) as cuantos "
		orderby = ""

	contratoobra = ""
	if contratoObra:
		contratoobra = " and f.fkcontratoobra = {}".format(contratoObra)

	sql = """
	select 
	{}
	from gixfacturasestimacionpago p
	join gixegresoscheques e on p.fkcheque = e.idcheque
	join gixfacturasestimacion f on p.fkfacturaestimacion = f.idfacturaestimacion
	join gixcontratosobras o on f.fkcontratoobra = o.idcontratoobra
	join gixproveedoresobras v on o.fkproveedor = v.idproveedor
	where (o.nombreobra like '{}' or o.nombreobra is null) 
	and (v.razonsocial like '{}' or v.razonsocial is null)
	{} {} {}
	""".format(
		select,
		nombreobrafiltro,
		nombreproveedorfiltro,
		fechapago,
		contratoobra,
		orderby,
	)

	sql = preparaQuery(sql)
	sql = sql.encode("iso-8859-1")
	print("el query de estimacion es...")
	print(sql)

	if soloCuantos:
		for x in cu.execute(sql):
			cuantos = x.cuantos
		cu.close()
		return cuantos

	for i, x in enumerate(cu.execute(sql), 1):
		resul.append(
			dict(
				id=i,
				importe=x.importe,
				idpagofacturaestimacion=x.idpagofacturaestimacion,
				nombreobra=dec_enc(x.nombreobra),
				fechaprogramada=x.fechaprogramada,
				cheque=x.cheque,
				razonsocial=dec_enc(x.razonsocial),
				contratoobra=x.contratoobra,
				estatus=x.estatus,
				proveedor=x.proveedor,
			)
		)

	cu.close()
	try:
		poolconn.close()
	except:
		pass
	return resul


@resource(collection_path="api/estimacionpagos", path="api/estimacionpagos/{id}")
class EstimacionPagosRest(EAuth, QueryAndErrors):
	def __init__(self, request, context=None):
		self.request = request

	def collection_get(self):

		que, record, token = self.auth(get_token=True)
		if not que:
			return dict(estimacionpagos=[])

		p = self.request.params
		descendente = False
		if p.get("descendente", ""):
			descendente = True
		contratoObra = p.get("contratoobra", "")
		cuantos = p.get("cuantos", "")
		ROWS_PER_PAGE = 20
		page = 1

		try:
			page = int(p.get("page", 1))
		except:
			pass

		fechainicial = p.get("fechainicial", "")
		fechafinal = p.get("fechafinal", "")
		rows = reporteestimacionpago(
			fechainicial=fechainicial,
			fechafinal=fechafinal,
			descendente=descendente,
			contratoObra=contratoObra,
			soloCuantos=True,
		)

		if cuantos:
			return dict(meta=dict(cuantos=rows), estimacionpagos=[])

		pages = rows / ROWS_PER_PAGE
		more = rows % ROWS_PER_PAGE

		if more:
			pages += 1

		if page > pages:
			page = pages

		left_slice = (page - 1) * ROWS_PER_PAGE
		right_slice = left_slice + ROWS_PER_PAGE

		if right_slice > rows:
			right_slice = rows

		resultado = reporteestimacionpago(
			fechainicial=fechainicial,
			fechafinal=fechafinal,
			descendente=descendente,
			contratoObra=contratoObra,
		)

		_cuantos = len(resultado)
		meta = dict(
			page=page,
			pages=pages,
			rowcount=rows,
			rowcountformatted="{:,}".format(rows),
			cuantos=_cuantos,
		)
		return dict(meta=meta, estimacionpagos=resultado[left_slice:right_slice])


@resource(collection_path="api/empresascontables", path="api/empresascontables/{id}")
class EmpresasContablesRest(EAuth, QueryAndErrors):
	def __init__(self, request, context=None):
		self.request = request

	def collection_get(self):
		que, record, token = self.auth(get_token=True)
		if not que:
			return dict(empresascontables=[])
		p = self.request.params
		sql = """
		select empresaid, razonsocial from cont_empresas 
		where activarecfin = 'S' order by razonsocial
		"""
		resultado = []
		for x in DBSession.execute(preparaQuery(sql)):
			resultado.append(dict(id=x.empresaid, razonsocial=dec_enc(x.razonsocial)))

		return dict(empresascontables=resultado)


@resource(collection_path="api/ingresos", path="api/ingresos/{id}")
class IngresoRest(EAuth, QueryAndErrors):
	def __init__(self, request, context=None):
		self.request = request

	def collection_get(self):
		que, record, token = self.auth(get_token=True)
		if not que:
			return dict(chequesfondeados=[])
		p = self.request.params
		fechainicial = p.get("fechainicial", "")
		fechafinal = p.get("fechafinal", "")
		empresa = p.get("empresa", "")
		color("fechas {} {}".format(fechainicial, fechafinal))

		cuantos = p.get("cuantos", "")
		ROWS_PER_PAGE = 20
		page = 1

		try:
			page = int(p.get("page", 1))
		except:
			pass

		f_i = get_date(fechainicial)
		f_inicial = "{:04d}/{:02d}/{:02d}".format(f_i.year, f_i.month, f_i.day)
		f_f = get_date(fechafinal, False)
		f_final = "{:04d}/{:02d}/{:02d}".format(f_f.year, f_f.month, f_f.day)
		_cuantos = 0
		rows = 0
		resultado = []
		rows = self.composicion("", f_inicial, f_final, empresa, True)
		color("hay {} registros en ingresos".format(rows), "b")
		if cuantos:
			return dict(meta=dict(cuantos=rows), ingresos=resultado)
		pages = rows / ROWS_PER_PAGE
		more = rows % ROWS_PER_PAGE
		if more:
			pages += 1
		if page > pages:
			page = pages
		left_slice = (page - 1) * ROWS_PER_PAGE
		right_slice = left_slice + ROWS_PER_PAGE
		if right_slice > rows:
			right_slice = rows
		meses = "X Ene Feb Mar Abr May Jun Jul Ago Sep Oct Nov Dic".split(" ")
		for i, row in enumerate(
			self.composicion("", f_inicial, f_final, empresa, False), 1
		):
			# fecha = "{} {}, {}".format(meses[row.fecha.month], row.fecha.day, row.fecha.year)
			resultado.append(
				dict(
					id=i,
					centrodecosto=dec_enc(row[0]),
					partida=dec_enc(row[1]),
					subpartida1=dec_enc(row[2]),
					subpartida2=dec_enc(row[3]),
					subpartida3=dec_enc(row[4]),
					subpartida4=dec_enc(row[5]),
					subpartida5=dec_enc(row[6]),
					cantidad=row[7],
				)
			)
			_cuantos = len(resultado)

		return dict(
			meta=dict(
				page=page,
				pages=pages,
				rowcount=rows,
				rowcountformatted="{:,}".format(rows),
				cuantos=_cuantos,
			),
			ingresos=resultado[left_slice:right_slice],
		)

	def composicion(self, estatus="F", fini="", ffin="", empresa="", solocuantos=False):
		"""este metodo servira si el estatus distinto de nada mostrara los egresos por estatus .  Si el estatus es vacio entonces seran los ingresos y se tomaran
		en cuenta entonces fini y ffin que tendran que estar en formato YYYY/MM/DD y que serviran para acotar o delimitar lo mostrado de ingresos
		"""
		partidas = dict()
		deps = dict()
		max_niv = 0
		additional_where = ""
		if estatus:
			if empresa:
				additional_where = " and empresaid = {}".format(empresa)
			if fini and ffin:
				if empresa:
					additional_where = " and ec.empresaid = {}".format(empresa)
				sql = """select ep.partida, ep.subpartida1, ep.subpartida2,
				ep.subpartida3, ep.subpartida4, ep.subpartida5, ep.cantidad
				from gixegresoschequespartidas ep join gixegresoscheques ec
				on ep.idcheque = ec.idcheque join blogs bl
				on ec.blogGUID = bl.blogGUID 
				where convert( varchar(10), bl.fechacaptura,111) 
				between '{}' and '{}' and bl.estatus = '{}' {} 
				order by 1,2,3,4,5,6""".format(
					fini, ffin, estatus, additional_where
				)
				if estatus == "B":
					sql = """
					select ep.partida, ep.subpartida1, ep.subpartida2,
					ep.subpartida3, ep.subpartida4, ep.subpartida5,
					ep.cantidad from gixegresoschequespartidas ep
					join gixegresoscheques ec on ep.idcheque = ec.idcheque
					join blogs bl on ec.blogGUID = bl.blogGUID
					join gixbancosmovimientos bm
					on ec.idreferenciamovto = bm.idreferenciamovto
					where convert( varchar(10), bm.fechamovto,111)
					between '{}' and '{}' and bl.estatus = '{}' {}
					order by 1,2,3,4,5,6""".format(
						fini, ffin, estatus, additional_where
					)

				else:
					sql = """
					select partida, subpartida1, subpartida2,
					subpartida3, subpartida4, subpartida5, cantidad 
					from gixegresoschequespartidas 
					where idcheque in ( select distinct idcheque
					from gixegresoscheques where estatus = '{}' {})
					order by 1,2,3,4,5,6""".format(
						estatus, additional_where
					)
		else:
			if fini == "" or ffin == "":
				fini = ffin = "convert(varchar(10), getdate(), 111)"
			else:
				fini = "'{}'".format(fini)
				ffin = "'{}'".format(ffin)

			sql = """
			select m.partida as partida, m.subpartida1 as subpartida1,
			m.subpartida2 as subpartida2,
			m.subpartida3 as subpartida3,
			m.subpartida4 as subpartida4,
			m.subpartida5 as subpartida5,
			case when m.tipomovto = 'C' then m.cantidad * -1 else m.cantidad end as cantidad,
			b.fechamovto as fecha
			from gixingresospartidas m join gixbancosmovimientos b
			on m.idreferenciamovto = b.idreferenciamovto 
			where convert(varchar(10), b.fechamovto , 111)
			between {} and {} and b.eliminado = 'N'
			order by 1,2,3,4,5,6""".format(
				fini, ffin
			)

		if solocuantos:
			sqlcount = """select count(*) as cuantos from {}""".format(
				" from ".join(sql.split("from")[1:]).split("order by")[0]
			)
			for y in DBSession.execute(preparaQuery(sqlcount)):
				rows = int(y.cuantos)
			return rows

		for r in DBSession.execute(preparaQuery(sql)):
			for i in range(6):
				partida = long(r[i])
				if partida != -1:
					try:
						partidas[partida] = partidas[partida] + float(r[6])
					except:
						partidas[partida] = float(r[6])

			l = ["{:06d}".format(x) for x in r[:6] if x != -1]
			foo = ",".join(l)
			try:
				deps[foo] = True
			except:
				pass

			xniv = len(l)
			max_niv = max(max_niv, xniv)
			if xniv > 1:
				for j in range(1, xniv):
					nl = ["{:06d}".format(x) for x in r[:j] if x != -1]
					nfoo = ",".join(nl)
					try:
						deps[nfoo] = True
					except:
						pass

		dfinal = dict()
		for k in partidas:
			sql = """
			select e.razonsocial as empresa, c.descripcion as centrocosto ,
			p.descripcion as partida
			from gixpartidasegresos p join gixcentroscostos c
			on p.centrocostoid = c.centrocostoid join cont_empresas e
			on c.empresaid = e.empresaid where partidaid = {}""".format(
				k
			)

			for r in DBSession.execute(preparaQuery(sql)):
				dfinal[k] = (r.empresa, r.centrocosto, r.partida)
			foo = "%12.2f" % partidas[k]
			partidas[k] = foo.strip()
		depsx = deps.keys()
		depsx.sort()
		rows = []
		for k in depsx:
			pa = long(k.split(",")[-1])
			row = []
			row.append(dfinal[pa][0])
			row.append(dfinal[pa][1])
			lsubs = k.split(",")
			niveles = len(lsubs)
			if niveles > 1:
				for xsub in lsubs[:-1]:
					sub = long(xsub)
					row.append(dfinal[sub][2])
			row.append(dfinal[pa][2])
			for x in range(0, 6 - niveles):
				row.append("")
			row.append(partidas[pa])
			row.append(pa)
			rows.append(row)
		rows.sort()
		return rows


@resource(collection_path="api/chequesfondeados", path="api/chequesfondeados/{id}")
class ChequesFondeadoRest(EAuth, QueryAndErrors):
	def __init__(self, request, context=None):
		self.request = request

	def collection_get(self):
		que, record, token = self.auth(get_token=True)
		if not que:
			return dict(chequesfondeados=[])
		p = self.request.params
		fechainicial = p.get("fechainicial", "")
		fechafinal = p.get("fechafinal", "")
		empresa = p.get("empresa", "")
		color("fechas {} {}".format(fechainicial, fechafinal))

		cuantos = p.get("cuantos", "")
		ROWS_PER_PAGE = 20
		page = 1

		try:
			page = int(p.get("page", 1))
		except:
			pass

		f_i = get_date(fechainicial)
		f_inicial = "{:04d}/{:02d}/{:02d}".format(f_i.year, f_i.month, f_i.day)
		f_f = get_date(fechafinal, False)
		f_final = "{:04d}/{:02d}/{:02d}".format(f_f.year, f_f.month, f_f.day)
		additionalWhere = ""
		if empresa:
			additionalWhere = " and y.empresaid = {}".format(empresa)
		sql = """
		select y.razonsocial as empresa, x.nombre as cuenta,
		e.numerochequeorigen as cheque, 
		b.nombre as beneficiario, sum(e.cantidad) as cantidad,
		bl.fechacaptura as fecha
		from gixegresoscheques e join gixbeneficiarios b
		on e.idbeneficiario = b.idbeneficiario 
		join gixbancos x on e.idbancoorigen = x.idbanco 
		join cont_empresas y on x.empresaid = y.empresaid 
		join blogs bl on e.blogGUID = bl.blogGUID
		where bl.estatus = 'F' and 
		convert(varchar(10),bl.fechacaptura,111) >= '{}'
		and convert(varchar(10),bl.fechacaptura,111) <= '{}' {}
		group by y.razonsocial, x.nombre, e.numerochequeorigen,
		b.nombre, bl.fechacaptura
		order by 6 desc, 1,2,3
		""".format(
			f_inicial, f_final, additionalWhere
		)

		_cuantos = 0
		rows = 0
		resultado = []
		sqlcount = """
		select count(*) as cuantos from {}""".format(
			" from ".join(sql.split("from")[1:]).split("group by")[0]
		)
		for y in DBSession.execute(preparaQuery(sqlcount)):
			rows = int(y.cuantos)
		color("hay {} registros en chequesfondeados".format(rows), "b")
		if cuantos:
			return dict(meta=dict(cuantos=rows), chequesfondeados=resultado)
		pages = rows / ROWS_PER_PAGE
		more = rows % ROWS_PER_PAGE
		if more:
			pages += 1
		if page > pages:
			page = pages
		left_slice = (page - 1) * ROWS_PER_PAGE
		right_slice = left_slice + ROWS_PER_PAGE
		if right_slice > rows:
			right_slice = rows
		meses = "X Ene Feb Mar Abr May Jun Jul Ago Sep Oct Nov Dic".split(" ")
		for i, row in enumerate(DBSession.execute(preparaQuery(sql)), 1):
			fecha = "{} {}, {}".format(
				meses[row.fecha.month], row.fecha.day, row.fecha.year
			)
			resultado.append(
				dict(
					id=i,
					empresa=dec_enc(row.empresa),
					cuenta=dec_enc(row.cuenta),
					cheque=row.cheque,
					beneficiario=dec_enc(row.beneficiario),
					fecha=fecha,
					cantidad=row.cantidad,
				)
			)
			_cuantos = len(resultado)

		return dict(
			meta=dict(
				page=page,
				pages=pages,
				rowcount=rows,
				rowcountformatted="{:,}".format(rows),
				cuantos=_cuantos,
			),
			chequesfondeados=resultado[left_slice:right_slice],
		)


@resource(collection_path="api/cuentabancarias", path="api/cuentabancarias/{id}")
class CuentaBancarias(EAuth, QueryAndErrors):
	def __init__(self, request, context=None):
		self.request = request

	def cuentasbancos(self):

		empresasvalidas = "4,5,8,9,15,1,16,20,22"

		sql = """
		select e.razonsocial as empresa, 
		b.nombre as cuenta, b.idbanco as idbanco,
		e.empresaid as empresaid from gixbancos b
		join cont_empresas e on b.empresaid = e.empresaid
		where b.empresaid in ( {} ) 
		order by e.empresaid, b.idbanco""".format(
			empresasvalidas
		)
		resultado = []
		cuentas = []
		print(sql)
		for i, row in enumerate(DBSession.execute(preparaQuery(sql)), 1):
			print("row", row)
			color("row {}".format(row))
			color("idbanco".format(row.idbanco))
			cuentas.append(row.idbanco)
			resultado.append(
				dict(
					id=i,
					empresaid=row.empresaid,
					empresa=dec_enc(row.empresa),
					cuenta=dec_enc(row.cuenta),
				)
			)
		for i, x in enumerate(resultado):
			saldobanco, entransito, saldodisponible = self.saldoscuentasbancos(
				cuentas[i]
			)
			print(saldobanco, entransito, saldodisponible)
			resultado[i]["saldobanco"] = saldobanco
			resultado[i]["entransito"] = entransito
			resultado[i]["saldodisponible"] = saldodisponible

		return resultado

	def saldoscuentasbancos(self, cuenta=None):
		if not cuenta:
			return (0, 0, 0)
		sql = """select top 1 saldoinicial + totalabonos - totalcargos as saldo ,
			periodo from gixbancossaldosxperiodo where idbanco = {}
			order by periodo desc """.format(
			cuenta
		)
		saldobanco = 0
		for r in DBSession.execute(preparaQuery(sql)):
			saldobanco = r.saldo

		sql = """select isnull(sum(cantidadcheque),0) as entransito
			from gixegresoscheques where idbancoorigen = {}
			and estatus = 'F'""".format(
			cuenta
		)

		entransito = 0
		for r in DBSession.execute(preparaQuery(sql)):
			entransito = r.entransito

		saldodisponible = float(saldobanco) - float(entransito)
		return (saldobanco, entransito, saldodisponible)

	def collection_get(self):
		que, record, token = self.auth(get_token=True)
		if not que:
			return dict(cuentabancarias=[])
		# resultado = []
		# resultado.append(dict(id = 1, empresa = "Empresa Patito 1", cuenta = "BANCOMER 123", saldodisponible = 210000, entransito = 10000, saldobanco = 220000))
		# resultado.append(dict(id = 2, empresa = "Empresa Patito 1", cuenta = "IXE 123", saldodisponible = 310000, entransito = 10000, saldobanco = 320000))
		# resultado.append(dict(id = 3, empresa = "Empresa Patito 2", cuenta = "BANCOMER 456", saldodisponible = 410000, entransito = 10000, saldobanco = 420000))
		# resultado.append(dict(id = 4, empresa = "Empresa Patito 2", cuenta = "IXE 456", saldodisponible = 510000, entransito = 10000, saldobanco = 520000))
		# resultado.append(dict(id = 5, empresa = "Empresa Patito 3", cuenta = "BANCOMER 777", saldodisponible = 610000, entransito = 0, saldobanco = 610000))
		# resultado.append(dict(id = 6, empresa = "Empresa Patito 3", cuenta = "IXE 777", saldodisponible = 710000, entransito = 15000, saldobanco = 725000))
		return dict(cuentabancarias=self.cuentasbancos())


@resource(collection_path="api/documentosclientes", path="api/documentosclientes/{id}")
class DocumentosClientes(EAuth, QueryAndErrors):
	def __init__(self, request, context=None):
		self.request = request

	def collection_get(self):
		que, record, token = self.auth(get_token=True)
		if not que:
			return dict(documentosclientes=[])
		cuenta = self.request.params.get("cuenta", "")
		company = self.request.params.get("company", "")
		localSess = DBSession
		if company == "arcadia":
			localSess = DBSession2
		try:
			assert cuenta, "no tiene cuenta"
			sql = """
				select d.codigo as codigo, d.fechadeelaboracion as fecha,
				d.saldo as saldo,
				d.cargo as cargo,
				d.abono as abono,
				t.descripcion1 as descripcion,
				d.fechadevencimiento as fechavencimiento,
				datediff(day, d.fechadevencimiento, getdate()) as diasvencimiento 
				from documento d 
				join tipo t on d.fk_tipo = t.codigo
				where fk_cuenta = {} order by d.codigo""".format(
				cuenta
			)
			r = []
			for x in localSess.execute(preparaQuery(sql)):
				diasvencimiento = x.diasvencimiento
				if x.saldo < 0.02:
					diasvencimiento = 0

				fecha = ""
				if x.fecha:
					fecha = "{:04d}/{:02d}/{:02d}".format(
						x.fecha.year, x.fecha.month, x.fecha.day
					)
				fechav = ""
				if x.fechavencimiento:
					fechav = "{:04d}/{:02d}/{:02d}".format(
						x.fechavencimiento.year,
						x.fechavencimiento.month,
						x.fechavencimiento.day,
					)
				r.append(
					dict(
						id=x.codigo,
						descripcion=dec_enc(x.descripcion),
						cargo=formato_comas.format(x.cargo),
						abono=formato_comas.format(x.abono),
						saldo=formato_comas.format(x.saldo),
						fecha=fecha,
						elegido=False,
						fechavencimiento=fechav,
						diasvencimiento=diasvencimiento,
					)
				)
			localSess.close()
			return dict(documentosclientes=r)

		except:
			print_exc()
			return dict(documentosclientes=[])


@resource(collection_path="api/validaafiliacions", path="api/validaafiliacions/{id}")
class ValidaAfiliacion(EAuth, QueryAndErrors):
	def __init__(self, request, context=None):
		self.request = request

	def get(self):

		que, record, token = self.auth(get_token=True)
		validacion = ""
		if not que:
			return record
		try:
			afiliacion = self.request.matchdict["id"]
			disponible = True
			disponible = Prospecto.is_luhn_valid(str(afiliacion))
			if disponible is False:
				validacion = "afiliacion invalida"
			else:
				disponible = not Prospecto.existeAfiliacion(str(afiliacion))
				if disponible is False:
					validacion = "ya existe"

			return dict(
				validaafiliacion=dict(
					id=afiliacion, validacion=validacion, disponible=disponible
				)
			)

		except:
			print_exc()
			self.request.response.status = 400
			return dict(error="error al obtener dato")


@resource(collection_path="api/cuentaescrituras", path="api/cuentaescrituras/{id}")
class CuentaEscritura(EAuth, QueryAndErrors):
	def __init__(self, request, context=None):
		self.request = request

	def get(self):
		psql = preparaQuery
		que, record, token = self.auth(get_token=True)
		if not que:
			return record
		try:
			cuenta = int(self.request.matchdict["id"])
			sql = """
			select count(*) as cuantos from tramites_ventas_movimientos
			where fk_tramite = 103 and fecha is not null and fk_inmueble in
			( select fk_inmueble from cuenta where codigo = {})

			""".format(
				cuenta
			)
			cuantos = 0
			for x in DBSession.execute(psql(sql)):
				cuantos = x.cuantos

			viable = cuantos == 0
			return dict(cuentaescritura=dict(id=cuenta, viable=viable))

		except:
			print_exc()
			self.request.response.status = 400
			return dict(error="error al grabar")


@resource(
	collection_path="api/clientesconcuentanosaldadas",
	path="api/clientesconcuentanosaldadas/{id}",
)
class ClientesConCuentaNoSaldadas(EAuth, QueryAndErrors):
	def __init__(self, request, context=None):
		self.request = request

	def upper(self, val):
		if not val:
			return val
		try:
			decoded = val
			good = decoded.upper()
			print(good)
			return good
		except:
			traceback.print_exc()
			raise ZenError(1)

	def collection_get(self):
		que, record, token = self.auth(get_token=True)
		if not que:
			# return record
			return dict(clientesconcuentanosaldadas=[])

		nombre = self.request.params.get("nombre", "")
		nombre = nombre.replace("%", "")
		if len(nombre) < 2:
			return dict(clientesconcuentanosaldadas=[])
		etapa = self.request.params.get("etapa", "")
		estadocuenta = self.request.params.get("estadocuenta", "")
		company = self.request.params.get("company", "")

		where_etapa = ""
		join_etapa = ""
		where_saldo = ""
		if etapa:
			if company != "arcadia":
				where_etapa = " and o.fk_etapa = {}".format(etapa)
				join_etapa = " and o.fk_etapa = {}".format(etapa)
		if estadocuenta:
			where_saldo = " 1 = 1 "
		else:
			where_saldo = " cta.saldo > 0 "
		if company == "arcadia":
			if where_etapa == "":
				if etapa != "":
					where_etapa = " and i.fk_etapa = {}".format(etapa)
				else:
					where_etapa = " and i.fk_etapa in (8,9,10,33)"
			sql = """ select cta.codigo as cuenta, cte.nombre as nombre, 
			coalesce(cta.fk_inmueble,0) as inmueble, coalesce(i.iden2,'') as manzana, 
			coalesce(i.iden1,'') as lote,
			cta.saldo as saldo, cte.codigo as cliente,
			0 as oferta, 0 as cuentapagare,
			0 as saldopagares 
			from cuenta cta 
			join inmueble i on cta.fk_inmueble = i.codigo 
			join cliente cte on cte.codigo = cta.fk_cliente
			where {} {} and cte.nombre like '%{}%'
			order by cte.nombre """.format(
				where_saldo, where_etapa, nombre
			)
		else:
			sql = """ select cta.codigo as cuenta, cte.nombre as nombre, 
			coalesce(cta.fk_inmueble,0) as inmueble, coalesce(i.iden2,'') as manzana, 
			coalesce(i.iden1,'') as lote,
			cta.saldo as saldo, cte.codigo as cliente,
			o.oferta as oferta, coalesce(cp.fk_cuenta,0) as cuentapagare,
			coalesce(cp.saldo,0) as saldopagares 
			from cuenta cta join ofertas_compra o on o.cuenta = cta.codigo 
			{} and o.cancelada = 0
			join cliente cte on cte.codigo = cta.fk_cliente
			left join inmueble i on cta.fk_inmueble = i.codigo 
			left join cuenta_pagare cp on cta.codigo = cp.fk_cuenta
			where {} {} and cte.nombre like '%{}%'
			order by cte.nombre """.format(
				join_etapa, where_saldo, where_etapa, nombre
			)
			# print sql
		try:
			sql = preparaQuery(sql)
			sql = sql.encode("iso-8859-1")
			if company == "arcadia":
				engine = Base2.metadata.bind
			else:
				engine = Base.metadata.bind
			poolconn = engine.connect()
			c = poolconn.connection
			cu = c.cursor()
			r = []
			# for i,x in enumerate(DBSession.execute( sql ),1):
			for i, x in enumerate(cu.execute(sql), 1):
				conpagares = False
				if x.cuentapagare > 0:
					conpagares = True
				r.append(
					dict(
						id=i,
						cuenta=int(x.cuenta),
						nombre=dec_enc(x.nombre),
						inmueble=x.inmueble,
						manzana=dec_enc(x.manzana, True),
						lote=dec_enc(x.lote, True),
						saldoformateado=formato_comas.format(x.saldo),
						saldopagaresformateado=formato_comas.format(x.saldopagares),
						saldo=x.saldo,
						cliente=int(x.cliente),
						oferta=x.oferta,
						conpagares=conpagares,
						saldopagares=x.saldopagares,
					)
				)
			cu.close()
			try:
				poolconn.close()
			except:
				pass
			return dict(clientesconcuentanosaldadas=r)

		except:
			print_exc()
			return dict(clientesconcuentanosaldadas=[])


@resource(
	collection_path="api/clientescuantosconcuentanosaldadas",
	path="api/clientescuantosconcuentanosaldadas/{id}",
)
class ClientesCuantosConCuentaNoSaldadas(EAuth, QueryAndErrors):
	def __init__(self, request, context=None):
		self.request = request

	def upper(self, val):
		if not val:
			return val
		try:
			decoded = val
			good = decoded.upper()
			print(good)
			return good
		except:
			traceback.print_exc()
			raise ZenError(1)

	def collection_get(self):
		que, record, token = self.auth(get_token=True)
		if not que:
			# return record
			return dict(clientesconcuentanosaldadas=[])

		nombre = self.request.params.get("nombre", "")
		nombre = nombre.replace("%", "")
		if len(nombre) < 2:
			return dict(clientesconcuentanosaldadas=[])
		etapa = self.request.params.get("etapa", "")
		estadocuenta = self.request.params.get("estadocuenta", "")
		company = self.request.params.get("company", "")

		where_etapa = ""
		join_etapa = ""
		where_saldo = ""
		if etapa:
			where_etapa = " and o.fk_etapa = {}".format(etapa)
			join_etapa = " and o.fk_etapa = {}".format(etapa)
		if estadocuenta:
			where_saldo = " 1 = 1 "
		else:
			where_saldo = " cta.saldo > 0 "
		if company == "arcadia":
			if where_etapa == "":
				where_etapa = " and i.fk_etapa in (8,9,10,33)"
			sql = """ select count(*) as cuantos
			from cuenta cta join inmueble i on cta.fk_inmueble = i.codigo
			join cliente cte on cte.codigo = cta.fk_cliente
			where {} {} and cte.nombre like '%{}%' 
			""".format(
				where_saldo, where_etapa, nombre
			)
		else:
			sql = """ select count(*) as cuantos
			from cuenta cta join ofertas_compra o on o.cuenta = cta.codigo 
			{} and o.cancelada = 0
			left join inmueble i on cta.fk_inmueble = i.codigo 
			join cliente cte on cte.codigo = cta.fk_cliente
			where {} {} and cte.nombre like '%{}%'
			""".format(
				join_etapa, where_saldo, where_etapa, nombre
			)

		try:

			sql = preparaQuery(sql)
			sql = sql.encode("iso-8859-1")
			if company == "arcadia":
				engine = Base2.metadata.bind
			else:
				engine = Base.metadata.bind
			poolconn = engine.connect()
			c = poolconn.connection
			cu = c.cursor()
			r = []
			# for i,x in enumerate(DBSession.execute( sql ),1):
			for i, x in enumerate(cu.execute(sql), 1):
				r.append(dict(id=1, cuantos=x.cuantos))

			cu.close()
			try:
				poolconn.close()
			except:
				pass
			return dict(clientescuantosconcuentanosaldadas=r)

		except:
			print_exc()
			return dict(clientescuantosconcuentanosaldadas=[])


@resource(
	collection_path="api/clientecuantofiltros", path="api/clientecuantofiltros/{id}"
)
class ClienteCuantoFiltro(EAuth, QueryAndErrors):
	def __init__(self, request, context=None):
		self.request = request

	def upper(self, val):
		if not val:
			return val
		try:
			decoded = val
			good = decoded.upper()
			print(good)
			return good
		except:
			traceback.print_exc()
			raise ZenError(1)

	def collection_get(self):
		print("entro aqui clientecuantofiltros")
		que, record, token = self.auth(get_token=True)
		cualquiera = "TD"
		if not que:
			# return record
			return dict(clientecuantofiltros=[])
		tipo = self.request.params.get("tipo", cualquiera)
		# CC = cuenta con saldo CS = cuenta sin saldo CV = cuenta vigente

		nombre = self.request.params.get("nombre", "")
		company = self.request.params.get("company", "")
		# nombre = nombre.replace("%", "")
		if len(nombre) < 2:
			return dict(clientecuantofiltros=[])
		etapa = self.request.params.get("etapa", "")
		try:
			etapa = int(etapa)
		except:
			etapa = ""
		where_etapa = ""
		join_etapa = ""
		if etapa:
			join_etapa = "join inmueble i on cta.fk_inmueble = i.codigo"
			where_etapa = " and i.fk_etapa = {}".format(etapa)

		where_saldo = ""
		if tipo == "CV":
			pass
		else:
			if tipo == "CC":
				where_saldo = " and cta.saldo > 0"
			if tipo == "CS":
				where_saldo = " and cta.saldo <= 0"

		try:
			if tipo == cualquiera:
				sql = """select count(*) as cuantos
				from cliente where nombre like '%{}%'""".format(
					self.upper(nombre)
				)
			elif tipo == "SC":
				sql = """select count(*) as cuantos
				from cliente where codigo not in select fk_cliente from cuenta 
				and nombre like '%{}%'""".format(
					self.upper(nombre)
				)
			else:
				sql = """ select count(*) as cuantos
				from cuenta cta  
				join cliente cte on cte.codigo = cta.fk_cliente
				{}
				where 1 = 1 {} {} and cte.nombre like '%{}%'
				""".format(
					join_etapa, where_etapa, where_saldo, self.upper(nombre)
				)

			sql = preparaQuery(sql)
			session = None
			if company == "arcadia":
				session = DBSession2
			else:
				session = DBSession
			r = []
			for i, x in enumerate(session.execute(sql), 1):
				r.append(dict(id=1, cuantos=x.cuantos))
			color("se resolvio query clientefiltros {} {}".format(tipo, len(r)))
			return dict(clientecuantofiltros=r)
		except:
			print_exc()
			try:
				poolconn.close()
			except:
				pass
			return dict(clientecuantofiltros=[])


@resource(collection_path="api/clientefiltros", path="api/clientesfiltros/{id}")
class ClienteFiltro(EAuth, QueryAndErrors):
	def __init__(self, request, context=None):
		self.request = request

	def upper(self, val):
		if not val:
			return val
		try:
			decoded = val
			good = decoded.upper()
			print(good)
			return good
		except:
			traceback.print_exc()
			raise ZenError(1)

	def collection_get(self):
		que, record, token = self.auth(get_token=True)
		cualquiera = "TD"
		if not que:
			# return record
			return dict(clientefiltros=[])
		tipo = self.request.params.get("tipo", cualquiera)
		# CC = cuenta con saldo CS = cuenta sin saldo CV = cuenta vigente
		company = self.request.params.get("company", "")
		nombre = self.request.params.get("nombre", "")
		# nombre = nombre.replace("%", "")
		if len(nombre) < 2:
			return dict(clientefiltros=[])
		etapa = self.request.params.get("etapa", "")
		try:
			etapa = int(etapa)
		except:
			etapa = ""
		where_etapa = ""
		join_etapa = ""
		if etapa:
			join_etapa = "join inmueble i on cta.fk_inmueble = i.codigo"
			where_etapa = " and i.fk_etapa = {}".format(etapa)

		where_saldo = ""
		if tipo == "CV":
			pass
		else:
			if tipo == "CC":
				where_saldo = " and cta.saldo > 0"
			if tipo == "CS":
				where_saldo = " and cta.saldo <= 0"

		try:
			if tipo == cualquiera:
				sql = """select 0 as cuenta, nombre, 0 as saldo,
				codigo as cliente from cliente where nombre like '%{}%' order by nombre""".format(
					self.upper(nombre)
				)

			elif tipo == "SC":
				sql = """select 0 as cuenta, nombre, 0 as saldo,
				codigo as cliente from cliente where codigo not in select fk_cliente from cuenta 
				and nombre like '%{}%'""".format(
					self.upper(nombre)
				)
			else:
				sql = """ select cta.codigo as cuenta, cte.nombre as nombre, 
				cta.saldo as saldo, cte.codigo as cliente
				from cuenta cta  
				join cliente cte on cte.codigo = cta.fk_cliente
				{}
				where 1 = 1 {} {} and cte.nombre like '%{}%'
				order by cte.nombre """.format(
					join_etapa, where_etapa, where_saldo, self.upper(nombre)
				)
			sql = preparaQuery(sql)
			print("viendo el query")
			print(sql)
			r = []
			session = None
			if company == "arcadia":
				session = DBSession2
			else:
				session = DBSession
			for i, x in enumerate(session.execute(sql), 1):
				r.append(
					dict(
						id=int(x.cliente),
						cuenta=int(x.cuenta),
						nombre=x.nombre,
						saldo=str(x.saldo),
					)
				)
			print(sql)
			color("se resolvio query clientefiltros {} {}".format(tipo, len(r)))
			return dict(clientefiltros=r)

		except:
			print_exc()
			try:
				poolconn.close()
			except:
				pass
			return dict(clientefiltros=[])


@resource(collection_path="api/comisionventas", path="api/comisionventas/{id}")
class ComisionVentas(EAuth, QueryAndErrors):
	def __init__(self, request, context=None):
		self.request = request

	def collection_get(self):
		que, record, token = self.auth(get_token=True)
		if not que:
			return record

		inmueble = self.request.params.get("inmueble", "")
		prospecto = self.request.params.get("prospecto", "")
		comision = False
		sql = """
		select i.codigo as inmueble, e.codigo as etapa,
		e.fk_desarrollo as desarrollo from inmueble i join etapa e 
		on i.fk_etapa = e.codigo
		where i.codigo = {} 

		""".format(
			inmueble
		)
		for x in DBSession.execute(sql):
			desarrollo = x.desarrollo

		sql = """
			select idvendedor as vendedor from gixprospectos where idprospecto = {}
		""".format(
			prospecto
		)
		vendedor = 0

		for x in DBSession.execute(sql):
			vendedor = x.vendedor

		sql = """
			select count(*) as cuantos from porcentaje_comision 
			where fk_vendedor = {} and fk_desarrollo = {}
		""".format(
			vendedor, desarrollo
		)

		cuantos = 0
		for x in DBSession.execute(sql):
			cuantos = x.cuantos

		if cuantos:
			comision = True
		return dict(comisionventas=[dict(id=1, comision=comision)])


@resource(collection_path="api/preciosubicacions", path="api/preciosubicacions/{id}")
class PreciosUbicaciones(EAuth, QueryAndErrors):
	def __init__(self, request, context=None):
		self.request = request

	def collection_get(self):
		que, record, token = self.auth(get_token=True)
		if not que:
			return dict(preciosubicacions=[])
		sql = """
		select d.descripcion as desarrollo,
		e.descripcion as etapa,
		p.fk_preciosetapaasignacion as tipoprecio,
		p.precioasignacion as precio,
		count(*) as total
		from gixpreciosetapaofertaasignacion p
		join etapa e on p.fk_etapa = e.codigo
		join desarrollo d on e.fk_desarrollo = d.codigo
		where p.fk_preciosetapaasignacion > 0
		group by d.descripcion, e.descripcion,
		p.fk_preciosetapaasignacion, p.precioasignacion order by 1,2,3
		"""
		resultado = []
		for i, x in enumerate(DBSession.execute(preparaQuery(sql)), 1):
			resultado.append(
				dict(
					id=i,
					desarrollo=dec_enc(x.desarrollo),
					etapa=dec_enc(x.etapa),
					tipoprecio=x.tipoprecio,
					precio=x.precio,
					total=x.total,
				)
			)
		return dict(preciosubicacions=resultado)


@resource(
	collection_path="api/asignadasporsemanas", path="api/asignadasporsemanas/{id}"
)
class AsignadasPorSemana(EAuth, QueryAndErrors):
	def __init__(self, request, context=None):
		self.request = request

	def collection_get(self):
		que, record, token = self.auth(get_token=True)
		if not que:
			return dict(asignadasporsemanas=[])
		try:
			p = self.request.params
			etapa = p.get("etapa", "")
			semanas = int(p.get("semanas", 10))
		except:
			print_exc()
			return dict(asignadasporsemanas=[])
		return dict(
			asignadasporsemanas=graficos.asignadasporsemana(DBSession, semanas, etapa)
		)


@resource(collection_path="api/panoramas", path="api/panoramas/{id}")
class PanoramaComercial(EAuth, QueryAndErrors):
	def __init__(self, request, context=None):
		self.request = request

	def collection_get(self):
		que, record, token = self.auth(get_token=True)
		if not que:
			return dict(panoramas=[])
		try:
			p = self.request.params
			etapa = p.get("etapa", "")
		except:
			print_exc()
			return dict(panoramas=[])
		return dict(
			panoramas=graficos.panoramacomercial(
				DBSession, etapa, resumen_elixir_breve(etapa)
			)
		)


@resource(collection_path="api/ventasporsemanas", path="api/ventasporsemanas/{id}")
class VentasPorSemana(EAuth, QueryAndErrors):
	def __init__(self, request, context=None):
		self.request = request

	def collection_get(self):
		que, record, token = self.auth(get_token=True)
		if not que:
			return dict(ventasporsemanas=[])
		try:
			p = self.request.params
			semanas = int(p.get("semanas", 10))
			etapa = p.get("etapa", "")
		except:
			print_exc()
			return dict(ventasporsemanas=[])
		return dict(
			ventasporsemanas=graficos.ventasporsemana(DBSession, semanas, etapa)
		)


@resource(collection_path="api/cobradasporsemanas", path="api/cobradasporsemanas/{id}")
class CobradasPorSemana(EAuth, QueryAndErrors):
	def __init__(self, request, context=None):
		self.request = request

	def collection_get(self):
		que, record, token = self.auth(get_token=True)
		if not que:
			return dict(cobradasporsemanas=[])
		try:
			p = self.request.params
			semanas = int(p.get("semanas", 10))
			etapa = p.get("etapa", "")
		except:
			print_exc()
			return dict(cobradasporsemanas=[])
		return dict(
			cobradasporsemanas=graficos.cobradasporsemana(DBSession, semanas, etapa)
		)


@resource(collection_path="api/ventaspordias", path="api/ventaspordias/{id}")
class VentasPorDia(EAuth, QueryAndErrors):
	def __init__(self, request, context=None):
		self.request = request

	def collection_get(self):
		que, record, token = self.auth(get_token=True)
		if not que:
			return dict(ventaspordias=[])
		try:
			p = self.request.params
			tipo = p.get("tipo", "1")

		except:
			print_exc()
			return dict(ventaspordias=[])
		return dict(ventaspordias=graficos.ventaspordia(DBSession, tipo))


@resource(
	collection_path="api/caracteristicasinmuebles",
	path="api/caracteristicasinmuebles/{id}",
)
class CaracteristicasInmuebles(EAuth, QueryAndErrors):
	def __init__(self, request, context=None):
		self.request = request

	def collection_get(self):
		que, record, token = self.auth(get_token=True)
		if not que:
			return dict(caracteristicasinmuebles=[])
		inmueble = self.request.params.get("inmueble", "")

		precio_catalogo = self.request.params.get("precioCatalogo", "0")
		precio = int(self.request.params.get("precio", "0"))
		etapa = self.request.params.get("etapa", "")
		if inmueble:
			sql = """
			select fk_etapa as etapa, coalesce(precio,0) as precio,
			coalesce(preciocatalogo,0) as preciocatalogo,
			coalesce(idpreciocatalogo,"") as idpreciocatalogo 
			from inmueble where codigo = {}
			""".format(
				inmueble
			)
			for x in DBSession.execute(preparaQuery(sql)):
				precio_catalogo = x.preciocatalogo
				precioventa = x.precio
				precio = x.idpreciocatalogo
				etapa = x.etapa

		precioutilizado = precio_catalogo
		if precioutilizado <= 0:
			precioutilizado = precioventa

		if not precio:
			sql = """select id from gixpreciosetapa
			where  precio = {} and fk_etapa = {}""".format(
				precioutilizado, etapa
			)
			for x in DBSession.execute(sql):
				precio = x.id

		sql = """
		select p.id as id, p.cantidad as cantidad, c.descripcion as descripcion from gixpreciosetapacaracteristicas p
		join gixcaracteristicasinmuebles c on c.id = p.fk_idcaracteristica
		where p.fk_idpreciosetapa = {} order by c.descripcion
		""".format(
			precio
		)

		try:
			caracteristicas = []
			for x in DBSession.execute(sql):
				caracteristicas.append(
					dict(
						id=x.id, cantidad=x.cantidad, descripcion=dec_enc(x.descripcion)
					)
				)

			return dict(caracteristicasinmuebles=caracteristicas)

		except:
			traceback.print_exc()
			return dict(caracteristicasinmuebles=[])


@resource(
	collection_path="api/afiliaciondisponibles", path="api/afiliaciondisponibles/{id}"
)
class AfiliacionDisponible(EAuth, QueryAndErrors, OperacionesAfiliacion):
	def __init__(self, request, context=None):
		self.request = request
		self.modelo = "afiliaciondisponible"

	def get(self):
		que, record, token = self.auth(get_token=True)
		if not que:
			return record
		disponibles = 0
		siguiente = ""
		try:
			id = int(self.request.matchdict["id"])
			disponibles = self.obtenDisponibles()
			siguiente = self.obtenAfiliacionDisponible(soloVer=True)
		except:
			print_exc()

		return dict(
			afiliaciondisponible=dict(
				id=id, disponibles=disponibles, siguiente=siguiente
			)
		)


@resource(
	collection_path="api/prospectoconclientes", path="api/prospectoconclientes/{id}"
)
class ProspectoConCliente(EAuth, QueryAndErrors):
	def __init__(self, request, context=None):
		self.request = request

	def get(self):
		que, record, token = self.auth(get_token=True)
		if not que:
			return record
		try:
			prospecto = int(self.request.matchdict["id"])
			record = DBSession.query(Prospecto).get(prospecto)
			fechacierre = ""
			cuenta = 0
			cliente = 0
			nombre = ""
			if record.fechacierre:
				fc = record.fechacierre
				d = fc.day
				m = fc.month
				y = fc.year
				fechacierre = "{:04d}/{:02d}/{:02d}".format(y, m, d)
				cuenta = record.cuenta
				assert cuenta, "no tiene cuenta"
				sql = "select cta.fk_cliente as cliente, cte.nombre as nombre from cuenta cta join cliente cte on cta.fk_cliente =  cte.codigo where cta.codigo = {}".format(
					cuenta
				)

				for x in DBSession.execute(sql):
					nombre = x.nombre or ""
					cliente = x.cliente or 0
				assert cliente, "no hay cliente"

			return dict(
				prospectoconcliente=dict(
					id=prospecto,
					cliente=cliente,
					nombre=dec_enc(nombre),
					cuenta=cuenta,
					fechacierre=fechacierre,
				)
			)
		except AssertionError as e:
			print_exc()
			self.request.response.status = 400
			error = e.args[0]
		except:
			print_exc()
			self.request.response.status = 400
			error = "datos invalidos"

		return dict(error=error)


@resource(
	collection_path="api/transicionprospectos", path="api/transicionprospectos/{id}"
)
class TransicionProspecto(EAuth, QueryAndErrors):
	def __init__(self, request, context=None):
		self.request = request

	def collection_get(self):
		que, record, token = self.auth(get_token=True)
		if not que:
			return dict(transicionprospectos=[])
		p = self.request.params
		prospecto = p.get("prospecto", "")
		try:
			assert prospecto, "No hay prospecto"
		except AssertionError as e:
			self.request.response.status = 400
			return self.edata_error(e.args[0])

		sql = """
		select t.idtransicion id,
		t.fecha as fecha,
		t.transicion as transicion,
		isnull(v.nombre,'') as vendedor,
		isnull(g.nombre,'') as gerente,
		t.notas as notas
		from gixprospectostransiciones t
		left join vendedor v on t.fkvendedor = v.codigo
		left join gerentesventas g on g.codigo = t.fkgerente
		where t.fkprospecto = {} order by t.idtransicion desc 
		""".format(
			prospecto
		)
		resultado = []
		for x in DBSession.execute(preparaQuery(sql)):
			resultado.append(
				dict(
					id=x.id,
					fecha=x.fecha.isoformat(),
					transicion=x.transicion,
					gerente=dec_enc(x.gerente),
					vendedor=dec_enc(x.vendedor),
					notas=dec_enc(x.notas),
				)
			)
		return dict(transicionprospectos=resultado)


@resource(
	collection_path="api/revinculacionprospectos",
	path="api/revinculacionprospectos/{id}",
)
class RevinculacionProspecto(EAuth, QueryAndErrors):
	def __init__(self, request, context=None):
		self.request = request
		self.modelo = "revinculacionprospecto"
		# print "valor de self.requ

	def store(self, record, id=None):
		engine = Base.metadata.bind
		poolconn = engine.connect()
		cn_sql = poolconn.connection
		self.cn_sql = cn_sql
		self.poolconn = poolconn
		tuser = cached_results.dicTokenUser.get(self.usertoken, None)
		usuario = "usuarioxxx"
		if tuser:
			usuario = tuser.get("usuario", "usuarioxxx")
		p = self.request.params

		vendedor = record.get("vendedor", None)
		gerente = record.get("gerente", None)
		prospecto = record.get("prospecto", None)

		sql = """
			select p.idvendedor vendedororiginal, 
			v.nombre as nombrevendedororiginal,
			g.nombre as gerenteoriginal,
			convert(varchar(100), BlogGUID) as blogguid,
			v.vendedorvirtual as vendedorvirtual from gixprospectos p
			join VENDEDOR v on v.codigo = p.idvendedor
			join gerentesventas g on g.codigo = p.idgerente
			where p.idprospecto = {}
		""".format(
			prospecto
		)

		for x in DBSession.execute(preparaQuery(sql)):
			vendedororiginal = x.vendedororiginal
			nombrevendedororiginal = x.nombrevendedororiginal.decode("iso-8859-1")
			gerenteoriginal = x.gerenteoriginal.decode("iso-8859-1")
			blogguid = x.blogguid

		sql = """select max(ciclo) as ciclo from gixprospectostransiciones
		where fkprospecto = {}""".format(
			prospecto
		)
		ciclo = 0

		for x in DBSession.execute(preparaQuery(sql)):
			ciclo = x.ciclo

		try:
			assert prospecto, "Prospecto Invalido"
			assert vendedor, "Vendedor invalido"
			assert gerente, "Gerente invalido"
			assert ciclo, "Ciclo invalido"
			assert blogguid, "Blog UID invalido"
			if self.gerente:
				assert gerente == self.gerente, "Gerente no coincide"
		except AssertionError as e:
			self.request.response.status = 400
			return self.edata_error(e.args[0])

		if True:
			try:
				sql = """
				update gixprospectos set idgerente = {}, idvendedor = {}
				where idprospecto = {}
				""".format(
					gerente, vendedor, prospecto
				)
				print(paint.blue(sql))
				ok, error = self.commit(preparaQuery(sql))
				if not ok:
					self.request.response.status = 400
					return self.edata_error(error)

				sql = """
				insert into gixprospectostransiciones
				(fkprospecto, fecha, transicion,
				fkgerente, fkvendedor, ciclo, notas)
				values ({}, getdate(), 'R', {}, {}, {},
				'Revinculacion del prospecto')
				""".format(
					prospecto, gerente, vendedor, ciclo
				)

				print(paint.blue(sql))
				ok, error = self.commit(preparaQuery(sql))
				if not ok:
					self.request.response.status = 400
					return self.edata_error(error)

				mensaje = (
					"Revinculación proveniente del vendedor: {}, Gerente: {}".format(
						nombrevendedororiginal, gerenteoriginal
					)
				)

				sql = """
				insert into Blogs
				(BlogGUID, FechaCaptura, UsuarioCaptura,
				ContenidoText, ContenidoBinario, Extension, Estatus)
				values ('{}', getdate(), '{}', '{}', '', '', '')
				""".format(
					blogguid, usuario, mensaje
				)
				sql = preparaQuery(sql)

				sqlx = sql.encode("iso-8859-1")

				ok, error = self.commit(sqlx)
				if not ok:
					self.request.response.status = 400
					return self.edata_error(error)

				record["id"] = 1
				try:
					poolconn.close()
				except:
					pass
				return dict(revinculacionprospecto=record)
			except:
				print_exc()
				try:
					poolconn.close()
				except:
					pass
				self.request.response.status = 400
				return self.edata_error("Error al grabar")

	@view(renderer="json")
	def collection_post(self):
		print("Revinculacion Prospecto")
		que, record, token = self.auth(self.modelo, get_token=True)
		self.usertoken = token
		if not que:
			self.request.response.status = 400
			return self.edata_error("credenciales invalidas")
			# return self.edata_error( e.args[0])
		user = cached_results.dicTokenUser.get(token)
		usuario = user.get("usuario", "")
		perfil = user.get("perfil", "")
		self.gerente = user.get("gerente", "")
		if perfil not in (
			"admin",
			"direccioncomercial",
			"subdireccioncomercial",
			"gerente",
		):

			self.request.response.status = 400
			color("invalid perfil revinculacion")
			error = "perfil no autorizado"
			return self.edata_error(error)
		return self.store(record)


@resource(collection_path="api/prospectos", path="api/prospectos/{id}")
class ProspectoData(EAuth, QueryAndErrors, OperacionesAfiliacion):
	def __init__(self, request, context=None):
		self.request = request
		self.modelo = "prospecto"
		# print "valor de self.request", self.request

	def quitaespacios(self, d):
		for x in d:
			if (
				x
				in "rfc curp lugardetrabajo telefonocelular extensionoficina telefonocasa mediopublicitariosugerido telefonooficina".split()
			):
				try:
					d[x] = d[x].strip()
				except:
					print_exc()
					d[x] = ""
		return d

	def collection_get(self):
		que, record, token = self.auth(get_token=True)

		prospecto = self.request.params.get("prospecto", "")
		if not que:
			return record
		try:
			query = DBSession.query(Prospecto)
			if prospecto:
				query = query.filter(Prospecto.idprospecto == prospecto)
			query = query.order_by(Prospecto.idprospecto)

			return dict(
				prospectos=[self.quitaespacios(x.cornice_json) for x in query.all()]
			)

		except:
			traceback.print_exc()
			return dict(prospectos=[])

	def get(self):

		que, record, token = self.auth(get_token=True)
		print("despues de auth en get de prospectos")
		if not que:
			return record

		try:
			print("antes de query")
			record = DBSession.query(Prospecto).get(int(self.request.matchdict["id"]))
			print("si obtuve prospecto")
			return dict(prospecto=record.cornice_json)
		except:
			pass
		return dict()

	def store(self, record, id=None):
		engine = Base.metadata.bind

		poolconn = engine.connect()
		cn_sql = poolconn.connection
		self.cn_sql = cn_sql
		self.poolconn = poolconn
		tuser = cached_results.dicTokenUser.get(self.usertoken, None)
		# record["fechaasignacion"] = None
		usuario = "usuarioxxx"
		if tuser:
			usuario = tuser.get("usuario", "usuarioxxx")

		fmap = dict(
			apellidopaterno="apellidopaterno1",
			apellidomaterno="apellidomaterno1",
			nombre="nombre1",
			fechadenacimiento="fechadenacimiento",
			telefonocasa="telefonocasa",
			telefonooficina="telefonooficina",
			extensionoficina="extensionoficina",
			telefonocelular="telefonocelular",
			lugardetrabajo="lugardetrabajo",
			idmediopublicitario="idmediopublicitario",
			mediopublicitariosugerido="mediopublicitariosugerido",
			contado="contado",
			congelado="congelado",
			hipotecaria="hipotecaria",
			pensiones="pensiones",
			fovisste="fovisste",
			gerente="idgerente",
			vendedor="idvendedor",
			rfc="rfc",
			curp="curp",
			afiliacion="afiliacionimss",
			fechaalta="fechaasignacion",
			fechacierre="fechacierre",
		)

		record["cuenta"] = None
		fmap["cuenta"] = "cuenta"

		print("record", record)
		if not id:
			prospecto = Prospecto()
		if True:
			try:
				afi = record.get("afiliacion", "")
				if afi and Prospecto.existeAfiliacion(afi):
					self.request.response.status = 400

					return self.edata_error("Existe la afiliacion ya en prospectos")

				if record.get("curp", "") == "":
					self.request.response.status = 400
					return self.edata_error("No ha definido Curp")

				if Prospecto.existeCurp(record.get("curp")):
					self.request.response.status = 400
					return self.edata_error("Existe Curp ya en prospectos")

				if id:
					prospecto = DBSession.query(Prospecto).get(id)
					print("processing id ", id)

			except:
				print_exc()
				self.request.response.status = 400
				return self.edata_error("problema al grabar")

		gerente = 0
		vendedor = 0
		afiliacion_en_rdb = ""
		for x in sorted(record.keys()):
			try:
				field = fmap.get(str(x))
				value = record.get(x)
				try:
					# print field, value
					if field in (
						"apellidopaterno1",
						"apellidomaterno1",
						"nombre1",
						"lugardetrabajo",
						"mediopublicitariosugerido",
					):
						v = value.encode("utf-8")
						# v = value
						value = upper2(v)
						# print "aftershave", field, value
						value = value.decode("utf-8").encode("iso-8859-1")

					if field in ("idmediopublicitario", "idgerente", "idvendedor"):
						value = int(value)
						if field == "idgerente":
							gerente = value
						if field == "idvendedor":
							vendedor = value
					elif field == "fechaasignacion":
						f = datetime.now()
						# fecha = "{}/{:02d}/{:02d}".format(f.year, f.month, f.day)
						value = datetime(year=f.year, month=f.month, day=f.day)
						fecha_asig = "{:04d}/{:02d}/{:02d}".format(
							value.year, value.month, value.day
						)
					elif field == "fechadenacimiento":
						if value:
							y, m, d = [int(x) for x in value.split("/")]
							value = datetime(year=y, month=m, day=d)
						else:
							value = None
					elif field == "fechacierre":
						value = None
					elif field == "cuenta":
						value = 0

					elif field == "afiliacionimss":
						if value:
							print("si hay valor de afiliacion y es ", value)
						else:
							value = self.obtenAfiliacionDisponible()
							afiliacion_en_rdb = value
							assert (
								afiliacion_en_rdb
							), "No se pudo obtener afiliacion automatica"
					else:
						if isinstance(value, bool):
							value = value
						else:
							value = str(value)
					setattr(prospecto, field, value)
				except AssertionError as e:
					self.request.response.status = 400
					return self.edata_error(e.args[0])
			except:
				print("algo no pronosticado ocurre en asignacion de campos")
				traceback.print_exc()
				return self.edata_error(
					"error no pronosticado en asignacion de campos de prospecto"
				)

		try:
			# assert 1 == 2, "escape momentaneo"
			nuevo_id = ""
			with transaction.manager:
				DBSession.add(prospecto)
				if DBSession.new:
					DBSession.flush()
					print("hice flush()")
				try:
					nuevo_id = prospecto.id
					print("el id es, ", nuevo_id)
				except:
					pass
			try:
				sql = """
					insert into gixprospectostransiciones
				(fkprospecto, fecha, transicion, fkgerente, fkvendedor, ciclo, notas)
				values ({}, getdate(), 'I', {}, {}, 1, 'Alta del prospecto (Inicio 1er. ciclo)')""".format(
					nuevo_id, gerente, vendedor
				)
				sql = sql.replace("\n", " ")
				sql = sql.replace("\t", " ")
				print(paint.blue(sql))

				ok, error = self.commit(sql)
				if not ok:
					self.request.response.status = 400
					return self.edata_error(error)

				sql = "select convert(varchar(100), BlogGUID) as blogguid from gixprospectos where idprospecto = {}".format(
					nuevo_id
				)
				for x in DBSession.execute(sql):
					blogguid = x.blogguid
				sql = """
				insert into Blogs (BlogGUID, FechaCaptura, UsuarioCaptura,
					ContenidoText, ContenidoBinario, Extension)
					values ('%s', getdate(), '{}', '{}', '{}', '{}')
				""".format(
					blogguid, usuario, "Alta del Prospecto", "", ""
				)

				sql = sql.replace("\n", " ")
				sql = sql.replace("\t", " ")
				print(paint.blue(sql))

				ok, error = self.commit(sql)
				if not ok:
					self.request.response.status = 400
					return self.edata_error(error)

				print("el id es ", nuevo_id)
				record["id"] = nuevo_id
				record["fechaalta"] = fecha_asig

				if afiliacion_en_rdb:
					record["afiliacion"] = afiliacion_en_rdb
					self.actualiza_prospecto_en_rdb(nuevo_id, afiliacion_en_rdb)
					# table.filter(dict(afiliacion = afiliacion_en_rdb)).update(dict(prospecto = True)).run()
				try:
					poolconn.close()
				except:
					pass
				return dict(prospecto=record)
			except:
				traceback.print_exc()
				try:
					poolconn.close()
				except:
					pass
				self.request.response.status = 400
				return self.edata_error("problema al grabar segundo paso")
		except:
			traceback.print_exc()
			self.request.response.status = 400
			return self.edata_error("problema al grabar")

	def deleterecord(self, id):
		try:
			return dict()  # escape momentaneo
			with transaction.manager:
				DBSession.delete(DBSession.query(Prospecto).get(id))
		except:
			traceback.print_exc()
			self.request.response.status = 400
			return dict(error="problema al borrar")
		return dict()

	@view(renderer="json")
	def collection_post(self):
		print("inserting Prospecto")
		que, record, token = self.auth(self.modelo, get_token=True)
		self.usertoken = token
		if not que:
			return record
		return self.store(record)

	@view(renderer="json")
	def put(self):
		print("updating Prospecto")
		que, record = self.auth(self.modelo, get_token=True)
		if not que:
			return record
		id = int(self.request.matchdict["id"])
		return self.store(record=record, id=id)

	@view(renderer="json")
	def delete(self):
		print("deleting Prospecto")
		que, record = self.auth(self.modelo, get_token=True)
		if not que:
			return record
		id = int(self.request.matchdict["id"])
		return self.deleterecord(id)


@resource(collection_path="api/hijos", path="api/hijoss/{id}")
class HijoRest(EAuth, QueryAndErrors):
	def __init__(self, request, context=None):
		self.request = request
		self.modelo = "chilpayate"

	def get(self):
		que, record, token = self.auth(get_token=True)
		if not que:
			return record
		return dict()

	def collection_get(self):
		# print self.request.headers
		que, record = self.auth()
		if not que:
			return record
		self.request.response.status = 400
		return dict(error="Pendiente de implementar")

	def collection_post(self):
		print("inserting Cliente")
		que, record, token = self.auth(self.modelo, get_token=True)
		user = cached_results.dicTokenUser.get(token)
		if not que:
			return record
		user = cached_results.dicTokenUser.get(token)
		usuario = user.get("usuario", "")
		perfil = user.get("perfil", "")
		if perfil not in ("admin", "comercial", "subdireccioncomercial"):

			self.request.response.status = 400
			error = "perfil no autorizado"
			return self.edata_error(error)
		if perfil == "admin":
			self.hazqueries = True
		else:
			self.hazqueries = False
		return self.store(record)

	def store(self, record, id=None):
		print("Voy a generar el hijo")
		queries = []

		error = "Pendiente de implementar"
		print(record)
		try:
			engine = Base.metadata.bind
			poolconn = engine.connect()
			self.poolconn = poolconn
			c = poolconn.connection
			cu = c.cursor()
			if not record["meses"]:
				record["meses"] = 0
			for x in "cliente meses anos sexo".split(" "):
				assert record.get(x, "") != "", "{} esta vacio".format(x)
				if x == "sexo":
					assert record.get("sexo", "") in ("M", "F"), "sexo debe ser M o F"
				elif x == "meses":
					assert int(record.get(x)) >= 0, "meses esta mal"
				else:
					assert int(record.get(x)) > 0, "{} debe ser entero".format(x)
			sql = "insert into hijos(fk_cliente, anios, meses, fecha) values ({},{},{}, getdate())".format(
				record.get("cliente"), record.get("anos"), record.get("meses")
			)
			cu.execute(sql)
			c.commit()
		except AssertionError as e:
			print_exc()
			error = e.args[0]
			self.request.response.status = 400
			return self.edata_error(error)
		except:
			print_exc()
			error = l_traceback()

			self.request.response.status = 400
			return self.edata_error(error)

		record["id"] = str(uuid.uuid4())
		try:
			poolconn.close()
		except:
			pass
		return dict(hijos=[record])
		# self.request.response.status = 400
		# return dict(errors = dict( resultado = [error]))


@resource(collection_path="api/desasignacions", path="api/desasignacions/{id}")
class DesasignacionRest(EAuth, QueryAndErrors):
	def __init__(self, request, context=None):
		self.request = request
		self.modelo = "desasignacion"

	def get(self):
		que, record, token = self.auth(get_token=True)
		if not que:
			return record
		return dict()

	def collection_get(self):
		# print self.request.headers
		que, record = self.auth()
		if not que:
			return record
		self.request.response.status = 400
		return dict(error="Pendiente de implementar")

	def collection_post(self):

		que, record, token = self.auth(self.modelo, get_token=True)
		user = cached_results.dicTokenUser.get(token)
		if not que:
			return record
		user = cached_results.dicTokenUser.get(token)
		usuario = user.get("usuario", "")
		perfil = user.get("perfil", "")
		if perfil not in ("admin", "auxiliarsubdireccion"):

			self.request.response.status = 400
			error = "perfil no autorizado"
			return self.edata_error(error)
		if perfil == "admin":
			self.hazqueries = True
		else:
			self.hazqueries = False
		return self.store(record)

	def store(self, record, id=None):
		print("Voy a hacer desasignacion")
		queries = []
		psql = preparaQuery

		error = "Pendiente de implementar"
		try:
			cuenta = record.get("cuenta", 0)
			assert cuenta, "no hay cuenta"
			oferta = record.get("oferta", 0)
			assert oferta, "no hay oferta"
			inmueble = 0
			contrato = 0
			sql = """
			select fk_inmueble as inmueble, contrato from cuenta where codigo={} 
			""".format(
				cuenta
			)

			for x in DBSession.execute(sql):
				inmueble = x.inmueble
				contrato = x.contrato
			assert inmueble, "no esta asignado"
			assert contrato == oferta, "contrato distinto a oferta"
			engine = Base.metadata.bind
			poolconn = engine.connect()
			c = poolconn.connection
			self.cn_sql = c
			self.poolconn = poolconn

			campos = "codigo,fecha, saldo, fk_cliente, fk_inmueble, fk_tipo_cuenta, contrato, tipo_contrato, inmueble_anterior, oferta_anterior, fk_etapa, fecha_prerecibo, monto_prerecibo, recibo_prerecibo, bruto_precalificacion, avaluoimpuesto_precalificacion, subsidio_precalificacion, pagare_precalificacion"
			sql = """ insert into desasignacion( {} ) select {} from cuenta where codigo = {}
			""".format(
				campos, campos, cuenta
			)
			sql = psql(sql)
			queries.append(sql)
			ok, error = self.commit(sql)
			if not ok:
				self.request.response.status = 400
				self.logqueries(queries)
				return self.edata_error(error)

			sql = """
			update cuenta set fk_inmueble=0, inmueble_anterior={}
			where codigo ={}
			""".format(
				inmueble, cuenta
			)
			sql = psql(sql)
			queries.append(sql)
			ok, error = self.commit(sql)
			if not ok:
				self.request.response.status = 400
				self.logqueries(queries)
				return self.edata_error(error)

			sql = """
			update anticipocomision set fk_inmueble=0 where fk_cuenta={}
			""".format(
				cuenta
			)
			sql = psql(sql)
			queries.append(sql)
			ok, error = self.commit(sql)
			if not ok:
				self.request.response.status = 400
				self.logqueries(queries)
				return self.edata_error(error)
			sql = """
			update comision set fk_inmueble=0 where fk_cuenta={}
			""".format(
				cuenta
			)
			sql = psql(sql)
			queries.append(sql)
			ok, error = self.commit(sql)
			if not ok:
				self.request.response.status = 400
				self.logqueries(queries)
				return self.edata_error(error)
			sql = """
			update inmueble set precio=0 where codigo={}
			""".format(
				inmueble
			)
			sql = psql(sql)
			queries.append(sql)
			ok, error = self.commit(sql)
			if not ok:
				self.request.response.status = 400
				self.logqueries(queries)
				return self.edata_error(error)

			try:
				poolconn.close()
			except:
				pass

		except AssertionError as e:
			print_exc()
			error = e.args[0]
			self.request.response.status = 400
			return self.edata_error(error)
		except:
			print_exc()
			error = l_traceback()
			self.request.response.status = 400
			return self.edata_error(error)

		self.logqueries(queries)

		record["id"] = contrato

		return dict(desasignacions=[record])


@resource(collection_path="api/clientes", path="api/clientes/{id}")
class ClienteRest(EAuth, QueryAndErrors):
	def __init__(self, request, context=None):
		self.request = request
		self.modelo = "cliente"

	def boolSql(self, value):
		return -1 if value else 0

	def cleanSql(self, sql):
		sql = sql.replace("\n", " ")
		sql = sql.replace("\t", " ")
		return sql

	def upper(self, key):
		# print( paint.yellow("la llave es {}".format(key)))
		try:
			val = self.record[key]
		except:
			traceback.print_exc()
			val = ""

		if not val:
			return val
		try:

			# print ( paint.yellow("a convertir {}".format(key)))
			decoded = val
			good = decoded.upper()
			# print good

			return good
		except:
			print(paint.red("saliendo por error en upper()"))
			traceback.print_exc()
			raise ZenError(1)

	def get(self):
		que, record, token = self.auth(get_token=True)
		if not que:
			return record
		company = self.request.params.get("company", "")
		if company:
			return dict(cliente=dict(nombre="si es arcadia"))
		else:
			return dict(cliente=dict(nombre="jajaja"))

	def collection_get(self):

		que, record = self.auth()
		if not que:
			return record
		self.request.response.status = 400
		return dict(error="Pendiente de implementar")

	def collection_post(self):
		print("inserting Cliente")
		que, record, token = self.auth(self.modelo, get_token=True)
		user = cached_results.dicTokenUser.get(token)
		if not que:
			return record
		user = cached_results.dicTokenUser.get(token)
		usuario = user.get("usuario", "")
		perfil = user.get("perfil", "")
		if perfil not in ("admin", "comercial", "subdireccioncomercial"):

			self.request.response.status = 400
			error = "perfil no autorizado"
			return self.edata_error(error)
		if perfil == "admin":
			self.hazqueries = True
		else:
			self.hazqueries = False
		return self.store(record)

	def store(self, record, id=None):
		print("Voy a generar el cliente")
		queries = []
		error = "Pendiente de implementar"
		ses = DBSession

		cliente = 0
		for x in ses.execute("select max(codigo) + 1 as cliente from cliente"):
			cliente = x.cliente

		try:
			self.record = record
			for row in sorted(record.keys()):
				pass

			assert cliente, "no se obtuvo el codigo de cliente correcto"
			engine = Base.metadata.bind
			poolconn = engine.connect()
			c = poolconn.connection
			self.poolconn = poolconn

			conyugefechanacimiento = "NULL"
			if record["conyugefechanacimiento"]:
				conyugefechanacimiento = "'{}'".format(record["conyugefechanacimiento"])

			fechanacimiento = "NULL"
			if record["fechanacimiento"]:
				fechanacimiento = "'{}'".format(record["fechanacimiento"])
			cu = c.cursor()
			sql = """
			insert into cliente(codigo, nombre, rfc, nacionalidad, lugardenacimiento, 
				fechadenacimiento, estadocivil, situacion, regimen, ocupacion, domicilio, colonia, cp, 
				ciudad, estado, telefonocasa, telefonotrabajo, conyugenombre,conyugenacionalidad, 
				conyugelugardenacimiento, conyugefechadenacimiento, conyugerfc, conyugeocupacion,
				curp, conyugecurp, email, imss, tipo_tramite, titular_ife, titular_ife_copias, 
				titular_afore_copias, titular_carta_empresa, titular_acta_nacimiento,
				titular_acta_nacimiento_copias, conyuge_ife, conyuge_ife_copias, conyuge_afore_copias, 
				conyuge_carta_empresa, conyuge_acta_nacimiento, conyuge_acta_nacimiento_copias,
				acta_matrimonio, acta_matrimonio_copias) values ({}, '{}', '{}',
				'{}','{}',{},
				'{}','{}','{}',
				'{}','{}','{}',
				'{}','{}','{}',
				'{}','{}','{}',
				'{}','{}',{},
				'{}','{}','{}',
				'{}','{}','{}',
				'{}',{},{},
				{},{},{},
				{},{},{},
				{},{},{},
				{},{},{})""".format(
				cliente,
				self.upper("nombre"),
				self.upper("rfc"),
				self.upper("nacionalidad"),
				self.upper("lugarnacimiento"),
				fechanacimiento,
				record["estadocivil"],
				record["situacion"],
				record["regimen"],
				record["ocupacion"],
				self.upper("domicilio"),
				self.upper("colonia"),
				record["codigopostal"],
				self.upper("ciudad"),
				self.upper("estado"),
				record["telefonocasa"],
				record["telefonotrabajo"],
				self.upper("conyugenombre"),
				self.upper("conyugenacionalidad"),
				self.upper("conyugelugarnacimiento"),
				conyugefechanacimiento,
				self.upper("conyugerfc"),
				record["conyugeocupacion"],
				self.upper("curp"),
				self.upper("conyugecurp"),
				record["email"],
				record["afiliacion"],
				record["tipoTramite"],
				self.boolSql(record["titularIfe"]),
				self.boolSql(record["titularCopiasIfe"]),
				self.boolSql(record["titularCopiaAfore"]),
				self.boolSql(record["titularCartaEmpresa"]),
				self.boolSql(record["titularActaNacimiento"]),
				self.boolSql(record["titularCopiasActaNacimiento"]),
				self.boolSql(record["conyugeIfe"]),
				self.boolSql(record["conyugeCopiasIfe"]),
				self.boolSql(record["conyugeCopiaAfore"]),
				self.boolSql(record["conyugeCartaEmpresa"]),
				self.boolSql(record["conyugeActaNacimiento"]),
				self.boolSql(record["conyugeCopiasActaNacimiento"]),
				self.boolSql(record["actaMatrimonio"]),
				self.boolSql(record["copiasActaMatrimonio"]),
			)

			sqlx = self.cleanSql(sql)
			sqlx = sqlx.encode("iso-8859-1")
			print(paint.blue(sqlx))

			cu.execute(sqlx)

			sql = """
			update referencias_rap set cliente = {} 
			where referencia in 
			( select min(referencia) from referencias_rap where isnull(cliente, 0) = 0
			and isnull(cuenta,0) = 0)""".format(
				cliente
			)

			sqlx = self.cleanSql(sql)

			print(paint.blue(sqlx))

			cu.execute(sqlx)

			c.commit()
		except AssertionError as e:
			print_exc()
			error = e.args[0]
			self.request.response.status = 400
			return self.edata_error(error)
		except:
			print_exc()
			error = l_traceback()
			self.request.response.status = 400
			return self.edata_error(error)
		record["id"] = cliente
		print("el cliente es ", cliente)
		try:
			poolconn.close()
		except:
			pass
		return dict(cliente=record)


@resource(collection_path="api/asignacions", path="api/asignacions/{id}")
class AsignacionRest(EAuth, QueryAndErrors):
	def __init__(self, request, context=None):
		self.request = request
		self.modelo = "asignacion"

	def get(self):
		que, record, token = self.auth(get_token=True)
		if not que:
			return record
		return dict()

	def collection_get(self):
		que, record = self.auth()
		if not que:
			return record
		self.request.response.status = 400
		return dict(error="Pendiente de implementar")

	def collection_post(self):
		color("haciendo asignacion")
		que, record, token = self.auth(self.modelo, get_token=True)
		user = cached_results.dicTokenUser.get(token)
		if not que:
			self.request.response.status = 400
			color("escaping inserting oferta")
			return self.edata_error("no autorizado")
		color("continues asignacion")
		user = cached_results.dicTokenUser.get(token)
		usuario = user.get("usuario", "")
		perfil = user.get("perfil", "")
		if perfil not in ("admin", "comercial", "subdireccioncomercial"):

			self.request.response.status = 400
			color("invalid perfil inserting oferta")
			error = "perfil no autorizado"
			return self.edata_error(error)
		if perfil == "admin":
			print(paint.blue("hazqueries true inserting oferta"))
			self.hazqueries = True
		else:
			self.hazqueries = False
		return self.store(record)

	def store(self, record, id=None):
		color("Voy a generar la Oferta", "y")
		psql = preparaQuery

		queries = []
		error = "Pendiente de implementar"
		ses = DBSession

		engine = Base.metadata.bind
		poolconn = engine.connect()
		cn_sql = poolconn.connection
		self.cn_sql = cn_sql
		self.poolconn = poolconn

		hoy = datetime.now()
		fechadeventa = record.get("fechadeventa", "")
		if not fechadeventa:
			fechadeventa = "{:04d}/{:02d}/{:02d}".format(hoy.year, hoy.month, hoy.day)

		oferta = record.get("oferta", 0)
		try:
			localerror = ""
			assert oferta, "oferta vacia"
		except AssertionError as e:
			print_exc()
			localerror = e.args[0]
		if localerror:
			error = localerror

			self.request.response.status = 400
			return self.edata_error(error)
		contrato = oferta
		i = record.get("inmueble", 0)
		try:
			localerror = ""
			assert i, "inmueble vacio"
		except AssertionError as e:
			print_exc()
			localerror = e.args[0]
		if localerror:
			error = localerror

			self.request.response.status = 400
			return self.edata_error(error)

		sql = """
		select i.codigo as inmueble, e.codigo as etapa, d.codigo as desarrollo,
		i.candadoprecio as candadoprecio, i.preciocatalogo as preciocatalogo 
		from inmueble i join etapa e 
		on i.fk_etapa = e.codigo
		join desarrollo d on e.fk_desarrollo = d.codigo
		where i.codigo = {}
		""".format(
			i
		)
		sql = psql(sql)
		queries.append(sql)
		for x in ses.execute(sql):
			inmueble = x.inmueble
			preciocatalogo = x.preciocatalogo
			candadoprecio = x.candadoprecio

			etapa = x.etapa
			desarrollo = x.desarrollo
			found = True
		if found:
			print(
				"Inmueble {}, etapa {}, desarrollo {}".format(
					inmueble, etapa, desarrollo
				)
			)
		else:
			error = "no hay inmueble {}".format(i)
			self.logqueries(queries)
			self.request.response.status = 400
			return self.edata_error(error)

		precio_id = record.get("precio", 0)
		try:
			localerror = ""
			assert preciocatalogo or precio_id, "no tiene precio"
		except AssertionError as e:
			print_exc()
			localerror = e.args[0]
		if localerror:
			error = localerror
			self.logqueries(queries)
			self.request.response.status = 400
			return self.edata_error(error)
		precio = preciocatalogo
		if not candadoprecio:
			found = False
			sql = """select precio from gixpreciosetapa
			where  id = {} and fk_etapa = {}""".format(
				precio_id, etapa
			)
			sql = psql(psql)
			queries.append(sql)
			precio = 0
			for x in ses.execute(sql):
				precio = x.precio
				found = True
			if not found:
				error = "no hay precio en base de datos"
				self.logqueries(queries)
				self.request.response.status = 400
				return self.edata_error(error)
		else:
			found = False
			sql = """select id from gixpreciosetapa
			where  precio = {} and fk_etapa = {}""".format(
				precio, etapa
			)
			sql = psql(sql)
			queries.append(sql)
			precio_id = 0
			for x in ses.execute(sql):
				precio_id = x.id
				found = True
			if not found:
				error = "no hay precio_id en base de datos"
				self.logqueries(queries)
				self.request.response.status = 400
				return self.edata_error(error)

		color("el precio es {}".format(precio))

		sql = """
			select o.cuenta as cuenta, o.cancelada as cancelada, isnull(c.codigo, 0) as cuentaencuenta 
			from ofertas_compra o left join cuenta c on o.cuenta = c.codigo 
			where o.fk_etapa = {}
			and o.oferta = {} 
		""".format(
			etapa, oferta
		)
		sql = psql(sql)
		queries.append(sql)
		cuenta = 0
		cancelada = True
		cuentaencuenta = 0
		for x in ses.execute(sql):
			cuenta = x.cuenta
			cancelada = x.cancelada
			cuentaencuenta = x.cuentaencuenta
		try:
			localerror = ""
			assert cuenta, "no tiene cuenta"
			assert cancelada == False, "la oferta esta cancelada"
			assert cuentaencuenta, "no existe la cuenta en tabla de cuenta"
		except AssertionError as e:
			print_exc()
			localerror = e.args[0]
		if localerror:
			error = localerror
			self.logqueries(queries)
			self.request.response.status = 400
			return self.edata_error(error)

		sql = """
		select referencia from referencias_rap where cuenta = {}
		""".format(
			cuenta
		)
		referenciarap = ""
		for x in ses.execute(sql):
			referenciarap = x.referencia
		try:
			localerror = ""
			assert referenciarap, "no tiene referencia rap"
		except AssertionError as e:
			print_exc()
			localerror = e.args[0]
		if localerror:
			error = localerror
			self.logqueries(queries)
			self.request.response.status = 400
			return self.edata_error(error)

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
			return self.edata_error(error)

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
			return self.edata_error(error)

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
			return self.edata_error(error)

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
			return self.edata_error(error)

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
			return self.edata_error(error)

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
			return self.edata_error(error)
		color("el valor de precio es ".format(precio))
		suma = (
			precalificacion
			+ avaluo
			+ subsidio
			+ pagare
			+ prerecibo
			+ prereciboadicional
		)
		localerror = ""
		if suma != precio:
			localerror = "precios es distinto a suma de precalificacion, avaluo, subsidio, pagare y prerecibos"
			localerror = False  # forzar el false

		if localerror:
			error = localerror
			self.logqueries(queries)
			self.request.response.status = 400
			return self.edata_error(error)

		# comienza asignacion

		sql = """
		update cuenta set fk_inmueble = {}, bruto_precalificacion = {}, avaluoimpuesto_precalificacion = {}, subsidio_precalificacion = {}, pagare_precalificacion = {} where codigo = {}
		""".format(
			inmueble, precalificacion, avaluo, subsidio, pagare, cuenta
		)
		# query con la unica afectacion
		sql = """
		update cuenta set fk_inmueble = {} where codigo = {}
		""".format(
			inmueble, cuenta
		)

		sql = psql(sql)
		queries.append(sql)
		print(paint.blue(sql))
		ok, error = self.commit(sql)
		if not ok:
			self.request.response.status = 400
			self.logqueries(queries)
			return self.edata_error(error)

		sql = """
			update anticipocomision set fk_inmueble = {} where fk_cuenta = {}
		""".format(
			inmueble, cuenta
		)
		sql = psql(sql)
		queries.append(sql)

		ok, error = self.commit(sql)
		if not ok:
			self.request.response.status = 400
			self.logqueries(queries)
			return self.edata_error(error)
		sql = """
			update comision set fk_inmueble = {} where fk_cuenta = {}
		""".format(
			inmueble, cuenta
		)
		sql = psql(sql)
		queries.append(sql)

		ok, error = self.commit(sql)
		if not ok:
			self.request.response.status = 400
			self.logqueries(queries)
			return self.edata_error(error)

		sql = """
			update inmueble set fechadeventa = '{}' , precio = {} where codigo = {}
		""".format(
			fechadeventa, precio, inmueble
		)

		sql = psql(sql)
		queries.append(sql)

		ok, error = self.commit(sql)
		if not ok:
			self.request.response.status = 400
			self.logqueries(queries)
			return self.edata_error(error)

		sql = """
		update ofertas_compra set fecha_asignacion = '{}', asignada = -1, monto_precalificacion = {} where oferta = {} and cuenta = {}
		""".format(
			fechadeventa, precalificacion, contrato, cuenta
		)
		# query afectando lo que realmente es

		sql = """
		update ofertas_compra set fecha_asignacion = '{}', asignada = -1  where oferta = {} and cuenta = {}
		""".format(
			fechadeventa, contrato, cuenta
		)

		sql = psql(sql)
		queries.append(sql)

		ok, error = self.commit(sql)
		if not ok:
			self.request.response.status = 400
			self.logqueries(queries)
			return self.edata_error(error)

		prerecibo = False  # para forzar el else
		prereciboadicional = False
		if prerecibo:

			queries.append("delta prerecibo true")
			sql = """
			insert into prerecibo ( fk_cuenta, fecha, cantidad, referencia) values ( {},'{}',{},'{}')
			""".format(
				cuenta, fechadeventa, prerecibo, referenciarap
			)
			sql = psql(sql)
			queries.append(sql)

			ok, error = self.commit(sql)
			if not ok:
				self.request.response.status = 400
				self.logqueries(queries)
				return self.edata_error(error)
			queries.append("delta prerecibo true end")
		else:
			queries.append("delta prerecibo false")
			queries.append("delta prerecibo false end")

		if prereciboadicional:
			queries.append("delta prereciboadicional true")
			sql = """
			insert into prerecibo ( fk_cuenta, fecha, cantidad, referencia) values ( {},'{}',{},'{}')
			""".format(
				cuenta, fechadeventa, prereciboadicional, referenciarap
			)
			sql = psql(sql)
			queries.append(sql)

			ok, error = self.commit(sql)
			if not ok:
				self.request.response.status = 400
				self.logqueries(queries)
				return self.edata_error(error)
			queries.append("delta prereciboadicional true end")

		else:
			queries.append("delta prereciboadicional false")
			queries.append("delta prereciboadicional false end")

		queries.append("delta alcanzaelfinal true")
		queries.append("delta alcanzaelfinal true end")
		# queries.append("el valor del contrato es {}".format(contrato))
		self.logqueries(queries)
		record["id"] = contrato
		try:
			self.poolconn.close()
		except:
			pass
		return dict(asignacions=[record])


@resource(collection_path="api/documentocuentas", path="api/documentocuentas/{id}")
class DocumentoCuentaRest(EAuth, QueryAndErrors):
	def __init__(self, request, context=None):
		self.request = request
		self.modelo = "documentocuenta"

	def store(self, record, id=None):
		color("Voy a generar el documento cuenta", "y")
		psql = preparaQuery

		queries = []
		error = "Pendiente de implementar"
		ses = DBSession
		engine = Base.metadata.bind
		poolconn = engine.connect()
		cn_sql = poolconn.connection
		self.cn_sql = cn_sql
		self.poolconn = poolconn
		localerror = ""
		cuenta = record.get("cuenta", "")
		cantidad = record.get("cantidad", "")

		tipo = record.get("tipo", "")
		try:
			assert cuenta, "cuenta vacia"
			assert cantidad, "cantidad vacia"
			assert tipo, "tipo vacio"
		except AssertionError as e:
			print_exc()
			localerror = e.args[0]

		if localerror:
			error = localerror
			self.request.response.status = 400
			return self.edata_error(error)

		try:
			a = float(cantidad)
			a = int(cuenta)
			a = int(tipo)
			assert tipo in (13, 8), "error en dato"
		except:
			print_exc()
			localerror = "dato invalido"

		if localerror:
			error = localerror
			self.request.response.status = 400
			return self.edata_error(error)

		hoy = datetime.now()
		fecha = "{:04d}/{:02d}/{:02d}".format(hoy.year, hoy.month, hoy.day)
		if True:
			# queries.append("delta apartado true")
			found = False
			sql = "select max(codigo) + 1 as codigodocumento from documento"
			queries.append(sql)
			codigodocumento = 0
			for x in ses.execute(sql):
				codigodocumento = x.codigodocumento
				found = True
			if not found:
				localerror = (
					"no se puede obtener consecutivo siguiente de codigo de documento"
				)

			if localerror:
				error = localerror
				self.logqueries(queries)
				self.request.response.status = 400
				return self.edata_error(error)

			sql = """
			insert into documento (codigo, fechadeelaboracion, fechadevencimiento, fechadevencimientovar,
						saldo, cargo, abono, fk_tipo, fk_cuenta, referencia)
						values ({}, '{}', '{}', '{}', {}, {}, 0, {}, {}, '')
						""".format(
				codigodocumento, fecha, fecha, fecha, cantidad, cantidad, tipo, cuenta
			)
			sql = psql(sql)
			# queries.append(sql)

			ok, error = self.commit(sql)
			if not ok:
				self.request.response.status = 400
				self.logqueries(queries)
				return self.edata_error(error)

			found = False
			sql = "select max(codigo) + 1 as codigomovimiento from movimiento"
			queries.append(sql)
			codigomovimiento = 0
			for x in ses.execute(sql):
				codigomovimiento = x.codigomovimiento
				found = True
			if not found:
				localerror = (
					"no se puede obtener consecutivo siguiente de codigo de movimiento"
				)
			if localerror:
				error = localerror
				self.logqueries(queries)
				self.request.response.status = 400
				return self.edata_error(error)

			sql = """
			insert into movimiento (codigo, cantidad, fecha, relaciondepago, cargoabono,
							fk_tipo, fk_documento)
							values ({}, {}, '{}', '1/1', 'C', {}, {})
							""".format(
				codigomovimiento, cantidad, fecha, tipo, codigodocumento
			)
			sql = psql(sql)
			# queries.append(sql)

			ok, error = self.commit(sql)
			if not ok:
				self.request.response.status = 400
				self.logqueries(queries)
				return self.edata_error(error)

			sql = """ update cuenta set saldo = saldo + {} where codigo = {} """.format(
				cantidad, cuenta
			)
			sql = psql(sql)
			# queries.append(sql)

			ok, error = self.commit(sql)
			if not ok:
				self.request.response.status = 400
				self.logqueries(queries)
				return self.edata_error(error)
			# queries.append("delta apartado true end")

			record["id"] = codigodocumento
			try:
				poolconn.close()
			except:
				pass
			return dict(documentocuentas=[record])

	def collection_post(self):
		color("inserting documento cuenta")
		que, record, token = self.auth(self.modelo, get_token=True)
		user = cached_results.dicTokenUser.get(token)
		if not que:
			self.request.response.status = 400
			color("escaping documento cuenta")
			return self.edata_error("no autorizado")

		user = cached_results.dicTokenUser.get(token)
		usuario = user.get("usuario", "")
		self.usuariorecibo = usuario
		perfil = user.get("perfil", "")
		if perfil not in ("admin", "auxiliarsubdireccion", "cobranza"):

			self.request.response.status = 400
			color("invalid perfil documento cuenta")
			error = "perfil no autorizado"
			return self.edata_error(error)
		# if perfil == "admin":
		# 	print(paint.blue("hazqueries true inserting oferta"))
		# 	self.hazqueries = True
		# else:
		if True:
			self.hazqueries = False
		return self.store(record)


@resource(collection_path="api/ofertas", path="api/ofertas/{id}")
class OfertaDeCompra(EAuth, QueryAndErrors):
	def __init__(self, request, context=None):
		self.request = request
		self.modelo = "oferta"

	def get(self):
		que, record, token = self.auth(get_token=True)
		if not que:
			return record
		return dict()

	def collection_get(self):
		que, record = self.auth()
		if not que:
			return record
		self.request.response.status = 400
		return dict(error="Pendiente de implementar")

	def store(self, record, id=None):
		color("Voy a generar la Oferta", "y")
		psql = preparaQuery

		queries = []
		error = "Pendiente de implementar"
		ses = DBSession
		engine = Base.metadata.bind
		poolconn = engine.connect()
		cn_sql = poolconn.connection
		self.cn_sql = cn_sql
		self.poolconn = poolconn
		i = record.get("inmueble", 0)
		hipotecaria = record.get("hipotecaria", 1)
		autorizacion = record.get("autorizacion", "")
		descuento_precio = float(record.get("descuento", "0"))
		asignar = record.get("asignar", False)
		try:
			localerror = ""
			if autorizacion:
				descuento_precio = autorizacion_descuento(autorizacion)
				assert descuento_precio, "autorizacion incorrecta"
		except AssertionError as e:
			print_exc()
			localerror = e.args[0]

		if localerror:
			error = localerror

			self.request.response.status = 400
			return self.edata_error(error)

		try:
			localerror = ""
			assert i, "inmueble vacio"
		except AssertionError as e:
			print_exc()
			localerror = e.args[0]
		if localerror:
			error = localerror

			self.request.response.status = 400
			return self.edata_error(error)

		sql = """
		select i.codigo as inmueble, e.codigo as etapa, d.codigo as desarrollo,
		i.candadoprecio as candadoprecio, i.preciocatalogo as preciocatalogo 
		from inmueble i join etapa e 
		on i.fk_etapa = e.codigo
		join desarrollo d on e.fk_desarrollo = d.codigo
		where i.codigo = {}
		""".format(
			i
		)
		sql = psql(sql)
		queries.append(sql)
		for x in ses.execute(sql):
			inmueble = x.inmueble
			preciocatalogo = x.preciocatalogo
			candadoprecio = x.candadoprecio
			etapa = x.etapa
			desarrollo = x.desarrollo
			found = True
		if found:
			print(
				"Inmueble {}, etapa {}, desarrollo {}".format(
					inmueble, etapa, desarrollo
				)
			)
		else:
			error = "no hay inmueble {}".format(i)
			self.logqueries(queries)
			self.request.response.status = 400
			return self.edata_error(error)

		precio_id = record.get("precio", 0)
		try:
			localerror = ""
			assert preciocatalogo or precio_id, "no tiene precio"
		except AssertionError as e:
			print_exc()
			localerror = e.args[0]
		if localerror:
			error = localerror
			self.logqueries(queries)
			self.request.response.status = 400
			return self.edata_error(error)
		precio = preciocatalogo
		if not candadoprecio:
			found = False
			sql = """select precio from gixpreciosetapa
			where  id = {} and fk_etapa = {}""".format(
				precio_id, etapa
			)
			sql = psql(sql)
			queries.append(sql)
			precio = 0
			for x in ses.execute(sql):
				precio = x.precio
				found = True
			if not found:
				error = "no hay precio en base de datos"
				self.logqueries(queries)
				self.request.response.status = 400
				return self.edata_error(error)
		else:
			found = False
			count = 0
			sqlcount = """select count(*) as cuantos from gixpreciosetapa where  precio={} and fk_etapa={}""".format(
				precio, etapa
			)
			for x in ses.execute(sqlcount):
				count = x.cuantos
			if count > 1:
				sqlprecio = """select idpreciocatalogo from inmueble where codigo = {}""".format(
					inmueble
				)
				for x in ses.execute(sqlprecio):
					precio_id = x.idpreciocatalogo
					found = True
			else:
				sql = """select id from gixpreciosetapa
				where  precio = {} and fk_etapa = {}""".format(
					precio, etapa
				)
				sql = psql(sql)
				queries.append(sql)
				precio_id = 0
				for x in ses.execute(sql):
					precio_id = x.id
					found = True
			if not found:
				error = "no hay precio_id en base de datos"
				self.logqueries(queries)
				self.request.response.status = 400
				return self.edata_error(error)

		color("el precio es {}".format(precio))
		prospecto = record.get("prospecto", 0)
		try:
			localerror = ""
			assert prospecto, "prospecto vacio"
		except AssertionError as e:
			print_exc()
			localerror = e.args[0]
		if localerror:
			error = localerror
			self.logqueries(queries)
			self.request.response.status = 400
			return self.edata_error(error)

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
			return self.edata_error(error)

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
			return self.edata_error(error)

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
			return self.edata_error(error)

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
			return self.edata_error(error)

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
			return self.edata_error(error)

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
			return self.edata_error(error)
		color("el valor de precio es ".format(precio))
		suma = (
			precalificacion
			+ avaluo
			+ subsidio
			+ pagare
			+ prerecibo
			+ prereciboadicional
		)
		localerror = ""
		if suma != precio:
			localerror = "precios es distinto a suma de precalificacion, avaluo, subsidio, pagare y prerecibos"

		if localerror:
			error = localerror
			self.logqueries(queries)
			self.request.response.status = 400
			return self.edata_error(error)

		sql = """
		select idvendedor as vendedor, idgerente as gerente, afiliacionimss, fechacierre, congelado, cuenta from gixprospectos where idprospecto = {}
		""".format(
			prospecto
		)
		queries.append(sql)
		found = False
		p_afiliacionimss = ""
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
				print("esta congelado")
				localerror = "prospecto congelado"
			if p_fechacierre:
				print("prospecto ya tuvo fecha de cierre")
				localerror = "prospecto con fecha de cierre"
			if p_cuenta:
				print("prospecto con cuenta asignada")
				localerror = "prospecto con cuenta asignada"
		else:
			print("no existe el prospecto")
			localerror = "no existe el prospecto"

		if localerror:
			error = localerror
			self.logqueries(queries)
			self.request.response.status = 400
			return self.edata_error(error)
		localerror = ""

		found = False
		sql = "select vendedor as empresavendedora,es_subvendedor, vendedorvirtual, desactivado from vendedor where codigo = {}".format(
			p_vendedor
		)
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
			return self.edata_error(error)
		localerror = ""
		color(
			"vendedor {}, es_subvendedor {}, vendedorvirtual {}, desactivado {}".format(
				p_vendedor, es_subvendedor, vendedorvirtual, desactivado
			)
		)

		sql = "select fk_vendedor as vendedor, porcentaje from porcentaje_comision where fk_vendedor in ({}, {}) and fk_desarrollo = {}".format(
			empresavendedora, p_vendedor, desarrollo
		)
		queries.append(sql)
		found = False
		conteo = 0
		for x in ses.execute(sql):
			color(
				"comision: vendedor {}, porcentaje {} ".format(x.vendedor, x.porcentaje)
			)
			conteo = conteo + 1
		if conteo < 2:
			localerror = "falta porcentaje de comision de subvendedor o vendedor"
		if localerror:
			error = localerror
			self.logqueries(queries)
			self.request.response.status = 400
			return self.edata_error(error)

		cliente = record.get("cliente", 0)

		try:
			localerror = ""
			assert cliente, "cliente vacio"
		except AssertionError as e:
			print_exc()
			localerror = e.args[0]
		if localerror:
			error = localerror
			self.logqueries(queries)
			self.request.response.status = 400
			return self.edata_error(error)

		found = False
		sql = """
		select imss from cliente where codigo = {}
		""".format(
			cliente
		)
		queries.append(sql)
		imss = ""
		localerror = ""
		for x in ses.execute(sql):
			imss = x.imss or ""
			found = True
		if found:
			if not p_afiliacionimss and not imss:
				localerror = "afiliaciones de prospecto y cliente vacias"
				# print error
			else:
				if p_afiliacionimss.strip() == imss.strip():

					pass
				else:
					localerror = (
						"no coinciden las afiliaciones de imss de prospecto y vendedor"
					)
					# print error
		else:
			localerror = "no existe el cliente"
			# print error
		if localerror:
			error = localerror
			self.logqueries(queries)
			self.request.response.status = 400
			return self.edata_error(error)

		apartado = record.get("apartado", 0)
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
			return self.edata_error(error)

		precio = float(precio)
		resto = precio - apartado

		try:
			localerror = ""
			assert apartado, "apartado vacio"
		except AssertionError as e:
			print_exc()
			localerror = e.args[0]
		if localerror:
			error = localerror
			self.logqueries(queries)
			self.request.response.status = 400
			return self.edata_error(error)

		color("apartado = {}, resto = {}".format(str(apartado), str(resto)))

		sql = "select contrato + 1 as siguientecontrato from desarrollo where codigo = {}".format(
			desarrollo
		)
		queries.append(sql)
		contrato = 0
		for x in ses.execute(sql):
			contrato = x.siguientecontrato

		sql = "update desarrollo set contrato = {} where codigo = {}".format(
			contrato, desarrollo
		)
		queries.append(sql)

		ok, error = self.commit(sql)
		if not ok:
			self.request.response.status = 400
			self.logqueries(queries)
			return self.edata_error(error)

		color("contrato es {}".format(contrato))

		cuenta = 0
		sql = "select max(codigo)  as maximocuenta from cuenta"
		queries.append(sql)
		for x in ses.execute(sql):
			maximocuenta = x.maximocuenta

		sql = "select max(codigo)  as maximocuentacancelada from cuenta_cancelada"
		queries.append(sql)
		for x in ses.execute(sql):
			maximocuentacancelada = x.maximocuentacancelada
		cuenta = maximocuenta
		if cuenta < maximocuentacancelada:
			cuenta = maximocuentacancelada
		cuenta += 1

		hoy = datetime.now()
		fechadeventa = record.get("fechadeventa", "")
		if not fechadeventa:
			fechadeventa = "{:04d}/{:02d}/{:02d}".format(hoy.year, hoy.month, hoy.day)
		sql = """
					insert into cuenta
					(codigo, fecha, saldo, fk_cliente, fk_inmueble, fk_tipo_cuenta, contrato, tipo_contrato, fk_etapa)
					values ({}, '{}', {}, {}, 0, 2, {}, '3', {})
		""".format(
			cuenta, fechadeventa, precio, cliente, contrato, etapa
		)
		sql = psql(sql)
		queries.append(sql)

		ok, error = self.commit(sql)
		if not ok:
			self.request.response.status = 400
			self.logqueries(queries)
			return self.edata_error(error)

		sql = """
		select precio, sustentable, id from gixpreciosetapa where fk_etapa = {} and activo = 1 and precio = {}
		""".format(
			etapa, precio
		)
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
			return self.edata_error(error)

		referenciarap = record.get("referencia", 0)
		localerror = ""
		if not referenciarap:
			localerror = "falta referenciarap"
		if localerror:
			error = localerror
			self.logqueries(queries)
			self.request.response.status = 400
			return self.edata_error(error)
		sql = """
		select referencia from referencias_rap where cuenta = 0 and cliente = {}
		""".format(
			cliente
		)
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
			return self.edata_error(error)

		localerror = ""
		tipocuenta = record.get("tipocuenta", "")
		if not tipocuenta:
			localerror = "no esta el tipo de cuenta"
		if localerror:
			error = localerror
			self.logqueries(queries)
			self.request.response.status = 400
			return self.edata_error(error)

		montocredito = record.get("montocredito", 0)
		localerror = ""
		if not montocredito and tipocuenta == "infonavit":
			localerror = "si es tipo cuenta infonavit debe tener monto de credito"
		if not montocredito and tipocuenta == "fovisste":
			localerror = "si es tipo cuenta fovisste debe tener monto de credito"
		if not montocredito and tipocuenta == "pensiones":
			localerror = "si es tipo cuenta pensiones debe tener monto de credito"
		if not montocredito and tipocuenta == "hipotecaria":
			localerror = "si es tipo cuenta hipotecaria debe tener monto de credito"

		if localerror:
			error = localerror
			self.logqueries(queries)
			self.request.response.status = 400
			return self.edata_error(error)

		anticipocomision = record.get("anticipocomision", 0)
		localerror = ""
		try:
			anticipocomision = float(anticipocomision)
		except:
			localerror = "anticipo comision no es numerico"
		if localerror:
			error = localerror
			self.logqueries(queries)
			self.request.response.status = 400
			return self.edata_error(error)

		localerror = ""
		seguro = record.get("seguro", 0)
		sql = """
		insert into ofertas_compra
							(fk_etapa, oferta, cliente, vendedor, subvendedor, fecha_oferta, gastos_admin, apartado,
							monto_credito, asignada, referencia_rap, precio, anticipo_comision, cuenta, cancelada,
							precio_seguro, habilitada, preciosustentable)
							values ({}, {}, {}, {}, {}, '{}', 0, {}, {}, 0, '{}', {}, {}, {}, 0, {}, -1, {}) 
		""".format(
			etapa,
			contrato,
			cliente,
			empresavendedora,
			p_vendedor,
			fechadeventa,
			apartado,
			montocredito,
			referenciarap,
			precio,
			anticipocomision,
			cuenta,
			seguro,
			sustentable1,
		)
		sql = psql(sql)
		queries.append(sql)
		ok, error = self.commit(sql)
		if not ok:
			self.request.response.status = 400
			self.logqueries(queries)
			return self.edata_error(error)
		found = False
		sql = "update referencias_rap set cuenta = {} where referencia = '{}'".format(
			cuenta, "{:010d}".format(int(referenciarap))
		)
		queries.append(sql)
		ok, error = self.commit(sql)
		if not ok:
			self.request.response.status = 400
			self.logqueries(queries)
			return self.edata_error(error)
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
			return self.edata_error(error)
		sql = """
		insert comision (codigo, aplicar, cantidad, saldo_cantidad, iva, saldo_iva,
									total, saldo_total, fk_inmueble, fk_vendedor, fk_cuenta, cancelada,
									cuenta_anterior, cuenta_original)
									values ({}, 'N', {}, {}, {}, {}, {}, {}, {}, {}, {}, 'N', 0, 0)
		""".format(
			codigocomision,
			anticipocomision,
			anticipocomision,
			anticipocomision * 0.16,
			anticipocomision * 0.16,
			anticipocomision * 1.16,
			anticipocomision * 1.16,
			0,
			empresavendedora,
			cuenta,
		)

		sql = psql(sql)
		queries.append(sql)

		ok, error = self.commit(sql)
		if not ok:
			self.request.response.status = 400
			self.logqueries(queries)
			return self.edata_error(error)

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
			return self.edata_error(error)
		sql = """
		insert comision (codigo, aplicar, cantidad, saldo_cantidad, iva, saldo_iva,
									total, saldo_total, fk_inmueble, fk_vendedor, fk_cuenta, cancelada,
									cuenta_anterior, cuenta_original)
									values ({}, 'N', {}, {}, {}, {}, {}, {}, {}, {}, {}, 'N', 0, 0)
		""".format(
			codigocomision,
			anticipocomision,
			anticipocomision,
			anticipocomision * 0.16,
			anticipocomision * 0.16,
			anticipocomision * 1.16,
			anticipocomision * 1.16,
			0,
			p_vendedor,
			cuenta,
		)

		sql = psql(sql)
		queries.append(sql)

		ok, error = self.commit(sql)
		if not ok:
			self.request.response.status = 400
			self.logqueries(queries)
			return self.edata_error(error)

		sql = """
		insert into gixpreciosetapaofertaasignacion
								(fk_etapa, oferta, fecha_oferta, fk_preciosetapaoferta,
								preciooferta, cuenta, fecha_asignacion, inmueble,
								fk_preciosetapaasignacion, precioasignacion, inmuebledefinitivo)
								values ({}, {}, '{}', {}, {}, {}, '{}',{},{},{},{})
								""".format(
			etapa,
			contrato,
			fechadeventa,
			gixprecioetapaid,
			precio,
			cuenta,
			fechadeventa,
			inmueble,
			precio_id,
			precio,
			inmueble,
		)
		sql = psql(sql)
		queries.append(sql)
		ok, error = self.commit(sql)
		if not ok:
			self.request.response.status = 400
			self.logqueries(queries)
			return self.edata_error(error)

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
				localerror = (
					"no se puede obtener consecutivo siguiente de codigo de documento"
				)

			if localerror:
				error = localerror
				self.logqueries(queries)
				self.request.response.status = 400
				return self.edata_error(error)

			sql = """
			insert into documento (codigo, fechadeelaboracion, fechadevencimiento, fechadevencimientovar,
						saldo, cargo, abono, fk_tipo, fk_cuenta, referencia)
						values ({}, '{}', '{}', '{}', {}, {}, 0, 7, {}, '{}')
						""".format(
				codigodocumento,
				fechadeventa,
				fechadeventa,
				fechadeventa,
				apartado,
				apartado,
				cuenta,
				referenciarap,
			)
			sql = psql(sql)
			queries.append(sql)

			ok, error = self.commit(sql)
			if not ok:
				self.request.response.status = 400
				self.logqueries(queries)
				return self.edata_error(error)

			found = False
			sql = "select max(codigo) + 1 as codigomovimiento from movimiento"
			queries.append(sql)
			codigomovimiento = 0
			for x in ses.execute(sql):
				codigomovimiento = x.codigomovimiento
				found = True
			if not found:
				localerror = (
					"no se puede obtener consecutivo siguiente de codigo de movimiento"
				)
			if localerror:
				error = localerror
				self.logqueries(queries)
				self.request.response.status = 400
				return self.edata_error(error)
			sql = """
			insert into movimiento (codigo, cantidad, fecha, relaciondepago, cargoabono,
							fk_tipo, fk_documento)
							values ({}, {}, '{}', '1/1', 'C', 7, {})
							""".format(
				codigomovimiento, apartado, fechadeventa, codigodocumento
			)
			sql = psql(sql)
			queries.append(sql)

			ok, error = self.commit(sql)
			if not ok:
				self.request.response.status = 400
				self.logqueries(queries)
				return self.edata_error(error)

			queries.append("delta apartado true end")

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
				localerror = (
					"no se puede obtener consecutivo siguiente de codigo de documento"
				)

			if localerror:
				error = localerror
				self.logqueries(queries)
				self.request.response.status = 400
				return self.edata_error(error)

			sql = """
			insert into documento (codigo, fechadeelaboracion, fechadevencimiento, fechadevencimientovar,
						saldo, cargo, abono, fk_tipo, fk_cuenta, referencia)
						values ({}, '{}', '{}', '{}', {}, {}, 0, 2, {}, '{}')
						""".format(
				codigodocumento,
				fechadeventa,
				fechadeventa,
				fechadeventa,
				resto,
				resto,
				cuenta,
				referenciarap,
			)
			sql = psql(sql)
			queries.append(sql)
			ok, error = self.commit(sql)
			if not ok:
				self.request.response.status = 400
				self.logqueries(queries)
				return self.edata_error(error)

			found = False
			sql = "select max(codigo) + 1 as codigomovimiento from movimiento"
			queries.append(sql)
			codigomovimiento = 0
			for x in ses.execute(sql):
				codigomovimiento = x.codigomovimiento
				found = True
			if not found:
				localerror = (
					"no se puede obtener consecutivo siguiente de codigo de movimiento"
				)
			if localerror:
				error = localerror
				self.logqueries(queries)
				self.request.response.status = 400
				return self.edata_error(error)
			sql = """
			insert into movimiento (codigo, cantidad, fecha, relaciondepago, cargoabono,
							fk_tipo, fk_documento)
							values ({}, {}, '{}', '1/1', 'C', 2, {})
							""".format(
				codigomovimiento, resto, fechadeventa, codigodocumento
			)
			sql = psql(sql)
			queries.append(sql)
			ok, error = self.commit(sql)
			if not ok:
				self.request.response.status = 400
				self.logqueries(queries)
				return self.edata_error(error)
			if descuento_precio > 0:
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
					return self.edata_error(error)

				sql = """
				insert into gixreservacionrecibo (codigorecibo, fk_desarrollo, consdesarrollo, usuario)
				select max(codigorecibo) + 1, {},
				(select max(consdesarrollo) + 1 from gixreservacionrecibo where fk_desarrollo = {}), '{}'
				from gixreservacionrecibo
				""".format(
					desarrollo, desarrollo, self.usuariorecibo
				)
				sql = psql(sql)
				queries.append(sql)
				ok, error = self.commit(sql)
				if not ok:
					self.request.response.status = 400
					self.logqueries(queries)
					return self.edata_error(error)

				# sql = """select scope_identity() as pkreservacionrecibo"""
				sql = """
				select max(pkreservacionrecibo) as pkreservacionrecibo 
				from gixreservacionrecibo
				"""
				sql = psql(sql)
				queries.append(sql)
				pkreservacionrecibo = 0
				for x in ses.execute(sql):
					pkreservacionrecibo = x.pkreservacionrecibo
					print("pkreservacionrecibo es", pkreservacionrecibo)
					found = True
				if not found:
					localerror = "no se puede obtener pkreservacionrecibo"
				if localerror:
					error = localerror
					self.logqueries(queries)
					self.request.response.status = 400
					return self.edata_error(error)

				sql = """select codigorecibo, consdesarrollo 
				from gixreservacionrecibo where pkreservacionrecibo = {}
				""".format(
					pkreservacionrecibo
				)
				sql = psql(sql)
				queries.append(sql)
				codigorecibo = 0
				consdesarrollo = 0
				for x in ses.execute(sql):
					codigorecibo = x.codigorecibo
					consdesarrollo = x.consdesarrollo
					found = True
				if not found:
					localerror = "no se puede obtener codigorecibo y consdesarrollo"
				if localerror:
					error = localerror
					self.logqueries(queries)
					self.request.response.status = 400
					return self.edata_error(error)

				sql = """
				insert into RECIBO
				(codigo, fechaemision, abonocapital, interesmoratorio, totalrecibo, referencia,
				status, fk_desarrollo, consdesarrollo)
				values ({}, '{}', {}, {}, {}, '{}', 'A', {}, {})
				""".format(
					codigorecibo,
					fechadeventa,
					float(descuento_precio),
					0,
					float(descuento_precio),
					"NINGUNA",
					desarrollo,
					consdesarrollo,
				)

				sql = psql(sql)
				queries.append(sql)
				ok, error = self.commit(sql)
				if not ok:
					self.request.response.status = 400
					self.logqueries(queries)
					return self.edata_error(error)

				sql = """
				update gixreservacionrecibo
				set utilizado = 1, referenciado = 1 
				where pkreservacionrecibo = {}
				""".format(
					pkreservacionrecibo
				)

				sql = psql(sql)
				queries.append(sql)
				ok, error = self.commit(sql)
				if not ok:
					self.request.response.status = 400
					self.logqueries(queries)
					return self.edata_error(error)

				sql = """
				insert into movimiento (codigo, cantidad, fecha, 
				relaciondepago, cargoabono, numrecibo, fechavencimientodoc,
				fk_tipo, fk_documento)
				values ({}, {}, '{}', 'DESCUENTO PRECIO', 'A', {}, {}, 4, {})
				""".format(
					codigomovimiento,
					descuento_precio,
					fechadeventa,
					codigorecibo,
					fechadeventa,
					codigodocumento,
				)
				sql = psql(sql)
				queries.append(sql)
				ok, error = self.commit(sql)
				if not ok:
					self.request.response.status = 400
					self.logqueries(queries)
					return self.edata_error(error)

				sql = """
				update documento set saldo = saldo - {} where codigo = {}""".format(
					descuento_precio, codigodocumento
				)
				ql = psql(sql)
				queries.append(sql)
				ok, error = self.commit(sql)
				if not ok:
					self.request.response.status = 400
					self.logqueries(queries)
					return self.edata_error(error)

				sql = """
				update cuenta set saldo = saldo - {} where codigo = {}""".format(
					descuento_precio, cuenta
				)
				ql = psql(sql)
				queries.append(sql)
				ok, error = self.commit(sql)
				if not ok:
					self.request.response.status = 400
					self.logqueries(queries)
					return self.edata_error(error)
				if autorizacion:
					autorizacion_descuento(autorizacion, cuenta)

			queries.append("delta resto true end")
		else:
			queries.append("delta resto false")
			queries.append("delta resto false end")

		sql = """
					update gixprospectos set cuenta = {}, fechacierre = convert(varchar(10), getdate(), 111)
					where idprospecto = {}
				""".format(
			cuenta, prospecto
		)
		sql = psql(sql)
		queries.append(sql)
		ok, error = self.commit(sql)
		if not ok:
			self.request.response.status = 400
			self.logqueries(queries)
			return self.edata_error(error)

		# comienza asignacion
		if asignar:
			sql = """
			update cuenta set fk_inmueble = {}, bruto_precalificacion = {}, avaluoimpuesto_precalificacion = {}, subsidio_precalificacion = {}, pagare_precalificacion = {} where codigo = {}
			""".format(
				inmueble, precalificacion, avaluo, subsidio, pagare, cuenta
			)
		else:
			sql = """
			update cuenta set  bruto_precalificacion = {}, avaluoimpuesto_precalificacion = {}, subsidio_precalificacion = {}, pagare_precalificacion = {} where codigo = {}
			""".format(
				precalificacion, avaluo, subsidio, pagare, cuenta
			)
		sql = psql(sql)
		queries.append(sql)
		print(paint.blue(sql))
		ok, error = self.commit(sql)
		if not ok:
			self.request.response.status = 400
			self.logqueries(queries)
			return self.edata_error(error)

		sql = """
			update anticipocomision set fk_inmueble = {} where fk_cuenta = {}
		""".format(
			inmueble, cuenta
		)
		sql = psql(sql)
		queries.append(sql)

		ok, error = self.commit(sql)
		if not ok:
			self.request.response.status = 400
			self.logqueries(queries)
			return self.edata_error(error)
		sql = """
			update comision set fk_inmueble = {} where fk_cuenta = {}
		""".format(
			inmueble, cuenta
		)
		sql = psql(sql)
		queries.append(sql)

		ok, error = self.commit(sql)
		if not ok:
			self.request.response.status = 400
			self.logqueries(queries)
			return self.edata_error(error)

		if asignar:
			sql = """
				update inmueble set fechadeventa = '{}' , precio = {} where codigo = {}
			""".format(
				fechadeventa, precio, inmueble
			)
		else:
			sql = """
				update inmueble set fechadeventa = fechadeventa , precio = precio where codigo = {}
			""".format(
				inmueble
			)

		sql = psql(sql)
		queries.append(sql)

		ok, error = self.commit(sql)
		if not ok:
			self.request.response.status = 400
			self.logqueries(queries)
			return self.edata_error(error)

		if asignar:
			sql = """
			update ofertas_compra set fecha_asignacion = '{}', asignada = -1, monto_precalificacion = {} where oferta = {} and cuenta = {}
			""".format(
				fechadeventa, precalificacion, contrato, cuenta
			)
		else:
			sql = """
			update ofertas_compra set fecha_asignacion = fecha_asignacion, asignada = asignada, monto_precalificacion = {} where oferta = {} and cuenta = {}
			""".format(
				precalificacion, contrato, cuenta
			)

		sql = psql(sql)
		queries.append(sql)

		ok, error = self.commit(sql)
		if not ok:
			self.request.response.status = 400
			self.logqueries(queries)
			return self.edata_error(error)

		if prerecibo:
			queries.append("delta prerecibo true")
			sql = """
			insert into prerecibo ( fk_cuenta, fecha, cantidad, referencia) values ( {},'{}',{},'{}')
			""".format(
				cuenta, fechadeventa, prerecibo, referenciarap
			)
			sql = psql(sql)
			queries.append(sql)

			ok, error = self.commit(sql)
			if not ok:
				self.request.response.status = 400
				self.logqueries(queries)
				return self.edata_error(error)
			queries.append("delta prerecibo true end")
		else:
			queries.append("delta prerecibo false")
			queries.append("delta prerecibo false end")

		if prereciboadicional:
			queries.append("delta prereciboadicional true")
			sql = """
			insert into prerecibo ( fk_cuenta, fecha, cantidad, referencia) values ( {},'{}',{},'{}')
			""".format(
				cuenta, fechadeventa, prereciboadicional, referenciarap
			)
			sql = psql(sql)
			queries.append(sql)

			ok, error = self.commit(sql)
			if not ok:
				self.request.response.status = 400
				self.logqueries(queries)
				return self.edata_error(error)
			queries.append("delta prereciboadicional true end")

		else:
			queries.append("delta prereciboadicional false")
			queries.append("delta prereciboadicional false end")

		queries.append("delta alcanzaelfinal true")
		queries.append("delta alcanzaelfinal true end")
		# queries.append("el valor del contrato es {}".format(contrato))

		self.logqueries(queries)
		record["id"] = contrato
		try:
			poolconn.close()
		except:
			pass

		# aqui va insert into zen_cuenta_hipotecaria (cuenta, hipotecaria) values  (cuenta, hipotecaria)
		args = dict(codigo=cuenta, hipotecaria=hipotecaria, escuenta=True)
		enbbcall("aplicahipotecariacuenta", args)

		args = dict(
			codigo=cuenta,
			inmueble=inmueble,
			idprecio=precio_id,
			precio=precio,
			escuenta=True,
		)
		enbbcall("creapreciosoriginales", args)

		return dict(ofertas=[record])

	def collection_post(self):
		color("inserting oferta")
		que, record, token = self.auth(self.modelo, get_token=True)
		user = cached_results.dicTokenUser.get(token)
		if not que:
			self.request.response.status = 400
			color("escaping inserting oferta")
			return self.edata_error("no autorizado")
		color("continues inserting oferta")
		user = cached_results.dicTokenUser.get(token)
		usuario = user.get("usuario", "")
		self.usuariorecibo = usuario
		perfil = user.get("perfil", "")
		if perfil not in ("admin", "comercial", "subdireccioncomercial"):

			self.request.response.status = 400
			color("invalid perfil inserting oferta")
			error = "perfil no autorizado"
			return self.edata_error(error)
		if perfil == "admin":
			print(paint.blue("hazqueries true inserting oferta"))
			self.hazqueries = True
		else:
			self.hazqueries = False
		return self.store(record)


def arcadiacuadro(enganche=False):
	lista = []
	# for x in range(2003, 2004):
	for x in range(2003, int(tiempo.date.today().year) + 1):
		m = "Foo Ene Feb Mar Abr May Jun Jul Ago Sep Oct Nov Dic"
		meses = m.split(" ")
		# total=0
		print("ano ", x)
		sql = "select i.codigo, c.fecha from inmueble i join cuenta c on c.fk_inmueble=i.codigo where datepart(yyyy, c.fecha) >={} and datepart(yyyy, c.fecha)<{} group by c.fecha, i.codigo".format(
			x, x + 1
		)
		if enganche:
			sql = "select fechadeelaboracion as fecha from documento where fk_tipo=1 and fk_cuenta in (select c.codigo from cuenta c join inmueble z on c.fk_inmueble=z.codigo where z.codigo in (select i.codigo from inmueble i join cuenta c on c.fk_inmueble=i.codigo where datepart(yyyy, c.fecha) >={} and datepart(yyyy, c.fecha)<{}))".format(
				x, x + 1
			)
		r_year = dict(year=x)
		for x in range(1, 13):
			r_year[meses[x]] = 0
		r_year["total"] = 0
		for i, x in enumerate(DBSession2.execute(sql)):
			if x.fecha.month == 1:
				r_year[meses[x.fecha.month]] = r_year[meses[x.fecha.month]] + 1
				r_year["total"] = r_year["total"] + 1
			elif x.fecha.month == 2:
				r_year[meses[x.fecha.month]] = r_year[meses[x.fecha.month]] + 1
				r_year["total"] = r_year["total"] + 1
			elif x.fecha.month == 3:
				r_year[meses[x.fecha.month]] = r_year[meses[x.fecha.month]] + 1
				r_year["total"] = r_year["total"] + 1
			elif x.fecha.month == 4:
				r_year[meses[x.fecha.month]] = r_year[meses[x.fecha.month]] + 1
				r_year["total"] = r_year["total"] + 1
			elif x.fecha.month == 5:
				r_year[meses[x.fecha.month]] = r_year[meses[x.fecha.month]] + 1
				r_year["total"] = r_year["total"] + 1
			elif x.fecha.month == 6:
				r_year[meses[x.fecha.month]] = r_year[meses[x.fecha.month]] + 1
				r_year["total"] = r_year["total"] + 1
			elif x.fecha.month == 7:
				r_year[meses[x.fecha.month]] = r_year[meses[x.fecha.month]] + 1
				r_year["total"] = r_year["total"] + 1
			elif x.fecha.month == 8:
				r_year[meses[x.fecha.month]] = r_year[meses[x.fecha.month]] + 1
				r_year["total"] = r_year["total"] + 1
			elif x.fecha.month == 9:
				r_year[meses[x.fecha.month]] = r_year[meses[x.fecha.month]] + 1
				r_year["total"] = r_year["total"] + 1
			elif x.fecha.month == 10:
				r_year[meses[x.fecha.month]] = r_year[meses[x.fecha.month]] + 1
				r_year["total"] = r_year["total"] + 1
			elif x.fecha.month == 11:
				r_year[meses[x.fecha.month]] = r_year[meses[x.fecha.month]] + 1
				r_year["total"] = r_year["total"] + 1
			elif x.fecha.month == 12:
				r_year[meses[x.fecha.month]] = r_year[meses[x.fecha.month]] + 1
				r_year["total"] = r_year["total"] + 1
				# r_year["Total"]= total
		lista.append(r_year)
	resultado = []
	for i, mes in enumerate(meses[1:]):
		row = dict()
		row["mes"] = mes
		row["id"] = i
		for i, x in enumerate(range(2003, int(tiempo.date.today().year) + 1)):
			row["a{}".format(x)] = lista[i][mes]
			row["id"] = f"{mes}{i}"
		resultado.append(row)
	row = dict()
	row["mes"] = "total"
	for i, x in enumerate(range(2003, int(tiempo.date.today().year) + 1)):
		row["a{}".format(x)] = lista[i]["total"]
		row["id"] = f"{i}"
	resultado.append(row)

	# row=["total"]
	# for i, x in enumerate(range(2003, int(tiempo.date.today().year)+1)):
	#    row["a{}".format(x)]=1
	# resultado.append()
	return resultado


@resource(
	collection_path="api/ventascuadroarcadias", path="api/ventascuadroarcadias/{id}"
)
class VentasCuadroArcadia(EAuth):
	def __init__(self, request, context=None):
		self.request = request

	def collection_get(self):
		p = self.request.params
		eng = p.get("enganche", "")
		return dict(
			meta=dict(maxyear=today(False).year),
			ventascuadroarcadias=arcadiacuadro(eng),
		)


@resource(collection_path="api/categoriasmenus", path="api/categoriasmenus/{id}")
class CategoriasMenu(EAuth):
	def __init__(self, request, context=None):
		self.request = request

	def collection_get(self):
		que, record, token = self.auth(get_token=True)
		if not que:
			return record
		cr = cached_results
		user = cached_results.dicTokenUser.get(token)
		usuario = user.get("usuario", "")
		rdb.connect(
			cached_results.settings.get("rethinkdb.host"),
			cached_results.settings.get("rethinkdb.port"),
		).repl()
		color("leyendo de rethinkdb {}".format(today()), "b")
		resultado = []
		table = rdb.db("iclar").table("menuitems")
		siguiente = 0
		for x in table.run():
			menu = x.get("item", "")
			if menu in cr.dicTokenUser.get(token).get("routes"):
				pass
			else:
				continue
			siguiente += 1
			resultado.append(dict(id=siguiente, categoria="todos", menu=menu))
			if "arcadia" in menu:
				siguiente += 1
				resultado.append(dict(id=siguiente, categoria="arcadia", menu=menu))
			if x.get("consulta", False):
				siguiente += 1
				resultado.append(dict(id=siguiente, categoria="consultas", menu=menu))

		table = rdb.db("iclar").table("categoriasmenu")
		for i, x in enumerate(table.run(), siguiente):
			menu = x.get("menu", "")
			if menu in cr.dicTokenUser.get(token).get("routes"):
				pass
			else:
				continue
			resultado.append(dict(id=i, categoria=x.get("categoria"), menu=menu))
		return dict(categoriasmenu=resultado)


@resource(collection_path="api/menus", path="api/menus/{id}")
class MenuZen(EAuth):
	print("aqui-------------------------")

	def __init__(self, request, context=None):
		self.request = request

	def collection_get(self):
		print("aqui 2-------------------------")
		que, record, token = self.auth(get_token=True)
		if not que:
			return record
		user = cached_results.dicTokenUser.get(token)
		usuario = user.get("usuario", "")
		rdb.connect(
			cached_results.settings.get("rethinkdb.host"),
			cached_results.settings.get("rethinkdb.port"),
		).repl()
		color("leyendo de rethinkdb {}".format(today()), "b")

		table = rdb.db("iclar").table("menuitems")
		return dict(
			menus=[
				dict(
					id=i,
					item=x.get("item"),
					title=x.get("title"),
					intro=x.get("intro"),
					consulta=x.get("consulta"),
					reciente=self.reciente(x.get("fecha", "")),
				)
				for i, x in enumerate(table.run(), 1)
			]
		)

	def reciente(self, fecha):
		if not fecha:
			return False
		y, m, d = [int(x) for x in fecha.split("/")]
		cuando = datetime(year=y, month=m, day=d)
		hoy = today(False)
		return (hoy - cuando).days <= 7


@resource(collection_path="api/menuperfils", path="api/menuperfils/{id}")
class MenuPerfil(EAuth):
	def __init__(self, request, context=None):
		self.request = request

	def collection_get(self):
		que, record, token = self.auth(get_token=True)
		if not que:
			return record
		user = cached_results.dicTokenUser.get(token)
		usuario = user.get("usuario", "")
		perfil = user.get("perfil", "")

		rdb.connect(
			cached_results.settings.get("rethinkdb.host"),
			cached_results.settings.get("rethinkdb.port"),
		).repl()
		color("leyendo de rethinkdb {}".format(today()), "b")

		table = rdb.db("iclar").table("zen_profiles")
		for x in table.filter(dict(profile=perfil)).run():
			menuitems = x.get("menuitems")

		return dict(menuperfils=[dict(id=i, item=x) for i, x in enumerate(menuitems)])


@resource(collection_path="api/zenusuarios", path="api/zenusuarios/{id}")
class UsuarioZen(EAuth):
	def __init__(self, request, context=None):
		self.request = request
		self.modelo = "zenusuario"

	def put(self):
		que, record, token = self.auth(self.modelo, get_token=True)

		if not que:
			return record
		user = cached_results.dicTokenUser.get(token)
		usuario = user.get("usuario", "")
		perfil = user.get("perfil", "")
		print("en record hay", record)
		menuitems = record.get("menuitems", "")
		color("obteniendo {}".format(menuitems))
		rdb.connect(
			cached_results.settings.get("rethinkdb.host"),
			cached_results.settings.get("rethinkdb.port"),
		).repl()
		color("Grabando en rethinkdb  {}".format(today()), "b")
		cached_results.dicTokenUser[token]["menuitems"] = menuitems.split(" ")

		table = rdb.db("iclar").table("usuarios")
		table.filter(dict(appid=user.get("id"))).update(
			dict(menuitems=[x for x in menuitems.split(" ")])
		).run()
		return dict(
			zenusuario=dict(id="1", usuario=usuario, perfil=perfil, menuitems=menuitems)
		)

	def get(self):
		que, record, token = self.auth(get_token=True)
		if not que:
			return record
		user = cached_results.dicTokenUser.get(token)
		usuario = user.get("usuario", "")
		perfil = user.get("perfil", "")
		menuitems = user.get("menuitems", [])
		return dict(
			zenusuario=dict(
				id="1", usuario=usuario, perfil=perfil, menuitems=" ".join(menuitems)
			)
		)


@resource(collection_path="api/gerentevendedors", path="api/gerentevendedors/{id}")
class GerenteVendedor(EAuth):
	def __init__(self, request, context=None):
		self.request = request

	def get(self):
		que, record, token = self.auth(get_token=True)
		if not que:
			return dict()

		d = cached_results.dicTokenUser[token]
		g = int(d.get("gerente", 0))
		v = int(d.get("vendedor", 0))
		return dict(gerentevendedor=dict(id="1", gerente=g, vendedor=v))


@resource(collection_path="api/zenversions", path="api/zenversions/{id}")
class Zenversion(EAuth):
	def __init__(self, request, context=None):
		self.request = request

	def collection_get(self):
		print(self.request.headers)
		que, record = self.auth()
		if not que:
			return record
		return dict()

	def get(self):
		que, record = self.auth()
		print("voy en get de Zenversion class")
		if not que:
			return record
		print("voy a regresar valor en Zenversion class")
		current = redis_conn.get("{}-current-deploy".format("zeniclar"))
		version = self.request.params.get("version", "")
		if version:
			current = version
		html = redis_conn.get(current)
		s = html.split("zeniclar-")[1][:7]
		return dict(zenversion=dict(id="1", version=s))


@resource(
	collection_path="api/prospectosbusquedas", path="api/prospectosbusquedas/{id}"
)
class ProspectosBusquedas(EAuth, QueryAndErrors, OperacionesAfiliacion):
	def __init__(self, request, context=None):
		self.request = request
		self.modelo = "prospectosbusqueda"

	def store(self, record, id=None):
		engine = Base.metadata.bind
		poolconn = engine.connect()
		self.cn_sql = poolconn.connection
		self.poolconn = poolconn

		afiliacion = self.obtenAfiliacionDisponible()
		if not afiliacion:
			self.request.response.status = 400
			return self.edata_error(error)

		sql = """
		update gixprospectos
		set afiliacionimss = '{}'
		where idprospecto = {}
		""".format(
			afiliacion, id
		)
		ok, error = self.commit(sql)
		if not ok:
			self.request.response.status = 400
			return self.edata_error(error)

		record["afiliacion"] = afiliacion
		record["id"] = id
		color("nueva afiliacion {}".format(afiliacion))
		self.actualiza_prospecto_en_rdb(id, afiliacion)
		try:
			self.poolconn.close()
		except:
			pass
		return dict(prospectosbusquedas=[record])

	@view(renderer="json")
	def put(self):
		print("updating Prospecto para grabar afiliacion automatica")
		que, record = self.auth(self.modelo, get_token=False)
		if not que:
			return record
		id = int(self.request.matchdict["id"])
		return self.store(record=record, id=id)

	def collection_get(self):
		if True:
			que, record, token = self.auth(get_token=True)
			if not que:
				return record
		print("voy en ProspectosBusquedas.collection_get()")
		user = cached_results.dicTokenUser.get(token)
		_vendedor = user.get("vendedor", 0)
		_gerente = user.get("gerente", 0)
		clase = Prospecto
		query = DBSession.query(clase).filter(clase.congelado == False)
		# query = query.filter(clase.idprospecto.in_( [161741,161742,161743] ))
		# print query.count()
		req = self.request.params
		ROWS_PER_PAGE = 20
		page = 1
		try:
			page = int(req.get("page", 1))
		except:
			pass
		gerente = 0
		try:
			gerente = int(req.get("gerente", 0))
		except:
			pass
		vendedor = 0
		try:
			vendedor = int(req.get("vendedor", 0))
		except:
			pass
		if _vendedor:
			vendedor = _vendedor
		if _gerente:
			gerente = _gerente

		mediopublicitario = int(req.get("mediopublicitario", 0))
		tipofecha = req.get("tipofecha", "")
		fechainicial = req.get("fechainicial", "")
		fechafinal = req.get("fechafinal", "")
		tipocuenta = req.get("tipocuenta", "")
		numeroprospecto = int(req.get("numeroprospecto", 0))
		nombreprospecto = req.get("nombreprospecto", "")
		nombrepilaprospecto = req.get("nombrepilaprospecto", "")
		apellidomaternoprospecto = req.get("apellidomaternoprospecto", "")
		afiliacion = req.get("afiliacion", "")
		s_cierre = req.get("sincierre", "")
		sincierre = False
		if s_cierre == "1":
			sincierre = True

		idprospectos = []
		if nombreprospecto or nombrepilaprospecto or apellidomaternoprospecto:
			where1 = ""
			if nombrepilaprospecto:
				where1 = " and nombre1 like '%{}%'".format(nombrepilaprospecto)
			where2 = ""
			if nombreprospecto:
				where2 = " and apellidopaterno1 like '%{}%'".format(nombreprospecto)
			where3 = ""
			if apellidomaternoprospecto:
				where3 = " and apellidomaterno1 like '%{}%'".format(
					apellidomaternoprospecto
				)
			sql = """
			select idprospecto as prospecto from gixprospectos where congelado = 0
			{} {} {}""".format(
				where1, where2, where3
			)

			sql = preparaQuery(sql)
			sql = sql.encode("iso-8859-1")

			engine = Base.metadata.bind
			poolconn = engine.connect()
			self.poolconn = poolconn
			c = poolconn.connection
			cu = c.cursor()

			for x in cu.execute(sql):
				idprospectos.append(x.prospecto)

			cu.close()

		if len(idprospectos) > 0:
			query = query.filter(clase.idprospecto.in_(idprospectos))
		if vendedor > 0:
			query = query.filter(clase.idvendedor == vendedor)
		if vendedor == 0 and gerente > 0:
			query = query.filter(clase.idgerente == gerente)
		# if prospecto > 0:
		# 	query = query.filter( clase.id == prospecto )
		if mediopublicitario > 0:
			query = query.filter(clase.idmediopublicitario == mediopublicitario)
		if tipocuenta:
			if tipocuenta == "infonavit":
				query = (
					query.filter(clase.contado == False)
					.filter(clase.hipotecaria == False)
					.filter(clase.fovisste == False)
					.filter(clase.pensiones == False)
				)
			elif tipocuenta == "contado":
				query = query.filter(clase.contado == True)
			elif tipocuenta == "hipotecaria":
				query = query.filter(clase.hipotecaria == True)
			elif tipocuenta == "fovisste":
				query = query.filter(clase.fovisste == True)
			elif tipocuenta == "pensiones":
				query = query.filter(clase.pensiones)
		# print "tipofecha", tipofecha
		if numeroprospecto:
			query = query.filter(clase.idprospecto == numeroprospecto)
		if nombreprospecto:
			np = nombreprospecto
			# query = query.filter( clase.apellidopaterno1.like("%{}%".format(np) ) )
		if nombrepilaprospecto:
			npp = nombrepilaprospecto
			# query = query.filter( clase.nombre1.like("%{}%".format(npp) ) )
		if apellidomaternoprospecto:
			amp = apellidomaternoprospecto
			# query = query.filter( clase.apellidomaterno1.like("%{}%".format(amp) ) )
		if afiliacion:
			query = query.filter(clase.afiliacionimss.like("%{}%".format(afiliacion)))
		if tipofecha:
			if tipofecha == "alta":
				fecha = clase.fechaasignacion
			else:
				fecha = clase.fechacierre
			print("fechainicial", fechainicial, "fechafinal", fechafinal)
			nohagas_nada = False
			if fechainicial != "" and fechafinal != "":
				fini = self.get_date(fechainicial)
				ffin = self.get_date(fechafinal, minimal_date=False)
			elif fechainicial == "" and fechafinal != "":
				fini = self.get_date("")
				ffin = self.get_date(fechafinal, minimal_date=False)
			elif fechainicial != "" and fechafinal == "":
				fini = self.get_date(fechainicial)
				ffin = self.get_date("", minimal_date=False)
			elif fechainicial == "" and fechafinal == "":
				fini = self.get_date("")
				ffin = self.get_date("", minimal_date=False)
			else:
				nohagas_nada = True
			if nohagas_nada is False:
				query = query.filter(fecha >= fini).filter(fecha <= ffin)
				if tipofecha == "alta" and sincierre:
					query = query.filter(clase.fechacierre == None)
		query = query.order_by(clase.idprospecto.desc())
		rows = query.count()

		pages = rows / ROWS_PER_PAGE
		more = rows % ROWS_PER_PAGE
		if more:
			pages += 1
		if page > pages:
			page = pages
		left_slice = (page - 1) * ROWS_PER_PAGE
		right_slice = left_slice + ROWS_PER_PAGE
		if right_slice > rows:
			right_slice = rows
		try:
			self.poolconn.close()
		except:
			pass
		# return dict( prospectosbusquedas = [ x.busq_cornice_json for x in query.all() ])
		return {
			"meta": {
				"page": page,
				"pages": pages,
				"rowcount": rows,
				"rowcountformatted": "{:,}".format(rows),
			},
			"prospectosbusquedas": self.include_rn(
				(x.busq_cornice_json for x in query[left_slice:right_slice]), left_slice
			),
		}

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
			que, record, token = self.auth(get_token=True)
			if not que:
				return record
		user = cached_results.dicTokenUser.get(token)
		_vendedor = user.get("vendedor", 0)
		_gerente = user.get("gerente", 0)
		id = int(self.request.matchdict["id"])

		clase = Prospecto
		ses = DBSession
		qo = ses.query(clase).filter(clase.id == id)
		q = qo.filter(clase.gerente == _gerente)
		q = q.filter(clase.vendedor == _vendedor)
		try:
			assert q.count() == 1, "Incorrecto, debe regresar un registro"
		except AssertionError as e:
			print_exc()
			return dict()
		r = qo.one()
		return r.busqueda_cornice_json

	def get_date(self, dateValue, minimal_date=True):
		if minimal_date:
			fecha = datetime(day=1, month=1, year=1999)
		else:
			fecha = datetime(day=31, month=12, year=2100)
		try:
			d, m, y = [int(x) for x in dateValue.split("/")]
			fecha = datetime(day=d, month=m, year=y)
		except:
			pass
		return fecha


@resource(collection_path="api/cuantosprospectos", path="api/cuantosprospectos/{id}")
class CuantosProspectos(EAuth):
	def __init__(self, request, context=None):
		self.request = request

	def collection_get(self):
		if True:
			que, record, token = self.auth(get_token=True)
			if not que:
				return record
		user = cached_results.dicTokenUser.get(token)
		_vendedor = user.get("vendedor", 0)
		_gerente = user.get("gerente", 0)
		clase = Prospecto
		query = DBSession.query(clase.idprospecto).filter(clase.congelado == False)
		req = self.request.params
		gerente = 0
		# print self.request.params
		try:
			gerente = int(req.get("gerente", 0))
		except:
			pass
		vendedor = 0
		try:
			vendedor = int(req.get("vendedor", 0))
		except:
			pass
		if _vendedor:
			vendedor = _vendedor
		if _gerente:
			gerente = _gerente

		mediopublicitario = int(req.get("mediopublicitario", 0))
		tipofecha = req.get("tipofecha", "")
		fechainicial = req.get("fechainicial", "")
		fechafinal = req.get("fechafinal", "")
		tipocuenta = req.get("tipocuenta", "")
		s_cierre = req.get("sincierre", "")
		sincierre = False
		if s_cierre == "1":
			sincierre = True

		if vendedor > 0:
			query = query.filter(clase.idvendedor == vendedor)
		if vendedor == 0 and gerente > 0:
			query = query.filter(clase.idgerente == gerente)
		if mediopublicitario > 0:
			query = query.filter(clase.idmediopublicitario == mediopublicitario)
		if tipocuenta:
			if tipocuenta == "infonavit":
				query = (
					query.filter(clase.contado == False)
					.filter(clase.hipotecaria == False)
					.filter(clase.fovisste == False)
					.filter(clase.pensiones == False)
				)
			elif tipocuenta == "contado":
				query = query.filter(clase.contado == True)
			elif tipocuenta == "hipotecaria":
				query = query.filter(clase.hipotecaria == True)
			elif tipocuenta == "fovisste":
				query = query.filter(clase.fovisste == True)
			elif tipocuenta == "pensiones":
				query = query.filter(clase.pensiones == True)
		# print "tipofecha", tipofecha
		if tipofecha:
			if tipofecha == "alta":
				fecha = clase.fechaasignacion
			else:
				fecha = clase.fechacierre
			print("fechainicial", fechainicial, "fechafinal", fechafinal)
			nohagas_nada = False
			if fechainicial != "" and fechafinal != "":
				fini = self.get_date(fechainicial)
				ffin = self.get_date(fechafinal, minimal_date=False)
			elif fechainicial == "" and fechafinal != "":
				fini = self.get_date("")
				ffin = self.get_date(fechafinal, minimal_date=False)
			elif fechainicial != "" and fechafinal == "":
				fini = self.get_date(fechainicial)
				ffin = self.get_date("", minimal_date=False)
			elif fechainicial == "" and fechafinal == "":
				fini = self.get_date("")
				ffin = self.get_date("", minimal_date=False)
			else:
				nohagas_nada = True
			if nohagas_nada is False:
				query = query.filter(fecha >= fini).filter(fecha <= ffin)
				if tipofecha == "alta" and sincierre:
					query = query.filter(clase.fechacierre == None)
		print(query)
		cuantos = query.count()

		return dict(
			cuantosprospectos=[
				dict(id="1", cuantos=cuantos, cuantosformateado="{:,}".format(cuantos)),
			]
		)

	def get_date(self, dateValue, minimal_date=True):
		if minimal_date:
			fecha = datetime(day=1, month=1, year=1999)
		else:
			fecha = datetime(day=31, month=12, year=2100)
		try:
			d, m, y = [int(x) for x in dateValue.split("/")]
			fecha = datetime(day=d, month=m, year=y)
		except:
			pass
		return fecha

	def get(self):
		que, record = self.auth()
		if not que:
			return record
		clase = Prospecto
		query = DBSession.query(clase)

		cuantos = query.count()
		return dict(
			cuantosprospecto=dict(
				id="1", cuantos=cuantos, cuantosformateado="{:,}".format(cuantos)
			)
		)


@resource(collection_path="api/gtevdors", path="api/gtevdors/{id}")
class Gtevdors(EAuth):
	def __init__(self, request, context=None):
		self.request = request

	def get(self):
		if True:
			que, record, token = self.auth(get_token=True)
			if not que:
				return record
		user = cached_results.dicTokenUser.get(token)
		vendedor = user.get("vendedor", 0)
		gerente = user.get("gerente", 0)
		id_vendedor = 0
		id_gerente = 0
		nombre_vendedor = ""
		nombre_gerente = ""
		try:

			if vendedor:
				try:
					v = (
						DBSession.query(Vendedor)
						.filter(Vendedor.codigo == int(vendedor))
						.filter(Vendedor.desactivado == False)
						.one()
					)
					nombre_vendedor = d_e(v.nombre)
					id_vendedor = v.id

				except:
					print_exc()
					print("saliendo en el vendedor")
					nombre_vendedor = ""
					id_vendedor = 0

				try:
					g = DBSession.query(GerentesVentas).get(int(gerente))
					nombre_gerente = d_e(g.nombre)
					id_gerente = g.id
				except:
					print_exc()
					print("saliendo en el vendedor zona gerente")
					nombre_gerente = ""
					id_gerente = 0

			elif gerente:
				try:
					v = (
						DBSession.query(Vendedor)
						.filter(Vendedor.desactivado == False)
						.order_by(Vendedor.nombre)
					)
				except:
					print_exc()
					print("saliendo en el gerente")
					nombre_gerente = ""
					id_gerente = 0
			else:
				pass
		except:
			print_exc()
			print("sali por quien sabe que causa")

		return dict(
			gtevdor=dict(
				id=1,
				idvendedor=id_vendedor,
				nombrevendedor=nombre_vendedor,
				idgerente=id_gerente,
				nombregerente=nombre_gerente,
			)
		)


@resource(collection_path="api/gixanips", path="api/gixanips/{id}")
class GixAnip(EAuth):
	def __init__(self, request, context=None):
		self.request = request

	def collection_get(self):
		if True:
			que, record = self.auth()
			if not que:
				return dict(gixanips=[])
			rdb.connect(
				cached_results.settings.get("rethinkdb.host"),
				cached_results.settings.get("rethinkdb.port"),
			).repl()

			usuariosvendedores = rdb.db("iclar").table("usuariosvendedores")
			sql = """
			select g.usuario as usuario,
			g.fkvendedor as vendedor,
			v.nombre as nombre
			from gixanip g
			join vendedor v on g.fkvendedor = v.codigo
			where v.desactivado = 0
			order by v.codigo desc
			"""
			resultado = []
			for i, x in enumerate(DBSession.execute(preparaQuery(sql)), 1):
				# print "gixanip", x
				try:
					if (
						usuariosvendedores.filter(dict(usuario=x.usuario)).count().run()
						> 0
					):
						continue
					resultado.append(
						dict(
							id=i,
							vendedor=x.vendedor,
							usuario=x.usuario,
							nombre=dec_enc(x.nombre),
						)
					)
				except:
					print("some error", x)

			return dict(gixanips=resultado)


@resource(
	collection_path="api/mediospublicitarios", path="api/mediospublicitarios/{id}"
)
class MediosPublicitarios(EAuth):
	def __init__(self, request, context=None):
		self.request = request

	def collection_get(self):
		if True:
			que, record = self.auth()
			if not que:
				return record
		clase = MedioPublicitario
		query = (
			DBSession.query(clase)
			.filter(clase.estatus == "A")
			.order_by(clase.descripcion)
		)
		return {"mediospublicitarios": [x.cornice_json for x in query.all()]}


def limpia_sql(sql):
	sql = sql.replace("\n", " ")
	sql = sql.replace("\t", " ")
	return sql


@resource(collection_path="api/clientesofertas", path="api/clientesofertas/{id}")
class ClientesOfertas(EAuth):
	def __init__(self, request, context=None):
		self.request = request

	def collection_get(self):
		if True:
			que, record = self.auth()
			if not que:
				return record

		cliente = self.request.params.get("cliente", "")
		sql = """select top 1 x.codigo as id , x.nombre as nombre, isnull(c.codigo,0) as cuenta from cliente x
			left join cuenta c on x.codigo = c.fk_cliente 
			where x.codigo = {}""".format(
			cliente
		)

		resul = []
		try:
			for x in DBSession.execute(limpia_sql(sql)):
				pass
				d = dict(
					id=x.id,
					nombre=x.nombre.decode("iso-8859-1").encode("utf-8"),
					cuenta=x.cuenta,
				)
				resul.append(d)

		except:
			print_exc()
		print(resul)
		return dict(clientesofertas=resul)


@resource(collection_path="api/inmuebledetalles", path="api/inmuebledetalles/{id}")
class InmuebleDetalle(EAuth):
	def __init__(self, request, context=None):
		self.request = request

	def collection_get(self):
		if True:
			que, record = self.auth()
			if not que:
				return dict(inmuebledetalles=[])
			p = self.request.params
			etapa = p.get("etapa", "")
			disponible = p.get("disponible", "")
			additionalWhere = ""
			if disponible:
				additionalWhere = " and coalesce(c.codigo, 0) = 0 "
			try:
				sql = """
					select i.codigo as inmueble, i.iden2 as manzana, 
					i.iden1 as lote, 
					coalesce(c.fk_inmueble, 0) as cuenta,
					i.habilitado as habilitado,
					i.candadoprecio as candado,
					coalesce(i.preciocatalogo, 0) as precio
					from inmueble i 
					left join cuenta c 
					on i.codigo = c.fk_inmueble
					where i.fk_etapa = {} {}
					order by 2,3
				""".format(
					etapa, additionalWhere
				)
				lista = []
				for x in DBSession.execute(preparaQuery(sql)):
					vendido = x.cuenta > 0
					d = dict(
						id=x.inmueble,
						manzana=dec_enc(x.manzana, True),
						lote=dec_enc(x.lote, True),
						precio=formato_comas.format(x.precio),
						habilitado=x.habilitado,
						candado=x.candado,
						vendido=vendido,
					)
					lista.append(d)
				return dict(inmuebledetalles=lista)
			except:
				print_exc()
				return dict(inmuebledetalles=[])


@resource(collection_path="api/clientessinofertas", path="api/clientessinofertas/{id}")
class ClientesSinOfertas(EAuth):
	def __init__(self, request, context=None):
		self.request = request

	def collection_get(self):
		if True:
			que, record = self.auth()
			if not que:
				return record

		sql = """select top 1 x.codigo as id , x.nombre as nombre, coalesce(c.codigo,0) as cuenta from cliente x
			left join cuenta c on x.codigo = c.fk_cliente 
			where c.codigo = 0 or c.codigo is null"""

		resul = []
		try:
			for x in DBSession.execute(limpia_sql(sql)):

				d = dict(
					id=x.id,
					nombre=x.nombre.decode("iso-8859-1").encode("utf-8"),
					cuenta=x.cuenta,
				)
				resul.append(d)

		except:
			print_exc()
		print(resul)
		return dict(clientessinofertas=resul)


@resource(collection_path="api/prospectosofertas", path="api/prospectosofertas/{id}")
class ProspectosOfertas(EAuth):
	def __init__(self, request, context=None):
		self.request = request

	def collection_get(self):
		if True:
			que, record, token = self.auth(get_token=True)
			if not que:
				return record
		r = self.request.params
		afiliacion = r.get("afiliacion", "")
		prospecto = r.get("prospecto", "")
		inmueble = r.get("inmueble", "")
		revinculacion = r.get("revinculacion", "")
		user = cached_results.dicTokenUser.get(token)
		# usuario = user.get("usuario", "")
		perfil = user.get("perfil", "")

		gerente = user.get("gerente", "")
		tieneComision = False
		if inmueble and prospecto:
			sql = """
			select fk_desarrollo as desarrollo from etapa where codigo in 
			( select fk_etapa from inmueble where codigo = {})
			""".format(
				inmueble
			)
			desarrollo = ""
			for x in DBSession.execute(preparaQuery(sql)):
				desarrollo = x.desarrollo
			if desarrollo:
				cuantos = 0
				sql = """select count(*) as cuantos 
				from porcentaje_comision where fk_vendedor in 
				( select idvendedor from gixprospectos where idprospecto = {} ) 
				and fk_desarrollo = {}""".format(
					prospecto, desarrollo
				)
				for x in DBSession.execute(preparaQuery(sql)):
					cuantos = int(x.cuantos)

				if cuantos:
					tieneComision = True

		resul = []

		if afiliacion:
			additionalWhere = "rtrim(ltrim(p.afiliacionimss)) = '{}'".format(
				afiliacion.strip()
			)

		if prospecto:
			additionalWhere = "p.idprospecto = {}".format(prospecto)
			if revinculacion:

				if perfil == "gerente":
					additionalWhere = "{} and p.idgerente = {}".format(
						additionalWhere, gerente
					)
				else:
					if perfil not in (
						"admin",
						"direccioncomercial",
						"subdireccioncomercial",
					):
						additionalWhere = "{} and 2 = 1".format(additionalWhere)
		try:
			assert afiliacion or prospecto, "no hay afiliacion o prospecto"
		except:
			print_exc()
			return dict(prospectosofertas=resul)

		DIRECTOR_VENTAS = 9  # gerente que tiene el director de ventas
		sql = """
			select top 1 p.idprospecto as prospecto, 
			rtrim(ltrim(p.apellidopaterno1)) + ' ' + rtrim(ltrim(p.apellidomaterno1)) + ' ' + rtrim(ltrim(p.nombre1)) as nombre,
			rtrim(ltrim(p.afiliacionimss)) as afiliacion,
			p.idvendedor as vendedor, 
			v.nombre as nombrevendedor,
			p.idgerente as gerente, 
			g.nombre as nombregerente,
			p.curp as curp from gixprospectos p
			join vendedor v on p.idvendedor = v.codigo
			join  gerentesventas g on p.idgerente = g.codigo
			where {} and p.fechacierre is null 
			and (p.cuenta = 0 or p.cuenta is null ) 
			and p.congelado = 0 and p.idgerente <> {} 
			order by p.idprospecto desc
		""".format(
			additionalWhere, DIRECTOR_VENTAS
		)
		sql = sql.replace("\n", " ")
		sql = sql.replace("\t", " ")
		try:
			for x in DBSession.execute(sql):
				pass
				d = dict(
					id=x.prospecto,
					nombre=x.nombre.decode("iso-8859-1").encode("utf-8"),
					afiliacion=x.afiliacion,
					vendedor=x.vendedor,
					nombrevendedor=x.nombrevendedor.decode("iso-8859-1").encode(
						"utf-8"
					),
					gerente=x.gerente,
					nombregerente=x.nombregerente.decode("iso-8859-1").encode("utf-8"),
					tieneComision=tieneComision,
					curp=x.curp,
				)
				resul.append(d)

		except:
			print_exc()
		print(resul)
		return dict(prospectosofertas=resul)


@resource(collection_path="api/preciosinmuebles", path="api/preciosinmuebles/{id}")
class PreciosInmuebles(EAuth):
	def __init__(self, request, context=None):
		self.request = request

	def collection_get(self):
		if True:
			que, record = self.auth()
			if not que:
				return record
		sql = """select descripcion, precio, sustentable, id, fk_etapa as etapa from gixpreciosetapa
		where  activo = 1 order by precio"""
		sql = sql.replace("\n", " ")
		sql = sql.replace("\t", " ")
		resul = []
		try:
			for x in DBSession.execute(sql):
				resul.append(
					dict(
						id=x.id,
						descripcion=x.descripcion,
						precio=formato_comas.format(x.precio),
						sustentable=x.sustentable,
						etapa=x.etapa,
						precioraw=x.precio,
					)
				)
		except:
			print_exc()
		print(resul)
		return dict(preciosinmuebles=resul)


@resource(
	collection_path="api/referenciasrapcuentas", path="api/referenciasrapcuentas/{id}"
)
class RapConCuentas(EAuth):
	def __init__(self, request, context=None):
		self.request = request

	def get(self):
		que, record = self.auth()

		if not que:
			return dict(referenciasrapcuentas=[])

		cual = self.request.matchdict["id"]
		cuenta = 0

		sql = """ select top 1 referencia, cuenta from referencias_rap
		where referencia = '{}' order by fecha desc
		""".format(
			cual
		)

		referencia = 0
		for x in DBSession.execute(sql):
			referencia = x.referencia
			cuenta = x.cuenta
		if not cuenta:
			error = "la referencia rap no tiene cuenta"
			self.request.response.status = 400
			return dict(error=error)

		sql = """
		select c.fk_cliente as cliente,
		cte.nombre as nombre,
		c.saldo as saldo,
		isnull(i.iden2, '') as manzana,
		isnull(i.iden1, '') as lote,
		isnull(e.descripcion, '') as etapa from
		cuenta c 
		join cliente cte on c.fk_cliente = cte.codigo
		left join inmueble i on c.fk_inmueble = i.codigo
		left join etapa e on i.fk_etapa = e.codigo
		where c.codigo = {}
		""".format(
			cuenta
		)

		for x in DBSession.execute(preparaQuery(sql)):
			cliente = x.cliente
			nombre = dec_enc(x.nombre)
			etapa = dec_enc(x.etapa)
			manzana = dec_enc(x.manzana)
			lote = dec_enc(x.lote)
			saldo = x.saldo

		return dict(
			referenciasrapcuentas=dict(
				id=cual,
				referencia=referencia,
				cuenta=cuenta,
				nombre=nombre,
				cliente=cliente,
				etapa=etapa,
				manzana=manzana,
				lote=lote,
				saldo=saldo,
			)
		)


@resource(
	collection_path="api/referenciasrapconclientesincuentas",
	path="api/referenciasrapconclientesincuentas/{id}",
)
class RapConClienteSinCuentas(EAuth):
	def __init__(self, request, context=None):
		self.request = request

	def get(self):
		que, record = self.auth()
		if not que:
			return record
		cual = self.request.matchdict["id"]
		sql = """ select top 1 referencia from referencias_rap where cliente = {} and cuenta = 0 order by fecha desc
		""".format(
			cual
		)
		referencia = 0
		for x in DBSession.execute(sql):
			referencia = x.referencia
		if not referencia:
			error = "no esta en referencias rap"
			self.request.response.status = 400
			return dict(error=error)
		return dict(referenciasrapconclientesincuenta=dict(id=1, referencia=referencia))


@resource(collection_path="api/parametrosetapas", path="api/parametrosetapas/{id}")
class ParametrosEtapas(EAuth):
	def __init__(self, request, context=None):
		self.request = request

	def get(self):
		que, record = self.auth()
		if not que:
			return record
		cual = self.request.matchdict["id"]

		sql = """select precio, valor_gastos as gastosadministrativos, precio_seguro as precioseguro, valor_apartado as apartado,
				anticipo_comision as antcipocomision, gastos_a_cuenta
				from PrecioEtapaDefault where fk_etapa = {}
			""".format(
			cual
		)
		sql = sql.replace("\n", " ")
		sql = sql.replace("\t", " ")
		d = dict()

		for x in DBSession.execute(sql):
			d = dict(
				id=cual,
				gastosadministrativos=x.gastosadministrativos,
				precioseguro=x.precioseguro,
				apartado=x.apartado,
				anticipocomision=x.antcipocomision,
			)
		if len(d) > 0:
			return dict(parametrosetapa=d)

		self.request.response.status = 401
		return dict(error="no se encuentra en parametrosetapas")


@resource(
	collection_path="api/inmuebleindividuals", path="api/inmuebleindividuals/{id}"
)
class inmuebleindividual(EAuth):
	def __init__(self, request, context=None):
		self.request = request

	def get(self):
		que, record = self.auth()
		if not que:
			return record
		cual = self.request.matchdict["id"]
		sql = """
		select domiciliooficial as domicilio,
		candadoprecio,
		preciocatalogo
		from inmueble
		where codigo={}""".format(
			cual
		)
		domicilio = ""
		hay = False
		for x in DBSession.execute(preparaQuery(sql)):
			hay = True
			domicilio = dec_enc(x.domicilio)
			candadoPrecio = x.candadoprecio
			precioCatalogo = x.preciocatalogo

		if hay:
			return dict(
				inmuebleindividual=dict(
					id=cual,
					domicilio=domicilio,
					candadoPrecio=candadoPrecio,
					precioCatalogo=precioCatalogo,
				)
			)
		else:
			return dict()


@resource(collection_path="api/etapasofertas", path="api/etapasofertas/{id}")
class EtapasOfertas(EAuth):
	def __init__(self, request, context=None):
		self.request = request

	def get(self):
		que, record = self.auth()
		if not que:
			return record
		cual = self.request.matchdict["id"]
		sql = """
			select distinct i.fk_etapa as netapa, 
			e.descripcion as etapa, 
			d.descripcion as desarrollo, 
			e.departamento as departamento
			from inmueble i 
			join etapa e on i.fk_etapa = e.codigo 
			join desarrollo d on e.fk_desarrollo = d.codigo 
			where i.codigo not in ( select distinct fk_inmueble from cuenta ) and i.fk_etapa > 39 and e.codigo={}
			""".format(
			cual
		)
		id = ""
		print("entro al get de etapasofertas")
		departamento = ""
		nombre = ""
		sql = sql.replace("\n", " ")
		sql = sql.replace("\t", " ")
		for x in DBSession.execute(sql):
			id = x.netapa
			departamento = x.departamento
			nombre = "{} - {}".format(
				x.desarrollo.decode("iso-8859-1").encode("utf-8"),
				x.etapa.decode("iso-8859-1").encode("utf-8"),
			)
		if id:
			print("si regreso algo")
			return dict(
				etapasofertas=dict(id=id, departamento=departamento, nombre=nombre)
			)
		else:
			print("no regreso nada")
			return dict()

	def collection_get(self):
		if True:
			que, record = self.auth()
			if not que:
				return record

		todas = self.request.params.get("todas", "")
		if todas:
			sql = """
			select distinct i.fk_etapa as netapa, 
			e.descripcion as etapa, 
			d.descripcion as desarrollo,
			e.departamento as departamento
			from inmueble i 
			join etapa e on i.fk_etapa = e.codigo 
			join desarrollo d on e.fk_desarrollo = d.codigo 
			where  i.fk_etapa > 39 order by 1 desc
			"""
		else:
			sql = """
			select distinct i.fk_etapa as netapa, 
			e.descripcion as etapa, 
			d.descripcion as desarrollo, 
			e.departamento as departamento
			from inmueble i 
			join etapa e on i.fk_etapa = e.codigo 
			join desarrollo d on e.fk_desarrollo = d.codigo 
			where  i.habilitado <> 0 and i.codigo not in ( select distinct fk_inmueble from cuenta ) and i.fk_etapa > 39 order by 1 desc
			"""
		sql = sql.replace("\n", " ")
		sql = sql.replace("\t", " ")
		resul = []
		try:
			for x in DBSession.execute(sql):
				resul.append(
					dict(
						id=x.netapa,
						departamento=x.departamento,
						nombre="{} - {}".format(
							x.desarrollo.decode("iso-8859-1").encode("utf-8"),
							x.etapa.decode("iso-8859-1").encode("utf-8"),
						),
					)
				)
		except:
			print_exc()
		print(resul)
		return dict(etapasofertas=resul)


@resource(collection_path="api/etapastramites", path="api/etapastramites/{id}")
class EtapasTramites(EAuth):
	def __init__(self, request, context=None):
		self.request = request

	def get(self):
		que, record = self.auth()
		if not que:
			return record
		cual = self.request.matchdict["id"]
		sql = """
			select distinct i.fk_etapa as netapa, 
			e.descripcion as etapa, 
			d.descripcion as desarrollo, 
			e.departamento as departamento
			from inmueble i 
			join etapa e on i.fk_etapa = e.codigo 
			join desarrollo d on e.fk_desarrollo = d.codigo 
			join (select distinct fk_inmueble as inmueble from tramites_ventas_movimientos) m 
			on i.codigo = m.inmueble
			where  e.codigo={}
			""".format(
			cual
		)
		sql = """
		select e.departamento as departamento , 
		e.descripcion as etapa,
		d.descripcion as desarrollo 
		from etapa e join desarrollo d on e.fk_desarrollo = d.codigo
		where e.codigo = {}
		""".format(
			cual
		)
		id = ""
		print("entro al get de etapastramites")
		departamento = ""
		nombre = ""
		sql = sql.replace("\n", " ")
		sql = sql.replace("\t", " ")
		for x in DBSession.execute(sql):
			id = cual
			departamento = x.departamento
			nombre = "{} - {}".format(
				x.desarrollo,
				x.etapa,
			)
		if id:
			print("si regreso algo")
			return dict(
				etapastramite=dict(id=id, departamento=departamento, nombre=nombre)
			)
		else:
			print("no regreso nada")
			return dict()

	def collection_get(self):
		if True:
			que, record = self.auth()
			if not que:
				return record

		todas = self.request.params.get("todas", "")
		company = self.request.params.get("company", "")
		if todas:
			if company == "arcadia":
				sql = """
				select 33 as netapa,
				'Etapa 4' as etapa,
				'Pinares Tapalpa' as desarrollo,
				'1' as departamento
				union
				select 10 as netapa,
				'Etapa 3' as etapa,
				'Pinares Tapalpa' as desarrollo,
				'1' as departamento
				union
				select 9 as netapa,
				'Etapa 2' as etapa,
				'Pinares Tapalpa' as desarrollo,
				'1' as departamento
				union
				select 8 as netapa,
				'Etapa 1' as etapa,
				'Pinares Tapalpa' as desarrollo,
				'1' as departamento
				union
				select 34 as netapa,
				'Etapa 5' as etapa,
				'Pinares Tapalpa' as desarrollo,
				'1' as departamento
				union
				select 35 as netapa,
				'Etapa 6' as etapa,
				'Pinares Tapalpa' as desarrollo,
				'1' as departamento
				order by 1 desc
				"""

			else:
				sql = """
				select distinct i.fk_etapa as netapa, 
				e.descripcion as etapa, 
				d.descripcion as desarrollo,
				e.departamento as departamento
				from inmueble i 
				join etapa e on i.fk_etapa = e.codigo 
				join desarrollo d on e.fk_desarrollo = d.codigo 
				where  i.fk_etapa > 39 order by 1 desc
			"""
		else:
			if company == "arcadia":
				sql = """
				select 33 as netapa,
				'Etapa 4' as etapa,
				'Pinares Tapalpa' as desarrollo,
				'1' as departamento
				union
				select 10 as netapa,
				'Etapa 3' as etapa,
				'Pinares Tapalpa' as desarrollo,
				'1' as departamento
				union
				select 9 as netapa,
				'Etapa 2' as etapa,
				'Pinares Tapalpa' as desarrollo,
				'1' as departamento
				union
				select 8 as netapa,
				'Etapa 1' as etapa,
				'Pinares Tapalpa' as desarrollo,
				'1' as departamento
				union
				select 34 as netapa,
				'Etapa 5' as etapa,
				'Pinares Tapalpa' as desarrollo,
				'1' as departamento
				union
				select 35 as netapa,
				'Etapa 6' as etapa,
				'Pinares Tapalpa' as desarrollo,
				'1' as departamento
				order by 1 desc
				"""

			else:
				sql = """
				select distinct i.fk_etapa as netapa, 
				e.descripcion as etapa, 
				d.descripcion as desarrollo, 
				e.departamento as departamento
				from inmueble i 
				join etapa e on i.fk_etapa = e.codigo 
				join desarrollo d on e.fk_desarrollo = d.codigo
				join (select distinct fk_inmueble as inmueble from tramites_ventas_movimientos) m 
				on i.codigo = m.inmueble order by 1 desc
			
				"""
		sql = sql.replace("\n", " ")
		sql = sql.replace("\t", " ")
		resul = []
		session = None
		print("llego aca")
		if company == "arcadia":
			session = DBSession2
		else:
			session = DBSession
		try:
			for x in session.execute(sql):
				resul.append(
					dict(
						id=x.netapa,
						departamento=x.departamento,
						nombre="{} - {}".format(x.desarrollo, x.etapa),
					)
				)
		except:
			print_exc()
		print(resul)
		hoy = today(False)
		hoy = "{:04}/{:02d}/{:02d}".format(hoy.year, hoy.month, hoy.day)
		return dict(meta=dict(hoy=hoy), etapastramites=resul)


@resource(collection_path="api/manzanastramites", path="api/manzanastramites/{id}")
class ManzanasTramites(EAuth):
	def __init__(self, request, context=None):
		self.request = request

	def collection_get(self):
		if True:
			que, record = self.auth()
			if not que:
				return record

		etapa = self.request.params.get("etapa", "")
		additionalWhere = ""
		if etapa:
			additionalWhere = " and i.fk_etapa = {}".format(etapa)

		sql = """
		select distinct i.iden2 as manzana from inmueble i 
		join (select distinct fk_inmueble as inmueble from tramites_ventas_movimientos) m 
		on i.codigo = m.inmueble
		where 1 = 1 {} order by 1
		""".format(
			additionalWhere
		)

		sql = sql.replace("\n", " ")
		sql = sql.replace("\t", " ")
		resul = []
		wasAnError = False
		try:
			for i, x in enumerate(DBSession.execute(sql), 1):
				resul.append(dict(id=i, manzana=x.manzana.strip()))
		except:
			print_exc()
			wasAnError = True
		print(resul)
		return dict(manzanastramites=resul)


@resource(
	collection_path="api/manzanasdisponibles", path="api/manzanasdisponibles/{id}"
)
class ManzanasDisponibles(EAuth):
	def __init__(self, request, context=None):
		self.request = request

	def collection_get(self):
		if True:
			que, record = self.auth()
			if not que:
				return record

		etapa = self.request.params.get("etapa", "")
		additionalWhere = ""
		if etapa:
			additionalWhere = " and i.fk_etapa = {}".format(etapa)
		sql = """
		select distinct i.iden2 as manzana from inmueble i
		where i.codigo not in ( select distinct fk_inmueble from cuenta ) and i.fk_etapa > 39
		{} order by 1""".format(
			additionalWhere
		)

		sql = """
		select distinct i.iden2 as manzana from inmueble i left join cuenta c 
		on i.codigo = c.fk_inmueble
		where c.fk_inmueble is null and i.fk_etapa > 39 and i.habilitado <> 0 {} order by 1
		""".format(
			additionalWhere
		)

		sql = sql.replace("\n", " ")
		sql = sql.replace("\t", " ")
		resul = []
		wasAnError = False
		try:
			for i, x in enumerate(DBSession.execute(sql), 1):
				resul.append(dict(id=i, manzana=x.manzana.strip()))
		except:
			print_exc()
			wasAnError = True
		print(resul)
		return dict(manzanasdisponibles=resul)


@resource(
	collection_path="api/inmueblesdisponibles", path="api/inmueblesdisponibles/{id}"
)
class InmueblesDisponibles(EAuth):
	def __init__(self, request, context=None):
		self.request = request

	def collection_get(self):
		if True:
			que, record = self.auth()
			if not que:
				return record

		etapa = self.request.params.get("etapa", "")

		additionalWhere = ""
		if etapa:
			additionalWhere = " and i.fk_etapa = {}".format(etapa)

		env = "prod"
		if "TEST" in cached_results.settings.get("sqlalchemy.url", ""):
			env = "test"

		if etapa:
			return dict(
				inmueblesdisponibles=tsql.inmueblesdisponibles(
					etapa=int(etapa), env=env
				)
			)

		sql = """
		select i.iden2 as manzana , iden1 as lote, i.codigo as codigo from inmueble i 
		left join cuenta c on i.codigo = c.fk_inmueble
		where c.fk_inmueble is null and i.fk_etapa > 39 {} and habilitado <> 0 order by i.iden2,i.iden1""".format(
			additionalWhere
		)

		sql = sql.replace("\n", " ")
		sql = sql.replace("\t", " ")
		de = "InmueblesDisponibles"
		print("armando query de {} a las {}".format(de, datetime.now().isoformat()))
		try:
			# cn = DBSession.connection()
			# c = cn.connection
			stream = True
			# cu = c.cursor()
		except:
			print_exc()
		resul = {
			"inmueblesdisponibles": [
				{
					"id": x.codigo,
					"manzana": x.manzana.strip(),
					"lote": dec_enc(x.lote, True),
				}
				for x in DBSession.execute(sql)
			]
		}
		print("resolviendo query de {} a las {}".format(de, datetime.now().isoformat()))
		return resul


@resource(collection_path="api/inmueblestramites", path="api/inmueblestramites/{id}")
class InmueblesTramites(EAuth):
	def __init__(self, request, context=None):
		self.request = request

	def collection_get(self):
		if True:
			que, record = self.auth()
			if not que:
				return record

		etapa = self.request.params.get("etapa", "")

		additionalWhere = ""
		if etapa:
			additionalWhere = " and i.fk_etapa = {}".format(etapa)

		sql = """
		select i.iden2 as manzana , iden1 as lote, i.codigo as codigo from inmueble i 
		
		where i.codigo in ( select distinct fk_inmueble from tramites_ventas_movimientos) 
		{} order by i.iden2,i.iden1""".format(
			additionalWhere
		)

		sql = sql.replace("\n", " ")
		sql = sql.replace("\t", " ")
		de = "InmueblesTramites"
		print("armando query de {} a las {}".format(de, datetime.now().isoformat()))

		resul = {
			"inmueblestramites": [
				{"id": x.codigo, "manzana": x.manzana.strip(), "lote": x.lote}
				for x in DBSession.execute(sql)
			]
		}
		print("resolviendo query de {} a las {}".format(de, datetime.now().isoformat()))
		return resul


@resource(collection_path="api/vendedors", path="api/vendedors/{id}")
class Vendedores(EAuth):
	def __init__(self, request, context=None):
		self.request = request

	def collection_get(self):
		if True:
			que, record, token = self.auth(get_token=True)
			if not que:
				return record
		user = cached_results.dicTokenUser.get(token)
		try:
			vendedor = user.get("vendedor", 0)
			if vendedor:

				query = (
					DBSession.query(Vendedor)
					.filter(Vendedor.codigo == int(vendedor))
					.filter(Vendedor.desactivado == False)
				)
			else:

				query = (
					DBSession.query(Vendedor)
					.filter(Vendedor.desactivado == False)
					.order_by(Vendedor.nombre)
				)
		except:
			print_exc()
			return dict(vendedors=[])

		return {"vendedors": [x.cornice_json for x in query.all()]}


@resource(collection_path="api/usuarios", path="api/usuarios/{id}")
class UsuariosRest(EAuth):
	def __init__(self, request, context=None):
		self.request = request

	def collection_get(self):
		if True:
			que, record, token = self.auth(get_token=True)
			if not que:
				return record
			user = cached_results.dicTokenUser.get(token)
			try:
				assert (
					user.get("perfil", "") == "admin"
				), "El usuario no es administrador"
			except AssertionError as e:
				print_exc()
				return dict()

		rdb.connect(
			cached_results.settings.get("rethinkdb.host"),
			cached_results.settings.get("rethinkdb.port"),
		).repl()
		t = rdb.db("iclar").table("usuarios").order_by("usuario")
		# return dict( usuarios = [ x for x in t.run() ])
		return dict(
			usuarios=[
				dict(
					id=x.get("id"),
					usuario=x.get("usuario"),
					password=x.get("password"),
					perfil=x.get("zen_profile"),
				)
				for x in t.run()
			]
		)


@resource(collection_path="api/gerentesventa", path="api/gerentesventa/{id}")
class GerentesVtas(EAuth):
	def __init__(self, request, context=None):
		self.request = request

	def collection_get(self):
		if True:
			que, record, token = self.auth(get_token=True)
			if not que:
				return record
		try:
			user = cached_results.dicTokenUser.get(token)
			gerente = user.get("gerente", 0)
			if gerente:
				query = (
					DBSession.query(GerentesVentas)
					.filter(GerentesVentas.codigo == int(gerente))
					.filter(GerentesVentas.activo == True)
				)
			else:
				query = (
					DBSession.query(GerentesVentas)
					.filter(GerentesVentas.activo == True)
					.order_by(GerentesVentas.nombre)
				)
		except:
			print_exc()
			return dict(gerentesventas=[])
		# return dict(foo="bar")
		return {"gerentesventas": [x.cornice_json for x in query.all()]}


@resource(collection_path="api/ofertasrecientes", path="api/ofertasrecientes/{id}")
class OfertasRecientes(EAuth):
	def __init__(self, request, context=None):
		self.request = request

	def collection_get(self):
		if True:
			que, record, token = self.auth(get_token=True)
			if not que:
				return record
		try:
			user = cached_results.dicTokenUser.get(token)
			p = self.request.params
			etapa = p.get("etapa", 0)
			cuantos = p.get("cuantos", 100)
			sininmueble = p.get("sininmueble", False)
			additionalWhere = ""
			additionalWhere2 = ""
			if etapa:
				additionalWhere = "where o.fk_etapa = {}".format(etapa)
			if sininmueble:
				additionalWhere2 = " and cta.fk_inmueble = 0"
			if cuantos:
				top = "top {}".format(cuantos)
			print("ofertasrecientes etapa {} cuantos {}".format(etapa, cuantos))
			sql = """
				select {} o.fk_etapa as etapa, o.oferta as oferta, o.cliente as cliente, 
				cte.nombre as nombrecliente, o.cuenta as cuenta, coalesce(i.iden2,'') as manzana, coalesce(i.iden1,'') as lote,
				cta.saldo as saldo, cta.fecha as fecha
				from ofertas_compra o join cliente cte on o.cliente = cte.codigo 
				join cuenta cta on o.cuenta = cta.codigo 
				left join inmueble i on cta.fk_inmueble = i.codigo  {} {} order by cta.codigo desc
				""".format(
				top, additionalWhere, additionalWhere2
			)

			return dict(
				ofertasrecientes=[
					dict(
						id=i,
						etapa=x.etapa,
						oferta=x.oferta,
						cliente=x.cliente,
						nombrecliente=x.nombrecliente.decode("iso-8859-1").encode(
							"utf-8"
						),
						cuenta=x.cuenta,
						manzana=dec_enc(x.manzana, True),
						lote=dec_enc(x.lote, True),
						saldo=x.saldo,
						fecha=x.fecha.isoformat(),
					)
					for i, x in enumerate(DBSession.execute(sql), 1)
				]
			)

		except:
			print_exc()
			return dict(ofertasrecientes=[])


@resource(collection_path="api/ofertabusquedas", path="api/ofertabusquedas/{id}")
class OfertaBusquedas(EAuth):
	def __init__(self, request, context=None):
		self.request = request

	def get_date(self, dateValue, minimal_date=True):
		if minimal_date:
			fecha = datetime(day=1, month=1, year=1999)
		else:
			fecha = datetime(day=31, month=12, year=2100)
		try:
			d, m, y = [int(x) for x in dateValue.split("/")]
			fecha = datetime(day=d, month=m, year=y)
		except:
			pass
		return fecha

	def collection_get(self):
		if True:
			que, record, token = self.auth(get_token=True)
			if not que:
				return record
		try:
			user = cached_results.dicTokenUser.get(token)
			p = self.request.params
			rdb = request_to_rdb(p)
			nombre_gerente = ""
			gerente = p.get("gerente", "")
			etapa = p.get("etapa", "")

			fechainicial = p.get("fechaInicial", "")
			fechafinal = p.get("fechaFinal", "")
			if fechainicial != "" and fechafinal != "":
				fini = self.get_date(fechainicial)
				ffin = self.get_date(fechafinal, minimal_date=False)
			elif fechainicial == "" and fechafinal != "":
				fini = self.get_date("")
				ffin = self.get_date(fechafinal, minimal_date=False)
			elif fechainicial != "" and fechafinal == "":
				fini = self.get_date(fechainicial)
				ffin = self.get_date("", minimal_date=False)
			elif fechainicial == "" and fechafinal == "":
				fini = self.get_date("")
				ffin = self.get_date("", minimal_date=False)
			ROWS_PER_PAGE = 20
			page = 1
			try:
				page = int(p.get("page", 1))
			except:
				pass
			estatus = p.get("estatus", "")
			orden = p.get("orden", "")

			select_list = """
				o.oferta as oferta,
				convert(varchar(10), o.fecha_oferta, 103) as fecha_oferta, 
				o.fk_etapa as etapa,
				case when o.fecha_cancelacion is null then '' else convert(varchar(10) , o.fecha_cancelacion, 103) end as fecha_cancelacion, 
				c.nombre as nombre_cliente, 
				c.telefonocasa as telefono,
				v.nombre as nombre_subvendedor, 
				E.codigo AS empresa, 
				D.codigo AS desarrollo, 
				E.razonsocial AS desc_empresa,
				D.descripcion AS desc_desarrollo, 
				T.descripcion AS desc_etapa,
				g.nombre as nombre_gerente
			"""
			select_list_cuantos = " count(*) as cuantos "
			sql = """ SELECT {}
				FROM ofertas_compra o, cliente c, vendedor v, EMPRESA E, DESARROLLO D, ETAPA T, gerentesventas g
				WHERE o.fk_etapa = T.codigo
				AND D.codigo = T.fk_desarrollo
				AND E.codigo = D.fk_empresa
				AND c.codigo = o.cliente
				AND v.codigo = o.subvendedor
				"""
			sql = """
				select {} from ofertas_compra o join
				etapa T on T.codigo = o.fk_etapa
				join desarrollo D on D.codigo = T.fk_desarrollo
				join empresa E on E.codigo = D.fk_empresa
				join cliente c on c.codigo = o.cliente
				join vendedor v on v.codigo = o.subvendedor
				join gerentesventas g on v.gerente = g.codigo
			"""
			if gerente:
				sql += " and v.gerente={}".format(gerente)
			if etapa:
				sql += " and o.fk_etapa={}".format(etapa)
			if True:
				sql += (
					" AND O.fecha_oferta >=  '{}'  AND o.fecha_oferta <= '{}'".format(
						fini, ffin
					)
				)
			if estatus:
				if int(estatus) == 1:
					sql += " AND o.cancelada = 0"
				if int(estatus) == 2:
					sql += " AND o.cancelada <> 0"

			cuantos = 0
			for x in DBSession.execute(preparaQuery(sql.format(select_list_cuantos))):
				cuantos = x.cuantos

			sql += " ORDER BY E.Codigo, D.Codigo, o.fk_etapa"

			if orden:
				if int(orden) == 1:
					sql += " ,oferta"
				if int(orden) == 2:
					sql += " ,nombre_cliente"
				if int(orden) == 3:
					sql += " ,nombre_subvendedor"
				if int(orden) == 4:
					sql += " ,fecha_cancelacion"

			rows = cuantos
			pages = rows / ROWS_PER_PAGE
			more = rows % ROWS_PER_PAGE
			if more:
				pages += 1
			if page > pages:
				page = pages
			left_slice = (page - 1) * ROWS_PER_PAGE
			right_slice = left_slice + ROWS_PER_PAGE
			if right_slice > rows:
				right_slice = rows

			query_list = []
			for x in DBSession.execute(preparaQuery(sql.format(select_list))):
				query_list.append(x)
			return dict(
				meta=dict(
					rdb=rdb,
					page=page,
					pages=pages,
					rowcount=rows,
					rowcountformatted="{:,}".format(rows),
				),
				ofertabusquedas=[
					dict(
						id=i,
						oferta=x.oferta,
						fecha_oferta=x.fecha_oferta,
						etapa=x.etapa,
						fecha_cancelacion=x.fecha_cancelacion,
						nombre_cliente=dec_enc(x.nombre_cliente),
						telefono=x.telefono,
						nombre_gerente=dec_enc(x.nombre_gerente)
						if x.nombre_gerente
						else "",
						nombre_subvendedor=dec_enc(x.nombre_subvendedor),
					)
					for i, x in enumerate(query_list[left_slice:right_slice], 1)
				],
			)

		except:
			print_exc()
			return dict(ofertabusquedas=[])


@resource(
	collection_path="api/prospectosrecientes", path="api/prospectosrecientes/{id}"
)
class ProspectosRecientes(EAuth):
	def __init__(self, request, context=None):
		self.request = request

	def collection_get(self):
		if True:
			que, record, token = self.auth(get_token=True)
			if not que:
				return record
		try:
			user = cached_results.dicTokenUser.get(token)
			gerente = user.get("gerente", 0)
			vendedor = user.get("vendedor", 0)
			p = self.request.params
			afiliacion_valida = p.get("afiliacionvalida", "")
			if afiliacion_valida in "01":
				pass
			else:
				print(
					"forzando afiliacion_valida por valor distinto a 0 o 1, valor ",
					afiliacion_valida,
				)
				afiliacion_valida = ""

			cuantos = 10
			if afiliacion_valida == "0":
				cuantos = 100
			limite = p.get("limite", cuantos)

			clase = Prospecto
			s = DBSession
			query = s.query(clase).filter(clase.congelado == False)

			if gerente:
				query = query.filter(clase.idgerente == gerente)
			if vendedor:
				query = query.filter(clase.idvendedor == vendedor)
			query = query.order_by(clase.idprospecto.desc()).limit(limite)

		except:
			print_exc()
			return dict(prospectosrecientes=[])
		# return dict(foo="bar")
		if afiliacion_valida == "":
			return {
				"prospectosrecientes": [x.reciente_cornice_json for x in query.all()]
			}
		if afiliacion_valida == "1":
			return {
				"prospectosrecientes": [
					x.reciente_cornice_json
					for x in query.all()
					if clase.is_luhn_valid(x.afiliacionimss)
				]
			}
		if afiliacion_valida == "0":
			return {
				"prospectosrecientes": [
					x.reciente_cornice_json
					for x in query.all()
					if clase.is_luhn_valid(x.afiliacionimss) == False
				][0:10]
			}


@lru_cache(maxsize=500)
def cantidad_a_palabras(que):
	texto, texto2 = "", ""

	try:
		cual = aletras(que)
		texto = cual.encode("UTF-8")
		texto2 = texto.split("PESO")[0].strip()
	except:
		pass
	return (texto, texto2)


@resource(collection_path="api/pesos", path="api/pesos/{id}")
class Pesos(object):
	def __init__(self, request, context=None):
		self.request = request

	@view(renderer="json")
	def get(self):
		id = int(self.request.matchdict["id"])
		que = id / 100.0
		formato_comas = "{:,.2f}"

		texto, texto2 = cantidad_a_palabras(que)
		return dict(
			id=id,
			texto=texto,
			texto2=texto2,
			importeformateado=formato_comas.format(que),
		)


@resource(collection_path="api/gravatars", path="api/gravatars/{id}")
class Gravatar(EAuth):
	def __init__(self, request, context=None):
		self.request = request

	@view(renderer="json")
	def get(self):
		que, record, token = self.auth(get_token=True)
		if not que:
			return record
		user = cached_results.dicTokenUser.get(token)
		usuario = user.get("usuario", "")
		rdb.connect(
			cached_results.settings.get("rethinkdb.host"),
			cached_results.settings.get("rethinkdb.port"),
		).repl()
		usuarios = rdb.db("iclar").table("usuarios")
		for x in usuarios.filter(rdb.row["usuario"] == usuario.upper()).run():
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
		return dict(gravatar=dict(id="1", gravatar=gv, gravataremail=gvemail))

	@view(renderer="json")
	def put(self):
		que, record, token = self.auth(content="gravatar", get_token=True)
		if not que:
			return record
		user = cached_results.dicTokenUser.get(token)
		usuario = user.get("usuario", "")
		gravataremail = ""
		try:
			gravataremail = record.get("gravataremail", "")
		except:
			print("no obtuve valor para gravataremail")
		if gravataremail:

			try:
				rdb.connect(
					cached_results.settings.get("rethinkdb.host"),
					cached_results.settings.get("rethinkdb.port"),
				).repl()
				gravatar = md5(gravataremail).hexdigest()
				rdb.db("iclar").table("usuarios").filter(
					rdb.row["usuario"] == usuario.upper()
				).update(dict(gravatar=gravatar, gravataremail=gravataremail)).run()
				print("gravatar actualizado")
			except:
				print("fallo actualizacion del gravatar")
		else:
			print("no habia valor de gravataremail")

		return dict(gravatar=dict(id=1, gravatar=gravatar, gravataremail=gravataremail))


@resource(
	collection_path="api/analisiscarteraarcadias",
	path="api/analisiscarteraarcadias/{id}",
)
class AnalisisCarteraArcadiaRest(QueryAndErrors, EAuth):
	def __init__(self, request, context=None):
		self.request = request

	def collection_get(self):
		if True:
			que, record, token = self.auth(get_token=True)
			print("valores que, record, token", que, record, token)
			if not que:
				return dict(analisiscarteraarcadias=[])
			p = self.request.params
			r = nbb.analisis_cartera_arcadia()
			contador = 0
			resultado = []

			for x in r.get("cartera_por_etapa"):
				contador += 1
				resultado.append(
					dict(
						id=contador,
						etapa=x[0],
						documentosnovencidos="",
						documentoshasta30="",
						vencidohasta30="",
						documentos3190="",
						vencido3190="",
						documentosmas90="",
						vencidomas90="",
						clientes=x[1],
						terrenos=x[2],
						vencido="",
						porcentaje="",
						saldo=x[3],
						reportevencido=False,
					)
				)

			for x in r.get("cartera_vencida_por_etapa"):
				contador += 1
				resultado.append(
					dict(
						id=contador,
						etapa=x[0],
						documentosnovencidos=x[1],
						documentoshasta30=x[2],
						vencidohasta30=x[3],
						documentos3190=x[4],
						vencido3190=x[5],
						documentosmas90=x[6],
						vencidomas90=x[7],
						clientes=x[8],
						terrenos=x[9],
						vencido=x[10],
						porcentaje=x[11],
						saldo="",
						reportevencido=True,
					)
				)
			return dict(analisiscarteraarcadias=resultado)


@resource(
	collection_path="api/resumencobranzaarcadias",
	path="api/resumencobranzaarcadias/{id}",
)
class ResumenCobranzaArcadiaRest(QueryAndErrors, EAuth):
	def __init__(self, request, context=None):
		self.request = request

	def collection_get(self):
		if True:
			que, record, token = self.auth(get_token=True)
			print("valores que, record, token", que, record, token)
			if not que:
				return dict(resumencobranzaarcadias=[])
			p = self.request.params
			years = int(p.get("years", 1))
			reconstruir = p.get("reconstruir", "")
			hardCall = False
			if reconstruir:
				hardCall = True
			resultado = []
			_tope = today(False).year - years
			tope = "{:04d}/".format(_tope)
			# hardCall = True
			# if years > 1:
			# 	hardCall = False
			color("hardCall es {}".format(hardCall))

			campos = "rubro enganche ocurrenciasenganche porcentajeenganche pagos ocurrenciaspagos porcentajepagos total"
			fecha, datos = nbb.resumen_cobranza_am(hardCall)
			for i, x in enumerate(datos, 1):
				donde = str(x[0])
				if tope in donde:
					break
				# print "x es ", x
				d = {campo: str(x[j]) for (j, campo) in enumerate(campos.split(" "))}
				d["id"] = i
				resultado.append(d)

			return dict(meta=dict(fecha=fecha), resumencobranzaarcadias=resultado)


@resource(
	collection_path="api/carteravencidaarcadias", path="api/carteravencidaarcadias/{id}"
)
class CarteraVencidaArcadiaRest(QueryAndErrors, EAuth):
	def __init__(self, request, context=None):
		self.request = request

	def collection_get(self):
		if True:
			que, record, token = self.auth(get_token=True)
			print("valores que, record, token", que, record, token)
			if not que:
				return dict(carteravencidaarcadias=[])
			dEtapas = {8: 1, 9: 2, 10: 3, 33: 4}
			etapas = ",".join([str(x) for x in dEtapas.keys()])

		sql = """ select i.fk_etapa as etapa,
		i.iden2 as manzana,
		i.iden1 as lote,
		k.codigo as cliente,
		d.fk_cuenta as cuenta,
		k.nombre as nombre,
		c.congelada as congelada,
		sum(d.saldo) as saldo
		from documento d join cuenta c
		on d.fk_cuenta = c.codigo
		join inmueble i on c.fk_inmueble = i.codigo
		join cliente k on c.fk_cliente = k.codigo
		join etapa e on i.fk_etapa = e.codigo
		where e.codigo in ({})
		and convert(varchar(10), d.fechadevencimiento, 111)
		< convert(varchar(10), getdate(),111) and d.saldo > 0
		group by i.fk_etapa, i.iden2, i.iden1, k.codigo, d.fk_cuenta, k.nombre, c.congelada
		order by i.fk_etapa, i.iden2, i.iden1""".format(
			etapas
		)

		resultado = []

		for i, x in enumerate(DBSession2.execute(preparaQuery(sql)), 1):
			resultado.append(
				dict(
					id=i,
					etapa=x.etapa,
					manzana=x.manzana,
					# dec_enc(x.manzana),
					lote=x.lote,
					# dec_enc(x.lote),
					cliente=x.cliente,
					cuenta=x.cuenta,
					nombre=x.nombre,
					# dec_enc(x.nombre),
					congelada=x.congelada,
					saldo=str(x.saldo),
				)
			)
		DBSession2.close()
		return dict(carteravencidaarcadias=resultado)


@resource(collection_path="api/inmueblearcadias", path="api/inmueblearcadias/{id}")
class InmuebleArcadiaRest(QueryAndErrors, EAuth):
	def __init__(self, request, context=None):
		self.request = request

	def collection_get(self):
		if True:
			que, record, token = self.auth(get_token=True)
			print("valores que, record, token", que, record, token)
			if not que:
				return dict(inmueblearcadias=[])
			r = self.request.params
			etapa = 0
			try:
				etapa = int(r.get("etapa", "0"))
			except:
				print_exc()
				return dict(inmueblearcadias=[])

			dEtapas = {0: 0, 1: 8, 2: 9, 3: 10, 4: 33}
			sql = """
			select i.codigo as inmueble,
			i.iden1 as manzana,
			i.iden2 as lote,
			isnull(c.codigo,0) as cuenta,
			isnull(c.fk_cliente,0) as cliente,
			isnull(cte.nombre, '') as nombre
			from inmueble i
			left join cuenta c on i.codigo = c.fk_inmueble
			left join cliente cte on cte.codigo = c.fk_cliente
			where i.fk_etapa = {}
			order by i.codigo
			""".format(
				dEtapas[etapa]
			)
			resultado = []
			for i, x in enumerate(DBSession2.execute(preparaQuery(sql)), 1):
				resultado.append(
					dict(
						id=i,
						inmueble=x.inmueble,
						manzana=x.manzana,
						# dec_enc(x.manzana),
						lote=x.lote,
						# dec_enc(x.lote),
						cuenta=x.cuenta,
						cliente=x.cliente,
						nombre=x.nombre
						# dec_enc(x.nombre)
					)
				)
			DBSession2.close()
			return dict(inmueblearcadias=resultado)


@resource(collection_path="api/vendidosarcadias", path="api/vendidosarcadias/{id}")
class TotalesVendidosArcadiaRest(QueryAndErrors, EAuth):
	def __init__(self, request, context=None):
		self.request = request

	def collection_get(self):
		if True:
			que, record, token = self.auth(get_token=True)
			print("valores que, record, token", que, record, token)
			if not que:
				return dict(vendidosarcadias=[])
			etapas = "8,9,10,33"
			sql = """
			select i.fk_etapa as etapa, 
			count(*) as total from
			inmueble i
			where i.fk_etapa in ( {} ) 
			group by i.fk_etapa
			order by i.fk_etapa
			""".format(
				etapas
			)
			resultado = []
			for i, x in enumerate(DBSession2.execute(preparaQuery(sql)), 1):
				resultado.append(dict(id=i, etapa=i, vendidos=0, totales=x.total))
			dEtapas = dict()
			for i, x in enumerate(etapas.split(","), 1):
				dEtapas[int(x)] = i

			sql = """
			select i.fk_etapa as etapa, 
			count(*) as vendidos from
			inmueble i
			join cuenta c
			on i.codigo = c.fk_inmueble
			where i.fk_etapa in ( {} ) 
			group by i.fk_etapa
			order by i.fk_etapa
			""".format(
				etapas
			)

			for x in DBSession2.execute(preparaQuery(sql)):
				i = dEtapas[x.etapa] - 1
				resultado[i]["vendidos"] = x.vendidos

			DBSession2.close()
			return dict(vendidosarcadias=resultado)


@resource(
	collection_path="api/lotesindividualesarcadiass",
	path="api/lotesindividualesarcadiass/{id}",
)
class LotesIndividualesArcadiaRest(QueryAndErrors, EAuth):
	def __init__(self, request, context=None):
		self.modelo = "lotesindividualesarcadia"
		self.request = request


@resource(
	collection_path="api/lotesindividualesarcadias",
	path="api/lotesindividualesarcadias/{id}",
)
class LotesIndividualesArcadiaRest(QueryAndErrors, EAuth):
	def __init__(self, request, context=None):
		self.modelo = "lotesindividualesarcadia"
		self.request = request

	def store(self, record, id=None):
		engine = Base2.metadata.bind
		poolconn = engine.connect()
		cn_sql = poolconn.connection
		self.cn_sql = cn_sql
		self.poolconn = poolconn
		sql = """
		update inmueble
		set preciopormetro = {}
		where codigo = {}
		""".format(
			record.get("preciopormetro"), id
		)
		print(sql)
		ok, error = self.commit(sql)
		# ok = True
		if not ok:
			print("hubo error")
			self.request.response.status = 400
			return self.edata_error(error)
		record["id"] = id
		try:
			poolconn.close()
		except:
			pass
		return dict(lotesindividualesarcadias=[record])

	@view(renderer="json")
	def put(self):
		print("updating inmueble arcadia")
		que, record = self.auth(self.modelo, get_token=False)
		if not que:
			return record
		id = int(self.request.matchdict["id"])
		return self.store(record=record, id=id)

	def collection_get(self):
		if True:
			que, record, token = self.auth(get_token=True)
			print("valores que, record, token", que, record, token)
			if not que:
				return dict(lotesindividualesarcadias=[])

			p = self.request.params
			etapa = int(p.get("etapa", "0"))
			tipo = p.get("tipo", "T")
			try:
				assert tipo in "TDV", "tipo invalido"
				if etapa:
					assert etapa in (1, 2, 3, 4, 5, 6), "etapa incorrecta"
			except:
				print_exc()
				return dict(lotesindividualesarcadias=[])

			etapas = [0, 8, 9, 10, 33, 34, 35]
			additionalWhere = ""
			if etapa:
				additionalWhere = " and i.fk_etapa = {}".format(etapas[etapa])
			else:
				additionalWhere = " and i.fk_etapa in ( 8,9,10,33,34,35 )"
			additionalWhere2 = ""
			if tipo == "D":
				additionalWhere2 = " and ( c.fk_inmueble is null or c.fk_inmueble = 0)"
			elif tipo == "V":
				additionalWhere2 = " and ( c.fk_inmueble > 0)"
			sql = """
			select i.iden1 as manzana, 
			i.iden2 as lote,
			i.codigo as inmueble,
			i.preciopormetro as preciopormetro,
			i.superficie as superficie
			from inmueble i 
			left join cuenta c
			on i.codigo = c.fk_inmueble
			where 1 = 1 {} {}
			order by i.iden1, i.iden2
			""".format(
				additionalWhere, additionalWhere2
			)
			print(sql)
			resultado = []

			for i, x in enumerate(DBSession2.execute(preparaQuery(sql)), 1):
				resultado.append(
					dict(
						id=x.inmueble,
						manzana=x.manzana,
						lote=x.lote,
						preciopormetro=float(f"{x.preciopormetro}"),
						inmueble=x.inmueble,
						superficie=x.superficie,
					)
				)
			DBSession2.close()
			return dict(lotesindividualesarcadias=resultado)


@resource(
	collection_path="api/lotespagadosarcadias", path="api/lotespagadosarcadias/{id}"
)
class LotesPagadosArcadiaRest(EAuth):
	def __init__(self, request, context=None):
		self.request = request

	def collection_get(self):
		que, record, token = self.auth(get_token=True)
		print("valores que, record, token", que, record, token)
		if not que:
			return dict(lotespagadosarcadias=[])
		result = []
		for x in nbb.lotes_pagados_arcadia():
			result.append(
				dict(
					id=x[0],
					etapa=x[0],
					pagados=x[1],
					noescriturados=x[2],
					escriturados=x[3],
				)
			)

		return dict(lotespagadosarcadias=result)


@resource(collection_path="api/clientesarcadias", path="api/clientesarcadias/{id}")
class ClientesArcadiaRest(EAuth):
	def __init__(self, request, context=None):
		self.modelo = "clientesarcadia"
		self.request = request

	def boolSql(self, value):
		return -1 if value else 0

	def conv_fecha(self, f):
		fecha = ""
		if f:
			fecha = "{:04d}/{:02d}/{:02d}".format(f.year, f.month, f.day)
		return fecha

	def upper(self, key):
		try:
			val = self.record[key]
		except:
			traceback.print_exc()
			val = ""
		if not val:
			return val
		try:
			decoded = val
			good = decoded.upper()
			return good
		except:
			traceback.print_exc()
			raise ZenError(1)

	def cleanSql(self, sql):
		sql = sql.replace("\n", " ")
		sql = sql.replace("\t", " ")
		return sql

	def put(self):
		que, record, token = self.auth(content="clientesarcadia", get_token=True)
		if not que:
			return dict(lotesdisponiblesarcadias=[])

		id = self.request.matchdict["id"]
		return self.store(record, id)

	def get(self):
		que, record = self.auth()
		print("entrando en vendedor")
		if not que:
			return dict(clientesarcadia=[])
		cual = int(self.request.matchdict["id"])
		ses = DBSession2
		record = dict()
		cuenta = None
		for x in ses.execute(f"select codigo as cta from cuenta where fk_cliente={cual}"):
			cuenta = x.cta
		for x in ses.execute("select * from cliente where codigo={}".format(cual)):
			print("viendo record----")
			print(x)
			# "codigo, nombre , rfc, nacionalidad , lugardenacimiento,fechadenacimiento       estadocivil     situacion     regimen  ocupacion       domicilio       colonia cp      ciudad  estado  telefonocasa    telefonotrabajo conyugenombre conyugenacionalidad      conyugelugardenacimiento        conyugefechadenacimiento        conyugerfc      conyugeocupacion       contpaq curp    conyugecurp     email"
			record = dict(
				id=x.codigo,
				nombre=x.nombre,
				rfc=x.rfc,
				curp=x.curp,
				nacionalidad=x.nacionalidad,
				lugarnacimiento=x.lugardenacimiento,
				fechanacimiento=self.conv_fecha(x.fechadenacimiento),
				estadocivil=x.estadocivil,
				situacion=x.situacion,
				regimen=x.regimen,
				ocupacion=x.ocupacion,
				domicilio=x.domicilio,
				colonia=x.colonia,
				codigopostal=x.cp,
				ciudad=x.ciudad,
				estado=x.estado,
				telefonocasa=x.telefonocasa,
				telefonotrabajo=x.telefonotrabajo,
				conyugenombre=x.conyugenombre,
				conyugecurp=x.conyugecurp,
				conyugerfc=x.conyugerfc,
				conyugefechanacimiento=self.conv_fecha(x.conyugefechadenacimiento),
				conyugelugarnacimiento=x.conyugelugardenacimiento,
				conyugenacionalidad=x.conyugenacionalidad,
				conyugeocupacion=x.conyugeocupacion,
				email=x.email,
				cuenta=cuenta
			)
		return dict(clientesarcadia=record)

	def collection_post(self):
		print("inserting Cliente")
		que, record, token = self.auth(content="clientesarcadia", get_token=True)
		user = cached_results.dicTokenUser.get(token)
		if not que:
			return record
		user = cached_results.dicTokenUser.get(token)
		usuario = user.get("usuario", "")
		perfil = user.get("perfil", "")
		if perfil not in ("admin", "comercial", "subdireccioncomercial"):

			self.request.response.status = 400
			error = "perfil no autorizado"
			return self.edata_error(error)

		return self.store(record)

	def store(self, record, codigo=None):
		print("Voy a generar el cliente")
		queries = []
		error = "Pendiente de implementar"
		ses = DBSession2

		cliente = 0
		if not codigo:
			for x in ses.execute("select max(codigo) + 1 as cliente from cliente"):
				cliente = x.cliente

		try:
			self.record = record
			for row in sorted(record.keys()):
				pass
			if not codigo:
				assert cliente, "no se obtuvo el codigo de cliente correcto"
			engine = Base2.metadata.bind
			poolconn = engine.connect()
			c = poolconn.connection
			self.poolconn = poolconn

			conyugefechanacimiento = "NULL"
			if record["conyugefechanacimiento"]:
				conyugefechanacimiento = "'{}'".format(record["conyugefechanacimiento"])

			fechanacimiento = "NULL"
			if record["fechanacimiento"]:
				fechanacimiento = "'{}'".format(record["fechanacimiento"])
			cu = c.cursor()

			if codigo:
				sql = """update cliente set nombre='{}', rfc='{}', nacionalidad='{}', lugardenacimiento='{}', 
					fechadenacimiento={}, estadocivil={}, situacion={}, regimen={}, ocupacion={},
					domicilio='{}', colonia='{}', cp='{}', ciudad='{}', estado='{}', telefonocasa='{}',
					telefonotrabajo='{}', conyugenombre='{}',conyugenacionalidad='{}',
					conyugelugardenacimiento='{}', conyugefechadenacimiento={}, conyugerfc='{}',
					conyugeocupacion='{}',curp='{}', conyugecurp='{}', email='{}'
					where codigo={}
					""".format(
					self.upper("nombre"),
					self.upper("rfc"),
					self.upper("nacionalidad"),
					self.upper("lugarnacimiento"),
					fechanacimiento,
					record["estadocivil"],
					record["situacion"],
					record["regimen"],
					record["ocupacion"],
					self.upper("domicilio"),
					self.upper("colonia"),
					record["codigopostal"],
					self.upper("ciudad"),
					self.upper("estado"),
					record["telefonocasa"],
					record["telefonotrabajo"],
					self.upper("conyugenombre"),
					self.upper("conyugenacionalidad"),
					self.upper("conyugelugarnacimiento"),
					conyugefechanacimiento,
					self.upper("conyugerfc"),
					record["conyugeocupacion"],
					self.upper("curp"),
					self.upper("conyugecurp"),
					record["email"],
					codigo,
				)
				sqlx = self.cleanSql(sql)
				# sqlx = sqlx.encode("iso-8859-1")
				print(paint.blue(sqlx))
				cu.execute(sqlx)
				c.commit()
			else:
				sql = """
				insert into cliente(codigo, nombre, rfc, nacionalidad, lugardenacimiento, 
					fechadenacimiento, estadocivil, situacion, regimen, ocupacion, domicilio, colonia, cp, 
					ciudad, estado, telefonocasa, telefonotrabajo, conyugenombre,conyugenacionalidad, 
					conyugelugardenacimiento, conyugefechadenacimiento, conyugerfc, conyugeocupacion,
					curp, conyugecurp, email) values ({}, '{}', '{}','{}','{}',
					{},'{}','{}','{}','{}','{}','{}','{}',
					'{}','{}','{}','{}','{}','{}',
					'{}',{},'{}','{}',
					'{}','{}','{}')""".format(
					cliente,
					self.upper("nombre"),
					self.upper("rfc"),
					self.upper("nacionalidad"),
					self.upper("lugarnacimiento"),
					fechanacimiento,
					record["estadocivil"],
					record["situacion"],
					record["regimen"],
					record["ocupacion"],
					self.upper("domicilio"),
					self.upper("colonia"),
					record["codigopostal"],
					self.upper("ciudad"),
					self.upper("estado"),
					record["telefonocasa"],
					record["telefonotrabajo"],
					self.upper("conyugenombre"),
					self.upper("conyugenacionalidad"),
					self.upper("conyugelugarnacimiento"),
					conyugefechanacimiento,
					self.upper("conyugerfc"),
					record["conyugeocupacion"],
					self.upper("curp"),
					self.upper("conyugecurp"),
					record["email"],
				)

				sqlx = self.cleanSql(sql)
				# sqlx = sqlx.encode("iso-8859-1")
				print(paint.blue(sqlx))
				cu.execute(sqlx)
				c.commit()
		except AssertionError as e:
			print_exc()
			error = e.args[0]
			self.request.response.status = 400
			return self.edata_error(error)
		except:
			print_exc()
			error = l_traceback()
			self.request.response.status = 400
			return self.edata_error(error)
		if not codigo:
			record["id"] = cliente
			print("el cliente es ", cliente)
		try:
			poolconn.close()
		except:
			pass
		return dict(clientesarcadias=record)


@resource(collection_path="api/vendedoresarcadias", path="api/vendedoresarcadias/{id}")
class VendedoresArcadiaRest(EAuth):
	def __init__(self, request, context=None):

		self.request = request

	def cleanSql(self, sql):
		sql = sql.replace("\n", " ")
		sql = sql.replace("\t", " ")
		return sql

	def put(self):
		que, record, token = self.auth(content="vendedoresarcadia", get_token=True)
		if not que:
			return dict(lotesdisponiblesarcadias=[])
		id = self.request.matchdict["id"]
		return self.store(record, id)

	def get(self):
		que, record = self.auth()
		print("entrando en vendedor")
		if not que:
			return dict(comisioncompartida=[])

		cual = int(self.request.matchdict["id"])
		print("vuendo el id ", cual)
		sql = """select codigo as codigo , nombre as nombre , domicilio as domicilio, colonia as colonia, cp as cp , ciudad as ciudad, estado as estado , telefono as telefono, rfc as rfc, email as email from vendedor where codigo={}
		""".format(
			cual
		)
		record = None
		for i, x in enumerate(DBSession2.execute(preparaQuery(sql)), 1):
			record = dict(
				id=x.codigo,
				codigo=x.codigo,
				nombre=x.nombre,
				domicilio=x.domicilio,
				colonia=x.colonia,
				cp=x.cp,
				ciudad=x.ciudad,
				estado=x.estado,
				telefono=x.telefono,
				rfc=x.rfc,
				email=x.email,
			)
		if record != None:
			return dict(vendedoresarcadias=record)
		else:
			return dict(vendedoresarcadias=dict())

	def collection_get(self):
		que, record, token = self.auth(get_token=True)
		print("valores que, record, token", que, record, token)
		if not que:
			return dict(lotesdisponiblesarcadias=[])
		sql = """select codigo as codigo , nombre as nombre , domicilio as domicilio, colonia as colonia, cp as cp , ciudad as ciudad, estado as estado , telefono as telefono, rfc as rfc, email as email from vendedor where activo<>0
		"""
		resultado = []
		for i, x in enumerate(DBSession2.execute(preparaQuery(sql)), 1):
			resultado.append(
				dict(
					id=x.codigo,
					codigo=x.codigo,
					nombre=x.nombre,
					domicilio=x.domicilio,
					colonia=x.colonia,
					cp=x.cp,
					ciudad=x.ciudad,
					estado=x.estado,
					telefono=x.telefono,
					rfc=x.rfc,
					email=x.email,
				)
			)
		print("cayo aqui")
		return dict(vendedoresarcadias=resultado)

	def collection_post(self):
		que, record, token = self.auth(content="vendedoresarcadia", get_token=True)
		if not que:
			return record
		return self.store(record)

	def store(self, record, codigo=None):
		ses = DBSession2
		engine = Base2.metadata.bind
		poolconn = engine.connect()
		cn_sql = poolconn.connection
		self.cn_sql = cn_sql
		self.poolconn = poolconn
		try:
			nombre = record.get("nombre", "")
			domicilio = record.get("domicilio", "")
			colonia = record.get("colonia", "")
			cp = record.get("cp", "")
			ciudad = record.get("ciudad", "")
			estado = record.get("estado", "")
			telefono = record.get("telefono", "")
			rfc = record.get("rfc", "")
			email = record.get("email", "")
			# assert user.get("perfil", "") in ("admin","comercial","subdireccioncomercial", "subdireccion", "auxiliarsubdireccion", "especialcomercial", "finanzas", "cobranza", "gestionurbana"), "perfil inadecuado"
			assert nombre, "no hay nombre"
			if codigo:
				print("si entro acaaaaa")
				sql = """update vendedor set nombre='{}', domicilio='{}', 
				colonia='{}', cp='{}', ciudad='{}', estado='{}', telefono='{}', rfc='{}',
				email='{}' where codigo={}""".format(
					nombre,
					domicilio,
					colonia,
					cp,
					ciudad,
					estado,
					telefono,
					rfc,
					email,
					codigo,
				)
				sql = self.cleanSql(sql)
				cu = cn_sql.cursor()
				cu.execute(sql)
				cn_sql.commit()
				try:
					poolconn.close()
				except:
					pass
				print("si termino")
				return dict(
					vendedoresarcadia=dict(nombre=nombre, codigo=codigo, id=codigo)
				)
			else:
				codigo = None
				for x in DBSession2.execute(
					"select max(codigo) as codigo from vendedor"
				):
					codigo = x.codigo + 1
				if codigo is not None:
					pass
				sql = """insert vendedor(codigo, nombre, activo) values ({},'{}',1)""".format(
					codigo, nombre
				)
				sql = self.cleanSql(sql)
				cu = cn_sql.cursor()
				cu.execute(sql)
				cn_sql.commit()
				print("inserting vendedor")
				try:
					poolconn.close()
				except:
					pass
				return dict(
					vendedoresarcadia=dict(nombre=nombre, codigo=codigo, id=codigo)
				)

		except AssertionError as e:
			print_exc()
			self.request.response.status = 400
			error = e.args[0]
			return self.edata_error(error)
		except:
			print_exc()
			self.request.response.status = 400
			error = "hubo error al grabar"
			return self.edata_error(error)


@resource(
	collection_path="api/lotesdisponiblesarcadias",
	path="api/lotesdisponiblesarcadias/{id}",
)
class LotesDisponiblesArcadiaRest(EAuth):
	def __init__(self, request, context=None):

		self.request = request

	def collection_get(self):
		if True:
			que, record, token = self.auth(get_token=True)
			print("valores que, record, token", que, record, token)
			if not que:
				return dict(lotesdisponiblesarcadias=[])
			sql = """
			select i.fk_etapa as etapa,
			count(*) as cuantos, 
			round(sum(i.superficie * i.preciopormetro),2) as valuacion
			from inmueble i left join cuenta c
			on  i.codigo = c.fk_inmueble
			where i.fk_etapa in (8,9,10,33,34,35) 
			and c.fk_inmueble is null 
			group by i.fk_etapa
			order by 1
			"""
			resultado = []
			tcuantos = 0
			tvaluacion = 0
			etapas = {8: 1, 9: 2, 10: 3, 33: 4, 34: 5, 35: 6}
			for i, x in enumerate(DBSession2.execute(preparaQuery(sql)), 1):
				tcuantos += x.cuantos
				tvaluacion += x.valuacion
				resultado.append(
					dict(
						id=i,
						etapa=str(etapas.get(x.etapa, 0)),
						cuantos=x.cuantos,
						valuacion=x.valuacion,
					)
				)
			resultado.append(
				dict(
					id=len(resultado) + 1,
					etapa="Total",
					cuantos=tcuantos,
					valuacion=tvaluacion,
				)
			)
			DBSession2.close()
			return dict(lotesdisponiblesarcadias=resultado)


@resource(collection_path="api/tramitesderechos", path="api/tramitesderechos/{id}")
class TramitesDerechosRest(EAuth):
	def __init__(self, request, context=None):

		self.request = request

	def collection_get(self):
		if True:
			que, record, token = self.auth(get_token=True)
			print("valores que, record, token", que, record, token)
			if not que:
				return record
		try:
			user = cached_results.dicTokenUser.get(token)

			assert user, "no hay usuario asociado a token"
			elUsuario = user.get("id", "")
			assert elUsuario, "no hay usuario de verdad"
			rdb.connect(
				cached_results.settings.get("rethinkdb.host"),
				cached_results.settings.get("rethinkdb.port"),
			).repl()
			ptable = rdb.db("iclar").table("usuarios")
			tramites = []
			for x in ptable.filter(rdb.row["appid"] == elUsuario).run():
				tramites = x.get("tramites", [])

			return dict(tramitesderechos=[dict(id=x, tramite=x) for x in tramites])
		except:
			print_exc()
			return dict(tramitesderechos=[])


@resource(collection_path="api/inmueblesfiltrados", path="api/inmueblesfiltrados/{id}")
class InmuebleFiltradoRest(EAuth):
	def __init__(self, request, context=None):

		self.request = request

	def collection_get(self):
		if True:
			que, record, token = self.auth(get_token=True)
			if not que:
				return record
		try:
			user = cached_results.dicTokenUser.get(token)
			p = self.request.params
			tramite = p.get("tramite", "")
			etapa = p.get("etapa", "")
			origen = p.get("origen", "")
			manzana = p.get("manzana", "")
			assert tramite and etapa, "tramite o etapa faltantes"
			assert origen, "no tiene origen"
			assert manzana, "no tiene manzana"
			sql1 = """
			select i.codigo as inmueble, i.iden1 as lote 
			from inmueble i
			join ( select distinct fk_inmueble as inmueble
			from tramites_ventas_movimientos  
			where fk_tramite = {} and fecha is not null ) x
			on x.inmueble = i.codigo
			where i.fk_etapa = {} and i.iden2 = '{}' order by i.iden2, i.iden1
			""".format(
				tramite, etapa, manzana
			)

			sql2 = """
			select i.codigo as inmueble, i.iden1 as lote 
			from inmueble i
			join ( select distinct m.inmueble as inmueble
			from integracion_fechas f 
			join incorporacion_maestro m on m.codigo = f.integracion 
			where f.requisito = {} and f.fecha_termino is not null) x
			on x.inmueble = i.codigo 
			where i.fk_etapa = {} and i.iden2 = '{}' order by i.iden2, i.iden1
			""".format(
				tramite, etapa, manzana
			)

			if origen == "c":
				sql = sql1
			else:
				sql = sql2

			sql = sql.replace("\t", " ")
			sql = sql.replace("\n", " ")

			lista = []
			for x in DBSession.execute(sql):
				lista.append(
					dict(
						id=x.inmueble,
						etapa=etapa,
						manzana=dec_enc(manzana, True),
						lote=dec_enc(x.lote, True),
						tramite=tramite,
					)
				)

			return dict(inmueblesfiltrados=lista)

		except:
			print_exc()
			return dict(inmueblesfiltrados=[])


@resource(collection_path="api/ofertasasignacions", path="api/ofertasasignacions/{id}")
class OfestasAsignacionRest(EAuth):
	def __init__(self, request, context=None):
		self.request = request

	def conv_fecha(self, f):
		fecha = ""
		if f:
			fecha = "{:04d}/{:02d}/{:02d}".format(f.year, f.month, f.day)
		return fecha

	def collection_get(self):
		if True:
			que, record, token = self.auth(get_token=True)
			if not que:
				return record
		try:
			user = cached_results.dicTokenUser.get(token)
			p = self.request.params
			etapa = p.get("etapa", "")
			oferta = p.get("oferta", "")

			sql = """
			select cuenta, cancelada from ofertas_compra where oferta = {} and fk_etapa = {}
			""".format(
				oferta, etapa
			)
			sess = DBSession
			for x in sess.execute(sql):
				cuenta = x.cuenta
				cancelada = x.cancelada

			assert not cancelada, "oferta esta cancelada esta cancelada"
			sql = """
			select saldo, fecha, fk_inmueble as inmueble, fk_cliente as clienteId from cuenta where
			codigo = {}
			""".format(
				cuenta
			)
			for x in sess.execute(sql):
				saldo = x.saldo
				fechaVenta = ""
				if x.fecha:
					fechaVenta = "{:04d}/{:02d}/{:02d}".format(
						x.fecha.year, x.fecha.month, x.fecha.day
					)

				inmueble = x.inmueble
				clienteId = x.clienteId

			sql = """
			select nombre from cliente where codigo = {}""".format(
				clienteId
			)
			for x in sess.execute(sql):
				nombreCliente = x.nombre.decode("iso-8859-1").encode("utf-8")
			d = dict(
				id=1,
				cuenta=cuenta,
				saldo=saldo,
				clienteId=clienteId,
				nombreCliente=nombreCliente,
				fechaVenta=fechaVenta,
				inmueble=inmueble or 0,
			)
			return dict(ofertasasignacions=[d])

		except:
			print_exc()
			return dict(ofertasasignacions=[])


@resource(collection_path="api/catalogoprecios", path="api/catalogoprecios/{id}")
class CatalogoPreciosRest(EAuth, QueryAndErrors):
	def __init__(self, request, context=None):
		self.request = request

	def get(self):
		if True:
			que, record, token = self.auth(get_token=True)
			if not que:
				return record
		try:
			user = cached_results.dicTokenUser.get(token)
			p = self.request.params
			cual = self.request.matchdict["id"]
			sql = """
			select coalesce(idpreciocatalogo,0) as idpreciocatalogo,
			preciocatalogo,
			candadoprecio,
			habilitado from inmueble
			where codigo = {}
			""".format(
				cual
			)
			hay = False
			for x in DBSession.execute(preparaQuery(sql)):
				hay = True
				precioCatalogo = float(x.preciocatalogo)
				candadoPrecio = x.candadoprecio
				habilitado = x.habilitado
				idPrecioCatalogo = x.idpreciocatalogo
			assert hay, "no hay inmueble asi"
			sql = """
				select count(*) as cuantos from cuenta where fk_inmueble = {} 
			""".format(
				cual
			)
			vendido = False
			for x in DBSession.execute(sql):
				if x.cuantos:
					vendido = True
			return dict(
				catalogoprecio=dict(
					id=cual,
					idPrecioCatalogo=idPrecioCatalogo,
					precioCatalogo=precioCatalogo,
					candadoPrecio=candadoPrecio,
					vendido=vendido,
					habilitarInmueble=habilitado,
				)
			)
		except:
			print_exc()
			return dict(catalogoprecio=dict())

	@view(renderer="json")
	def put(self):
		que, record, token = self.auth(content="catalogoprecio", get_token=True)
		if not que:
			return record
		ses = DBSession
		engine = Base.metadata.bind
		poolconn = engine.connect()
		cn_sql = poolconn.connection
		self.cn_sql = cn_sql
		self.poolconn = poolconn
		d = dict()
		user = cached_results.dicTokenUser.get(token, d)
		usuario = user.get("usuario", "")

		try:
			id = self.request.matchdict["id"]
			idPrecioCatalogo = record.get("idPrecioCatalogo")
			precioCatalogo = record.get("precioCatalogo")
			candadoPrecio = record.get("candadoPrecio")
			vendido = record.get("vendido")
			habilitarInmueble = record.get("habilitarInmueble")

			assert user.get("perfil", "") in (
				"admin",
				"recursosfinancieros",
				"auxiliarsubdireccion",
			), "perfil inadecuado"
			assert usuario, "no hay usuario"

			if True:
				if candadoPrecio:
					candado = "-1"
				else:
					candado = "0"
				if habilitarInmueble:
					habilitado = "-1"
				else:
					habilitado = "0"

				sql = """
				update inmueble 
				set idpreciocatalogo = {},
				preciocatalogo = {},
				candadoprecio = {}, 
				habilitado = {} where codigo = {}
				""".format(
					idPrecioCatalogo, precioCatalogo, candado, habilitado, id
				)

			print(sql)
			ok, error = self.commit(preparaQuery(sql))
			if not ok:
				self.request.response.status = 400
				return self.edata_error(error)
			try:
				poolconn.close()
			except:
				pass
			return dict(
				catalogoprecio=dict(
					id=id,
					idPrecioCatalogo=idPrecioCatalogo,
					precioCatalogo=precioCatalogo,
					candadoPrecio=candadoPrecio,
					vendido=vendido,
					habilitarInmueble=habilitarInmueble,
				)
			)

		except:
			print_exc()
			self.request.response.status = 400
			error = "hubo error al grabar"
			return self.edata_error(error)


@resource(
	collection_path="api/catalogotramiteporagregars",
	path="api/catalogotramiteporagregars/{id}",
)
class CatalogoTramitesPorAgregarRest(EAuth):
	def __init__(self, request, context=None):

		self.request = request

	def collection_get(self):
		if True:
			que, record, token = self.auth(get_token=True)
			if not que:
				return record
		try:
			user = cached_results.dicTokenUser.get(token)
			p = self.request.params
			inmueble = p.get("inmueble", "")
			assert inmueble, "debe tener inmueble"
			sql = """
			select 'g' as origen, codigo, descripcion, responsable
			from tramites where tipo = '2' and codigo not in 
			( select distinct requisito from integracion_fechas where integracion in 
			( select codigo from incorporacion_maestro where inmueble = {}))
			order by 2
			""".format(
				inmueble
			)
			sql = sql.replace("\t", " ")
			sql = sql.replace("\n", " ")
			lista = []
			for x in DBSession.execute(sql):
				lista.append(
					dict(
						id=x.codigo,
						origen=x.origen,
						descripcion=x.descripcion.decode("iso-8859-1").encode("utf-8"),
						responsable=x.responsable.decode("iso-8859-1").encode("utf-8"),
					)
				)
			return dict(catalogotramiteporagregars=lista)

		except:
			print_exc()
			return dict(catalogotramiteporagregars=[])


@resource(collection_path="api/catalogotramites", path="api/catalogotramites/{id}")
class CatalogoTramitesRest(EAuth):
	def __init__(self, request, context=None):
		self.request = request

	def collection_get(self):
		if True:
			que, record, token = self.auth(get_token=True)
			if not que:
				return record
		try:
			user = cached_results.dicTokenUser.get(token)
			p = self.request.params
			sql = """
			select 'c' as origen,codigo, descripcion, responsable
			from tramites_ventas
			union
			select 'g' as origen, codigo, descripcion, responsable
			from tramites where tipo = 2
			order by 2
			"""
			sql = sql.replace("\t", " ")
			sql = sql.replace("\n", " ")
			lista = []
			for x in DBSession.execute(sql):
				lista.append(
					dict(
						id=x.codigo,
						origen=x.origen,
						descripcion=x.descripcion.decode("iso-8859-1").encode("utf-8"),
						responsable=x.responsable.decode("iso-8859-1").encode("utf-8"),
					)
				)
			return dict(catalogotramites=lista)

		except:
			print_exc()
			return dict(catalogotramites=[])


@resource(collection_path="api/tramitegestions", path="api/tramitegestions/{id}")
class TramiteGestionRest(EAuth, QueryAndErrors):
	def __init__(self, request, context=None):

		self.request = request

	def collection_get(self):
		if True:
			que, record, token = self.auth(get_token=True)
			if not que:
				return record
		try:
			user = cached_results.dicTokenUser.get(token)
			p = self.request.params
			criterio = p.get("criterio", "C")
			etapa = p.get("etapa", "")
			tipo = p.get("tipo", "C").upper()
			assert tipo in "CG", "tipo invalido"
			assert etapa, "no hay etapa"
			tramites = p.get("tramites", "")
			# tramites = "{} ".format(tramites)
			assert tramites, "debe haber minimo un tramite"
			if tramites:
				for x in tramites:
					assert x in " 0123456789", "tramites invalidos"

			additional_where = ""
			additional_where2 = ""

			if tramites:
				if tipo == "C":
					additional_where = " and t.fk_tramite in ({})".format(
						tramites.replace(" ", ",")
					)
				else:
					additional_where = " and f.requisito in ({})".format(
						tramites.replace(" ", ",")
					)

			pseudo_table = """
				join
				( select integracion, institucion, requisito, 
				max(solicitud) as foo from
				integracion_fechas 
				group by integracion, institucion, requisito ) as w
				on f.integracion = w.integracion and f.institucion = w.institucion 
				and f.requisito = w.requisito and f.solicitud = w.foo"""
			pseudo_table = ""

			dicTramites = dict()
			if tipo == "C":
				tabla = "tramites_ventas"
			else:
				tabla = "tramites"

			sql = "select codigo, descripcion from {}".format(tabla)
			for x in DBSession.execute(sql):
				dicTramites[x.codigo] = x.descripcion

			resultado = []
			dicKeys = dict()
			sql = """
			select i.codigo as inmueble, i.iden2 as manzana, i.iden1  as lote
			from inmueble i where i.fk_etapa = {}""".format(
				etapa
			)
			rows = 0
			for i, x in enumerate(DBSession.execute(preparaQuery(sql)), 1):
				for tr in tramites.split(" "):
					id = rows + 1
					resultado.append(
						dict(
							id=id,
							inmueble=x.inmueble,
							manzana=dec_enc(x.manzana),
							lote=dec_enc(x.lote),
							fecha="",
							tramite=int(tr),
							descripcion=dec_enc(dicTramites[int(tr)]),
						)
					)
					dicKeys["{:08d}{:04d}".format(x.inmueble, int(tr))] = rows
					rows += 1
			if tipo == "G":

				sql = """
				select f.fecha_termino as fecha, f.requisito as tramite, 
				i.codigo as inmueble
				from integracion_fechas f join incorporacion_maestro m
				on f.integracion = m.codigo
				join tramites t on f.requisito = t.codigo 
				join inmueble i on m.inmueble = i.codigo
				{}
				where i.fk_etapa = {} {} {} and f.fecha_termino is not null
				order by i.iden2, i.iden1, f.requisito
				""".format(
					pseudo_table, etapa, additional_where, additional_where2
				)
			else:
				sql = """
				select t.fecha as fecha, t.fk_tramite as tramite,
				i.codigo as inmueble
				from tramites_ventas_movimientos t
				join tramites_ventas c on t.fk_tramite = c.codigo 
				join inmueble i on t.fk_inmueble = i.codigo
				where i.fk_etapa = {} {} {}
				order by i.iden2, i.iden1, t.fk_tramite """.format(
					etapa, additional_where, additional_where2
				)
			sql = sql.replace("\t", " ")
			sql = sql.replace("\n", " ")
			lista = []
			for i, x in enumerate(DBSession.execute(sql), 1):
				fecha = ""
				if not x.fecha is None:
					fecha = "{:04d}/{:02d}/{:02d}".format(
						x.fecha.year, x.fecha.month, x.fecha.day
					)
				if fecha:
					resultado[dicKeys["{:08d}{:04d}".format(x.inmueble, x.tramite)]][
						"fecha"
					] = fecha
			if criterio == "F":
				resultado = [x for x in resultado if x.get("fecha")]
			if criterio == "S":
				resultado = [x for x in resultado if x.get("fecha") == ""]
			return dict(tramitegestions=resultado)

		except:
			print_exc()
			return dict(tramitegestions=[])


@resource(collection_path="api/tramites", path="api/tramites/{id}")
class TramitesPorCasaRest(EAuth, QueryAndErrors):
	def __init__(self, request, context=None):

		self.request = request

	def get(self):

		if True:
			que, record, token = self.auth(get_token=True)
			if not que:
				return record
		try:
			user = cached_results.dicTokenUser.get(token)
			p = self.request.params
			cual = self.request.matchdict["id"]
			tramite = cual[:4]
			inmueble = cual[4:]
			tramite = int(tramite)
			inmueble = int(inmueble)

			sql = """
			select 'c' as origen, t.fk_tramite as tramite, t.fk_inmueble as inmueble, 
			t.montocredito as montoCredito, t.montosubsidio as montoSubsidio, 
			t.numerocredito as numeroCredito,
			t.fecha as fechaInicial, t.nota as comentario, c.descripcion as descripcion,
			null as fechaInicio,
			null as fechaRealEntrega,
			null as fechaEstEntrega,
			null as fechaVencimiento
			from tramites_ventas_movimientos t
			join tramites_ventas c on t.fk_tramite = c.codigo 
			where t.fk_inmueble = {} and t.fk_tramite = {}
			union
			select 'g' as origen, f.requisito as tramite, m.inmueble as inmueble,
			0 as montoCredito, 0 as montoSubsidio, 0 as numeroCredito,
			null as fechaInicial, '' as comentario, c.descripcion as descripcion,
			f.fecha_inicio as fechaInicio,
			f.fecha_termino as fechaRealEntrega,
			f.fecha_entrega as fechaEstEntrega,
			f.fecha_vencimiento as fechaVencimiento
			from integracion_fechas f 
			join incorporacion_maestro m on m.codigo = f.integracion 
			join tramites c on f.requisito = c.codigo
			where m.inmueble = {} and f.requisito = {}
			""".format(
				inmueble, tramite, inmueble, tramite
			)
			sql = sql.replace("\t", " ")
			sql = sql.replace("\n", " ")
			lista = []
			for x in DBSession.execute(sql):
				lista.append(
					dict(
						id=cual,
						tramite=x.tramite,
						inmueble=x.inmueble,
						montoCredito=x.montoCredito,
						montoSubsidio=x.montoSubsidio,
						numeroCredito=x.numeroCredito,
						fechaInicial=self.conv_fecha(x.fechaInicial),
						fechaInicio=self.conv_fecha(x.fechaInicio),
						fechaRealEntrega=self.conv_fecha(x.fechaRealEntrega),
						fechaEstEntrega=self.conv_fecha(x.fechaEstEntrega),
						fechaVencimiento=self.conv_fecha(x.fechaVencimiento),
						comentario=self.varchar(x.comentario),
						descripcion=dec_enc(x.descripcion),
						origen=x.origen,
					)
				)

			if len(lista) == 0:
				return dict(tramites=dict())
			return dict(tramites=lista[0])

		except:
			print_exc()
			return dict(tramite={})

	def conv_fecha(self, f):
		fecha = ""
		if f:
			fecha = "{:04d}/{:02d}/{:02d}".format(f.year, f.month, f.day)
		return fecha

	def cleanSql(self, sql):
		sql = sql.replace("\n", " ")
		sql = sql.replace("\t", " ")
		return sql

	def fecha_sql(self, fecha):
		if fecha:
			return "'{}'".format(fecha)
		else:
			return "NULL"

	def collection_get(self):
		if True:
			que, record, token = self.auth(get_token=True)
			if not que:
				return record
		try:
			user = cached_results.dicTokenUser.get(token)
			p = self.request.params
			inmueble = p.get("inmueble", "")
			assert inmueble, "no hay inmueble"
			# aqui empieza
			sql = """
			select 'c' as origen, t.fk_tramite as tramite, t.fk_inmueble as inmueble, 
			t.montocredito as montoCredito, t.montosubsidio as montoSubsidio, 
			CAST (t.numerocredito as float) as numeroCredito,
			t.fecha as fechaInicial, t.nota as comentario, c.descripcion as descripcion,
			null as fechaInicio,
			null as fechaRealEntrega,
			null as fechaEstEntrega,
			null as fechaVencimiento
			from tramites_ventas_movimientos t
			join tramites_ventas c on t.fk_tramite = c.codigo 
			where t.fk_inmueble = {} 
			union
			select 'g' as origen, f.requisito as tramite, m.inmueble as inmueble,
			CAST ('0' as float) as montoCredito, 0 as montoSubsidio, 0 as numeroCredito,
			null as fechaInicial, '' as comentario, c.descripcion as descripcion,
			f.fecha_inicio as fechaInicio,
			f.fecha_termino as fechaRealEntrega,
			f.fecha_entrega as fechaEstEntrega,
			f.fecha_vencimiento as fechaVencimiento
			from integracion_fechas f 
			join incorporacion_maestro m on m.codigo = f.integracion 
			join tramites c on f.requisito = c.codigo
			where m.inmueble = {}
			order by 2""".format(
				inmueble, inmueble
			)
			sql = sql.replace("\t", " ")
			sql = sql.replace("\n", " ")
			lista = []
			for x in DBSession.execute(sql):
				id = "{:04d}{:08d}".format(int(x.tramite), int(x.inmueble))
				lista.append(
					dict(
						id=id,
						tramite=x.tramite,
						inmueble=x.inmueble,
						montoCredito=x.montoCredito,
						montoSubsidio=x.montoSubsidio,
						numeroCredito=x.numeroCredito,
						fechaInicial=self.conv_fecha(x.fechaInicial),
						fechaInicio=self.conv_fecha(x.fechaInicio),
						fechaRealEntrega=self.conv_fecha(x.fechaRealEntrega),
						fechaEstEntrega=self.conv_fecha(x.fechaEstEntrega),
						fechaVencimiento=self.conv_fecha(x.fechaVencimiento),
						comentario=self.varchar(x.comentario),
						descripcion=dec_enc(x.descripcion),
						origen=x.origen,
					)
				)
			return dict(tramites=lista)

		except:
			print_exc()
			return dict(tramites=[])

	def varchar(self, que):
		if que is None:
			return ""
		else:
			return que.decode("iso-8859-1").encode("utf-8")

	def isnull(self, que):
		if not que:
			return "NULL"
		else:
			return que

	def collection_post(self):
		print("inserting")
		que, record, token = self.auth(content="tramite", get_token=True)
		if not que:
			return record
		ses = DBSession
		engine = Base.metadata.bind
		poolconn = engine.connect()
		cn_sql = poolconn.connection
		self.cn_sql = cn_sql
		self.poolconn = poolconn
		d = dict()
		user = cached_results.dicTokenUser.get(token, d)
		usuario = user.get("usuario", "")

		try:
			tramite = record.get("tramite")
			inmueble = record.get("inmueble")

			assert user.get("perfil", "") in (
				"admin",
				"comercial",
				"subdireccioncomercial",
				"subdireccion",
				"auxiliarsubdireccion",
				"especialcomercial",
				"finanzas",
				"cobranza",
				"gestionurbana",
			), "perfil inadecuado"
			assert usuario, "no hay usuario"
			assert tramite, "no hay tramite"
			assert inmueble, "no hay inmueble"

			sql = "select codigo from incorporacion_maestro where inmueble = {}".format(
				inmueble
			)
			integracion = ""
			for x in DBSession.execute(sql):
				integracion = x.codigo
			assert (
				integracion
			), "No hubo registro en incorporacion_maestro para el inmueble"
			if True:

				sql = """
				insert integracion_fechas(integracion,requisito, fecha_inicio, fecha_entrega, solicitud)
				values({},{},convert(varchar(10),getdate(),111),convert(varchar(10),getdate(),111),1)
				
				""".format(
					integracion, tramite
				)
			sql = self.cleanSql(sql)
			sql = sql.encode("iso-8859-1")
			cu = cn_sql.cursor()
			cu.execute(sql)
			cn_sql.commit()
			print("inserting integracion_fechas")
			try:
				poolconn.close()
			except:
				pass
			return dict(
				tramite=dict(
					id=tramite,
					tramite=tramite,
					inmueble=inmueble,
					montoCredito=0,
					montoSubsidio=0,
					numeroCredito=0,
					fechaInicial="",
					fechaInicio="",
					fechaRealEntrega="",
					fechaEstEntrega="",
					fechaVencimiento="",
					comentario="",
					descripcion="",
					origen="g",
				)
			)

		except AssertionError as e:
			print_exc()
			self.request.response.status = 400
			error = e.args[0]
			return self.edata_error(error)
		except:
			print_exc()
			self.request.response.status = 400
			error = "hubo error al grabar"
			return self.edata_error(error)

	@view(renderer="json")
	def put(self):
		que, record, token = self.auth(content="tramite", get_token=True)
		if not que:
			return record
		ses = DBSession
		engine = Base.metadata.bind
		poolconn = engine.connect()
		cn_sql = poolconn.connection
		self.cn_sql = cn_sql
		self.poolconn = poolconn
		d = dict()
		user = cached_results.dicTokenUser.get(token, d)
		usuario = user.get("usuario", "")

		try:
			cual = self.request.matchdict["id"]
			tramite = record.get("tramite")
			inmueble = record.get("inmueble")
			montoCredito = record.get("montoCredito")
			montoSubsidio = record.get("montoSubsidio")
			numeroCredito = record.get("numeroCredito")
			fechaInicial = record.get("fechaInicial")
			fechaInicio = record.get("fechaInicio")
			fechaRealEntrega = record.get("fechaRealEntrega")
			fechaEstEntrega = record.get("fechaEstEntrega")
			fechaVencimiento = record.get("fechaVencimiento")
			comentario = record.get("comentario")
			descripcion = record.get("descripcion")
			origen = record.get("origen")
			assert user.get("perfil", "") in (
				"admin",
				"comercial",
				"subdireccioncomercial",
				"subdireccion",
				"auxiliarsubdireccion",
				"especialcomercial",
				"finanzas",
				"cobranza",
				"gestionurbana",
			), "perfil inadecuado"
			assert usuario, "no hay usuario"

			if origen == "c":

				if fechaInicial:
					fecha = "'{}'".format(fechaInicial)

				else:
					fecha = "NULL"
				sql = "select coalesce(candado_alta,0) as candado_alta, coalesce(candado_baja,0) as candado_baja from tramites_ventas where codigo = {}".format(
					tramite
				)
				candado_alta = 0
				candado_baja = 0
				for x in ses.execute(sql):
					candado_alta = x.candado_alta
					candado_baja = x.candado_baja
				if candado_alta > 0:
					if candado_alta < 100:
						sql = """select count(*) as cuantos from integracion_fechas f
							join incorporacion_maestro m on f.integracion = m.codigo
							where m.inmueble = {} and f.requisito = {}
							""".format(
							inmueble, candado_alta
						)
						cuantos = 0
						for x in ses.execute(preparaQuery(sql)):
							cuantos = x.cuantos
						assert cuantos, "Falta el tramite {}".format(candado_alta)
					else:
						sql = """select count(*) as cuantos from tramites_ventas_movimientos 
							where fk_inmueble = {} and fk_tramite = {} and fecha is not null""".format(
							inmueble, candado_alta
						)
						cuantos = 0
						for x in ses.execute(preparaQuery(sql)):
							cuantos = x.cuantos
						assert cuantos, "Falta el tramite {}".format(candado_alta)
				if candado_baja > 0:
					sql = """select count(*) as cuantos from tramites_ventas_movimientos 
							where fk_inmueble = {} and fk_tramite = {} and fecha is not null""".format(
						inmueble, candado_baja
					)
					cuantos = 0
					for x in ses.execute(preparaQuery(sql)):
						cuantos = x.cuantos
					assert (
						cuantos == 0
					), "No puede quitarse fecha pues existe el tramite {}".format(
						candado_baja
					)

				sql = """
				update tramites_ventas_movimientos
				set fecha = {}, montocredito = {}, numerocredito = {}, 
				montosubsidio = {},
				nota = '{}'
				where fk_tramite = {} and fk_inmueble = {}
				""".format(
					fecha,
					self.isnull(montoCredito),
					self.isnull(numeroCredito),
					self.isnull(montoSubsidio),
					comentario,
					tramite,
					inmueble,
				)
				print(sql)

			else:

				sql = """
				update f 
				set f.fecha_inicio = {} ,
				f.fecha_termino = {},
				f.fecha_entrega = {},
				f.fecha_vencimiento = {}
				from integracion_fechas f 
				inner join incorporacion_maestro m on m.codigo = f.integracion 
				where m.inmueble = {} and f.requisito = {}
				""".format(
					self.fecha_sql(fechaInicio),
					self.fecha_sql(fechaRealEntrega),
					self.fecha_sql(fechaEstEntrega),
					self.fecha_sql(fechaVencimiento),
					inmueble,
					tramite,
				)
			sql = self.cleanSql(sql)
			sql = sql.encode("iso-8859-1")
			cu = cn_sql.cursor()
			cu.execute(sql)
			cn_sql.commit()
			if descripcion == "PORCENTAJE":
				sql = """select fk_etapa as etapa, iden2 as manzana, iden1 as lote from 
				inmueble where codigo={}""".format(
					inmueble
				)
				etapa = 0
				manzana = 0
				lote = 0
				for x in DBSession.execute(preparaQuery(sql)):
					etapa = x.etapa
					manzana = x.manzana
					lote = x.lote
					sql = """select count(*) as cuantos from gcmex_porcentajes where lote={} """.format(
						lote
					)
					cuantos = 0
				for x in DBSession.execute(preparaQuery(sql)):
					cuantos = x.cuantos
					if cuantos:
						sql = """select fk_etapa as etapa, iden2 as manzana, iden1 as lote from inmueble where codigo={}""".format(
							inmueble
						)
						etapa = 0
						manzana = 0
						lote = 0
						for x in DBSession.execute(preparaQuery(sql)):
							etapa = x.etapa
							manzana = x.manzana
							lote = x.lote
							porcentaje = 0
							if numeroCredito != "":
								porcentaje = float(numeroCredito)
								print("el valor {}".format(porcentaje))
							sql = """update gcmex_porcentajes set porcentaje = {} where lote={}""".format(
								porcentaje, lote
							)
							sql = self.cleanSql(sql)
							sql = sql.encode("iso-8859-1")
							cu.execute(sql)
						cn_sql.commit()
						print("si esta otro mas")
					else:
						sql = """select fk_etapa as etapa, iden2 as manzana, iden1 as lote from inmueble where codigo={}""".format(
							inmueble
						)
						etapa = 0
						manzana = 0
						lote = 0
						for x in DBSession.execute(preparaQuery(sql)):
							etapa = x.etapa
							manzana = x.manzana
							lote = x.lote
							porcentaje = 0
							if numeroCredito != "":
								porcentaje = float(numeroCredito)
								sql = """insert into gcmex_porcentajes (fk_etapa, manzana, lote, porcentaje) values ({},{},{},{})""".format(
									etapa, manzana, lote, porcentaje
								)
								sql = self.cleanSql(sql)
								sql = sql.encode("iso-8859-1")
								cu.execute(sql)
					cn_sql.commit()
			try:
				poolconn.close()
			except:
				pass
			return dict(
				tramite=dict(
					id=cual,
					tramite=tramite,
					inmueble=inmueble,
					montoCredito=montoCredito,
					montoSubsidio=montoSubsidio,
					numeroCredito=numeroCredito,
					fechaInicial=fechaInicial,
					fechaInicio=fechaInicio,
					fechaRealEntrega=fechaRealEntrega,
					fechaEstEntrega=fechaEstEntrega,
					fechaVencimiento=fechaVencimiento,
					comentario=comentario.encode("utf-8"),
					descripcion=descripcion,
					origen=origen,
				)
			)
		except AssertionError as e:
			print_exc()
			self.request.response.status = 400
			error = e.args[0]
			return self.edata_error(error)

		except:

			print_exc()
			self.request.response.status = 400
			error = "hubo error al grabar"
			return self.edata_error(error)

	def deleterecord(self, id, token):
		color("el id de tramite a borrar es {}".format(id))
		ses = DBSession
		engine = Base.metadata.bind
		poolconn = engine.connect()
		cn_sql = poolconn.connection
		self.cn_sql = cn_sql
		self.poolconn = poolconn
		d = dict()
		user = cached_results.dicTokenUser.get(token, d)
		usuario = user.get("usuario", "")

		try:
			cual = self.request.matchdict["id"]
			assert user.get("perfil", "") in (
				"admin",
				"comercial",
				"subdireccioncomercial",
				"subdireccion",
				"auxiliarsubdireccion",
				"gestionurbana",
			), "perfil inadecuado"
			assert usuario, "no hay usuario"
			tramite = int(cual[:4])
			inmueble = int(cual[4:])
			assert tramite, "no hay tramite"
			assert inmueble, "no hay inmueble"
			sql = """
			delete from integracion_fechas where requisito = {} and integracion 
			= ( select codigo from incorporacion_maestro where inmueble = {})
			""".format(
				tramite, inmueble
			)
			sql = self.cleanSql(sql)
			cu = cn_sql.cursor()
			cu.execute(sql)
			cn_sql.commit()

		except AssertionError as e:
			print_exc()
			self.request.response.status = 400
			error = e.args[0]
			return self.edata_error(error)

		except:

			print_exc()
			self.request.response.status = 400
			error = "hubo error al grabar"
			return self.edata_error(error)
		try:
			poolconn.close()
		except:
			pass
		return dict()

	@view(renderer="json")
	def delete(self):
		print("deleting tramite")
		que, record, token = self.auth(get_token=True)
		if not que:
			return record
		id = self.request.matchdict["id"]
		return self.deleterecord(id, token)


@resource(collection_path="api/pagares", path="api/pagares/{id}")
class GlobalesPagares(EAuth, QueryAndErrors):
	def __init__(self, request, context=None):
		self.request = request

		def cual(que):
			return len(que) + 1

		g = []
		# CuentaPagare.all_alterno()
		hoy = datetime.now().isoformat()

		try:
			# g.append( dict( id = cual(g) , descripcion = hoy, importe = 0, importeformateado = "", importeletras = ""))
			formateo = "{:,.2f}"
			saldo = CuentaPagare.saldoglobal()
			g.append(
				dict(
					id=cual(g),
					descripcion="Saldo Pagare",
					importe=saldo,
					importeformateado=formateo.format(saldo),
					importeletras=aletras(saldo),
				)
			)
			saldo = Documento.saldoglobal()
			g.append(
				dict(
					id=cual(g),
					descripcion="Saldo en Documentos tipo 17",
					importe=saldo,
					importeformateado=formateo.format(saldo),
					importeletras=aletras(saldo),
				)
			)
			deudores = Documento.deudores()
			g.append(
				dict(
					id=cual(g),
					descripcion="Clientes con pagares con saldo",
					importe=deudores,
					importeformateado=str(deudores),
					importeletras=aletras(deudores, tipo="numero"),
				)
			)
			consaldo = Cuenta.consaldo(0)
			g.append(
				dict(
					id=cual(g),
					descripcion="Clientes con saldo",
					importe=consaldo,
					importeformateado=str(consaldo),
					importeletras=aletras(consaldo, tipo="numero"),
				)
			)
			consaldo = Cuenta.consaldo(100000)
			g.append(
				dict(
					id=cual(g),
					descripcion="Clientes con saldo > 100000",
					importe=consaldo,
					importeformateado=str(consaldo),
					importeletras=aletras(consaldo, tipo="numero"),
				)
			)
			sumasaldos = Cuenta.sumasaldos()
			g.append(
				dict(
					id=cual(g),
					descripcion="Suma de saldos",
					importe=sumasaldos,
					importeformateado=formateo.format(sumasaldos),
					importeletras=aletras(sumasaldos, tipo="numero"),
				)
			)
			sumasaldos = Cuenta.sumasaldos(cualquiera=False, sinasignar=True)
			g.append(
				dict(
					id=cual(g),
					descripcion="Suma de saldos de clientes sin vivienda",
					importe=sumasaldos,
					importeformateado=formateo.format(sumasaldos),
					importeletras=aletras(sumasaldos, tipo="numero"),
				)
			)
			vencido = DocumentoPagare.vencido_a(1)
			total = vencido
			g.append(
				dict(
					id=cual(g),
					descripcion="Cartera Vencida",
					importe=total,
					importeformateado=formateo.format(vencido),
					importeletras=aletras(vencido),
				)
			)
			vencido = DocumentoPagare.vencido_a(30)
			vencidomenosde30 = total - vencido
			vencido30omas = vencido
			vencido60omas = DocumentoPagare.vencido_a(60)
			vencido30a59 = vencido30omas - vencido60omas
			vencido90omas = DocumentoPagare.vencido_a(90)
			vencido60a89 = vencido60omas - vencido90omas
			vencido = vencido90omas
			g.append(
				dict(
					id=cual(g),
					descripcion="Cartera Vencida menos de 30 dias",
					importe=vencidomenosde30,
					importeformateado=formateo.format(vencidomenosde30),
					importeletras=aletras(vencidomenosde30),
				)
			)
			g.append(
				dict(
					id=cual(g),
					descripcion="Cartera Vencida 30 a 59 dias",
					importe=vencido30a59,
					importeformateado=formateo.format(vencido30a59),
					importeletras=aletras(vencido30a59),
				)
			)
			g.append(
				dict(
					id=cual(g),
					descripcion="Cartera Vencida 60 a 89 dias",
					importe=vencido60a89,
					importeformateado=formateo.format(vencido60a89),
					importeletras=aletras(vencido60a89),
				)
			)
			g.append(
				dict(
					id=cual(g),
					descripcion="Cartera Vencida 90 o mas dias",
					importe=vencido,
					importeformateado=formateo.format(vencido),
					importeletras=aletras(vencido),
				)
			)
			morosos = DocumentoPagare.morosos()
			g.append(
				dict(
					id=cual(g),
					descripcion="Clientes con pagares vencidos",
					importe=morosos,
					importeformateado=str(morosos),
					importeletras=aletras(morosos, tipo="numero"),
				)
			)
			abonado = DocumentoPagare.abonado()
			g.append(
				dict(
					id=cual(g),
					descripcion="Abonado a Pagares",
					importe=abonado,
					importeformateado=formateo.format(abonado),
					importeletras=aletras(abonado),
				)
			)
			self.pagares = g
		except:
			print_exc()
			self.pagares = []

	def collection_get(self):
		if True:
			que, record, token = self.auth(get_token=True)
			if not que:
				return record
		return {"pagares": self.pagares}


@resource(collection_path="api/zenvendedors", path="api/zenvendedors/{id}")
class ZenVendedors(EAuth):
	def __init__(self, request, context=None):
		self.request = request

	def collection_get(self):
		if True:
			que, record, token = self.auth(get_token=True)
			if not que:
				return record
		try:
			sql = """
			select nombre as nombre, codigo as codigo form vendedor v where not exists 
			(select * from gixanip g where v.codigo = g.fkvendedor)
			and v.desactivado = 0 order by v.codigo desc

			"""
		except:
			return dict(resultado=[])


@resource(collection_path="api/printers", path="api/printers/{id}")
class PrinterRest(EAuth):
	def __init__(self, request, context=None):
		print("entrando a la clase PrinterRest")
		self.request = request

	def collection_get(self):
		if True:
			que, record, token = self.auth(get_token=True)
			if not que:
				return record
		try:
			user = cached_results.dicTokenUser.get(token)
			print("resolviendo printers... para", user)
			assert user, "no hay usuario asociado a token"
			printers = printers_info(user)
			print(paint.red(printers))
			error = ""
			if isinstance(printers, dict):
				error = printers.get("error", "")
			print(printers)
			if error:
				print(paint.red("hubo error al llamar printers_info"))
				raise ZenError(1)
			return dict(printers=printers)
		except:
			print_exc()
			return dict(printers=[])

	@view(renderer="json")
	def get(self):
		if True:
			que, record, token = self.auth(get_token=True)
			if not que:
				return record
		try:
			user = cached_results.dicTokenUser.get(token)
			print("resolviendo printers... para", user)
			assert user, "no hay usuario asociado a token"

			cual = self.request.matchdict["id"]
			print("cual es", cual)
			return dict(printer=printers_info(user, cual).get("printers", "")[0])
		except:
			print_exc()
			return dict()


def resumen(y=None, m=None, d=None):
	c = dict()
	rdb.connect(
		cached_results.settings.get("rethinkdb.host"),
		cached_results.settings.get("rethinkdb.port"),
	).repl()
	try:
		if y:
			f = datetime(year=y, month=m, day=d, hour=19, minute=30).isoformat()

			foo = (
				rdb.db("iclar")
				.table("historia_resumen")
				.filter(rdb.row["fecha"] > f)
				.order_by("fecha")
				.limit(1)
				.run()
			)
		else:
			foo = rdb.db("iclar").table("resumen_reciente").run()
		for x in foo:
			c = x
	except:
		print_exc()

	return c


@view_config(route_name="ropiclar", renderer="json", request_method="GET")
def resumenoperativo(request):
	y = 0
	m = 0
	d = 0
	r = request.params
	try:
		y = int(r["y"])
		m = int(r["m"])
		d = int(r["d"])
	except:
		pass
	if y and m and d:
		return resumen(y, m, d)
	elixir = r.get("elixir", "")
	breve = r.get("breve", "")
	etapa = r.get("etapa", "")
	if elixir:
		if breve:
			return resumen_elixir_breve(etapa)
		else:
			return resumen_elixir()
	return resumen()


def resumen_elixir():
	r = enbb.resumenoperativo()
	# print "desplegando resumen elixir"
	# print r
	result = r.get("response")
	result["fecha"] = today()
	return result


def resumen_elixir_breve(etapa=None):
	r = enbb.resumenoperativo()
	result = r.get("response")
	# result["fecha"] = today()
	d = []
	kv = result.get("kvalores2")
	v = result.get("valores")
	for i, x in enumerate(kv, 1):
		acum = 0
		rubro = v.get(str(x))
		for y in rubro:
			if y not in ("-1", "0"):
				if etapa:
					if y == str(etapa):
						acum += rubro[y]
				else:
					acum += rubro[y]
		d.append(dict(id=i, rubro=rubro["-1"], total=acum, req=str(x)))

	return dict(result=d)


def resumen_operativo_iclar(request):
	pass


@view_config(route_name="printtest", renderer="json", request_method="GET")
def print_test(request):
	printer = request.params.get("printer", "")
	if not printer:
		return dict(printed="0")
	file_to_print = "solfea.pdf"
	conf_file = "./zen/conf.json"
	params = json.load(open(conf_file))

	print_pdf(file_to_print, params.get(printer))
	return dict(printed="1")


@view_config(route_name="printtest2", renderer="json", request_method="GET")
def print_test2(request):
	printer = request.params.get("printer", "")
	tipo = request.params.get("tipo", "caracteristicas")
	oferta = int(request.params.get("oferta", 0))
	etapa = int(request.params.get("etapa", 0))
	cliente = int(request.params.get("cliente", 0))
	if not printer:
		return dict(printed="0")
	try:
		func_name = "obtener{}{}".format(tipo[0].upper(), tipo[1:])
		if tipo in ("caracteristicas oferta anexo".split(" ")):
			datos = eval(func_name)(etapa, oferta)
		elif tipo in (["rap"]):
			datos = eval(func_name)(cliente)
		elif tipo in (["otro"]):
			datos = eval(func_name)()
		else:
			return dict()
		template = "{}.html".format(tipo)
		ok, nombre = pdfCreate(datos=datos, template=template, tipo=tipo)
		conf_file = "./zen/conf.json"
		params = json.load(open(conf_file))

		print_pdf(nombre, params.get(printer))
		return dict(ok=nombre, printed="1")
	except:
		print_exc()

	return dict()


def log_error_to_rdb(funcion="", topico=""):
	t_error = l_traceback()
	rdb.connect(
		cached_results.settings.get("rethinkdb.host"),
		cached_results.settings.get("rethinkdb.port"),
	).repl()
	ptable = rdb.db("printing").table("errors")
	ptable.insert(
		dict(
			error=t_error,
			date=datetime.now().isoformat(),
			topico=topico,
			funcion=funcion,
		)
	).run()


def amount_and_cents_with_commas(v, name="(Unknown name)", md={}):
	try:
		v = "%.2f" % v
	except:
		v = ""
	return thousands_commas(v)

def contrato_template(cuenta):
	path = os.getcwd()
	src_image=f"{path}/zen/static/logopinares.jpg"
	contrato = "contrato"
	razonsocial = "razon soc"
	representantelegal = "replec"
	nombrecliente = "nombre cliente"
	letra = "letra"
	modulo = "modulo"
	desarrollo = "desarrollo"
	dciudad = "ciudad"
	destado = "estado"
	superficie = "superfiecie"
	titulo1 = "titulo1"
	lindero1 = "lindero1"
	titulo2 = "titulo2"
	lindero2 = "lindero2"
	titulo3 = "titulo3"
	lindero3 = "lindero3"
	titulo4 = "titulo4"
	lindero4 = "lindero4"
	rfccliente = "rfccliente"
	totalapagarq = "totalapagarq"
	totalapagarl = "totalapagarl"
	acredito = False
	c2p1 = ""
	sabe = "%"
	sabe2 = "%"
	eciudad = "eciudad"
	eestado = "eestado"
	edomicilio = "edomicilio"
	domiciliocliente = "domiciliocliente"
	ciudadcliente = "ciudadcliente"
	estadocliente = "estadocliente"
	fechadia = "fechadia"
	fechames = "fechames"
	fechaano = "fechano"
	razonsocial = "razonsocial"
	representantelegal = "representantelegal"
	nombrecliente = "nombrecliente"
	nombrevendedor = "nombrevendedor"
	engancheq = "engancheq"
	enganchel = "enganchel"
	restoq = "restoq"
	restol = "restol"
	plazomeses = ""

	# cu.execute("select razonsocial, representantelegal, ciudad, estado, domicilio, colonia from empresa where codigo = 1")
	# row = fetchone(cu)
	ses = DBSession2
	for x in ses.execute(
		"select razonsocial, representantelegal, ciudad, estado, domicilio, colonia from empresa where codigo = 1"
	):
		razonsocial = x.razonsocial
		representantelegal = x.representantelegal
		eciudad = x.ciudad
		eestado = x.estado
		edomicilio = x.domicilio
		if x.colonia:
			edomicilio += " Col. " + x.colonia

	# query = f"""
	# 	select count(*) as cuantos from gixamortizaciondetalle
	# 	where fkamortizacion = {258} and eliminado = 0
	#
	# 	"""
	# este es el que estaba en gix
	amortizacion = None
	for x in ses.execute(f"select pkamortizacion as pk from gixamortizacion where cuenta={cuenta}"):
		amortizacion=x.pk
	query = f"""
		select convert(varchar(10), fechadepago, 103) from gixamortizaciondetalle
		where fkamortizacion = {amortizacion} and eliminado = 0 order by fechadepago
		"""

	sql = (query.replace("\n", " ")).replace("\t", " ")
	rows = []
	for x in ses.execute(sql):
		rows.append(x)

	if rows:
		acredito = True
		print("entro en rows")
		# if True validacion 0 es igual a credito not self.GetControl(ID_CHOICEAMORFUNC1FORMADEPAGO).GetSelection() == 0:
		# return "", 0
		plazotabla = len(rows)
		di, mi, ai = str(rows[0][0]).split("/")
		df, mf, af = str(rows[len(rows) - 1][0]).split("/")
		query = f"""
		select sum(pagofijo) as suma from gixamortizaciondetalle where fkamortizacion = {258} and eliminado = 0
		"""
		sql = (query.replace("\n", " ")).replace("\t", " ")
		for x in ses.execute(sql):
			totaltabla = x.suma

	else:
		# validacion 1 es igual a contado if not self.GetControl(ID_CHOICEAMORFUNC1FORMADEPAGO).GetSelection() == 1:
		# return "", 0

		plazotabla, totaltabla = 0, 0
		di, mi, ai, df, mf, af = 0, 0, 0, 0, 0, 0

	
	#todavia no se bien como funciona este query
	query = f"""
	select count(*) as cuantos from gixamortizaciondetalle
	where fkamortizacion = {amortizacion} and eliminado = 0 and insertado <> 0
	"""
	sql = (query.replace("\n", " ")).replace("\t", " ")
	cuantos = None
	for x in ses.execute(sql):
		cuantos = x.cuantos
	pagoextra = False
	if cuantos is not None:
		if int(cuantos) > 0:
			pagoextra = True
		else:
			plazotabla, totaltabla = 0, 0
	# cada linea los campos que hay del query de abajo
	# 0-3
	# 4-5
	# 6
	# 7-14
	# 15
	# 16-20
	# 21-24
	query = f"""
	select rtrim(ltrim(i.iden1)) as iden1, rtrim(ltrim(i.iden2)) as iden2, a.enganchec as enganchec, i.superficie as superficie,
	a.saldoafinanciar as saldoafinanciar, i.preciopormetro as preciopormetro,
	case a.plazomeses when 0 then (a.saldoafinanciar + a.enganchec) else ((a.pagomensualfijo * a.plazomeses) + a.enganchec) end as totalapagarq,
	i.titulo1 as titulo1, i.lindero1 as lindero1, i.titulo2 as titulo2, i.lindero2 as lindero2, i.titulo3 as titulo3, i.lindero3 as lindero3, i.titulo4 as titulo4, i.lindero4 as lindero4,
	case a.plazomeses when 0 then a.saldoafinanciar else (a.pagomensualfijo * a.plazomeses) end as restoq,
	a.plazomeses as plazomeses, a.pagomensualfijo as pagomensualfijo, convert(varchar(10), a.fechaelaboracion, 103) as fechaelaboracion, a.fkcliente as cliente, a.fkvendedor as vendedor,
	a.contrato as contrato, a.cuenta, convert(varchar(10), a.fechaprimerpago, 103), convert(varchar(10), a.fechaenganche, 103) as fechaenganche
	from gixamortizacion a
	join INMUEBLE i on a.fkinmueble = i.codigo
	where a.pkamortizacion = {amortizacion}
	"""
	sql = (query.replace("\n", " ")).replace("\t", " ")
	general = None
	for x in ses.execute(sql):
		general = dict(
			iden1=x.iden1,
			iden2=x.iden2,
			enganchec=x.enganchec,
			superficie=x.superficie,
			saldoafinanciar=x.saldoafinanciar,
			preciopormetro=x.preciopormetro,
			vendedor=x.vendedor,
			cliente=x.cliente,
			contrato=x.contrato,
			totalapagarq=x.totalapagarq,
			restoq=x.restoq,
			plazomeses=x.plazomeses,
			pagomensualfijo=x.pagomensualfijo,
			fechaelaboracion=x.fechaelaboracion,
			titulo1=x.titulo1,
			lindero1=x.lindero1,
			titulo2=x.titulo2,
			lindero2=x.lindero2,
			titulo3=x.titulo3,
			lindero3=x.lindero3,
			titulo4=x.titulo4,
			lindero4=x.lindero4,
			fechaenganche= x.fechaenganche
		)
		print("viendo general")
		print(general)
	if general["cliente"]:
		sql = "select nombre, domicilio, colonia, ciudad, estado, rfc from cliente where codigo = {}".format(
			general["cliente"]
		)
		for x in ses.execute(sql):
			nombrecliente = x.nombre
			domiciliocliente = x.domicilio
			if x.colonia:
				domiciliocliente += " Col. " + x.colonia
			ciudadcliente = x.ciudad
			estadocliente = x.estado
			rfccliente = x.rfc

		sql = f"select nombre as nombre from vendedor where codigo = {general['vendedor']}"
		for x in ses.execute(sql):
			nombrevendedor = x.nombre

		for x in ses.execute(
			"select contrato, descripcion, ciudad, estado from desarrollo where codigo = 5"
		):
			contrato = x.contrato
			desarrollo = x.descripcion
			dciudad = x.ciudad
			destado = x.estado

		# esto no lo vamos a cenesitar, si existia el contrato lo regresa leyendolo de disco duro para no segur con la operacion
		# if int(row[21]) > 0:
		# 	contrato = int(row[21])
		# 	if int(row[22]) > 0:
		# 		archivo = "Contrato%s_%s.pdf" % (contrato, self.pkamortizacion)
		# 		try:
		# 			f = open(archivo, 'r')
		# 			f.close()
		# 			return "", contrato
		# 		except:
		# 			pass

		# esto tamien lo vamos a quitar ya que aqui trata de generar un contrato nuevo aqui solo vamos a leer contratos
		# else:
		# 	sql = "update desarrollo set contrato = %s where codigo = 5" % (contrato + 1)
		# 	todook, trash = self.QueryUpdateRecord(sql, conexion = r_cngcmex)
		# 	if not todook:
		# 		Mensajes().Info(self, u"� Problemas al actualizar el contrato (1) !", u"Atenci�n")
		# 		return ""

		# 	sql = "update gixamortizacion set contrato = %s where pkamortizacion = %s" % (contrato, self.pkamortizacion)
		# 	todook, trash = self.QueryUpdateRecord(sql, conexion = r_cngcmex)
		# 	if not todook:
		# 		Mensajes().Info(self, u"� Problemas al actualizar el contrato (2) !", u"Atenci�n")
		# 		return ""

		# 	self.DisplayContrato(contrato)

		mes = (
			"",
			"Ene",
			"Feb",
			"Mar",
			"Abr",
			"May",
			"Jun",
			"Jul",
			"Ago",
			"Sep",
			"Oct",
			"Nov",
			"Dic",
		)
		meses = (
			"",
			"Enero",
			"Febrero",
			"Marzo",
			"Abril",
			"Mayo",
			"Junio",
			"Julio",
			"Agosto",
			"Septiembre",
			"Octubre",
			"Noviembre",
			"Diciembre",
		)
		letra = general["iden1"]
		modulo = general["iden2"]
		engancheq = formato_comas.format(general["enganchec"])
		enganchel = aletras(float(general["enganchec"]), tipo="pesos")
		superficie = formato_comas.format(float(general["superficie"]))
		saldoafinanciar = formato_comas.format(float(general["saldoafinanciar"]))
		preciom2 = formato_comas.format(float(general["preciopormetro"]))
		if totaltabla > 0:
			totalapagarq = formato_comas.format(
				(float(totaltabla) + float(general["enganchec"]))
			)
			totalapagarl = aletras(
				(float(totaltabla) + float(general["enganchec"])), tipo="pesos"
			)
			restoq = formato_comas.format(float(totaltabla))
			restol = aletras(float(totaltabla), tipo="pesos")
		else:
			totalapagarq = formato_comas.format((float(general["totalapagarq"])))
			totalapagarl = aletras(float(general["totalapagarq"]), tipo="pesos")
			restoq = formato_comas.format(float(general["restoq"]))
			restol = aletras(float(general["restoq"]), tipo="pesos")

		titulo1 = general["titulo1"]
		lindero1 = general["lindero1"].replace("centÃ\xadmetros", "centímetros")
		titulo2 = general["titulo2"]
		lindero2 = general["lindero2"].replace("centÃ\xadmetros", "centímetros")
		titulo3 = general["titulo3"]
		lindero3 = general["lindero3"].replace("centÃ\xadmetros", "centímetros")
		titulo4 = general["titulo4"]
		lindero4 = general["lindero4"].replace("centÃ\xadmetros", "centímetros")
		profeco = "Número de Autorización de la Profeco: PFC.B.E.7/007544-2015"

		if plazotabla > 0:
			plazomeses = plazotabla
		else:
			plazomeses = int(general["plazomeses"])

		pagomensualq = formato_comas.format(float(general["pagomensualfijo"]))
		pagomensuall = aletras(float(general["pagomensualfijo"]), tipo="pesos")
		fechadia, fechames, fechaano = str(general["fechaelaboracion"]).split("/")

		# aqui empeiza validacion de si es a credito hace lo del if
		if True:
			if pagoextra:
				c2p1 = f"""
				<div style="text-align: justify;"><br>1.- La cantidad de ${engancheq},
				({enganchel}), que manifiesta "LA PROMITENTE VENDEDORA" recibir en
				este acto a su entera satisfacción, sirviendo el presente contrato de
				formal recibo por la entrega de dicha cantidad.<br>
				</div>
				<div style="text-align: justify;"><br>2.- El resto de la contraprestación o
				sea la cantidad de ${restoq}, ({restol}) la deberá pagar
				"EL(LOS) PROMITENTE(S) COMPRADOR(ES)" mediante {plazomeses} amortizaciones
				de la siguiente forma:<br><br>
				</div>
				"""
		tableheader=""		
		# tableheader = """
		# <div style="text-align: right;">
		# <span style="font-weight: bold; font-style: italic;">
		# No. de Pago&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;
		# Fecha de Pago&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;
		# Saldo Inicial&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;
		# Pago&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;
		# Saldo Final&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;
		# </span></div>
		# """
		c2p1 += tableheader
	if acredito:
		c2p1 = f"""
				<div style="text-align: justify;"><br>1.- La cantidad de ${engancheq},
				({enganchel}), manifiesta "LA PROMITENTE VENDEDORA" quien la recibe en
				este acto a su entera satisfacción, sirviendo el presente contrato de
				formal recibo por la entrega de dicha cantidad.<br>
				</div>
				<div style="text-align: justify;"><br>2.- El resto de la contraprestación o
				sea la cantidad de ${restoq}, ({restol}) la deberá(n) pagar
				"EL(LOS) PROMITENTE(S) COMPRADOR(ES)" mediante {plazomeses} amortizaciones
				mensuales, consecutivas sin intereses del día {di} de
				{meses[int(mi)]} de {ai} al día {df} de {meses[int(mf)]} de
				{af}, cada una por la cantidad de ${pagomensualq},
				({pagomensuall}).<br>
				</div>
				<div style="text-align: justify;"><br>3.- Las amortizaciones mensuales a
				que se refiere el punto anterior, se documentan mediante pagaré(s) que en 
				este acto suscribe "EL(LOS) PROMITENTE(S)
				COMPRADOR(ES)" quien(es) esté(n) de acuerdo en que dicho(s) título(s)
				de crédito sea(n) descontado(s) con terceras personas físicas o morales
				a elección de "LA PROMITENTE VENDEDORA".<br>
				</div>
				<div style="text-align: justify;"><br>4.- En caso de que "EL(LOS)
				PROMITENTE(S) COMPRADOR(ES)" incurra(n) en mora en el pago de las
				amortizaciones se causarán intereses moratorios por todo el tiempo que
				se mantenga insoluto dicho pago a una tasa del 25 % anual.<br>
				</div>
				
				<div style="text-align: justify;">
				Si "LA PROMITENTE VENDEDORA" incurre
				en gastos judiciales o extrajudiciales para realizar la cobranza de los
				pagos vencidos en su caso, "EL(LOS) PROMITENTE(S) COMPRADOR(ES)"
				estará(n) obligado(s) a reembolsarle éstos gastos a "LA PROMITENTE
				VENDEDORA".<br><br><br>
				</div>
				<div style="text-align:justify;><span style="font-weight: bold;">TERCERA.- INCUMPLIMIENTO.<br></div>
				<div style="text-align: justify;">La falta de pago puntual de
				dos de las amortizaciones mensuales, se considerará como incumplimiento
				por parte de "EL(LOS) PROMITENTE(S) COMPRADOR(ES)" al presente
				contrato, por lo que "LA PROMITENTE VENDEDORA" tendrá la opción de
				rescindirlo en los términos que se establecen en la
				cláusula séptima del mismo.<br>
				</div>
				<div style="text-align: justify;"><br>"EL(LOS) PROMITENTE(S)
				COMPRADOR(ES)", se obliga(n) a contribuir con los gastos generales, que
				se originen en la conservación y buen funcionamiento de las áreas de
				uso común, en la proporción que le corresponda, tal como lo establece
				el reglamento del desarrollo. Dicha obligación la adquiere a partir de
				la firma del presente documento. La falta de pago puntual de por lo
				menos dos de los pagos mensuales que anteriormente se detallan, se
				considerará como incumplimiento por parte de "EL(LOS) PROMITENTE(S)
				COMPRADOR(ES)", y por lo tanto, "LA PROMITENTE VENDEDORA", tendrá la
				opción de rescindir el presente contrato en los términos que se
				establecen en la cláusula séptima de este contrato.<br><br>
				</div>
				<div style="text-align:justify;><span style="font-weight: bold;">CUARTA.- PRECIO PACTADO.<br></div>
				<div style="text-align: justify;">Las partes manifiestan que
				el precio pactado en esta operación, es justo y válido, por lo tanto en
				este contrato no existe error, ni enriquecimiento ilegítimo de alguna
				de las partes.<br>
				</div>
				"""
	else:
		d, m, a = general["fechaenganche"].split("/")
		fechaenganche = f"""{int(d)} de {meses[int(m)]} de {int(a)}"""
		c2p1 = f"""
				<div><h1></div>
				<br>
				<div style="text-align: justify;"><br>1.- La cantidad de ${engancheq},
				({enganchel}), la deberá pagar "EL(LOS) PROMITENTE(S) COMPRADOR(ES)"
				mediante un pago único el día {fechaenganche}.<br>
				</div>

				<div style="text-align: justify;"><br>TERCERA.- "EL(LOS) PROMITENTE(S)
				COMPRADOR(ES)", se obliga(n) a contribuir con los gastos generales, que
				se originen en la conservación y buen funcionamiento de las áreas de
				uso común, en la proporción que le corresponda, tal como lo establece
				el reglamento del desarrollo. Dicha obligación la adquiere a partir de
				la firma del presente documento. La falta de pago puntual de por lo
				menos dos de los pagos mensuales que anteriormente se detallan, se
				considerará como incumplimiento por parte de "EL(LOS) PROMITENTE(S)
				COMPRADOR(ES)", y por lo tanto, "LA PROMITENTE VENDEDORA", tendrá la
				opción de rescindir el presente contrato en los términos que se
				establecen en la cláusula séptima de este contrato.<br><br>
				</div>

				<div style="text-align: justify;"><br>CUARTA.- Las partes manifiestan que
				el precio pactado en esta operación, es justo y válido, por lo tanto en
				este contrato no existe error, ni enriquecimiento ilegítimo de alguna
				de las partes.<br>
				</div>
				"""
	template = f"""<html>
		<head>
			<meta name="pdfkit-page-size" content="letter"/>
			<meta charset="utf-8">
		</head>
		<body>
		<table style="width:100%">
			<tbody>
				<tr style="width:100%">
					<td style="width:50%;">
					<img style="width:283px; height:259px;" src='{src_image}' />
					</td>
					<td style="text-align: right; width:50%;">
					</td>
				</tr>
			</tbody>
		</table>

		<div style="text-align: right;">
			NUMERO&nbsp; {general["contrato"]} A<br>
			</div>
			<div style="text-align: justify;">
			CONTRATO DE PROMESA DE COMPRA VENTA
			QUE CELEBRAN POR UNA PARTE {razonsocial} REPRESENTADA EN ESTE ACTO POR EL SEÑOR
			{representantelegal}, A QUIEN EN LO SUCESIVO SE LE
			DENOMINARÁ "LA PROMITENTE VENDEDORA", Y POR OTRA PARTE, EL(LOS) SEÑOR(ES)
			{nombrecliente}, POR SU PROPIO DERECHO, A QUIEN(ES) EN LO SUCESIVO SE LE(S) DENOMINARÁ "EL(LOS) PROMITENTE(S)
			COMPRADOR(ES)", A AMBOS EN SU CONJUNTO SE LES DENOMINARÁ "LAS PARTES",
			EL CUAL SUJETAN AL CONTENIDO DE LAS SIGUIENTES DECLARACIONES Y CLÁUSULAS:
			<br>

			</div>
			<div style="text-align: center;"><span style="font-weight: bold;"><br>DECLARACIONES:</span><br>
			</div>
			<br>

			I.- Declara el representante de "LA PROMITENTE VENDEDORA", por conducto de su representante:<br>
			<div style="text-align: justify;">a) Que su representada es una sociedad
			mercantil legalmente constituida mediante escritura pública número
			43,065, otorgada el día 16 de agosto de 1991, ante la fé del Licenciado
			Felipe Ignacio Vázquez Aldana Sauza, Notario Público Suplente Adscrito
			y Asociado número 2 de Tlaquepaque, Jalisco, la cual se registró bajo
			inscripción 311-312 del tomo 410 del Libro Primero del Registro Público
			de Comercio de Guadalajara, Jalisco.<br>
			</div>

			<div style="text-align: justify;"><br>b)Que su representante cuenta con las facultades
			juridicas necesarias para contratar y obligarla en los terminos de este contrato,
			manifestando bajo protesta de decir verdad, que dichas facultades no le han sido revocadas,
			limitadas, o modificadas en forma alguna.<br>
			</div>

			<div style="text-align: justify;"><br>c) Que su representada se encuentra inscrita
			en el Registro Federal de Contribuyentes bajo la Clave: APR910816FJ3.<br>
			</div>

			<div style="text-align: justify;"><br>d) Que tiene interés en vender a "EL(LOS) PROMITENTE(S) COMPRADOR(ES)", 
			el inmueble que acontinuación se describe:<br>
			</div>

			<div style="text-align: justify;"><br>Lote marcado con la Letra {letra} del
			Módulo {modulo}, perteneciente al Desarrollo Campestre Recreativo
			conocido como "{desarrollo}", ubicado en el municipio de {dciudad},
			{destado}, dicho inmueble tiene una Superficie de {superficie} m2.
			y las siguientes medidas y linderos:<br>
			</div>

			<br>
			{titulo1} :&nbsp;&nbsp;&nbsp; {lindero1}<br>
			{titulo2} :&nbsp;&nbsp;&nbsp; {lindero2}<br>
			{titulo3} :&nbsp;&nbsp;&nbsp; {lindero3}<br>
			{titulo4} :&nbsp;&nbsp;&nbsp; {lindero4}<br>
			(en lo sucesivo "EL INMUEBLE").<br>
			<div style="text-align: justify;">
			<br>
			e) Que "EL INMUEBLE" se encuentra libre de todo gravamen, limitacion
			de dominio y de cualquier responsabilidad, al corriente en el pago del impuesto predial y demas
			contribuciones que le corresponden, así como de los servicios con que cuentan.<br>
			</div>

			<div style="text-align: justify;"><br>f) Que ha ofrecido en venta "EL INMUEBLE", y que "EL(LOS)
			PROMITENTE(S) COMPRADOR(ES)" ha(n) tomado y aceptado en todos sus términos, la oferta realizada
			de conformidad con los dispuesto en el presente Contrato.
			<br></div>

			
			<div style="text-align: justify;"><br>II.- Declara "El(LOS) RPOMINENTE(S) COMPRADOR(ES)":
			</div>
			<div style="text-align:justify;">a) Ser persona(s), física(s), de nacionalidad mexicana, mayor(es) de edad,
			y que cuenta(n) con la capacidad jurídica para contratarse en términos del presente instrumento.
			<br></div>

			<div style="text-align:justify;"><br>b) Que se encuentra(n) inscrito(s) en el Registro Federal de Contribuyentes
			bajo Clave(s) {rfccliente}:
			</div>

			<div style="text-align:justify;"><br>c) Que en su deseo de adquirir de "LA PROMITENTE VENDEDORA" "EL INMUEBLE",
			bajo los términos y condiciones que más adelante se establecen.
			<br></div>

			<div style="text-align:justify;"><br>III.- Declaran "LAS PARTES", la primera por conducto de su representante:
			<br></div>

			<div style="text-align: justify;">a) Que reconocen como ciertas las Declaraciones anteriores.
			</div>

			<div style="text-align: justify;"><br>b) Que se reconocen la personalidad con la que comparecen a la firma de este Contrato.
			</div>

			<div style="text-align: justify;"><br>c) Que comparecen en este acto al otorgar su consentimiento,
			manifestando conocer plenamente el sentido del presente Contrato, no existiendo dolo, mala fe, enriquecimiento ilegitimo,
			lesión o error que pudiera invalidarlo.
			<br></div>

			<div style="text-align: justify;"><br>En base a las Declaraciones que anteceden, "LAS PARTES" convienen en celebrar el presente
			CONTRATO de Promesa de Compraventa, de conformidad con las siguientes,
			<br></div>
			<br>

			<div style="text-align: center;"><span style="font-weight: bold;">CLÁUSULAS:<br></span>
			</div>

			<div style="text-align:justify;><span style="font-weight: bold;">PRIMERA.-OBJETO<br></div>

			<div style="text-align:justify;">Por virtud del presente Instrumento "LA PROMITENTE VENDEDORA" promete vender "EL INMUEBLE" "ad corpus" a
			"EL(LOS) PROMITENTE(S) COMPRADOR(ES)" quien(es) se obliga(n) a comprarlo, y pagar el precio acordado por "LAS PARTES", bajo los términos
			y condiciones que más adelante se establecen. 
			<br><br></div>

			<div style="text-align:justify;><span style="font-weight: bold;">SEGUNDA.- PRECIO Y FORMA DE PAGO<br></div>
			
			<div style="text-align: justify;">El precio que "LAS PARTES"
			han pactado por concepto de contraprestación asciende a la cantidad de
			${totalapagarq}, ({totalapagarl}), el cual se establece por todo
			el "INMUEBLE" materia de Contrato, ya que la presente operación se
			realiza "ad corpus", por lo que en el supuesto de que al verificarse la
			medición del mismo, éste resulte de mayor o menor superficie, el
			precio no sufrirá alteración, tal como disponen los artículos 1858 y
			1860 del Código Civil para el estado de Jalisco. "LAS PARTES" convienen en que el precio será pagado de la siguiente forma:<br>
			</div>
			
				{c2p1}
				
			
			<div><br><br></div>
			<div style="text-align:justify;><span style="font-weight: bold;">QUINTA.- ESCRITURACIÓN<br></div>

			<div style="text-align: justify;">"LA PROMITENTE VENDEDORA" se obliga a escriturar a "EL(LOS) PROMITENTE(S) COMPRADOR(ES)"
			"EL INMUEBLE", una vez que este(os) haya(n) liquidado
			la totalidad del precio de venta, y serán a cargo de "EL(LOS) PROMITENTE(S) COMPRADOR(ES)" 
			todos los gastos que genera dicha transmisión de propiedad,
			tanto en el otorgamiento del presente Contrato como en la escritura
			pública correspondiente, como son Impuesto Sobre Transmisión
			Patrimonial, derechos del Registro Público de la Propiedad, Avalúo y
			honorarios notariales o cualquier otro gasto, impuesto o derecho que se
			cause con la propia escritura, siendo únicamente a cargo de "LA PROMITENTE
			VENDEDORA" &nbsp;el impuesto &nbsp;sobre la &nbsp;Renta &nbsp;que &nbsp;
			se &nbsp;llegase &nbsp;a causar por la
			venta &nbsp;de &nbsp;"EL INMUEBLE"; &nbsp;asimismo "EL(LOS) PROMITENTE(S) COMPRADOR((ES)",
			en su caso,deberá(n) estar al
			corriente en las cuotas condominales y se obliga(n) a entregar toda la
			documentación que sea necesaria al Fedatario Público correspondiente
			para el otorgamiento de la referida escritura.<br>
			
			</div>
			<div style="text-align: justify;"><br>"LA PROMITENTE VENDEDORA"
			girará instrucción al Notario Público de su elección 30 (treinta) días
			naturales después de liquidado el precio de operación, misma que tendrá
			una vigencia de 45 (cuarenta y cinco) días naturales para que "EL(LOS) PROMITENTE(S)
			COMPRADOR(ES) acuda(n) ante Dicho Notario, presente(n) su documentación y firme(n)
			la escritura correspondiente.&nbsp; En caso de no formalizar la
			escritura pública de que se trata en el plazo de la vigencia de la
			instrucción, "LA PROMITENTE VENDEDORA" podrá girar nueva instrucción con un
			costo administrativo a cargo de "EL(LOS) PROMITENTE(S) COMPRADOR(ES)" de $ 100.00 ( CIEN
			PESOS 00/100 M.N.) por cada día transcurrido desde la fecha de
			caducidad de la primera instrucción y hasta la fecha de la nueva
			instrucción.<br><br>
			</div>			
			

			<div style="text-align:justify;><span style="font-weight: bold;">SEXTA.- ENTREGA DE LA POSESIÓN DE "EL INMUEBLE".<br></div>

			<div style="text-align: justify;">La posesión material de "EL
			INMUEBLE", la entrega en este acto "LA PROMITENTE VENDEDORA" a "EL(LOS) PROMITENTE(S)
			COMPRADOR(ES)", a su entera satisfacción. En caso de rescisión del
			presente contrato "EL(LOS) PROMITENTE(S) COMPRADOR(ES)", deberá(n)
			restituir la posesión de dicho inmueble a "LA PROMITENTE VENDEDORA",
			dentro de un plazo no mayor a 5 días contados a partir de la fecha en
			que ocurra la rescisión. "LAS PARTES" convienen expresamente que en caso
			de incumplimiento de "EL(LOS) PROMITENTE(S) COMPRADOR(ES)" en cuanto a
			la restitución de la posesión dentro del plazo convenido, pagará(n) a
			"LA PROMITENTE VENDEDORA" por concepto de pena convencional una
			cantidad equivalente a 9 veces el salario mínimo vigente en esta
			ciudad de Guadalajara, Jalisco, por cada día de retraso en la entrega de dicha posesión.<br><br>
			</div>
			

			<div style="text-align:justify;><span style="font-weight: bold;">SEPTIMA.- PENA CONVENCIONAL.<br></div>

			<div style="textrm-align: justify;">En caso de incumplimiento
			de alguna de las obligaciones que asumen "LAS PARTES" en el presente
			Contrato, la parte que incumpla pagará a la otra por concepto de pena
			convencional una cantidad equivalente al 25 {sabe} calculado sobre el monto
			total del precio pactado. En caso de que el incumplimiento fuere por
			parte de "EL(LOS) PROMITENTE(S) COMPRADOR(ES)", "LA PROMITENTE
			VENDEDORA" podrá optar por rescindir el presente contrato sin necesidad
			de declaración judicial previa, mediante simple notificación hecha por
			escrito.<br><br>
			</div>
			

			<div style="text-align:justify;><span style="font-weight: bold;">OCTAVA.- CESIÓN.<br></div>

			<div style="text-align: justify;">En caso de que "EL(LOS)
			PROMITENTE(S) COMPRADOR(ES)", quisiere(n) ceder los derechos del
			presente contrato, deberá de notificarlo por escrito a "LA PROMITENTE
			VENDEDORA" y además se obliga(n) a pagarle a esta, una cantidad
			equivalente al 5{sabe2} sobre el valor total de la correspondiente cesión de
			derechos. Sin el consentimiento expreso por escrito de "LA PROMITENTE
			VENDEDORA", la cesión de derechos no surtirá efecto legal alguno.<br><br>
			</div>

			<div style="text-align:justify;><span style="font-weight: bold;">NOVENA.- IMPUESTOS PREDIAL DE "EL INMUEBLE".<br></div>

			<div style="text-align: justify;">"EL(LOS) PROMITENTE(S)
			COMPRADOR(ES)" se obliga(n) a pagar a partir de esta fecha el impuesto
			predial correspondiente a "EL INMUEBLE" y "LA PROMITENTE VENDEDORA" se obliga a entregar al
			corriente el saldo del impuesto.<br><br>
			</div>
			
			<div style="text-align:justify;><span style="font-weight: bold;">DECIMA.- CONSTRUCCION DE "EL INMUEBLE".<br></div>

			<div style="text-align: justify;">"EL(LOS) PROMITENTE(S)
			COMPRADOR(ES)", se obliga(n) a acatar las características de obra que
			señale la Dirección de Obras Públicas del
			H. Ayuntamiento respectivo,
			así como las limitaciones que señala el
			reglamento del Desarrollo,
			respecto a la construcción que edifiquen sobre "EL INMUEBLE" misma que
			deberá ser recreativa campestres.<br><br>
			</div>

			<div style="text-align:justify;><span style="font-weight: bold;">DECIMO PRIMERA.- REGIMEN DE PROPIEDAD EN CONDOMINIO.<br></div>

			<div style="text-align: justify;">"LAS PARTES" convienen
			en que "LA PROMITENTE VENDEDORA" podrá, sin requerir el consentimiento
			de "EL(LOS) PROMITENTE(S) COMPRADOR(ES)", sujetar "EL INMUEBLE" al Regimen de Propiedad y Condominio.
			En caso de que "EL INMUEBLE" se afecte al Régimen de Propiedad y Condominio "LA
			PROMITENTE VENDEDORA", se obliga a transmitir a "EL(LOS) PROMITENTE(S)
			COMPRADOR(ES)", el mismo conjuntamente con la acción de dominio
			indivisa sobre las áreas comunes que corresponda al lote condominal.
			&nbsp; por su cuenta "EL(LOS) PROMITENTE(S) COMPRADOR(ES)" se obliga(n) a cumplir y acatar
			en todos sus términos el
			reglamento de administración del condominio.
			"El inmueble" deberá estar libre de gravamen, al corriente de sus
			obligaciones fiscales y "LA PROMITENTE VENDEDORA" se obligará al
			saneamiento para el caso de evicción en los términos de Ley.<br><br><br>
			</div>

			<div style="text-align:justify;><span style="font-weight: bold;">DECIMA SEGUNDA.- GASTOS.<br></div>

			<div style="text-align: justify;">Los gastos
			ocasionados por el presente contrato, así como los gastos, impuestos,
			derechos y honorarios ocasionados por la escritura de compra venta
			definitiva serán a cargo de "EL(LOS) PROMITENTE(S) COMPRADOR(ES)". El
			impuesto sobre la renta será pagado por "LA PROMITENTE VENDEDORA".<br><br>
			</div>

			<div style="text-align:justify;><span style="font-weight: bold;">DECIMA TERCERA.- TRIBUNALES COMPETENTES<br></div>

			<div style="text-align: justify;">Para la
			interpretación y cumplimiento del presente contrato "LAS PARTES" se
			someten expresamente a los Tribunales de esta ciudad de Guadalajara,
			Jalisco, renunciando al fuero presente o futuro que por cualquier causa
			pudiere corresponderles.<br><br>
			</div>

			<div style="text-align:justify;><span style="font-weight: bold;">DECIMA CUARTA.- DEPOSITARIO DE "EL INMUEBLE".<br></div>

			<div style="text-align: justify;">En el caso de que "LA
			PROMITENTE VENDEDORA" exigiere judicialmente, el cumplimiento de las
			obligaciones a cargo de "EL(LOS) PROMITENTE(S) COMPRADOR(ES)", éste(os)
			conviene(n) en que no será(n) depositario(s) de "EL INMUEBLE" objeto de este
			contrato, y por lo tanto se obliga a entregar a "LA PROMITENTE
			VENDEDORA" al depositario que ésta nombre dicho inmueble; siendo
			responsable(s) de cualquier daño o perjuicio que el inmueble sufra
			mientras el depositario no tome posesión de su cargo.<br>
			</div>
			
			<div style="text-align: justify;"><br>Todas las prestaciones derivadas de
			este contrato, deberá pagarlas y cumplirlas "EL(LOS) PROMITENTE(S)
			COMPRADOR(ES)" en la Ciudad de {eciudad}, {eestado}, en las oficinas de
			la empresa ubicadas en {edomicilio}, o en las que designe con
			posterioridad, mediante aviso dado por escrito a "EL(LOS) PROMITENTE(S)
			COMPRADOR(ES)". El cambio de domicilio, los emplazamientos y demás diligencias judiciales o extrajudiciales
			, se practicarán en el domicilio señalado en la presente cláusula<br><br>
			</div>
			

			<div style="text-align:justify;><span style="font-weight: bold;">DECIMA QUINTA.- <br></div>

			<div style="text-align: justify;">Para todos los efectos judiciales relativos al presente contrato, el acreditado
			señala como su domicilio {domiciliocliente} en la Ciudad de {ciudadcliente}, {estadocliente}.  Mientras 
			"EL(LOS) PROMINENTES COMPRADOR(ES)" no notifiquen por escrito a la "PROMINENTE VENDEDORA" el cambio de domicilio, 
			los emplazamientos y demas diligencias judiciales o extrajudiciales, se practicarán en el domicilio señalado en la presente cláusula.
			<br><br>
			</div>
			
			<div style="text-align: justify;"><br>
			Enteradas "LAS PARTES" del valor,
			alcances y consecuencias legales del presente contrato, lo ratifican y
			firman por duplicado en la Ciudad de Guadalajara, Jalisco, a los
			{fechadia} días del mes de {fechames} de {fechaano}.<br><br><br>
			</div>
			<div style="text-align: center;"><br>"LA PROMITENTE VENDEDORA"<br>{razonsocial}
			<br><br>
			_______________________________________________<br>
			<br>
			REPRESENTADA POR EL {representantelegal}<br>
			<br><br><br>
			"EL(LOS) PROMITENTE(S) COMPRADOR(ES)"<br>
			<br><br>
			_______________________________________________<br>
			{nombrecliente}<br>
			<br><br><br>
			</div>
			<table
			style="text-align: left; width: 100px; margin-left: auto; margin-right: auto;"
			border="0" cellpadding="2" cellspacing="2">
			<tbody>
			<tr>
			<td style="vertical-align: top; text-align: center;">TESTIGO<br>
			<br><br>
			__________________________________________<br>
			Sr. Vicente Bejarano Casillas
			</td>
			<td style="vertical-align: top; text-align: center; width: 100px;"><br>
			</td>
			<td style="vertical-align: top; text-align: center;">TESTIGO<br>
			<br><br>
			__________________________________________<br>
			{nombrevendedor}
			</td>
			</tr>
			</tbody>
			</table>
			** N&uacute;mero de Autorizaci&oacute;n de la Profeco: PFC.B.E.7/007544-2015 **
			</body>
			</html>
			"""
	return template

def pagare_template(cuenta):
	ses = DBSession2
	plazo = None
	amortizacion = None
	pagoextra = False
	for x in ses.execute(f"select pkamortizacion as pk from gixamortizacion where cuenta={cuenta}"):
		amortizacion=x.pk
	

	query = f"""
	select count(*) as cuantos from gixamortizaciondetalle
	where fkamortizacion = {amortizacion} and eliminado = 0 and insertado <> 0
	"""
	for x in ses.execute(query):
		if x.cuantos > 0:
			pagoextra = True
	
	if pagoextra:
		return GetHtmlPagarePagosExtras(amortizacion)
	else:
		return GetHtmlPagarePagos(amortizacion)

def GetHtmlPagarePagosExtras(amortizacion):
	ses = DBSession2
	mes = {1:"Enero", 2:"Febrero", 3:"Marzo", 4:"Abril", 5:"Mayo", 6:"Junio", 7:"Julio",
		       8:"Agosto", 9:"Septiembre", 10:"Octubre", 11:"Noviembre", 12:"Diciembre"}
	messmall = {1:"Ene", 2:"Feb", 3:"Mar", 4:"Abr", 5:"May", 6:"Jun", 7:"Jul",
				8:"Ago", 9:"Sep", 10:"Oct", 11:"Nov", 12:"Dic"}
		
	query =f"""
		select sum(pagofijo) as total from gixamortizaciondetalle where fkamortizacion = {amortizacion} and eliminado = 0
		"""
	for x in ses.execute(query):
		totaltabla = x.total
	
	query = f"""
		select count(*) as cuantos from gixamortizaciondetalle
		where fkamortizacion = {amortizacion} and eliminado = 0
		"""
	for x in ses.execute(query):
		plazotabla=x.cuantos

	totaltablac = formato_comas.format(float(totaltabla))
	totaltablal = aletras(float(totaltabla), tipo="pesos")

	query = f"""
	select c.nombre as nombre, c.domicilio as domicilio, c.colonia as colonia,
	c.telefonocasa as telcasa, ltrim(rtrim(c.ciudad + ', ' + c.estado + ' ' + c.cp)) as ciudadestadocp,
	convert(varchar(10), a.fechaelaboracion, 103) as fechaelaboracion from gixamortizacion a
	join cliente c on a.fkcliente = c.codigo
	where a.pkamortizacion = {amortizacion}
	"""

	for x in ses.execute(query):
		nombre = x.nombre
		domicilio = x.domicilio
		if  x.colonia:
			domicilio+= f" Col. {x.colonia}"
		telefono = x.telcasa
		ciudadestadocp = x.ciudadestadocp
		de, me, ae = x.fechaelaboracion.split("/")
		mes = mes[int(me)]
	
	rows =[]
	query = f"""
		select numerodepago as numerodepago, convert(varchar(10), fechadepago, 103) as fechadepago, pagofijo as pagofijo
		from gixamortizaciondetalle
		where fkamortizacion = {amortizacion} and eliminado = 0 order by numerodepago
		"""
	for x in ses.execute(query):
		rows.append(dict(numerodepago=x.numerodepago, fechadepago=x.fechadepago, pagofijo=x.pagofijo))
	
	template = f"""
	<html>
	<head>
	<meta name="pdfkit-page-size" content="letter"/>
	<meta charset="utf-8">
	</head>
	<body>
	<div style="text-align: center;"><big><big><big><big><span
	style="font-family: Arial; font-weight: bold;"><br>
	PAGARE</span></big></big></big></big><br>
	<div style="text-align: left;"><br>
	<br>
	<br>
	<big><big><big><span style="font-family: Arial;">IMPORTE: <span
	style="font-weight: bold;">${totaltablac}</span></span></big></big></big><br>
	<br>
	<br>
	<div style="text-align: justify;"><big><big><span
	style="font-family: Arial;">Por
	medio de este pagaré reconozco(emos) deber y me(nos) obligo(amos) a
	pagar incondicionalmente a la orden de Arcadia Promotora S. de R.L. de
	C.V., la cantidad total de <span style="font-weight: bold;">${totaltablac}
	({totaltablal})</span>, en el domicilio de Av. Hidalgo 1443 Piso 9,
	mediante <span style="font-weight: bold;">{plazotabla}</span> pagos
	de la siguiente forma:<span style="font-weight: bold;"></span><span
	style="font-weight: bold;"></span><span style="font-weight: bold;"></span><span
	style="font-weight: bold;"></span><span style="font-weight: bold;"></span><span
	style="font-weight: bold;"></span><span style="font-weight: bold;"></span></span><br>
	<br>
	"""
	
	tableheader = f"""<table>
	<thead>
					<tr>
					<th scope="col">No. de Pago</th>
					<th scope="col">Fecha de Pago</th>
					<th scope="col">Saldo Inicial</th>
					<th scope="col">Pago</th>
					<th scope="col">Saldo Final</th>
					</tr>
				</thead><tbody>"""
	template += tableheader
	lines = 18
	sini = float(totaltabla)
	for row in rows:
		numerodepago = str(int(row["numerodepago"]))
		d, m, a = row["fechadepago"].split("/")
		fechadepago = "%02d/%s/%04d" % (int(d), messmall[int(m)], int(a))
		saldoinicial = formato_comas.format(sini)
		pagofijo = formato_comas.format(float(row["pagofijo"]))
		sfin = sini - float(row["pagofijo"])
		if sfin < 0:
			sfin = 0.00
		saldofinal = formato_comas.format(float(sfin))
		sini = sfin
		tag = ''
		#lines += 1
		#if lines > 27:
			#tag = '<div><h1></div><br><br><br><br><br><br><br><br><br><br>'
			#lines = 0
			
		template+= f"""
				<tr>
					<td style="width: 100px; height: 10px; text-align: center;"><small>{numerodepago}</small></td>
					<td style="width: 165px; height: 10px; text-align: center;"><small>{fechadepago}</small></td>
					<td style="width: 140px; height: 10px; text-align: center;"><small>{saldoinicial}</small></td>
					<td style="width: 100px; height: 10px; text-align: center;"><small>{pagofijo}</small></td>
					<td style="width: 110px; height: 10px; text-align: center;"><small>{saldofinal}</small></td>
				</tr>
				"""
		#template += line + tag
		#if tag:
		#	template += tableheader
	template+= f"""</tbody></table>"""
	
	h2 = f"""
	<br>
	<span style="font-family: Arial;">En caso de mora en el principal, la
	suma de que se trate causará intereses moratorios por todo el tiempo que se mantenga insoluto dicho pago a una tasa del 25 % anual.<br>
	<br>
	<span style="font-family: Arial;">La falta de pago oportuno del capital
	de por los menos dos mensualidades, traerá como consecuencia que sea
	exigible en su totalidad el saldo insoluto de la cantidad que ampara el
	presente pagaré, aún cuando las mensualidades que sucedan a dicha
	mensualidad, no se encuentren vencidas.<br>
	<br>
	Este pagaré queda relevado de protesto.<br>
	<br>
	Para todo lo relacionado con este pagaré, incluyendo su interpretación
	y cumplimiento, me someto expresamente a las leyes y tribunales
	vigentes y competentes en la ciudad de Guadalajara, Jalisco,
	renunciando al fuero que por cualquier otra causa pudiera corresponder.<br>
	<br>
	Suscribo el presente pagaré en la ciudad de Guadalajara, Jalisco a los <span
	style="font-weight: bold;">{de} d�as del mes de {me} del {ae}.</span><br>
	<br>
	<br>
	<small>Nombre del Suscriptor:</small><br>
	<span style="font-weight: bold;">{nombre}<small><br>
	</small></span><small>Domicilio:</small><br>
	<span style="font-weight: bold;">{domicilio}</span><br>
	<small>Tel�fono:</small><br>
	<span style="font-weight: bold;">{telefono}</span><br>
	<small>Ciudad y Estado:</small><br>
	<span style="font-weight: bold;">{ciudadestadocp}</span></span></span></big></big><br>
	<div style="text-align: center;">
	<div style="text-align: left;"><big><big><span
	style="font-family: Arial;"><span style="font-family: Arial;"></span></span></big></big><br>
	<br>
	<br>
	<div style="text-align: left;"><big><big><span
	style="font-family: Arial;">ACEPTO(AMOS)</span></big></big>______________________________________________________________________<br>
	</div>
	</div>
	<big><big><span style="font-family: Arial;"><span
	style="font-family: Arial;"></span></span></big></big></div>
	</div>
	</div>
	</div>
	</body>
	</html>
	"""
		 
	template = template + h2
	# h = (h.replace('\n',' ')).replace('\t',' ')
	# h = (h.replace('{$TOTALTABLAC}',totaltablac)).replace('{$TOTALTABLAL}',totaltablal)
	# h = h.replace('{$PLAZOTABLA}',str(plazotabla))
	# h = ((h.replace('{$DIA}',de)).replace('{$MES}',me)).replace('{$ANIO}',ae)
	# h = (h.replace('{$NOMBRE}',nombre)).replace('{$DOMICILIO}',domicilio)
	# h = (h.replace('{$TELEFONO}',telefono)).replace('{$CDEDO}',ciudadestadocp)
	
	return template

def GetHtmlPagarePagos(amortizacion):
	#cuenta en 2
	ses = DBSession2
	mes = {1:"Enero", 2:"Febrero", 3:"Marzo", 4:"Abril", 5:"Mayo", 6:"Junio", 7:"Julio",
			8:"Agosto", 9:"Septiembre", 10:"Octubre", 11:"Noviembre", 12:"Diciembre"}
	
	query = f"""
	select count(*) as cuantos from gixamortizaciondetalle
	where fkamortizacion = {amortizacion} and eliminado = 0
	"""
	for x in ses.execute(query):
		cuantos = x.cuantos

	query = f"""
	select top(1) convert(varchar(10), fechadepago, 103) as fecha from gixamortizaciondetalle
	where fkamortizacion = {amortizacion} and eliminado = 0 order by fecha
	"""
	for x in ses.execute(query):
		di, mi, ai = x.fecha.split("/")
	
	query = f"""
	select top(1) convert(varchar(10), fechadepago, 103) as fecha from gixamortizaciondetalle
	where fkamortizacion = {amortizacion} and eliminado = 0 order by fecha desc
	"""
	for x in ses.execute(query):
		df, mf, af = x.fecha.split("/")
	
	query = f"""
	select count(*) as cuantos from gixamortizaciondetalle
	where fkamortizacion = {amortizacion} and eliminado = 0
	"""
	for x in ses.execute(query):
		plazotabla=x.cuantos
	
	query =f"""
		select sum(pagofijo) as total from gixamortizaciondetalle where fkamortizacion = {amortizacion} and eliminado = 0
		"""
	for x in ses.execute(query):
		totaltabla = x.total
	
	
	query=f""" select pagomensualfijo as pago from gixamortizacion where pkamortizacion={amortizacion}"""
	for x in ses.execute(query):
		pagofijo = x.pago
	#va en el 8
	#pagomensualq = formato_comas.format(float(general["pagomensualfijo"]))
	#pagomensuall = aletras(float(general["pagomensualfijo"]), tipo="pesos")
	totaltablac = formato_comas.format(float(totaltabla))
	totaltablal = aletras(float(totaltabla), tipo="pesos")
	fechainicial = f"{int(di)} de {mes[int(mi)]} de {int(ai)}"
	fechafinal = f"{int(df)} de {mes[int(mf)]} de {int(af)}"
	pagofijoc = formato_comas.format(pagofijo)
	pagofijol = aletras(pagofijo, tipo="pesos")

	query = f"""
	select c.nombre as nombre, c.domicilio as domicilio, c.colonia as colonia,
	c.telefonocasa as telcasa, ltrim(rtrim(c.ciudad + ', ' + c.estado + ' ' + c.cp)) as ciudadestadocp,
	convert(varchar(10), a.fechaelaboracion, 103) as fechaelaboracion from gixamortizacion a
	join cliente c on a.fkcliente = c.codigo
	where a.pkamortizacion = {amortizacion}
	"""

	for x in ses.execute(query):
		nombre = x.nombre
		domicilio = x.domicilio
		if  x.colonia:
			domicilio+= f" Col. {x.colonia}"
		telefono = x.telcasa
		ciudadestadocp = x.ciudadestadocp
		de, me, ae = x.fechaelaboracion.split("/")
		mes = mes[int(me)]

	
	template = f"""
	<html>
	<head>
	<meta name="pdfkit-page-size" content="letter"/>
	<meta charset="utf-8">
	</head>
	<body>
	<div style="text-align: center;"><big><big><big><span
	style="font-family: Arial; font-weight: bold;"><br>
	PAGARE</span></big></big></big><br>
	<div style="text-align: left;"><br>
	<br>
	<big><big><span style="font-family: Arial;">IMPORTE: <span
	style="font-weight: bold;">${totaltablac}</span></span></big></big><br>
	<br>
	<div style="text-align: justify;"><big><big><span
	style="font-family: Arial;">Por
	medio de este pagaré reconozco(emos) deber y me(nos) obligo(amos) a
	pagar incondicionalmente a la orden de Arcadia Promotora S. de R.L. de
	C.V., la cantidad total de <span style="font-weight: bold;">${totaltablac}
	({totaltablal})</span>, en el domicilio de Av. Hidalgo 1443 Piso 9,
	mediante <span style="font-weight: bold;">{plazotabla}</span> pagos
	mensuales consecutivos sin intereses del día <span
	style="font-weight: bold;">{fechainicial}</span><span
	style="font-weight: bold;"></span><span style="font-weight: bold;"></span>
	al <span style="font-weight: bold;">{fechafinal}</span><span
	style="font-weight: bold;"></span><span style="font-weight: bold;"></span>,
	cada una por la cantidad de <span style="font-weight: bold;">${pagofijoc}
	({pagofijol})</span>.</span><br>
	<br>
	<span style="font-family: Arial;">En caso de mora en el principal, la
	suma de que se trate causará intereses moratorios del 
	25 por ciento anual.<br>
	<br>
	<span style="font-family: Arial;">La falta de pago oportuno del capital
	de por los menos dos mensualidades, traerá como consecuencia que sea
	exigible en su totalidad el saldo insoluto de la cantidad que ampara el
	presente pagaré, aún cuando las mensualidades que sucedan a dicha
	mensualidad, no se encuentren vencidas.<br>
	<br>
	Este pagaré queda relevado de protesto.<br>
	<br>
	Para todo lo relacionado con este pagaré, incluyendo su interpretación
	y cumplimiento, me someto expresamente a las leyes y tribunales
	vigentes y competentes en la ciudad de Guadalajara, Jalisco,
	renunciando al fuero que por cualquier otra causa pudiera corresponder.<br>
	<br>
	Suscribo el presente pagaré en la ciudad de Guadalajara, Jalisco a los <span
	style="font-weight: bold;">{de} días del mes de {me} del {ae}.</span><br>
	<br>
	<br>
	<small>Nombre del Suscriptor:</small><br>
	<span style="font-weight: bold;">{nombre}<small><br>
	</small></span><small>Domicilio:</small><br>
	<span style="font-weight: bold;">{domicilio}</span><br>
	<small>Teléfono:</small><br>
	<span style="font-weight: bold;">{telefono}</span><br>
	<small>Ciudad y Estado:</small><br>
	<span style="font-weight: bold;">{ciudadestadocp}</span></span></span></big></big><br>
	<div style="text-align: center;">
	<div style="text-align: left;"><big><big><span
	style="font-family: Arial;"><span style="font-family: Arial;"></span></span></big></big><br>
	<br>
	<br>
	<div style="text-align: left;"><big><big><span
	style="font-family: Arial;">ACEPTO(AMOS)</span></big></big>______________________________________________________________________<br>
	</div>
	</div>
	<big><big><span style="font-family: Arial;"><span
	style="font-family: Arial;"></span></span></big></big></div>
	</div>
	</div>
	</div>
	</body>
	</html>
	"""
		
	#h = ((h.replace('{$TOTALTABLAC}',totaltablac)).replace('{$TOTALTABLAL}',totaltablal)).replace('{$PLAZOTABLA}',str(plazotabla))
	#h = (h.replace('{$FECHAINICIAL}',fechainicial)).replace('{$FECHAFINAL}',fechafinal)
	#h = (h.replace('{$PAGOFIJOC}',pagofijoc)).replace('{$PAGOFIJOL}',pagofijol)
	#h = ((h.replace('{$DIA}',de)).replace('{$MES}',me)).replace('{$ANIO}',ae)
	#h = (h.replace('{$NOMBRE}',nombre)).replace('{$DOMICILIO}',domicilio)
	#h = (h.replace('{$TELEFONO}',telefono)).replace('{$CDEDO}',ciudadestadocp)

	#jump = '<style>@media print {h1 {page-break-before:always}}</style>'
	
	return template

def tablatemplate(cuenta):
	path = os.getcwd()
	src_image=f"{path}/zen/static/logopinares.jpg"

	ses = DBSession2
	plazo = None
	amortizacion = None
	pagoextra = False
	messmall = {1:"Ene", 2:"Feb", 3:"Mar", 4:"Abr", 5:"May", 6:"Jun", 7:"Jul",
				8:"Ago", 9:"Sep", 10:"Oct", 11:"Nov", 12:"Dic"}

	for x in ses.execute(f"select pkamortizacion as pk from gixamortizacion where cuenta={cuenta}"):
		amortizacion=x.pk

	query = f"""
		select sum(abonocapital) + sum(interes) as abonointeres from gixamortizaciondetalle where fkamortizacion = {amortizacion} and eliminado = 0
		"""
		#sql = (query.replace('\n',' ')).replace('\t',' ')
		#cu = r_cngcmex.cursor()
		#cu.execute(str(sql))
		#dato = fetchone(cu)
		#cu.close()
	abonointeres = 0
	for x in ses.execute(query):
		abonointeres = x.abonointeres

	query = f"""
	select rtrim(ltrim(i.iden2)) + '-' + rtrim(ltrim(i.iden1)) as lote, a.enganchec as enganche, i.superficie as superficie, a.saldoafinanciar as saldoafinanciar, i.preciopormetro as preciom2,
	case a.plazomeses when 0 then (a.saldoafinanciar + a.enganchec) else ((a.pagomensualfijo * a.plazomeses) + a.enganchec) end as totalapagaraux,
	a.cuenta
	from gixamortizacion a
	join INMUEBLE i on a.fkinmueble = i.codigo
	where a.pkamortizacion = {amortizacion}
	"""
	#pagofijoc = formato_comas.format(pagofijo)
	#pagofijol = aletras(pagofijo, tipo="pesos")
	for x in ses.execute(query):
		lote = x.lote
		enganche = x.enganche
		superficie = x.superficie
		saldoafinanciar = x.saldoafinanciar
		preciom2 = x.preciom2
		totalapagaraux=x.totalapagaraux
	if abonointeres:
		totalapagar = str(formato_comas.format(abonointeres + enganche))
	else:
		totalapagar = str(formato_comas.format(totalapagaraux))

	enganche = formato_comas.format(enganche)
	superficie = formato_comas.format(superficie)
	saldoafinanciar = formato_comas.format(saldoafinanciar)
	preciom2 = formato_comas.format(preciom2)
	
	meses = ("", "Ene", "Feb", "Mar", "Abr", "May", "Jun", "Jul", "Ago", "Sep", "Oct", "Nov", "Dic")
	
	t = datetime.now()
	z = t.time()
	fecha = "{:04d}/{:02d}/{:02d}".format(t.year, t.month, t.day)
	hora = "{:02d}:{:02d}:{:02d}".format(z.hour, z.minute, z.second)

	css=""" body {
        font-family: 'Tangerine', serif;
        font-size: 48px;
      }"""
	
	template = f"""
	<html>
	<head>
	<meta name="pdfkit-page-size" content="Letter"/>
	<meta charset="utf-8">
	<meta name="enable-local-file-access" content="true"/>
	<link rel="stylesheet"
          href="https://fonts.googleapis.com/css?family=Open+Sans">
	<style>
	{css}
    </style>
	</head>
	<body>
	<table style="width:100%">
		<tbody>
			<tr style="width:100%">
				<td style="width:50%;">
				 <img style="width:315px; height:288px;" src='{src_image}' />
				</td>
				<td style="text-align: right; width:50%;">
					<big><big
					style="font-weight: bold;">Id de la Tabla:&nbsp; {amortizacion} 
					</big></big>
					<br> {fecha} &nbsp; {hora} </span><br>
				</td>
			</tr>
		</tbody>
	</table>
	"""
	
	template += f"""
	<div style="text-align: right;">
	</div>
	<hr style="width: 100%; height: 2px;">
	<table style="text-align: left; width: 985px; height: 82px; margin-left: auto; margin-right: auto;"
	border="0" cellpadding="2" cellspacing="2">
		<tbody>
		<tr>
			<td></td>
			<td style="text-align: right; width: 190px;"><big>Lote:</big></td>
			<td style="text-align: left; font-weight: bold; width: 183px;"><big> {lote} </big></td>
			<td style="text-align: right; width: 219px;"><big>Enganche:</big></td>
			<td style="font-weight: bold; width: 357px;"><big> {enganche} </big></td>
			<td></td>
		</tr>
		<tr>
			<td></td>
			<td style="text-align: right; width: 190px;"><big>Superficie:</big></td>
			<td style="text-align: left; font-weight: bold; width: 183px;"><big> {superficie} </big></td>
			<td style="text-align: right; width: 219px;"><big>Saldo a Financiar:</big></td>
			<td style="font-weight: bold; width: 357px"><big> {saldoafinanciar} </big></td>
			<td></td>
		</tr>
		<tr>
			<td></td>
			<td style="text-align: right; width: 190px;"><big>Precio M2:</big></td>
			<td style="text-align: left; font-weight: bold; width: 183px;"><big> {preciom2} </big></td>
			<td style="text-align: right; width: 219px;"><big>Total a Pagar:</big></td>
			<td style="font-weight: bold; width: 357px"><big> {totalapagar} </big></td>
			<td></td>
		</tr>
		</tbody>
	</table>
	<hr style="width: 100%; height: 1px;" noshade="noshade">
	"""
	
	query = f"""
	select numerodepago as numerodepago, convert(varchar(10), fechadepago, 103) as fecha1, saldoinicial as saldoinicial, pagofijo as pagofijo, abonocapital as abonocapital, interes as interes, saldofinal as saldofinal
	from gixamortizaciondetalle where fkamortizacion = {amortizacion} and eliminado = 0 order by fechadepago, numerodepago
	"""
	rows=[]
	for x in ses.execute(query):
		rows.append(dict(numerodepago=x.numerodepago, fecha1=x.fecha1, saldoinicial=x.saldoinicial, pagofijo=x.pagofijo, abonocapital=x.abonocapital, interes=x.interes, saldofinal=x.saldofinal))
	
	detail, footer = "", ""
	lines, page, pages = 0, 1, 1
	template += """
	<div style="width:100%">
	<table class="table" style="width:100%">
	<thead>
    <tr style="width:100%">
      <th scope="col">No. de Pago</th>
      <th scope="col">Fecha de Pago</th>
      <th scope="col">Saldo Inicial</th>
      <th scope="col" style="padding-left:40px;">Pago</th>
	  <th scope="col">Abono a Capital</th>
      <th scope="col">Intereses</th>
      <th scope="col">Saldo Final</th>
    </tr>
	</thead>
	<tbody>
	"""
	
	#return template
	aux = len(rows) / 35.0
	pages = int(aux)
	if (aux - int(aux)) > 0:
		pages += 1
		
	for row in rows:
		numerodepago = str(int(row["numerodepago"]))
		d, m, a = row["fecha1"].split("/")
		fechadepago = "%02d/%s/%04d" % (int(d), meses[int(m)], int(a))
		saldoinicial = formato_comas.format(float(row["saldoinicial"]))
		pagofijo = formato_comas.format(float(row["pagofijo"]))
		abonocapital = formato_comas.format(float(row["abonocapital"]))
		interes = formato_comas.format(float(row["interes"]))
		saldofinal = formato_comas.format(float(row["saldofinal"]))
		tag = ''; lines += 1
		if lines > 34:
			tag = '<div><h1></div>'
			lines = 0
			page += 1
			
		line = """
			<tr style="width:100%">
				<td style="width: 50px; height: 10px; text-align: center;">""" + numerodepago + """</td>
				<td style="width: 140px; height: 10px; text-align: center;">""" + fechadepago + """</td>
				<td style="width: 110px; height: 10px; text-align: right;">""" + saldoinicial + """</td>
				<td style="width: 80px; height: 10px; text-align: right; padding-left:40px;">""" + pagofijo + """</td>
				<td style="width: 100px; height: 10px; text-align: center;">""" + abonocapital + """</td>
				<td style="width: 100px; height: 10px; text-align: center;">""" + interes + """</td>
				<td style="width: 100px; height: 10px; text-align: right;">""" + saldofinal + """</td>
			</tr>
		"""
		template+=line
	template+= """
	</tbody>
	</table>
	</div>
	</body>
	</html>
	"""
	return template


@view_config(route_name="otroprint", renderer="json", request_method="GET")
def otroprint(request):
	cuenta=request.params.get("cuenta", "")
	tipo=request.params.get("tipo", "")
	template=""
	if tipo=="contrato" and cuenta != None:
		template=contrato_template(cuenta)
	if tipo=="pagare" and cuenta !=None:
		template=pagare_template(cuenta)
	if tipo=="tabla" and cuenta !=None:
		template=tablatemplate(cuenta)

	pdfkit.from_string(template, "out.pdf", {'page-size': 'Letter', 'encoding': "UTF-8", 'enable-local-file-access':"", "footer-right": '[page] de [topage]', "footer-left": '{} cuenta: {}'.format(tipo, cuenta), "footer-spacing":10, "margin-bottom":"30mm"})
	response = FileResponse("out.pdf", request=request, content_type="application/pdf")
	return response


@view_config(route_name="otro", renderer="json", request_method="GET")
def otro(request):
	print("llego a la ruta otro")
	value = printdispatcher(request)
	if value.get("printed", "0") == "0":
		request.response.status_code = 400
	return value


def printdispatcher(request):
	r = request.params
	print(paint.blue(r))
	print("esto fue aca 2000")
	printer = r.get("printer", "")
	email = r.get("email", "")
	if email:
		printer = "email"
		print(paint.yellow("email is {}".format(email)))
	if not printer:
		return dict(name="", printed="0", error="impresora no especificada")
	pdf = r.get("pdf", "")
	xls = r.get("xls", "")
	user = r.get("user", "")
	copies = int(r.get("copies", "1"))
	tipo = r.get("tipo", "caracteristicas")
	oferta = int(r.get("oferta", 0))
	etapa = int(r.get("etapa", 0))
	cliente = int(r.get("cliente", 0))
	precalificacion = float(r.get("precalificacion", "0"))
	subsidio = float(r.get("subsidio", "0"))
	pagare = float(r.get("pagare", "0"))
	avaluo = float(r.get("avaluo", "0"))
	validToken, token = get_token(request)
	devolucion = float(r.get("devolucion", "0"))
	cuenta = int(r.get("cuenta", "0"))
	repparamsid = r.get("repparamsid", "")
	solicitudcheque = int(r.get("solicitudcheque", 0))
	filename = r.get("filename", "")
	pago = int(r.get("pago", 0))
	recibo = int(r.get("recibo", 0))
	try:

		assert validToken, "Token invalido"
		user = cached_results.dicTokenUser[token].get("id", "")

		if pdf:

			name = pdf
			# reprint = True
			reprint = False

		else:
			func_name = "obtener{}{}".format(tipo[0].upper(), tipo[1:])
			if tipo in ("caracteristicas oferta".split(" ")):
				datos = eval(func_name)(etapa, oferta)
			elif tipo in (["anexo"]):
				datos = eval(func_name)(
					etapa, oferta, precalificacion, avaluo, subsidio, pagare
				)
			elif tipo in (["cancelacion", "docscliente"]):
				datos = eval(func_name)(cliente, cuenta, user, etapa, devolucion)
			elif tipo in (["rap"]):
				datos = eval(func_name)(cliente)
			elif tipo in (["otro", "recientesofertas"]):
				datos = eval(func_name)()
			elif tipo in (["otro2", "ofertaventa"]):
				datos = eval(func_name)(repparamsid)
			elif tipo in (["tramites"]):
				datos = eval(func_name)(etapa)
			elif tipo in (["pagocomision"]):
				datos = eval(func_name)(pago)
			elif tipo in (["solicitudcheque"]):
				print("si entro aquiiiiii 1000")
				datos = eval(func_name)(solicitudcheque)
			elif tipo in (["generaexcel"]):
				datos = eval(func_name)(filename)
			elif tipo in (["recibo"]):
				datos = eval(func_name)(recibo)
			elif tipo in (["resumen"]):
				datos = None
			else:
				return dict()
			template = "{}.html".format(tipo)
			reprint = False

			ok, name = pdfCreate(datos=datos, template=template, tipo=tipo)
			if not ok:
				return dict(name="", printed="0", error="hubo error al generar pdf")
		if printer == "null":
			return dict(name=name, printed="1", error="")

		rdb.connect(
			cached_results.settings.get("rethinkdb.host"),
			cached_results.settings.get("rethinkdb.port"),
		).repl()
		if printer == "email":
			table = rdb.db("printing").table("email_jobs")
		else:
			table = rdb.db("printing").table("jobs")
		ts = rdb.expr(datetime.now(rdb.make_timezone("00:00")))
		if printer == "email":
			print(paint.blue("inserting in email_jobs ( rethinkdb  )"))
			table.insert(
				dict(filepath=name, user=user, email=email, timestamp=ts)
			).run()
			if xls:
				excel_name = "{}.xls".format(name.split(".")[0])
				table.insert(
					dict(filepath=excel_name, user=user, email=email, timestamp=ts)
				).run()

		else:

			reprint = table.filter(dict(filepath=name)).count().run() > 0
			table.insert(
				dict(
					filepath=name,
					target=printer,
					copies=copies,
					user=user,
					reprint=reprint,
					timestamp=ts,
				)
			).run()

		print(paint.blue("dispatching {}".format(name)))
		return dict(name=name, printed="1", error="")
	except:
		log_error_to_rdb("printdispatcher")
		print_exc()

	return dict(name="", printed="0", error="hubo error")


@view_config(route_name="listprinters", renderer="json", request_method="GET")
def listprinters(request):
	rdb.connect(
		cached_results.settings.get("rethinkdb.host"),
		cached_results.settings.get("rethinkdb.port"),
	).repl()
	ptable = rdb.db("printing").table("printers")
	valid = True
	try:
		validToken, token = get_token(request)
		assert validToken, "token invalido"
		user = cached_results.dicTokenUser.get(token)
		assert user.get("perfil", "") == "admin", "Solo un admin puede incorporar"
	except:
		print_exc()
		valid = False

	if not valid:
		request.response.status = 400

	rlist = request.params.get("rlist", "")
	if rlist:
		printers = []
		for x in ptable.run():
			printers.append(x)
		return printers
	printers = printers_info(match=False)
	for printer in printers:
		registered_printer = False
		for x in ptable.filter(rdb.row["printerid"] == printer.get("printerid")).run():
			registered_printer = True
		if not registered_printer:
			ptable.insert(
				dict(
					printerid=printer.get("printerid"),
					name=printer.get("name"),
					displayname=printer.get("displayname"),
				)
			).run()
	return printers


def dump_request_response(request):
	for x in dir(request.response):
		if x in (["status_int", "status", "status_code"]):
			x = "{} = {}".format(x, getattr(request.response, x))
		print(paint.yellow("{}".format(x)))


@view_config(route_name="deleteprinter", renderer="json", request_method="POST")
def deleteprinter(request):
	error = ""
	valid = True
	rdb.connect(
		cached_results.settings.get("rethinkdb.host"),
		cached_results.settings.get("rethinkdb.port"),
	).repl()
	try:
		printers = rdb.db("printing").table("printers")
		r = request.params
		validToken, token = get_token(request)
		assert validToken, "token invalido"
		user = cached_results.dicTokenUser.get(token)
		assert user.get("perfil", "") == "admin", "Solo un admin puede eliminar"
		print(paint.blue("Aqui vamos"))
		printerid = r.get("printerid", "")
		assert printerid, "No se especifico printerid"
		assert (
			printers.filter(dict(printerid=printerid)).count().run() == 1
		), "No existe impresora"
		printers.filter(dict(printerid=printerid)).delete().run()
	except:
		print_exc()
		error = "falla eliminacion"
		valid = False
	print(paint.blue("en deleteprinter is valid es {}".format(valid)))
	if not valid:
		try:
			dump_request_response(request)
			request.response.status_code = 400
		except:
			print_exc()
	return dict(success=valid, error=error)


@view_config(route_name="useremail", renderer="json", request_method="POST")
def useremail(request):
	try:
		r = request.params
		email = r.get("email", "")
		query = r.get("query", "")
		valid, tokenValue = get_token(request)
		assert valid, "token invalido"
		print(paint.blue("Chequeo de token es {} y vale {}".format(valid, tokenValue)))
		token = tokenValue
		user = cached_results.dicTokenUser.get(token)
		print(user)
		usuario = user.get("id")
		usuario = str(usuario)
		assert usuario, "no existe usuario"
		rdb.connect(
			cached_results.settings.get("rethinkdb.host"),
			cached_results.settings.get("rethinkdb.port"),
		).repl()
		if query:
			r_email = ""
			q = rdb.db("printing").table("useremail").filter(rdb.row["user"] == usuario)
			for x in q.run():
				r_email = x.get("email", "")

			return dict(success="1", email=r_email)

		rdb.db("printing").table("useremail").filter(
			rdb.row["user"] == usuario
		).delete().run()

		if email:
			rdb.db("printing").table("useremail").insert(
				dict(user=usuario, email=email)
			).run()
			print("adding user and email")

		return dict(success="1")

	except:
		print_exc()
		return dict(success="0")


@view_config(route_name="userprinter", renderer="json", request_method="POST")
def userprinter(request):
	try:
		rdb.connect(
			cached_results.settings.get("rethinkdb.host"),
			cached_results.settings.get("rethinkdb.port"),
		).repl()
		# print cached_results.dicTokenUser
		ptable = rdb.db("printing").table("userprinter")
		printers = rdb.db("printing").table("printers")
		r = request.params
		printerid = r.get("printerid", "")
		copies = int(r.get("copies", 0))
		valid, tokenValue = get_token(request)
		print(paint.blue("Chequeo de token es {} y vale {}".format(valid, tokenValue)))
		# token = r.get("token", "")
		token = tokenValue
		user = cached_results.dicTokenUser.get(token)
		print(user)

		assert printerid, "no hay printerid"
		print("copies ", copies)
		assert copies in (0, 1, 2, 3), "copies invalido"
		usuario = user.get("id")
		assert (
			printers.filter(rdb.row["printerid"] == printerid).count().run() > 0
		), "No existe la impresora en tabla printers"
		cuantos = (
			ptable.filter(rdb.row["user"] == usuario)
			.filter(rdb.row["printerid"] == printerid)
			.count()
			.run()
		)
		print("cuantos ", cuantos)
		if cuantos:
			print("updating")
			if copies == 0:

				ptable.filter(rdb.row["user"] == usuario).filter(
					rdb.row["printerid"] == printerid
				).delete().run()
			else:
				ptable.filter(rdb.row["user"] == usuario).filter(
					rdb.row["printerid"] == printerid
				).update(dict(copies=copies)).run()
		else:
			print("inserting")
			ptable.insert(dict(printerid=printerid, user=usuario, copies=copies)).run()
		return dict(success="1")

	except:
		print_exc()
		return dict(success="0")


def printers_info(user=None, id=None, match=True):
	try:
		rdb.connect(
			cached_results.settings.get("rethinkdb.host"),
			cached_results.settings.get("rethinkdb.port"),
		).repl()
		ptable = rdb.db("printing").table("userprinter")
		printers = rdb.db("printing").table("printers")
		config = dict()
		emptyDic = dict()
		print("user vale", user)
		#este es es un hack para evirtar el cloudspooler
		return []
		if user:
			print(paint.yellow("printers_info user is ... {}".format(user)))
			for x in ptable.filter(rdb.row["user"] == user.get("id")).run():
				config[x.get("printerid")] = dict(
					copies=x.get("copies", 0), email=x.get("email", 0)
				)
		p = cloudSpooler.getPrinters()
		ps = []
		for i, x in enumerate(p, 1):
			if "google" not in x:
				if match and printers.filter(dict(printerid=x)).count().run() == 0:
					continue
				else:
					if id and x != id:
						continue
					y = p[x]
					status = cloudSpooler.getPrinterStatus(x)
					ps.append(
						dict(
							id=i,
							printerid=x,
							name=y.get("name", ""),
							displayname=y.get("displayName", ""),
							status=status,
							timestamp=datetime.now().isoformat(),
							copies=config.get(x, emptyDic).get("copies", 0),
						)
					)
		return ps
	except:
		print_exc()
		return dict(error="error")


@view_config(route_name="foo", renderer="json", request_method="GET")
def foo(request):
	return dict(foo="foo", bar=len(cached_results.dicAuthToken))


@view_config(route_name="lotespinares", renderer="json", request_method="GET")
def lotespinares(request):
	rp = request.params
	try:
		etapa = rp.get("etapa", "")
	except:
		pass
	sql = """
	select i.fk_etapa as etapa, preciopormetro as preciopormetro,
	i.iden1 as manzana, i.iden2 as lote, coalesce(c.fk_inmueble, '') as estatus 
	from inmueble i left join cuenta c
	on  i.codigo = c.fk_inmueble
	where i.fk_etapa in ({})""".format(
		etapa
	)
	return dict(lotes=[])
	resultado = []
	for x in DBSession2.execute(sql):
		manzana = x.manzana
		idval = "{}{}".format(manzana, x.lote)
		resultado.append(
			dict(
				id=idval,
				manzana=manzana.strip(),
				lote=x.lote.strip(),
				etapa=x.etapa,
				estatus=x.estatus,
				preciopormetro=float(f"{x.preciopormetro}"),
			)
		)
	DBSession2.close()
	response = Response(body=dict(lotes=resultado))
	response.headers.update({
            'Access-Control-Allow-Origin': '*',
    })
	return response
	#return dict(lotes=resultado)


@view_config(route_name="routeauth", renderer="json", request_method="POST")
def routeauth(request):
	tname = "zen_token_hub"
	tname1 = "zen_routes_log"
	tname2 = "zen_features"
	rp = request.params
	cr = cached_results
	features = dict()
	crs = cr.settings
	valid_in_header, token_in_header = get_token(request)
	default_token = ""
	if valid_in_header:
		default_token = token_in_header
	try:
		token = rp.get("token", default_token)
		route = rp.get("route", "")
	except:
		pass
	valid = "1"
	usuario = cr.dicTokenUser.get(token).get("usuario", "")
	appid = cr.dicTokenUser.get(token).get("id", "")
	perfil = cr.dicTokenUser.get(token).get("perfil", "")
	try:
		assert token != "", "Empty token"
		assert route != "", "Empty route"
		assert route in cr.dicTokenUser.get(token).get("routes"), "Route not valid"
		assert usuario, "Usuario not valid"
		assert perfil, "Perfil not valid"
	except:
		print_exc()
		valid = "0"

	if perfil != "admin" and route == "mantenimientoprecios":
		for mi in rdb.db("iclar").table("menuitems").filter(dict(item=route)).run():
			try:
				assert mi.get("suspendido", False) is False, "ruta bloqueada"
			except:
				print_exc()
				valid = "0"

	if valid == "1":
		rdb.connect(crs.get("rethinkdb.host"), crs.get("rethinkdb.port")).repl()
		try:
			ts = rdb.expr(datetime.now(rdb.make_timezone("00:00")))
			rdb.db("iclar").table(tname).filter(rdb.row["token"] == token).update(
				dict(route=route)
			).run()
			rdb.db("iclar").table(tname1).insert(
				dict(usuario=usuario.lower(), route=route, timestamp=ts, token=token)
			).run()
			color("actualizando log para {} en ruta {}".format(usuario, route))
			for row in rdb.db("iclar").table(tname2).filter(dict(route=route)).run():
				f = row.get("feature", "")
				# isInFeature = False
				value = False
				if f:
					users = row.get("users", [])
					if appid in users:
						# isInFeature = True
						value = row.get("value")
					features[f] = value  # isInFeature

			for row in (
				rdb.db("iclar").table(tname2).filter(dict(route="application")).run()
			):
				f = row.get("feature", "")
				# isInFeature = False
				value = False
				if f:
					users = row.get("users", [])
					if appid in users:
						# isInFeature = True
						value = row.get("value")

					features[f] = value  # isInFeature

			soy = cr.settings.get("pyraconfig", "indefinido")
			req = "{}.{} {} / {}".format(soy, today(), usuario.lower(), route)
			color(req, "c")
			c = redis.Redis(
				host=crs.get("redis.host"), port=crs.get("redis.port"), db=0
			)
			c.publish("pyramid.routeauth", req)

		except:
			print_exc()
	return dict(access=valid, features=features)


@view_config(route_name="revoke", renderer="json", request_method="POST")
def revoke(request):
	print("revoking")
	tname = "zen_token_hub"
	cr = cached_results
	token = request.params.get("token", "")
	# print "el token es ", token
	try:
		if token:
			user = cr.dicTokenUser.get(token)
			usuario = user.get("usuario", "")
			rdb.connect(
				cr.settings.get("rethinkdb.host"), cr.settings.get("rethinkdb.port")
			).repl()
			rdb.db("iclar").table("usuarios").filter(
				dict(usuario=usuario.upper())
			).update(dict(isTwoFactorAuthenticated=False)).run()
			rdb.db("iclar").table(tname).filter(rdb.row["token"] == token).update(
				dict(active=False)
			).run()
			del cr.dicAuthToken[token]
			del cr.dicTokenUser[token]
		else:
			print("el token no existe")
	except:
		print_exc()


def get_google_token(code, refresh=False, source=None):
	print(paint.blue("code google {}".format(code)))
	success = True
	rdb.connect(
		cached_results.settings.get("rethinkdb.host"),
		cached_results.settings.get("rethinkdb.port"),
	).repl()
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

			d = dict(
				refresh_token=tk,
				client_id=CLIENT_ID,
				client_secret=CLIENT_KEY,
				grant_type="refresh_token",
			)
		else:
			d = dict(
				code=code,
				client_id=CLIENT_ID,
				client_secret=CLIENT_KEY,
				redirect_uri=ICLAR_REDIRECT_URI,
			)
			d["grant_type"] = "authorization_code"

		resul = requests.post(URL, data=d).json()
		print("resul from oauth is ,", resul)
		if source:
			print(paint.blue("source is {}".format(source)))
		access_token = resul.get("access_token", "")
		assert access_token, "no hay token"
		refresh_token = resul.get("refresh_token", "")
		if redis_conn.get("google_code") != code:
			redis_conn.set("google_code", code)
			db.table("google_code").insert(dict(code=code)).run()
		redis_conn.set("google_token", access_token)
		ts = rdb.expr(datetime.now(rdb.make_timezone("00:00")))
		db.table("google_token").insert(
			dict(token=access_token, active=True, timestamp=ts)
		).run()
		if refresh_token:
			redis_conn.set("google_refresh_token", refresh_token)
			db.table("google_refresh_token").insert(dict(token=refresh_token)).run()
	except:
		print_exc()
		success = False
	return dict(success=success)


@view_config(route_name="oauth", renderer="json", request_method="GET")
def oauth(request):
	rqp = request.params
	print("oauth content from google, ", rqp)
	code = rqp.get("code", "")
	try:
		assert code, "no charcha"
		return get_google_token(code)
	except AssertionError as e:
		print_exc()
		return dict(success=False)


@view_config(route_name="refreshtoken", renderer="json", request_method="POST")
def refreshtoken(request):
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
			return get_google_token(code, True)
		except:
			print_exc()
	return dict(success=False)


@view_config(route_name="logosmall")
def logosmall(request):
	archivo = "logosmall.jpg"
	if request.params.get("engris", ""):
		archivo = "logo55gris.jpg"

	response = FileResponse(
		"/home/smartics/pyramidzen/zen/{}".format(archivo),
		request=request,
		content_type="image/jpeg",
	)
	return response


@view_config(route_name="token", renderer="json", request_method="POST")
def token(request):
	return general_token(request)


def general_token(request, version=1):
	rqp = request.params
	email = str(rqp.get("username", ""))
	password = rqp.get("password", "")
	color("cachando {} {}".format(email, password))
	cr = cached_results
	gravatar_email = str(rqp.get("gravatar_email", ""))
	# rdb.connect(cr.settings.get("rethinkdb.host"), cr.settings.get("rethinkdb.port")).repl()
	rdb.connect(cr.settings.get("127.0.0.1"), cr.settings.get("rethinkdb.port")).repl()
	usuarios = rdb.db("iclar").table("usuarios")
	gravatar = ""
	usuarioActivo = False
	for x in usuarios.filter(rdb.row["usuario"] == email.upper()).run():
		print("usuario ", email.upper(), "password", x["password"])
		try:
			usuarioActivo = x.get("activo", False)
			gravatar = x["gravatar"]
		except:
			print_exc()
	try:
		assert "" not in (email, password), "Credenciales vacias"
		assert usuarioActivo, "Usuario inactivo"
		print("usuario es activo")
	except AssertionError as e:
		request.response.status = 401
		return dict(error=e.args[0])
	try:
		assert (
			rdb.db("iclar")
			.table("usuarios")
			.filter(rdb.row["usuario"] == email.upper())
			.filter(rdb.row["password"] == password)
			.count()
			.run()
			== 1
		), "Credenciales invalidas"

	except AssertionError as e:
		print_exc()
		request.response.status = 401
		return dict(error=e.args[0])
	menuitems = []
	for x in (
		rdb.db("iclar")
		.table("usuarios")
		.filter(rdb.row["usuario"] == email.upper())
		.filter(rdb.row["password"] == password)
		.run()
	):
		id_user = int(x["appid"])
		domains = x["domains"]
		try:
			zen_profile = x["zen_profile"]
		except:
			zen_profile = ""
		try:
			menuitems = x["menuitems"]
		except:
			menuitems = []
	try:
		assert zen_profile != "", "Perfil invalido"
	except AssertionError as e:
		print_exc()
		request.response.status = 401
		return dict(error=e.args[0])

	routes = []
	for x in (
		rdb.db("iclar")
		.table("zen_profiles")
		.filter(rdb.row["profile"] == zen_profile)
		.run()
	):
		routes = x["routes"]
		profiles_menuitems = x.get("menuitems", [])
		if len(menuitems) == 0:
			menuitems = profiles_menuitems
		else:
			for el in profiles_menuitems:
				if el not in menuitems:
					menuitems.append(el)
	gerente = 0
	try:
		if (
			zen_profile
			in "admin direccioncomercial subdireccioncomercial comercial presidencia auxiliarpresidencia subdireccion cobranza auxiliarsubdireccion finanzas direccion auxiliardireccion especialcomercial recepcion recursoshumanos gestionurbana direccioncontable subdireccioncontable gestioncrediticia mercadotecnia"
		):
			vendedor = 0
		elif zen_profile == "vendedor":
			for x in (
				rdb.db("iclar")
				.table("usuariosvendedores")
				.filter(rdb.row["usuario"] == email.upper())
				.run()
			):
				vendedor = x["vendedor"]
			v = DBSession.query(Vendedor).get(int(vendedor))
			gerente = v.gerente
		elif zen_profile == "gerente":
			for x in (
				rdb.db("iclar")
				.table("usuariosgerentes")
				.filter(rdb.row["usuario"] == email.upper())
				.run()
			):
				gerente = int(x["gerente"])
				vendedor = 0
		else:
			raise ZenError(1)
	except:
		print_exc()
		request.response.status = 401
		return dict(error="error en definicion de perfil")

	if gravatar_email:
		try:
			gravatar = md5(gravatar_email).hexdigest()
			rdb.db("iclar").table("usuarios").filter(
				rdb.row["usuario"] == email.upper()
			).update(dict(gravatar=gravatar)).run()
			print("gravatar actualizado")
		except:
			print("fallo actualizacion del gravatar")

	auth_token = request.session.new_csrf_token()
	request.session["auth_token"] = auth_token
	tname = "zen_token_hub"
	tname2 = "zen_track_casas_ofertas"
	tname3 = "zen_track_prospectos"
	try:
		rdb.db("iclar").table_create(tname).run()
	except:
		pass

	cr.dicAuthToken[auth_token] = True
	cr.dicTokenUser[auth_token] = dict(
		id=id_user,
		gravatar=gravatar,
		routes=routes,
		menuitems=menuitems,
		gerente=gerente,
		vendedor=vendedor,
		usuario=email,
		perfil=zen_profile,
	)

	try:
		ts = rdb.expr(datetime.now(rdb.make_timezone("00:00")))
		rdb.db("iclar").table(tname).insert(
			dict(
				usuario=email.upper(),
				token=auth_token,
				created=datetime.now().isoformat(),
				timestamp=ts,
				active=True,
				dic_token_user=cr.dicTokenUser[auth_token],
			)
		).run()
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

	color(auth_token, "y")
	color(cr.dicTokenUser[auth_token])
	request.session["user_id"] = id_user
	if version == 2:
		return dict(expires_in=1000, refresh_token=auth_token)
	return dict(access_token=auth_token)


def gcp():
	conf_file = "./zen/conf.json"
	params = json.load(open(conf_file))
	params["conf_file"] = conf_file
	cloudSpooler = CloudSpooler(params["email"], params["password"], params["OAUTH"])
	return cloudSpooler


class CachedResults(object):
	def __init__(self):
		self.dicAuthToken = ExpiringDict(max_len=300, max_age_seconds=3600)
		self.dicTokenUser = dict()
		self.settings = dict()


cached_results = CachedResults()
cloudSpooler = gcp()
