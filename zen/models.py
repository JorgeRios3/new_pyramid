from sqlalchemy import (
	Column,
    	Integer,
    	String,
    	Text,
    	Unicode,
    	Date,
    	Boolean,
    	Numeric,
    	ForeignKey,
	DateTime,
	literal_column,
	func,
	distinct,
	or_,
	and_
    	)

from sqlalchemy.ext.declarative import declarative_base

from sqlalchemy.orm import (
    	scoped_session,
    	sessionmaker,
	joinedload,
    	relationship
    	)
from sqlalchemy.ext.hybrid import hybrid_property
from zope.sqlalchemy import register
import pytz
from datetime import datetime
import traceback

def d_e( what ):
	try:
		return what.decode("iso-8859-1").encode("utf-8")
	except:
		traceback.print_exc()
		return ""

def fecha_local( fecha ):
	"""
	ember-data hace no se que que cambia las fechas por ende me desisto por lo pronto de usar el tipo date
	y en su lugar lo hago tipo string , de cualquier manera es para efectos de consulta y no de 
	grabacion por el momento .  Esta funcion empezo de otra manera y acabo asi pero aun probando distintos escenarios no se pudo 
	"""
	timezone = "America/Mexico_City"
	local_timezone = pytz.timezone( timezone )
	fmt = '%Y-%m-%d %H:%M:%S %Z%z'
	fmt = '%Y-%m-%d'
	f = fecha 
	#tzorigen = pytz.utc si en la base estuviera en UTC aqui seria asi
	# tzorigen = local_timezone # por ende no hay confersion pero se aprovecha la funcion para formatear
	# fecha = datetime( f.year, f.month, f.day, f.hour, f.minute, f.second, f.microsecond, tzinfo = local_timezone)
	# return fecha.astimezone( pytz.utc ).strftime( fmt )

DBSession = scoped_session(sessionmaker())
Base = declarative_base()
DBSession2 = scoped_session(sessionmaker())
Base2 = declarative_base()
session = DBSession

class Mfa_Device( Base ):
	__tablename__ = "MFA_DEVICE"
	id = Column( Integer,  primary_key = True)
	id_app_user = Column( Integer, ForeignKey("MFA_USER.id" ), nullable = False)
	iam_username = Column( String( 50) , nullable = False)
	description = Column( String(70), nullable = False)
	arn = Column( String( 70 ), nullable = False)
	access_key = Column( String( 50) , nullable = False)
	secret_key = Column( String( 50) , nullable = False)
	def __repr__( self ):
		try:
			return "Mfa_Device ( instance ) : id={} id_app_user={} iam_username={} description={} arn={} access_key={} secret_key={}".format(self.id, self.id_app_user, self.iam_username, self.description, self.arn, self.access_key, self.secret_key )
		except:
			return "Mfa_Device ( instance ) : invalid"

class Mfa_User( Base ):
	__tablename__ = "MFA_USER"
	id = Column( Integer,  primary_key = True)
	user = Column( String( 50 ), nullable = False, unique = True)
	active = Column( Boolean, default = True )
	devices = relationship( Mfa_Device, primaryjoin = id == Mfa_Device.id_app_user, order_by = Mfa_Device.id, backref = "myuser" )


	def __repr__( self ):
		try:
			return "Mfa_User (instance) : id={} user={} active={} number_of_devices={}".format(self.id, self.user, self.active, len(self.devices) )
		except:
			return "Mfa_User (instance) : invalid"


	@property
	def cornice_json( self ):
		return dict ( id = self.id , user = self.user, active = self.active, devices_count = len( self.devices )) 

	@classmethod
	def active_users( cls ):
		try:
			users = [ str(x.user) for x in session.query(cls).filter( cls.active == True).order_by(cls.user) ]
		except:
			#traceback.print_exc()
			users = []
		return users

	@classmethod
	def valid_user( cls, user ):
		try:
			assert session.query(cls).filter( cls.user == user.upper() ).count() == 1
			return True
		except:
			return False

	@classmethod
	def user_status( cls, user ):
		try:
			assert session.query(cls).filter( cls.user == user.upper() ).filter(cls.active == True).count() == 1
			return True
		except:
			return False

	@classmethod
	def toggle_user( cls, user ):
		status = False
		try:
			u = session.query(cls).filter( cls.user == user.upper() ).one()
			u.active = not u.active
			status = u.active
			session.add( u )
			session.commit()
		except:
			pass
		return status

	@classmethod
	def byName( cls, user):
		"""
		return Mfa_User record given a user name, None if not present
		"""
		try:
			u = session.query(cls).filter( cls.user == user.upper() ).one()
			return u
		except:
			return None

	@classmethod
	def byId( cls, id):
		"""
		return Mfa_User record given a user id, None if not present
		"""
		try:
			u = session.query(cls).get( int( id ))
			return u
		except:
			return None


class CPImport(Base):
	__tablename__ = 'cpimport'
	id = Column(Integer, primary_key=True)
	d_codigo = Column( Text )
	d_asenta = Column( Text )
	d_tipo_asenta = Column( Text )
	d_mnpio = Column( Text )
	d_estado = Column( Text )
	d_ciudad = Column( Text )
	d_cp = Column( Text )
	c_estado = Column( Text )
	c_oficina = Column( Text )
	c_cp = Column( Text )
	c_tipo_asenta = Column( Text )
	c_mnpio = Column( Text )
	id_asenta_cpcons = Column( Text )
	d_zona = Column( Text )
	c_cve_ciudad = Column( Text )


class CPEstado(Base):
	__tablename__ = 'cp_estado'
	id = Column( Integer, primary_key = True )
	descripcion = Column( String )

class CPMunicipio(Base):
	__tablename__ = 'cp_municipio'
	id = Column( Integer, primary_key = True )
	idestado = Column( Integer, ForeignKey("cp_estado.id" ), nullable = False)
	descripcion = Column( String )
	estado = relationship("CPEstado")

class CPCiudad(Base):
	__tablename__ ='cp_ciudad'
	id = Column( Integer, primary_key = True )
	idmunicipio = Column( Integer, ForeignKey("cp_municipio.id" ), nullable = False)
	descripcion = Column( String )
	municipio = relationship("CPMunicipio")

class CPCodigoPostal(Base):
	__tablename__ ='cp_codigo_postal'
	id = Column( Integer, primary_key = True )
	idmunicipio = Column( Integer, ForeignKey("cp_municipio.id" ), nullable = False)
	codigo_postal = Column( String(6))
	asentamiento = Column( String)
	tipo_asentamiento = Column( String )
	municipio = relationship("CPMunicipio")

class CuentaPagare( Base ):
	__tablename__ = "cuenta_pagare"
	codigo  = Column( Integer,  primary_key = True)
	#fk_documento = Column( Integer, ForeignKey("documento.codigo" ))
	fk_documento = Column( Integer)
	#fk_cuenta = Column( Integer, ForeignKey("cuenta.codigo" ))
	fk_cuenta = Column( Integer)
	saldo = Column(Numeric(14,3) )
	#documentos = relationship( "DocumentoPagare", backref = "mi_cuenta", order_by = "DocumentoPagare.codigo")

	@property
	def id(self):
		return self.codigo
	@property
	def documento(self):
		return self.fk_documento

	@property
	def cuenta(self):
		return self.fk_cuenta

	@classmethod
	def saldoglobal(cls):
		return session.query( func.sum(cls.saldo ) ).one()[0]

	@property
	def saldoformateado(self):
		formato_comas = "{:,.2f}"
		return formato_comas.format( self.saldo ) 


	@property
	def saldovencido(self):
		dias = 30
		q = DBSession.query( func.isnull(func.sum(DocumentoPagare.saldo ),0) ).filter( DocumentoPagare.fk_cuenta == self.codigo ).filter( func.datediff( literal_column('d'), DocumentoPagare.fechven, func.getdate() ) >= dias )
		#print str(q)
		r = q.one()[0]
		if r is None:
			return 0
		else:
			return r
	@classmethod
	def all_alterno( cls ):
		try:
			q = DBSession.query( CuentaPagare, func.isnull(func.sum(DocumentoPagare.saldo),0).alias("saldovencido2") ).join( DocumentoPagare ).options( joinedload( CuentaPagare.codigo )).filter(func.datediff(literal_column('d'))).group_by(CuentaPagare.codigo)
		except:
			q = "error en CuentaPagare.all_alterno"
			traceback.print_exc()
		print(q)
		return q

	@property
	def cornice_json( self ):

		warning = "1"
		if self.saldo == self.lacuenta.saldo:
			if self.saldoformateado == self.documento17.saldoformateado:
				warning = ""
		q = DBSession.query( DocumentoPagare ).filter( DocumentoPagare.fk_cuenta == self.codigo ).order_by( DocumentoPagare.codigo)
		#observacion importante tuve que crear un vw_documento_pagare porque el tamano de ciertos nombre de campo era muy grande de acuerdo al api de acceso a base de datos ( tal vez el pyodbc o el freetds abajo de el )
		documentos  = [ x.cornice_json for x in q.all()] 
		abonosdocumento17 = dict()
		if True:
			q = DBSession.query( Movimiento ).\
				filter( Movimiento.fk_documento == self.documento ).\
				filter( Movimiento.cargoabono == 'A').order_by( Movimiento.codigo )
			abonosdocumento17  = [ x.cornice_json for x in q.all()]
		#print "abonos ", len(abonosdocumento17)

		formato_comas = "{:,.2f}"
		sv = self.saldovencido
		if sv > 0:
			saldovencido = formato_comas.format( self.saldovencido )
		else:
			saldovencido = ""
		d = dict ( id = self.codigo , documento = self.documento, cuenta = self.cuenta, saldo = self.saldo, saldoformateado = self.saldoformateado, clavecliente = self.lacuenta.cliente.codigo , nombrecliente = self.lacuenta.cliente.nombre.decode("ISO-8859-1").encode("UTF-8"), fechacuenta = self.lacuenta.fecha.strftime("%Y-%m-%d"), saldocuenta = self.lacuenta.saldo, saldocuentaformateado = "{:,.2f}".format(self.lacuenta.saldo ), casa = "{} {}".format(self.lacuenta.inmueble.iden2, self.lacuenta.inmueble.iden1), pagares = DocumentoPagare.cuantos_pagares( self.codigo ), pagaresconsaldo  = DocumentoPagare.cuantos_pagares( self.codigo, True ) , saldodocto17formateado = self.documento17.saldoformateado, warning = warning, telefonocasa = self.lacuenta.cliente.telefonocasa or "", telefonotrabajo = self.lacuenta.cliente.telefonotrabajo or "", email = self.lacuenta.cliente.email or "", documentos = documentos , saldovencido = saldovencido, abonosdocumento17 = abonosdocumento17 )
		#print "calling conrnice_json with ", d
		return d

class DocumentoPagare( Base ):
	__tablename__ = "vw_documento_pagare"

	codigo  = Column( Integer,  primary_key = True)
	fechela = Column( Date) # forzado esto por problemas con el tamano del nombre
	fechven= Column( Date)
	fechvenvar= Column( Date)
	saldo = Column( Numeric( 14,3 ))
	cargo = Column( Numeric( 14,3 ))
	abono = Column( Numeric( 14,3 ))
	#fk_cuenta = Column( Integer, ForeignKey("cuenta_pagare.codigo" ))
	fk_cuenta = Column( Integer, ForeignKey("cuenta_pagare.codigo" ))

	@property
	def id(self):
		return self.codigo

	@hybrid_property
	def diasvencidos( self ):
		dias = DBSession.query( func.isnull(func.datediff( literal_column('d'), DocumentoPagare.fechven, func.getdate()),0)).filter( DocumentoPagare.codigo == self.codigo).one()[0]
		return dias

	@diasvencidos.expression
	def diasvencidos( self ):
		return func.datediff( literal_column('d') , self.fechven, func.getdate())

	@property
	def vencido(self):
		try:
			#dias = self.diasvencidos
			dias = DBSession.query( func.isnull(func.datediff( literal_column('d'), DocumentoPagare.fechven, func.getdate()),0)).filter( DocumentoPagare.codigo == self.codigo).filter(DocumentoPagare.saldo > 0).one()[0]
			if dias < 0:
				dias = 0   
			return dias
		except:
			return 0

	@property
	def cornice_json(self):
		f = "%Y-%m-%d"
		vencido = 0
		try:
			vencido = self.vencido
		except:
			print("error en la obtencion de documentos vencidos")
			

		formato_comas = "{:,.2f}"
		d  = dict( 	id = self.codigo,
				fechadeelaboracion = self.fechela.strftime(f),
				fechadevencimiento = self.fechven.strftime(f),
				fechadevencimientovar = self.fechvenvar.strftime(f),
				saldo = formato_comas.format( self.saldo ),
				cargo = formato_comas.format( self.cargo ),
				abono = formato_comas.format( self.abono ), vencido = vencido )
		return d

	@classmethod
	def cuantos_pagares( cls , cuenta, con_saldo = False ):
		q = DBSession.query( cls ).filter( cls.fk_cuenta == cuenta )
		if con_saldo:
			q = q.filter( cls.saldo > 0 )
		return q.count()

	@classmethod
	def vencido_a(cls, dias):
		try:
			q = DBSession.query( func.sum(cls.saldo ) ).filter( func.datediff( literal_column('d'), cls.fechven, func.getdate() ) >= dias )
		except:
			traceback.print_exc()
			q = "lo vencido no se pudo hacer query"
		return q.one()[0]

	@classmethod
	def morosos(cls):
		dias = 30
		dias = 1 
		return DBSession.query( cls.fk_cuenta ).filter( func.datediff( literal_column('d'), cls.fechven, func.getdate() ) >= dias ).filter( cls.saldo > 0).distinct().count()
		
	@classmethod
	def implicados(cls):
		return  DBSession.query( cls.fk_cuenta ).distinct().count()
	
	@classmethod
	def abonado(cls):
		return  DBSession.query( func.sum(cls.abono) ).one()[0]

class MovimientoPagare( Base ):
	__tablename__ = "movimiento_pagare"
	codigo  = Column( Integer,  primary_key = True)
	cantidad = Column(Numeric(14,3) )
	fecha = Column( Date)
	relaciondepago = Column( String( 20 ) )
	cargoabono = Column( String(1 ))
	numrecibo = Column( Integer, ForeignKey("recibo_pagare.codigo" ))
	fechavencimientodoc = Column( Date)
	fk_documento = Column( Integer, ForeignKey("documento_pagare.codigo" ))
	fk_tipo = Column( Integer )

	@property
	def id(self):
		return self.codigo

class ReciboPagare( Base ):
	__tablename__ = "recibo_pagare"
	codigo  = Column( Integer,  primary_key = True)
	fechaemision = Column( Date)
	abonocapital = Column(Numeric(14,3) )
	interesmoratorio = Column(Numeric(14,3) )
	totalrecibo = Column(Numeric(14,3) )
	referencia = Column( String( 255 ))
	status = Column( String(1))
	fk_desarrollo = Column( Integer )
	consdesarrollo = Column( Integer )
	devolucion = Column( String(1))
	fechacaptura = Column( Date)

	@property
	def id(self):
		return self.codigo

class Cuenta( Base ):
	"""
        Se parte del criterio de que todo comienza arriba de la 14672
	que fue la cuenta con codigo mas alto que tiene un inmueble en la 39
        """
	__tablename__ = "cuenta"
	codigo  = Column( Integer,  primary_key = True)
	fecha = Column( DateTime )
	fk_cliente = Column( Integer, ForeignKey("cliente.codigo" ))
	fk_inmueble  = Column( Integer, ForeignKey("inmueble.codigo" ))
	saldo = Column(Numeric(14,3) )
	#inmueble = relationship( "Inmueble", backref = "mi_cuenta" )
	#cliente = relationship( "Cliente", backref = "mi_cuenta" )

	def __repr__(self):
		print("Cuenta")

	@property
	def id(self):
		return self.codigo

	@property
	def fecha_local( self ):
		return fecha_local( self.fecha )

	@classmethod
	def consaldo(cls, saldo = 0):
		return session.query( func.count( cls.saldo )).filter( cls.saldo > saldo ).filter(cls.codigo > 14672).one()[0]

	@classmethod
	def sumasaldos( cls, cualquiera = True, sinasignar = False ):
		#return session.query( func.sum( cls.saldo)).filter( cls.inmueble.fk_etapa > 39).one()[0]
		q = session.query( func.isnull(func.sum( cls.saldo),0)).filter(cls.codigo > 14672 )
		if not cualquiera:
			if sinasignar:
				q = q.filter( or_(cls.fk_inmueble == None, cls.fk_inmueble == 0) )
			else:
				q = q.filter( and_(cls.fk_inmueble != None, cls.fk_inmueble != 0) )
		#print q
		cuanto = q.one()[0]
		#print cuanto
		return cuanto


class Documento( Base ):
	__tablename__ = "documento"
	codigo  = Column( Integer,  primary_key = True)
	fk_tipo = Column( Integer )
	saldo = Column(Numeric(14,3) )

	@classmethod
	def saldoglobal(cls):
		return session.query( func.sum(cls.saldo ) ).filter( cls.fk_tipo == 17).one()[0]

	@classmethod
	def deudores(cls):
		return session.query( func.count(cls.saldo ) ).filter( cls.fk_tipo == 17).filter(cls.saldo > 0 ).one()[0]

	@property
	def saldoformateado(self):
		formato_comas = "{:,.2f}"
		return formato_comas.format( self.saldo ) 

	
class Movimiento( Base ):
	__tablename__ = "movimiento"
	codigo = Column( Integer, primary_key = True)
	numrecibo = Column( Integer )
	fecha = Column( DateTime )
	cantidad = Column( Numeric(14,3))
	fk_documento = Column( Integer )
	cargoabono = Column( String(1))

	@property
	def cornice_json(self):
		f = "%Y-%m-%d"
		formato_comas = "{:,.2f}"
		d  = dict( 	id = self.codigo,
				recibo = self.numrecibo,
				fecha = self.fecha.strftime(f),
				cantidad = formato_comas.format( self.cantidad ) )
		return d


class Cliente( Base ):
	__tablename__ = "cliente"
	codigo  = Column( Integer,  primary_key = True)
	nombre = Column( String(100 ))
	rfc = Column( String(15))
	telefonocasa = Column( String(15))
	telefonotrabajo = Column( String(15))
	email = Column( String(60))

	@property
	def id(self):
		return self.codigo

class Inmueble( Base ):
	__tablename__ = "inmueble"
	codigo  = Column( Integer,  primary_key = True)
	iden2 = Column(String(3))
	iden1 = Column(String(3))
	fk_etapa = Column( Integer )
	#etapa = relationship( "Etapa" )

	@property
	def id(self):
		return self.codigo

class Etapa( Base ):
	__tablename__ = "etapa"
	codigo  = Column( Integer,  primary_key = True)
	descripcion = Column( String(50 ) )
	
	@property
	def id(self):
		return self.codigo

class GerentesVentas( Base ):
	__tablename__ = "gerentesventas"
	codigo = Column( Integer, primary_key = True)
	nombre = Column( String(50))
	activo = Column( Boolean )
	prospectador = Column( Boolean )

	@property
	def id(self):
		return self.codigo

	@property
	def cornice_json(self):
		return dict( id = self.id, nombre = self.nombre, activo = self.activo, prospectador = self.prospectador)
		
class Vendedor( Base ):
	__tablename__ = "vendedor"
	codigo = Column( Integer, primary_key = True)
	nombre = Column( String(50))
	gerente = Column( Integer)
	desactivado = Column( Boolean)

	@property
	def id(self):
		return self.codigo

	@property
	def cornice_json(self):
		return dict( id = self.id, nombre = d_e(self.nombre), gerente = self.gerente)
	
class MedioPublicitario( Base ):
	__tablename__ = "gixmediospublicitarios"
	idmediopublicitario = Column( Integer, primary_key = True)
	descripcion = Column( String(50))
	estatus = Column( String(1))
	
	@property
	def id(self):
		return self.idmediopublicitario

	@property
	def cornice_json(self):
		return dict( id = self.id, nombre = d_e(self.descripcion))


class Prospecto( Base ):
	__tablename__ = "gixprospectos"
	idprospecto = Column( Integer , primary_key = True)
	idgerente = Column( Integer )
	idvendedor = Column( Integer )
	nombre1 = Column( String(60))
	apellidopaterno1 = Column( String(60))
	apellidomaterno1 = Column( String(60))
	fechadenacimiento = Column( DateTime )
	rfc = Column( String(15))
	curp = Column( String(20))
	telefonocasa = Column( String(20))
	telefonooficina = Column( String(20))
	extensionoficina = Column( String(5))
	telefonocelular = Column(String(20))
	lugardetrabajo = Column(String(60))
	cuenta = Column(Integer)
	#nombre = Column(String(50))
	afiliacionimss = Column( String(20))
	#telefonos = Column( String(50) )
	fechaasignacion = Column( DateTime )
	fechacierre = Column( DateTime, nullable = True )
	#fechaseguimiento = Column( DateTime )
	#fechanoseguimiento = Column( DateTime )
	idmediopublicitario = Column( Integer )
	mediopublicitariosugerido = Column( String(150))
	contado = Column( Boolean )
	pensiones = Column( Boolean )
	fovisste = Column( Boolean )
	hipotecaria = Column( Boolean )
	congelado = Column( Boolean )
	#blogGUID = Column( String, nullable = True)


	@property
	def id(self):
		return self.idprospecto

	@classmethod
	def is_luhn_valid(cls, cc):
		print("cc es", cc)
		try:
			assert len(cc) == 11, "diferente de 11 caracteres"
		except:
			print("diferente de 11")
			return False

		for x in cc:
			if x not in "0987654321":
				print("no es caracter valido")
				return False
		num = [int(x) for x in str(cc)]
		return sum(num[::-2] + [ sum(divmod(d * 2, 10)) for d in num[-2::-2]] ) % 10 == 0 

	@classmethod
	def existeAfiliacion(cls, afiliacion):
		try:
			query = session.query(Prospecto).filter(Prospecto.afiliacionimss == afiliacion)
			query = query.filter(Prospecto.congelado == False)
			return query.count() > 0
		except:
			traceback.print_exc()
			return False

	@classmethod
	def existeCurp(cls, curp):
		try:
			query = session.query(Prospecto).filter(Prospecto.curp == curp)
			query = query.filter(Prospecto.congelado == False)
			return query.count() > 0
		except:
			traceback.print_exc()
			return False

		
	@property
	def nombre_gerente(self):
		try:
			g = DBSession.query(GerentesVentas).get(self.idgerente)
			nombre = g.nombre
		except:
			nombre = ""
		return nombre
	
	@property
	def nombre_vendedor(self):

		try:
			g = DBSession.query(Vendedor).get(self.idvendedor)
			nombre = g.nombre
		except:
			nombre = ""
		return nombre

	def fecha_ember(self,fecha):
		fechax = ""
		try:
			fechax = "{:04d}{:02d}{:02d}".format(fecha.year, fecha.month, fecha.day)
		except:
			pass
		return fechax

	def fecha_especial(self, fecha):

       
		meses = "XXX Ene Feb Mar Abr May Jun Jul Ago Sep Oct Nov Dic".split(" ")
		fechax = ""
		try:
			d = fecha.day
			m = fecha.month
			y = fecha.year
			fechax = "{} {}, {}".format( meses[m], d, y)
		except:
			pass
		return fechax 

	@property
	def fechacierre_especial(self):
		return self.fecha_especial(self.fechacierre)
		
	@property
	def fechaasignacion_especial(self):
		return self.fecha_especial( self.fechaasignacion )

	@property
	def fechacierre_ember(self):
		return self.fecha_ember(self.fechacierre)
		
	@property
	def fechaasignacion_ember(self):
		return self.fecha_ember( self.fechaasignacion )

	@property
	def fechadenacimiento_ember(self):
		return self.fecha_ember( self.fechadenacimiento )

		
	@property
	def cornice_json(self):
		d = dict( id = self.id, apellidopaterno = d_e(self.apellidopaterno1),
		 	 apellidomaterno = d_e(self.apellidomaterno1),
		 	 nombre = d_e(self.nombre1),
		 	 afiliacion = self.afiliacionimss,
		 	 fechaalta = self.fechaasignacion_ember,
		 	 fechacierre = self.fechacierre_ember,
		 	 fechadenacimiento = self.fechadenacimiento_ember,
		 	 rfc = self.rfc,
		 	 curp = self.curp,
		 	 telefonocasa = self.telefonocasa,
		 	 telefonooficina = self.telefonooficina,
		 	 extensionoficina = self.extensionoficina,
		 	 telefonocelular = self.telefonocelular,
		 	 lugardetrabajo = self.lugardetrabajo,
		 	 idmediopublicitario = self.idmediopublicitario,
		 	 mediopublicitariosugerido = self.mediopublicitariosugerido,
		 	 contado = self.contado,
		 	 hipotecaria = self.hipotecaria,
		 	 fovisste = self.hipotecaria,
		 	 pensiones = self.pensiones,
		 	 congelado = self.congelado,
		 	 gerente = self.idgerente,
		 	 vendedor = self.idvendedor)
		return d
    	
	@property
	def busq_cornice_json(self):
		d = dict( id = self.id, apellidopaterno = d_e(self.apellidopaterno1),
		 	 apellidomaterno = d_e(self.apellidomaterno1),
		 	 nombre = d_e(self.nombre1),
		 	 afiliacion = self.afiliacionimss,
		 	 fechaalta = self.fechaasignacion_especial,
		 	 fechacierre = self.fechacierre_especial,
		 	 rfc = self.rfc,
		 	 curp = self.curp,
		 	 gerente = d_e(self.nombre_gerente),
		 	 vendedor = d_e(self.nombre_vendedor),
             afiliacionvalida = Prospecto.is_luhn_valid(self.afiliacionimss))

		return d	

	@property
	def reciente_cornice_json(self):
		d = dict( id = self.id, apellidopaterno = d_e(self.apellidopaterno1),
		 	apellidomaterno = d_e(self.apellidomaterno1),
		 	 nombre = d_e(self.nombre1),
		 	 afiliacion = self.afiliacionimss,
		 	 fecha = self.fechaasignacion_especial,
		 	 gerente = d_e(self.nombre_gerente),
		 	 vendedor = d_e(self.nombre_vendedor),
		     cuenta = self.cuenta,
		     afiliacionvalida = Prospecto.is_luhn_valid(self.afiliacionimss))
		 	 

		return d

	@property 
	def foo(self):
		return "foo"

CuentaPagare.lacuenta = relationship( "Cuenta", primaryjoin = CuentaPagare.fk_cuenta == Cuenta.codigo,   foreign_keys = CuentaPagare.fk_cuenta , remote_side = Cuenta.codigo, backref = "mi_cuenta_pagare", viewonly = True)

Cuenta.cliente = relationship("Cliente", primaryjoin = Cuenta.fk_cliente == Cliente.codigo,  foreign_keys = Cuenta.fk_cliente, remote_side = Cliente.codigo, backref = "mi_cuenta", viewonly = True)
Cuenta.inmueble = relationship("Inmueble", primaryjoin = Cuenta.fk_inmueble == Inmueble.codigo,  foreign_keys = Cuenta.fk_inmueble, remote_side = Inmueble.codigo, backref = "mi_cuenta_c", viewonly = True)

CuentaPagare.documento17 = relationship("Documento", primaryjoin = CuentaPagare.fk_documento == Documento.codigo, foreign_keys = CuentaPagare.fk_documento, remote_side = Documento.codigo, backref = "mi_cuenta_alterna" ,viewonly = True )

CuentaPagare.documentos = relationship("DocumentoPagare", primaryjoin = CuentaPagare.codigo == DocumentoPagare.fk_cuenta, foreign_keys = DocumentoPagare.fk_cuenta, remote_side = CuentaPagare.codigo, backref = "mi_cuenta", viewonly = True )
