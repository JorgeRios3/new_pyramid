import redis, sys, json, argparse
import pickle as pickle
from datetime import datetime
import msgpack as mp
#parser = argparse.ArgumentParser("NBB client")
#parser.add_argument("-t", "--test", action = "store_true", help = "Init to Test Databases")
#parser.add_argument("-T", "--table", help = "Table usend in columns function")
#parser.add_argument("-s", "--server", help = "Redis Server")
#parser.add_argument("-p", "--port", help = "Redis Server Port")
#args = parser.parse_args()
#if args.test:
#	db = 1
#else:
#	db = 0

#host, port, db  = "10.0.1.107", 6379, 0
PREFIX = "elixir"
#if args.server:
#	host = args.server

#if args.port:
#	host = int(args.port)
r = None

def start(prod=False):
    global r
    db = 1
    if prod:
        db = 8
    r = redis.Redis(host = "10.0.1.124", port = 6379, db = db )

def nbb_process_request(func = "dummy", source = "test", user = "test", arguments = None, msgpack = False):
	k = r.incr("{}-nbb-key".format(PREFIX))
	nk = "{}-P{:010d}".format(PREFIX,k)
	rk = "{}-R{:010d}".format(PREFIX,k)
	d = dict(func = func, source = source, user = user)
	if arguments and isinstance( arguments, dict):
		d["args"] = arguments
	if msgpack:
		rk = "msgpack:{}".format(rk)


	msg = json.dumps( d )
	r.set(nk, msg)
	r.rpush("{}-nbb-queue".format(PREFIX),k)
	ps = r.pubsub()
	ps.subscribe(rk)

	result = ""
	for x in ps.listen():
		if x and x.get("type") == "message":
			result = x.get("data")
			ps.unsubscribe(x.get("channel"))
	if msgpack:
		result = json.dumps(mp.unpackb(result))
	return result

def nbb_one_way_process_request(func = "dummy", source = "test", user = "test", arguments = None):
	k = r.incr("{}-nbb-key".format(PREFIX))
	nk = "{}-P{:010d}".format(PREFIX,k)
	d = dict(func = func, source = source, user = user)
	if arguments and isinstance( arguments, dict):
		d["args"] = arguments
	msg = json.dumps( d )
	r.set(nk, msg)
	r.rpush("{}-nbb-queue".format(PREFIX),k)
	return

process_request = nbb_process_request
one_way_process_request = nbb_one_way_process_request

def test_snippets():
	thetable = "users"
	if args.table:
		thetable = args.table

	#now testing from snippets
	for x in "cuantos_usuarios cuantos_usuarios_2 columns".split():
		print("testing {}".format(x))
		if x == "columns":
			d = dict( table = thetable )

			print(process_request( request = x, source = "bar", user = "foo", arguments = d ))
		else:
			print(process_request( request = x, source = "bar", user = "foo" ))

def test_arcadia():
	print(process_request( request = "lotes_arcadia", source = "bar", user = "foo"))
	resul = json.loads(process_request( request = "lotes_disponibles_arcadia", source = "bar", user = "foo"))
	for x in resul:
		print(x, type(x))
		for val in x:
			print(val)

def cartera_vencida_arcadia(cliente = None):
	if cliente:
		print(process_request( request = "cartera_vencida_arcadia_por_cliente", source = "bar", user = "foo", arguments = dict(cliente = cliente)))
	else:
		print(process_request( request = "cartera_vencida_arcadia", source = "bar", user = "foo"))

def analisis_cartera_arcadia():
	return json.loads(process_request( request = "analisis_cartera", source = "bar", user = "foo"))

def venta_por_vendedor_arcadia_terrenos():
	print("entro aqui venta_por_vendedor_arcadia")
	print(process_request( func = "pruebajorge", source = "zen", user = "zen", arguments = dict(vendedor = "0", con_enganche = False)))
	#print process_request( func = "venta_por_vendedor_arcadia_terrenos", source = "zen", user = "zen", arguments = dict(vendedor = "0", con_enganche = False))

def venta_por_vendedor_arcadia(vendedor = 0, con_enganche = False):
	print("acaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
	#print process_request( request = "venta_por_vendedor_arcadia", source = "zen", user = "zen", arguments = dict( vendedor = vendedor, con_enganche = con_enganche))
	#print process_request("cartera_vencida_arcadia", "bar", "foo")
	#p = json.loads(process_request( func = "venta_por_vendedor_arcadia", source= "zen", user="zen", arguments = dict(vendedor = vendedor, con_enganche = con_enganche)))
	p = json.loads(process_request( func = "tablecount", source= "zen", user="foo", arguments = dict(table = "usuarios")))
	print(("viendo la pppp", p))

def resumen_cobranza_am(hardCall = True):
	RESUMEN_FILE = "rescobarcadia.p"
	if hardCall:
		with open(RESUMEN_FILE, "wb") as f:
			pickle.dump([datetime.now().isoformat(),json.loads(process_request( request = "resumen_cobranza_am", source = "bar", user = "foo"  ))],f)

	with open(RESUMEN_FILE, "rb") as f:
		resumencobranza = pickle.load(f)
	return (resumencobranza[0], resumencobranza[1])

def resumen_operativo_mobile():
	one_way_process_request( request = "resumen_operativo_mobile", source = "bar", user = "foo" )
	print("peticion enviada")

def duo():
	p = process_request( request = "duosecurity_sign_request", source = "bar", user = "foo", arguments = dict(user = "smartics"))
	d = json.loads(p)
	value = d.get("value", "shit")
	print(value)
	p  = process_request( request = "duosecurity_verify_response", source = "bar", user = "foo", arguments = dict(sig_response = value ))

	d = json.loads(p)
	value = d.get("value", "shit")
	print(value)

def mfa_devices(cual):
	p = process_request( request = "get_mfa_devices", source= "bar", user="foo", arguments = dict( usuario = "CESAR"))
	print("hey", json.loads(p))
	p = process_request( request = "get_mfa_device", source= "bar", user="foo", arguments = dict( usuario = "CESAR", index=cual))
	print("hey", json.loads(p))

def vaciado_lotes_arcadia():
	p = process_request( request = "vaciado_lotes_arcadia", source= "bar", user="foo")
	print("hey", len(  p ))
	print(p)

def arcadia_dropbox():
	one_way_process_request( request = "arcadia_dropbox", source= "bar", user="foo")
	return
	
def tableCount(table = "usuarios", source="bar"):
	p = json.loads(process_request( func = "tablecount", source= source, user="foo", arguments = dict(table = "usuarios")))
	return p

def tableCountMany(source="bar"):
	p = json.loads(process_request( func = "tablecountmany", source= source, user="foo", arguments = dict(tables = ["usuarios", "menuitems", "zen_profiles", "historia_resumen", "inmueble", "matriz_tramites" ])))
	return p

def saldoporcuenta(cuenta="", source="bar"):
	p = json.loads(process_request( func = "saldoporcuenta", source= source, user="foo", arguments = dict(cuenta =cuenta)))
	return p

def mssqltableCount(source="bar"):
	p = json.loads(process_request( func = "mssqltablecount", source= source, user="foo", arguments = dict(table = "cuenta")))
	return p

def resumenoperativo(source="bar"):
	p = json.loads(process_request( func = "resumenoperativo", source= source, user="foo"))
	return p

if __name__ == "__main__":
	start()
	#from datetime import datetime
	hoy = datetime.now().isoformat()
	print(hoy)
	for x in range(2):
		mssqltableCount(hoy)
		tableCountMany(hoy)
		tableCount(hoy)
		saldoporcuenta(30394, hoy)
		resumenoperativo(hoy)
	#process_request()
	#test_snippets()
	#test_arcadia()
	#cartera_vencida_arcadia()
	#cartera_vencida_arcadia(172)
	#analisis_cartera_arcadia()
	#venta_por_vendedor_arcadia_terrenos()
	#resumen_cobranza_am()
	#resumen_operativo_mobile()
	#duo()
	#mfa_devices(0)
	#mfa_devices(1)
	#vaciado_lotes_arcadia()
	#arcadia_dropbox()

