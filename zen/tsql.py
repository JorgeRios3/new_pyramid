from subprocess import check_output, CalledProcessError
from traceback import print_exc


def inmueblesdisponibles(etapa=0, env=""):
    # print "etapa,url", etapa, env
    if not env:
        return list()
    if env == "prod":
        command_line = "/home/smartics/bup2s3/queryinmuebles.sh"
    else:
        command_line = "/home/smartics/bup2s3/queryinmueblespruebas.sh"
    print "command_line", command_line
    command_list = command_line.split(" ")
    try:
        output = check_output(command_list)
        # print "output mide", len(output)
        principio = False
        a = list()
        for rec, line in enumerate(output.split("\n"), 1):
            print "rec es", rec
            if "codigo" in line:
                principio = True
                # print "principio es True"
                continue
            if principio:
                record = list(line.split("\t"))
                try:
                    xetapa = int(record[0])
                    id = int(record[3])
                    manzana = record[1].strip()
                    lote = record[2].strip()
                    condominio = record[4] or ""
                    domicilioOficial = record[5] or ""
                except:
                    # print_exc()
                    continue
                if etapa and xetapa <> etapa:
                    continue
                a.append(
                    dict(
                        id=id,
                        manzana=manzana,
                        lote=lote,
                        condominio=condominio,
                        domicilioOficial=domicilioOficial,
                    )
                )
                # print "agregando rec", rec

    except CalledProcessError, e:
        # return e.output
        print_exc()
        pass
    except:
        print "tronando por otro lado"
        print_exc()
    # print "a vale", a
    return a
