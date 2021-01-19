import calendar


def psql(sql):
    sql = sql.replace("\n", " ")
    sql = sql.replace("\t", " ")
    return sql


def asignadasporsemana(cn, semanas=10, et="", w=750, h=350):
    semanas = int(semanas)
    c = calendar.Calendar()
    hoy = calendar.datetime.datetime.today()
    y = yr = hoy.year
    m = mt = hoy.month
    d = dy = hoy.day
    if mt == 12:
        mt = 2
        yr += 1
    elif mt == 11:
        mt = 1
        yr += 1
    else:
        mt += 2
    meses = int((semanas / 4) + 4)
    days_interval = []
    for mes in range(meses):
        for dn, wd in c.itermonthdays2(yr, mt):
            if dn:
                days_interval.append([yr, mt, dn, wd])
        if mt == 1:
            mt = 12
            yr -= 1
        else:
            mt -= 1
    days_interval.sort()
    days_interval.reverse()
    union = ""
    orden = semanas
    query = []
    tip = {}
    nmeses = "* Ene Feb Mar Abr May Jun Jul Ago Sep Oct Nov Dic"
    lmeses = nmeses.split()
    additionalWhere = ""
    if et:
        additionalWhere = " and fk_etapa = %s " % et
    for i, v in enumerate(days_interval):
        if v[0] == y and v[1] == m and v[2] == d:
            for j in range(i, len(days_interval)):
                if days_interval[j][3] == 5:
                    for semana in range(semanas):
                        periodo = "Semana del %02d/%s/%04d al %02d/%s/%04d" % (
                            days_interval[j][2],
                            lmeses[days_interval[j][1]],
                            days_interval[j][0],
                            days_interval[j - 6][2],
                            lmeses[days_interval[j - 6][1]],
                            days_interval[j - 6][0],
                        )
                        tip[orden] = periodo
                        fini = "%04d/%02d/%02d" % (
                            days_interval[j][0],
                            days_interval[j][1],
                            days_interval[j][2],
                        )
                        ffin = "%04d/%02d/%02d" % (
                            days_interval[j - 6][0],
                            days_interval[j - 6][1],
                            days_interval[j - 6][2],
                        )
                        sql = (
                            " %s select %s, count(*) from ofertas_compra where fk_etapa >= 39 and convert(varchar(10), fecha_asignacion, 111) between '%s' and '%s' and asignada = 1 and cancelada <> 1  %s"
                            % (union, orden, fini, ffin, additionalWhere)
                        )
                        query.append(sql)
                        union = "union"
                        j += 7
                        orden -= 1
                    break
            break

    sql = " ".join(query)
    sql = "{} order by 1 desc".format(sql)
    ventas = []
    for i, valor in enumerate(cn.execute(sql), 1):
        ventas.append(
            dict(
                id=i, semana=int(valor[0]), valor=valor[1], intervalo=tip[int(valor[0])]
            )
        )
    return ventas


def ventasporsemana(cn, semanas=10, et="", w=750, h=350):
    semanas = int(semanas)
    c = calendar.Calendar()
    hoy = calendar.datetime.datetime.today()
    y = yr = hoy.year
    m = mt = hoy.month
    d = dy = hoy.day
    if mt == 12:
        mt = 2
        yr += 1
    elif mt == 11:
        mt = 1
        yr += 1
    else:
        mt += 2
    meses = int((semanas / 4) + 4)
    days_interval = []
    for mes in range(meses):
        for dn, wd in c.itermonthdays2(yr, mt):
            if dn:
                days_interval.append([yr, mt, dn, wd])
        if mt == 1:
            mt = 12
            yr -= 1
        else:
            mt -= 1
    days_interval.sort()
    days_interval.reverse()
    union = ""
    orden = semanas
    query = []
    tip = {}
    nmeses = "* Ene Feb Mar Abr May Jun Jul Ago Sep Oct Nov Dic"
    lmeses = nmeses.split()
    additionalWhere = ""
    if et:
        additionalWhere = " and fk_etapa = %s " % et
    for i, v in enumerate(days_interval):
        if v[0] == y and v[1] == m and v[2] == d:
            for j in range(i, len(days_interval)):
                if days_interval[j][3] == 5:
                    for semana in range(semanas):
                        periodo = "Semana del %02d/%s/%04d al %02d/%s/%04d" % (
                            days_interval[j][2],
                            lmeses[days_interval[j][1]],
                            days_interval[j][0],
                            days_interval[j - 6][2],
                            lmeses[days_interval[j - 6][1]],
                            days_interval[j - 6][0],
                        )
                        tip[orden] = periodo
                        fini = "%04d/%02d/%02d" % (
                            days_interval[j][0],
                            days_interval[j][1],
                            days_interval[j][2],
                        )
                        ffin = "%04d/%02d/%02d" % (
                            days_interval[j - 6][0],
                            days_interval[j - 6][1],
                            days_interval[j - 6][2],
                        )
                        sql = (
                            " %s select %s, count(*) from ofertas_compra where fk_etapa >= 39 and convert(varchar(10), fecha_oferta, 111) between '%s' and '%s' %s"
                            % (union, orden, fini, ffin, additionalWhere)
                        )
                        query.append(sql)
                        union = "union"
                        j += 7
                        orden -= 1
                    break
            break

    sql = " ".join(query)
    sql = "{} order by 1 desc".format(sql)

    ventas = []
    for i, valor in enumerate(cn.execute(sql), 1):
        ventas.append(
            dict(
                id=i, semana=int(valor[0]), valor=valor[1], intervalo=tip[int(valor[0])]
            )
        )
    for x in ventas:
        print(x)
    return ventas


def cobradasporsemana(cn, semanas=10, et="", w=750, h=350):
    semanas = int(semanas)
    c = calendar.Calendar()
    hoy = calendar.datetime.datetime.today()
    y = yr = hoy.year
    m = mt = hoy.month
    d = dy = hoy.day
    if mt == 12:
        mt = 2
        yr += 1
    elif mt == 11:
        mt = 1
        yr += 1
    else:
        mt += 2
    meses = int((semanas / 4) + 4)
    days_interval = []
    for mes in range(meses):
        for dn, wd in c.itermonthdays2(yr, mt):
            if dn:
                days_interval.append([yr, mt, dn, wd])
        if mt == 1:
            mt = 12
            yr -= 1
        else:
            mt -= 1
    days_interval.sort()
    days_interval.reverse()
    union = ""
    orden = semanas
    query = []
    tip = {}
    nmeses = "* Ene Feb Mar Abr May Jun Jul Ago Sep Oct Nov Dic"
    lmeses = nmeses.split()
    additionalWhere = ""
    if et:
        additionalWhere = " and i.fk_etapa = %s " % et
    for i, v in enumerate(days_interval):
        if v[0] == y and v[1] == m and v[2] == d:
            for j in range(i, len(days_interval)):
                if days_interval[j][3] == 5:
                    for semana in range(semanas):
                        periodo = "Semana del %02d/%s/%04d al %02d/%s/%04d" % (
                            days_interval[j][2],
                            lmeses[days_interval[j][1]],
                            days_interval[j][0],
                            days_interval[j - 6][2],
                            lmeses[days_interval[j - 6][1]],
                            days_interval[j - 6][0],
                        )
                        tip[orden] = periodo
                        fini = "%04d/%02d/%02d" % (
                            days_interval[j][0],
                            days_interval[j][1],
                            days_interval[j][2],
                        )
                        ffin = "%04d/%02d/%02d" % (
                            days_interval[j - 6][0],
                            days_interval[j - 6][1],
                            days_interval[j - 6][2],
                        )
                        sql = (
                            " %s select %s , count(* ) from tramites_ventas_movimientos t join inmueble i on t.fk_inmueble = i.codigo where i.fk_etapa >= 39 and t.fk_tramite = 105 and convert(varchar(10), t.fecha, 111) between '%s' and '%s' %s"
                            % (union, orden, fini, ffin, additionalWhere)
                        )

                        query.append(sql)
                        union = "union"
                        j += 7
                        orden -= 1
                    break
            break

    sql = " ".join(query)
    sql = "{} order by 1 desc".format(sql)
    cobradas = []
    for i, valor in enumerate(cn.execute(sql), 1):
        cobradas.append(
            dict(
                id=i, semana=int(valor[0]), valor=valor[1], intervalo=tip[int(valor[0])]
            )
        )
    return cobradas


def ventaspordia(cn, tipo="1"):
    lista = []
    dias = [
        "Nada",
        "Domingo",
        "Lunes",
        "Martes",
        "Miercoles",
        "Jueves",
        "Viernes",
        "Sabado",
    ]
    sql = """select datepart(dw, t.fecha_oferta) as dia,
     avg(t.total) as promedio, sum(t.total) as total 
     from (select fecha_oferta, count(*) as total
     from ofertas_compra where fk_etapa >= 39 group by fecha_oferta) as t
     group by datepart(dw,t.fecha_oferta) order by 1"""

    if tipo == "1":
        titulo = "Promedios por Dia"
    else:
        titulo = "Totales por Dia"

    for i, x in enumerate(cn.execute(psql(sql)), 1):
        if tipo == "1":
            valor = x.promedio

        else:
            valor = x.total
        lista.append(dict(id=i, dia=dias[x.dia], valor=valor))

    return lista


def panoramacomercial(cn, etapa="", breve={}):
    rubros = [
        "Libres",
        "Ventas Activas por Ingresar",
        "Expediente con Detalles o Diferencias",
        "Asignadas por Cerrar",
        "Por capturar",
        "Capturados",
        "En Firma de Escrituras",
        "Firmadas sin Cotejo",
        "Cotejadas",
    ]
    additionalWhere = ""
    if etapa:
        additionalWhere = " and fk_etapa = {}".format(etapa)

    sql = """
        select count(*) as cuantos from inmueble
        where codigo not in 
        ( select fk_inmueble from cuenta) and 
        fk_etapa > 39 {}""".format(
        additionalWhere
    )

    for row in cn.execute(psql(sql)):
        libres = row.cuantos

    sql = """
        select codigo from etapa where codigo >= 39
    """
    las_etapas = []
    for row in cn.execute(psql(sql)):
        las_etapas.append(row.codigo)

    sql = """
        select x.etapa, sum(x.cuantos) as cuantos from (select i.fk_etapa as etapa,count(*) as cuantos
        from inmueble i
        join cuenta c on i.codigo = c.fk_inmueble
        where i.fk_etapa >= 39 and i.codigo not in (
        select  distinct m.inmueble from
        integracion_Fechas f
        join incorporacion_maestro m
        on f.integracion = m.codigo
        where f.fecha_termino is not null and f.requisito = 86 and f.solicitud = 1
        ) group by i.fk_etapa
        union all
        select i.fk_etapa as etapa, count(*) as cuantos from integracion_fechas f
        join incorporacion_maestro m on f.integracion = m.codigo
        join inmueble i on m.inmueble = i.codigo
        where i.fk_etapa >= 39 and
        f.solicitud = 1 and f.requisito = 86 and f.fecha_termino is not null
        and m.inmueble not in
        (select i.codigo from integracion_fechas f
        join incorporacion_maestro m on f.integracion = m.codigo
        join inmueble i on m.inmueble = i.codigo
        where i.fk_etapa >= 39 and
        f.solicitud = 1 and f.requisito = 87 and f.fecha_termino is not null)
        group by i.fk_etapa ) x group by etapa"""

    dicAsigPorCerrar = dict()
    for x in las_etapas:
        dicAsigPorCerrar[x] = 0

    totalAsigPorCerrar = 0
    for row in cn.execute(psql(sql)):
        try:
            dicAsigPorCerrar[row.etapa] += row.cuantos
        except:
            dicAsigPorCerrar[row.etapa] = row.cuantos
            totalAsigPorCerrar += row.cuantos
            asignadasPorCerrar = totalAsigPorCerrar
    if etapa:
        asignadasPorCerrar = dicAsigPorCerrar[int(etapa)]

    sql = """
       select o.fk_etapa as etapa,count(*) as cuantos
       from ofertas_compra o
       join cuenta c on o.cuenta = c.codigo where o.cancelada = 0
       and (c.fk_inmueble  is null or c.fk_inmueble = 0 ) 
       and o.fk_etapa >= 39 group by o.fk_etapa"""

    dicVtasActivasXIngresar = dict()
    totVtasActivasXIngresar = 0
    for row in cn.execute(psql(sql)):
        suman = row.cuantos + dicAsigPorCerrar.get(row.etapa, 0)
        dicVtasActivasXIngresar[row.etapa] = suman
        totVtasActivasXIngresar += suman

    sql = """
       select codigo as etapa from etapa where codigo > 39 and codigo not in 
       ({})
    """.format(
        ",".join([str(x) for x in list(dicVtasActivasXIngresar.keys())])
    )

    for row in cn.execute(psql(sql)):
        cual = dicAsigPorCerrar.get(row.etapa, 0)
        totVtasActivasXIngresar += cual
        dicVtasActivasXIngresar[row.etapa] = cual

    vtasActivasXIngresar = totVtasActivasXIngresar

    if etapa:
        vtasActivasXIngresar = dicVtasActivasXIngresar[int(etapa)]
    print(breve["result"])
    expConDetallesODif = [
        x.get("total") for x in breve["result"] if x.get("req") == "-37"
    ][0]
    porCapturar = [x.get("total") for x in breve["result"] if x.get("req") == "-28"][0]
    capturados = [x.get("total") for x in breve["result"] if x.get("req") == "-29"][0]
    enFirma = [x.get("total") for x in breve["result"] if x.get("req") == "-30"][0]
    firmadasSinCotejo = [
        x.get("total") for x in breve["result"] if x.get("req") == "-31"
    ][0]
    cotejadas = [x.get("total") for x in breve["result"] if x.get("req") == "-32"][0]
    sample = [
        libres,
        vtasActivasXIngresar,
        expConDetallesODif,
        asignadasPorCerrar,
        porCapturar,
        capturados,
        enFirma,
        firmadasSinCotejo,
        cotejadas,
    ]
    lista = []

    if True:
        for i, x in enumerate(list(range(0, len(rubros))), 1):
            lista.append(dict(id=i, rubro=rubros[x], valor=sample[x]))
        return lista
