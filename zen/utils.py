#!/bin/env python
# -*- coding: iso-8859-15 -*-

from traceback import print_exc


class CantidadAPalabras(object):
    resultado = ""
    centenas = {
        "0": "",
        "1": "ciento",
        "2": "doscientos",
        "3": "trescientos",
        "4": "cuatrocientos",
        "5": "quinientos",
        "6": "seiscientos",
        "7": "setecientos",
        "8": "ochocientos",
        "9": "novecientos",
    }
    unoaveintinueve = {
        "00": "",
        "01": "un",
        "02": "dos",
        "03": "tres",
        "04": "cuatro",
        "05": "cinco",
        "06": "seis",
        "07": "siete",
        "08": "ocho",
        "09": "nueve",
        "10": "diez",
        "11": "once",
        "12": "doce",
        "13": "trece",
        "14": "catorce",
        "15": "quince",
        "16": "dieciseis",
        "17": "diecisiete",
        "18": "dieciocho",
        "19": "diecinueve",
        "20": "veinte",
        "21": "veintiun",
        "22": "veintidos",
        "23": "veintitres",
        "24": "veinticuatro",
        "25": "veinticinco",
        "26": "veintiseis",
        "27": "veintisiete",
        "28": "veintiocho",
        "29": "veintinueve",
    }

    decenas = {
        "3": "treinta",
        "4": "cuarenta",
        "5": "cincuenta",
        "6": "sesenta",
        "7": "setenta",
        "8": "ochenta",
        "9": "noventa",
    }

    fraccion = ""

    def __init__(self, cantidad=0, tipo="pesos", currency=None):
        """otro tipo es 'numero'
        y significaria por ejemplo...
        5347.17
        CINCO MIL TRESCIENTOS CUARENTA Y SIETE ( solo la cantidad entera expresada en palabras ).
        """
        self.tipo = tipo
        if tipo == "pesos":
            self.antesdedecimales = ("PESO", "PESOS")
            self.despuesdedecimales = "/100 M.N."
        elif tipo == "numero":
            self.antesdedecimales = ("", "")
            self.despuesdedecimales = ""
        elif tipo == "other":
            self.antesdedecimales = ("", "")
            self.despuesdedecimales = "/100 {0}".format(currency)
        self.cantidad = cantidad

    def texto(self):
        strcantidad = ""
        self.resultado = ""
        try:
            scantidad = str(float(self.cantidad))
            strcantidad = scantidad.split(".")[0]
            fraccion = scantidad.split(".")[1]
        except:
            if self.tipo == "numero":
                fraccion = ""
            else:
                fraccion = "00"
        if self.tipo in ("pesos", "other"):
            if len(fraccion) == 1:
                fraccion += "0"
            if len(fraccion) != 2:
                fraccion = fraccion[:2]
        self.fraccion = fraccion

        if ((len(strcantidad) - 1) % 3) == 0 or len(strcantidad) == 1:
            strcantidad = "0" + strcantidad
        for indiceDigito in range(-1, -len(strcantidad) - 1, -1):
            if abs(indiceDigito) % 3 == 0:
                if (
                    strcantidad[indiceDigito] == "1"
                    and strcantidad[indiceDigito + 1 : indiceDigito + 3] == "00"
                ):
                    self.resultado = "cien " + self.resultado
                else:
                    self.resultado = (
                        self.centenas[strcantidad[indiceDigito]] + " " + self.resultado
                    )
            elif (abs(indiceDigito) + 1) % 3 == 0:
                if indiceDigito == -2:
                    digitos = strcantidad[indiceDigito:]
                else:
                    digitos = strcantidad[indiceDigito : indiceDigito + 2]

                millares_millones = ""
                if abs(indiceDigito) in (5, 11):
                    millares_millones = "mil "
                if abs(indiceDigito) in (8, 14):
                    millares_millones += "millones "
                iDigitos = int(digitos)
                if 0 <= iDigitos <= 29:
                    self.resultado = (
                        self.unoaveintinueve[digitos]
                        + " "
                        + millares_millones
                        + self.resultado
                    )
                else:
                    unidades = self.unoaveintinueve["0" + strcantidad[indiceDigito + 1]]
                    if unidades != "":
                        unidades = " y " + unidades
                    self.resultado = (
                        self.decenas[strcantidad[indiceDigito]]
                        + unidades
                        + " "
                        + millares_millones
                        + self.resultado
                    )
        try:
            if int(strcantidad[-9:-6]) == 1:
                self.resultado = self.resultado.replace("millones", "millon")
        except:
            pass
        if self.resultado.strip() == "un" and self.tipo == "pesos":
            return (
                self.resultado.upper()
                + self.antesdedecimales[0]
                + " "
                + fraccion
                + self.despuesdedecimales
            )
        else:
            if self.resultado.strip() == "":
                self.resultado = "cero "
            elif self.resultado.strip() == "ciento":
                self.resultado = "cien "
            elif self.resultado.strip().endswith("un") and self.tipo == "numero":
                self.resultado = self.resultado.strip() + "o"

            temp = (
                self.resultado.upper()
                + self.antesdedecimales[1]
                + " "
                + fraccion
                + self.despuesdedecimales
            )

            temp = (
                " ".join(temp.split())
                .strip()
                .replace("MILLON MIL", "MILLON")
                .replace("MILLONES MIL", "MILLONES")
                .replace("MILLON PESOS", "MILLON DE PESOS")
                .replace("MILLONES PESOS", "MILLONES DE PESOS")
            )  # esto para eliminar posibles dobles espacios y cuando de millon pasa a centenas sin miles. Es un horrible hack pero funciona.
            if temp.endswith(" 0"):
                temp = temp[:-2]  # otro hack en lo que se encuentra la causa logica.
            return temp
        palabras = texto


def aletras(cantidad, tipo="pesos", currency=None):
    c = CantidadAPalabras(cantidad, tipo=tipo, currency=currency)
    return c.texto()


def consonantes(ap_paterno, ap_materno, nombre):
    vocals = "AEIOU"
    c = []
    if len(ap_paterno) < 3:
        return ""
    mat = ap_materno.replace(" ", "")
    if len(mat) < 3:
        return ""
    for x in ap_paterno[1:]:
        if x not in vocals:
            c.append(x)
            break
    for x in ap_materno[1:]:
        if x not in vocals:
            c.append(x)
            break

    for x in nombre[1:]:
        if x not in vocals:
            c.append(x)
            break
    cons = "".join(c)
    cons.replace("Ñ", "X")
    return cons


def curp(ap_paterno, ap_materno, nombre, estado, sexo, rfc, fecha_nac):

    caracteres = "0123456789ABCDEFGHIJKLMNÑOPQRSTUVWXYZ"
    estados = "AS BC BS CC CL CM CS CH DF DG GT GR HG JC MC MN MS NT NL OC PL QT QR SP SL SR TC TS VZ YN ZS NE"
    assert estado in estados.split(" "), "Estado invalido"
    factor = 19
    anio = fecha_nac.year  # asumo fecha_nac datetime
    digito = "0"
    suma = 0
    if anio >= 2000:
        digito = "A"
    if sexo == "M":
        sexo = "H"
    else:
        sexo = "M"

    micurp = "{}{}{}{}{}".format(
        rfc[:10], sexo, estado, consonantes(ap_paterno, ap_materno, nombre), digito
    )
    print("micurp", micurp)
    for x in micurp:
        posicion = 0
        for i, y in enumerate(caracteres, 0):
            if y == x:
                posicion = i
                print("posicion ahora es ", posicion)
        factor -= 1
        suma += posicion * factor
    print("suma", suma)
    residuo = suma % 10
    dig = 10 - residuo
    if dig == 10:
        dig = 0
    micurp = "{}{}".format(micurp, dig)
    return micurp


def upper2(valor):
    try:
        u = valor.decode("utf-8")
        um = u.upper()
        resultado = um.encode("utf-8")
    except:
        print_exc()
        resultado = ""
    return resultado
