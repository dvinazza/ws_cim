#!/usr/bin/env python3

from database import Estacion, Registro, iniciarSesion, dumpCSV

# Librerias para scrapear
import mechanicalsoup
import re
import json
from datetime import datetime
from sqlalchemy.exc import IntegrityError  # TODO: No deberia estar aca

# variables predefinidas
base_url = "http://fich.unl.edu.ar/cim/"

# DB
s = iniciarSesion()

br = mechanicalsoup.Browser()
pagina = br.get(base_url + "alturas-rio-parana")
estaciones = []

for link in pagina.soup.find_all('a'):
    if "historico" in link['href']:
        estaciones.append(link['href'])

print("Encontré %d historicos de estaciones" % len(estaciones))

# Scrap
for e in estaciones:
    print("Conectando a %s" % e)
    pagina = br.get(base_url + e)

    nombre = pagina.soup.find('div', class_="marco-wireframe").\
        find('div', class_="col-xs-12 col-sm-6 col-md-3").\
        text.replace("Estación: ", "").\
        rstrip().lstrip()

    busqueda = re.compile('data: (\[.*\]).*\];', re.MULTILINE | re.DOTALL)

    # horrible que sea case sensitive el language
    for js in pagina.soup.find_all('script', {'language': "JavaScript"}):
        if busqueda.findall(js.text):
            datos = json.loads(busqueda.findall(js.text)[0])
            # print(datos)
            break

    print("%s: Encontre %d historicos" % (nombre, len(datos)))

    # Paso el timestamp a epoch porque está en milisegundos...
    datos = [[int(x/1000), y] for x, y in datos]

    # Store
    estacion = s.query(Estacion).filter_by(nombre=nombre).first()
    if estacion is None:  # No la tengo registrada aún
        estacion = Estacion(nombre=nombre)
        s.add(estacion)
        s.commit()

    print(estacion)

    for d in datos:
        registro = Registro(eid=estacion.eid,
                            valor=d[1],
                            fecha=datetime.fromtimestamp(d[0]))
        try:
            s.add(registro)
            s.commit()
        except IntegrityError as error:
            s.rollback()
