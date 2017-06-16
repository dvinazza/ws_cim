#!/usr/bin/env python3

# Librerias y clases para la DB
from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, ForeignKey, UniqueConstraint
from sqlalchemy.types import Integer, String, TIMESTAMP, Float

from sqlalchemy.orm import sessionmaker, relationship

import csv

db = declarative_base()


def iniciarSesion():
    # Inicializo en la DB una sesion
    # engine = create_engine('sqlite:///:memory:', echo=False)
    engine = create_engine('sqlite:///cim.db', echo=False)
    db.metadata.create_all(engine)

    Session = sessionmaker()
    Session.configure(bind=engine)
    return Session()


def dumpCSV(archivo):
    print("Escribiendo CSV: %s" % archivo)
    with open(archivo, 'w') as csvfile:
        dump = csv.DictWriter(csvfile, delimiter=',',
                              fieldnames=["estacion", "fecha", "valor"],
                              quotechar='"', quoting=csv.QUOTE_MINIMAL)
        dump.writeheader()
        for r in iniciarSesion().query(Registro):
            dump.writerow(r.asDict())


class Estacion(db):
    __tablename__ = 'estaciones'

    eid = Column(Integer, primary_key=True, autoincrement=True)
    nombre = Column(String, unique=True)

    def __repr__(self):
        return "Estacion: %s (%d)" % (self.nombre, self.eid)


class Registro(db):
    __tablename__ = 'registros'

    rid = Column(Integer, primary_key=True, autoincrement=True)
    eid = Column(Integer, ForeignKey("estaciones.eid"), nullable=False)
    fecha = Column(TIMESTAMP)  # Podriamos guardarlo como int directamente...
    valor = Column(Float)

    estacion = relationship("Estacion")

    __table_args__ = (UniqueConstraint('eid', 'fecha'),)

    def asDict(self):
        d = {}
        d['estacion'] = self.estacion.nombre
        d['fecha'] = self.fecha.timestamp()
        d['valor'] = self.valor
        return d

    def __repr__(self):
        return "%s,%s,%f" % (self.estacion.nombre, self.fecha, self.valor)
