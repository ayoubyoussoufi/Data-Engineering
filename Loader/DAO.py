# coding: utf-8

from __future__ import unicode_literals
from __future__ import division
from sqlalchemy import *
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import mapper
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
import yaml
from path import Path

current_directory = Path(__file__).parent
config_file = current_directory / 'config.yml'

with open(config_file, 'r') as file:
    try:
        config = yaml.safe_load(file)
    except yaml.YAMLError as exc:
        print("Error in configuration file: {exc}")



Base = declarative_base()
engine = create_engine(
        'mssql+pymssql://%s:%s@%s/%s' % (
            config['_dbuser'],
           config['_dbpassword'],
            config['_dbserver'],
            #_dbport,
            config['_dbname']
        ),
        echo=False
    )

Session = sessionmaker(bind=engine)
session = Session()

metadata = MetaData(engine)

def defaultEAN(context):
    return context.get_current_parameters()['EAN']

days = Table('Day', metadata, autoload=True)


products = Table(
        'Product', metadata,
        Column('Disabled', Integer, default=0),         #looks like reflection ignores default values set in DB.  TODO: check for a better solution.
        Column('IsAutoDisabled', Integer, default=0),
        #Column('EAN_Libelle', String, default=defaultEAN),
        autoload=True
)
loadprocesses = Table('LoadProcess', metadata, autoload=True)
outlets = Table('Outlet', metadata,
        Column('Disabled', Integer, default=0),
        autoload=True
)
destinations = Table('Destination', metadata, autoload=True)
nations = Table('Nation', metadata, autoload=True)
rawdatas = Table('RawData', metadata, autoload=True)
currencys = Table('Currency', metadata, autoload=True)
extractprocesses = Table('ExtractProcess', metadata, autoload=True)
businessassistants = Table('BusinessAssistant', metadata, autoload=True)
sourceproductnames = Table('SourceProductNames', metadata, autoload=True)
sourcecodificationproduct = Table('SourceCodificationProduct', metadata, autoload=True)
sourcecodificationproducttram = Table('SourceCodificationProductTRAM', metadata, autoload=True)
exchangerate = Table('FullExchangeRate', metadata, autoload=True)

class Day(object):
    pass

daymapper = mapper(Day, days)

class Product(object):
    pass

productmapper = mapper(Product, products)

class SourceCodificationProduct(object):
    pass

sourcecodificationproduct = mapper(SourceCodificationProduct, sourcecodificationproduct)

class SourceCodificationProductTRAM(object):
    pass

sourcecodificationproducttram = mapper(SourceCodificationProductTRAM, sourcecodificationproducttram)

class BusinessAssistant(object):
    pass

businessassistantmapper = mapper(BusinessAssistant, businessassistants)

class LoadProcess(object):
    pass

loadprocessmapper = mapper(LoadProcess, loadprocesses)

class Outlet(object):
    pass

outletmapper = mapper(Outlet, outlets)

class Destination(object):
    pass

destinationmapper = mapper(Destination, destinations)

class Nation(object):
    pass

nationmapper = mapper(Nation, nations)

class RawData(object):
    pass

rawdatamapper = mapper(RawData, rawdatas)

class Currency(object):
    pass

currencymapper = mapper(Currency, currencys)

class SourceProductNames(object):
    pass
sourceproductnamesmapper = mapper(SourceProductNames,sourceproductnames)

class ExtractProcess(object):
    pass

extractmapper = mapper(ExtractProcess, extractprocesses)

class FullExchangeRate(object):
    pass

fullexchangeratemapper = mapper(FullExchangeRate, exchangerate)

