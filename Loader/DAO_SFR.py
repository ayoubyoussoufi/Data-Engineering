# coding: utf-8

from __future__ import division
from __future__ import unicode_literals

from sqlalchemy import *
from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import mapper
from sqlalchemy.orm import sessionmaker
import yaml
from path import Path

current_directory = Path(__file__).parent
config_file = current_directory / 'config.yml'

with open(config_file, 'r') as file:
    try:
        config = yaml.safe_load(file)
    except yaml.YAMLError as exc:
        # Use format() for string formatting in Python 3.5
        print("Error in configuration file: {}".format(exc))


Base = declarative_base()
engine = create_engine(
    'mssql+pymssql://%s:%s@%s:%s/%s' % (
        config['_dbuser'],
        config['_dbpassword'],
        config['_dbserver'],
        config['_dbport'],
        config['_dbname']
    ),
    echo=False
    #,isolation_level="READ_UNCOMMITTED"

)

_WeekId = config.get('_WeekId', None)
ADULT_DIST = config.get('ADULT_DIST', [])
_OthersWeightNewsCata = config.get('_OthersWeightNewsCata', {})
_OthersWeight = config.get('OthersWeight', {})
# create a Session
Session = sessionmaker(bind=engine)
session = Session()

metadata = MetaData(engine)

rawdatas = Table('RawData', metadata, autoload=True)
datadatypes = Table('DataType', metadata, autoload=True)
sources = Table('Source', metadata, autoload=True)
days = Table('Calendar', metadata, autoload=True)
products = Table('Product', metadata, autoload=True)


# These are the empty classes that will become our data classes
class RawData(object):
    pass


rawdatamapper = mapper(RawData, rawdatas)


class DataType(object):
    pass


datatypemapper = mapper(DataType, datadatypes)


class Source(object):
    pass


sourcemapper = mapper(Source, sources)


class Day(object):
    pass


daymapper = mapper(Day, days)


class Product(object):
    pass


productmapper = mapper(Product, products)
