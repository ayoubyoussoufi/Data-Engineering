# coding: utf-8

from __future__ import unicode_literals
from __future__ import division
from decimal import *
import unicodecsv as csv
from datetime import datetime
import re
from path import Path

from config import _IN_Sales, _delete, _IN_FTP_PROD_EMEA
from DAO import *
import _ConvertExcelFiles




def ImportData(filename, _LoadProcess, sourcename, currency):
    HEADER = ['SourceProductCode',
              'SourceProductName',
              'Exclusif_Code',
              'EAN',
              'Material_Code',
              'SourceOutletCode',
              'SourceOutletName',
              'Volume',
              'Valeur',
              'Currency',
              'Stock (pour TRAM)',
              'Volume',
              'Valeur',
              'Currency']
    HEADER_NAMES = [['ITEM ID', 'ITEM NUMBER', 'ITEM CODE'],  # SourceProductCode
                    ['DESCRIPTION', 'NAME', 'ITEM NAME'],  # SourceProductName
                    ['ITEM ID', 'ITEM NUMBER', 'ITEM CODE'],  # Exclusif_Code
                    ['BARCODE', 'EAN CODE', 'EAN'],  # EAN
                    ['VENDOR CODE', 'VAN CODE'],  # Material_Code
                    [],  # SourceOutletCode
                    [],  # SourceOutletName
                    ['QTY'],  # Volume
                    ['VALUE', 'VALUE IN USD'],  # Valeur
                    [],  # Currency
                    ['STOCK'],  # Stock (pour TRAM)
                    [],  # Volume
                    [],  # Valeur
                    [],  # Currency
                    []]

    HEADER_IDX = [-2 if not x else -1 for x in HEADER_NAMES]
    MONTH = ['JAN', 'FEB', 'MAR', 'APR', 'MAY', 'JUN', 'JUL', 'AUG', 'SEP', 'OCT', 'NOV', 'DEC']
    #SOURCENAMEPRODUCT = 'EMEA_STANDARD'

    _NbTotalLine = 0
    _NbLoadedLine = 0
    _NbCreatedProduct = 0
    _NbUpdatedProduct = 0
    _NbLoadedProduct = 0
    _NbCreatedOutlet = 0
    _NbLoadedOutlet = 0
    _NbUpdatedOutlet = 0
    _NbCreatedRawData = 0
    _NbCreatedRawDataN1 = 0
    _NbRejectedRawData = 0
    _TotalSalesVolume = 0
    _TotalSalesValue = 0
    _LoadedSalesVolume = 0
    _LoadedSalesValue = 0

    _TotalStockVolume = 0
    _TotalStockValue = 0
    _LoadedStockVolume = 0
    _LoadedStockValue = 0

    _LoadedOutlets = []
    _LoadedProducts = []

    p = Path(filename)
    pattern = r"^(?P<period>20\d{2}12(?:0[1-9]|1[012]))_.*\.csv$"
    match = re.search(pattern, p.basename(), re.UNICODE | re.IGNORECASE)
    assert match, "Unexpected filename!"
    periodStr = match.group('period')
    _day = session.query(Day).filter(Day.MonthId == periodStr).one()
    print _day.DayId

    #dayid = periodStr[:4] + '{}01'
    #month_3c = MONTH[int(periodStr[-2:])-1]

    #specifying outlet
    # sourceOutletCode = 'DEPARTURE'
    # sourceOutletName = 'Mumbai'

    sourcenamestock = sourcename + '_STOCK'
    sourceoutletnamestock = sourceoutletcodestock = 'Mumbai'

    delete_r = _delete.format(sourcename, _day.DayId, sourcename, _day.DayId, sourcename, _day.DayId)
    print(delete_r)
    session.execute(delete_r)
    session.commit()

    delete_r = _delete.format(sourcenamestock, _day.DayId, sourcenamestock, _day.DayId, sourcenamestock, _day.DayId)
    print(delete_r)
    session.execute(delete_r)
    session.commit()

    # build Caches
    outletcache = {}
    for soc, oid in session.query(Outlet.SourceOutletCode, Outlet.OutletId).filter(
            Outlet.SourceName == sourcename).all():
        outletcache[soc] = oid

    outletcachestock = {}
    for soc, oid in session.query(Outlet.SourceOutletCode, Outlet.OutletId).filter(
            Outlet.SourceName == sourcenamestock).all():
        outletcachestock[soc] = oid

    productcache = {}
    for spc, pid in session.query(Product.SourceProductCode, Product.ProductId).filter(
            Product.SourceName == sourcename).all():
        productcache[spc] = pid

    currencycache = {}
    for cc, ci in session.query(Currency.CurrencyCode, Currency.CurrencyId).all():
        currencycache[cc] = ci

    spncache = set()
    for id in session.query(SourceProductNames.ProductId).filter(SourceProductNames.SourceName == sourcename).all():
        spncache.add(id[0])

    CodifMaterialCode = set()
    for mc in session.query(SourceCodificationProduct.Material_code).all():
        CodifMaterialCode.add(mc[0].upper())

    uniqueProductCode = set()

    outlet_dico = {}

    with open(filename, 'rb') as f:
        reader = csv.reader(f, delimiter=b',', quoting=csv.QUOTE_MINIMAL, encoding='utf-8')
        ite = enumerate(reader, 1)
        # skip useless rows until header and locate current period column
        header_found = False
        outlet_keywords = ["DEPARTURES", "AHMEDABAD"]
        for i, row in ite:
            if not header_found:
                if any(keyword in (str(cell).upper() for cell in row) for keyword in outlet_keywords):
                    for k, o in enumerate(row):
                        if o.upper().strip() not in ['', None, 'TOTAL', 'STOCK'] and k < len(row):
                            outlet_dico[o] = k
                        if outlet_dico:
                            header_found = True
            for i, header in enumerate(row):
                for j, headerNames in enumerate(HEADER_NAMES):
                    if header.upper().strip() in headerNames:
                        if HEADER_IDX[j] < 0:
                            HEADER_IDX[j] = i
            if header_found and row[2].strip():
                break

        print("this is outlet dicot : %s" % outlet_dico)
        # print(outlet_dico)

        for i, header in enumerate(HEADER_NAMES):
            if HEADER_IDX[i] == -1:
                error_msg = "Le champs {} n'a pas été trouvé en utilisant la liste d'en-têtes suivante: {} ".format(
                    HEADER[i], "/".join(header))
                raise Exception(error_msg.encode('utf-8'))

        # CURRENCY UNIQUE
        currencyid = currencycache.get(currency)

        for sourceOutletCode in outlet_dico.keys():
            #OUTLET UNIQUE
            outletid = outletcache.get(sourceOutletCode)
            outletidstock = outletcachestock.get(sourceOutletCode)
            if not outletid:
                _outlet = session.query(Outlet).filter(Outlet.SourceName == sourcename). \
                    filter(Outlet.SourceOutletCode == sourceOutletCode).one_or_none()
                if _outlet is None:
                    print 'creation mag'

                    _outlet = Outlet()
                    _outlet.SourceName = sourcename
                    _outlet.SourceOutletCode = sourceOutletCode
                    _outlet.SourceOutletName = sourceOutletCode
                    _outlet.CreatedByLoadProcessId = _LoadProcess.LoadProcessId

                    session.add(_outlet)
                    session.commit()

                    _NbCreatedOutlet = _NbCreatedOutlet + 1

                outletcache[sourceOutletCode] = outletid = _outlet.OutletId

            # OUTLET UNIQUE
            # outletidstock = outletcachestock.get(sourceoutletcodestock)

            if not outletidstock:
                _outlet = session.query(Outlet).filter(Outlet.SourceName == sourcenamestock). \
                    filter(Outlet.SourceOutletCode == sourceOutletCode).one_or_none()
                if _outlet is None:
                    print 'creation mag stock'

                    _outlet = Outlet()
                    _outlet.SourceName = sourcenamestock
                    _outlet.SourceOutletCode = sourceOutletCode
                    _outlet.SourceOutletName = sourceoutletnamestock
                    _outlet.CreatedByLoadProcessId = _LoadProcess.LoadProcessId

                    session.add(_outlet)
                    session.commit()

                    _NbCreatedOutlet = _NbCreatedOutlet + 1

                outletcachestock[sourceOutletCode] = _outlet.OutletId

        for i, row in ite:
            sourceProductCode = row[HEADER_IDX[0]].strip()
            sourceProductName = row[HEADER_IDX[1]].strip()
            exclusifCode = row[HEADER_IDX[2]].strip()
            ean = row[HEADER_IDX[3]].strip()
            material_Code = row[HEADER_IDX[4]].strip()
            # sourceOutletCode = row[HEADER_IDX[5]].strip()
            # sourceOutletName = row[HEADER_IDX[6]].strip()
            #salesVolume = row[HEADER_IDX[7]].replace(' ', '').replace(',', '')
            #adding stockvalue
            # stockValue = row[HEADER_IDX[8]].replace(' ', '').replace(',', '')
            # currency = row[HEADER_IDX[9]].strip()
            # stock = row[HEADER_IDX[10]].strip()
            # Combine both stock values
            # salesVolumely = row[HEADER_IDX[11]].replace(' ', '').replace(',', '')
            # salesValuely = row[HEADER_IDX[12]].replace(' ', '').replace(',', '')

            snp = sourcename
            productid = productcache.get(sourceProductCode)

            if sourceProductName in ['', None]:
                continue

            if sourceProductCode in ['', None]:
                print i
                _LoadProcess.Status = -1
                session.commit()
                session.execute(delete_r)
                session.commit()
                raise ValueError('SourceProductCode Vide', sourceProductName)

            #PRODUCT
            if not productid:
                _product = session.query(Product).filter(Product.SourceName == snp).filter(
                    Product.SourceProductCode == sourceProductCode).one_or_none()

                if _product is None:
                    print 'creation produit'
                    _product = Product()
                    _product.SourceName = snp
                    _product.SourceProductCode = sourceProductCode
                    _product.Material_code = material_Code
                    _product.EAN_Libelle = ean
                    _product.Exclusif_Code = exclusifCode
                    _product.SourceProductName = sourceProductName

                    _product.CreatedByLoadProcessId = _LoadProcess.LoadProcessId

                    session.add(_product)
                    session.commit()

                    _NbCreatedProduct = _NbCreatedProduct + 1

                productcache[sourceProductCode] = productid = _product.ProductId

            # if productid not in spncache and snp==SOURCENAMEPRODUCT:
            #     spn = session.query(SourceProductNames).filter(SourceProductNames.SourceName == sourcename).filter(
            #         SourceProductNames.ProductId == productid).one_or_none()
            #
            #     if spn is None:
            #         print 'creation sourceProductName'
            #         _spn = SourceProductNames()
            #         _spn.ProductId = productid
            #         _spn.SourceName = sourcename
            #         _spn.SourceProductName = sourceProductName
            #         _spn.CreatedByLoadProcessId = _LoadProcess.LoadProcessId
            #
            #         session.add(_spn)
            #         session.commit()
            #
            #     spncache.add(productid)
            for sourceOutletCode in outlet_dico.keys():
                if sourceOutletCode in ['ARRIVAL', 'DEPARTURE', 'ARRIVALS', 'DEPARTURES']:
                    stock = row[outlet_dico[sourceOutletCode]].strip()
                    outletidstock = outletcachestock[sourceOutletCode]
                    #STOCK
                    if stock not in ['', '0', '-', None]:
                        # RAWDATA
                        _rawdata = RawData()
                        _rawdata.DayId = _day.DayId
                        _rawdata.OutletId = outletidstock
                        _rawdata.ProductId = productid
                        _rawdata.StockVolume = Decimal(stock)
                        _rawdata.CurrencyId = currencyid
                        _rawdata.LoadProcessId = _LoadProcess.LoadProcessId

                        _LoadedStockVolume += _rawdata.StockVolume
                        _TotalStockVolume += _rawdata.StockVolume

                        session.add(_rawdata)
                        _NbCreatedRawData += 1

            for sourceOutletCode in outlet_dico.keys():
                if sourceOutletCode in ['ARRIVAL', 'DEPARTURE', 'ARRIVALS', 'DEPARTURES']:
                    salesVolume = row[outlet_dico[sourceOutletCode] + 1].strip()
                    salesValue = row[outlet_dico[sourceOutletCode] + 2].strip()
                    outletid = outletcache[sourceOutletCode]

                    if salesVolume not in ['', '0']:
                        # print(
                        # sourcename, sourceProductCode, sourceProductName, exclusifCode, material_Code, ean, salesVolume,
                        # salesValue, sourceOutletCode, stock)
                        # RAWDATA
                        _rawdata = RawData()
                        _rawdata.DayId = _day.DayId
                        _rawdata.OutletId = outletid
                        _rawdata.ProductId = productid
                        _rawdata.SalesVolume = Decimal(salesVolume)
                        _rawdata.SalesValue = Decimal(salesValue)
                        _rawdata.CurrencyId = currencyid
                        _rawdata.LoadProcessId = _LoadProcess.LoadProcessId

                        _LoadedSalesVolume += _rawdata.SalesVolume
                        _LoadedSalesValue += _rawdata.SalesValue
                        _TotalSalesVolume += _rawdata.SalesVolume
                        _TotalSalesValue += _rawdata.SalesValue

                        session.add(_rawdata)
                        _NbCreatedRawData += 1

                        if outletid not in _LoadedOutlets:
                            _LoadedOutlets.append(outletid)
                            _NbLoadedOutlet = _NbLoadedOutlet + 1

                if productid not in _LoadedProducts:
                    _LoadedProducts.append(productid)
                    _NbLoadedProduct = _NbLoadedProduct + 1

                if i % 100 == 0:
                    session.commit()

                _NbLoadedLine = _NbLoadedLine + 1

        _LoadProcess.lastModified = datetime.now()
        _LoadProcess.Period = unicode(_day.MonthId)

        _LoadProcess.NbTotalLine = _NbTotalLine
        _LoadProcess.NbLoadedLine = _NbLoadedLine
        _LoadProcess.NbCreatedProduct = _NbCreatedProduct
        _LoadProcess.NbUpdatedProduct = _NbUpdatedProduct
        _LoadProcess.NbLoadedProduct = _NbLoadedProduct
        _LoadProcess.NbCreatedOutlet = _NbCreatedOutlet
        _LoadProcess.NbUpdatedOutlet = _NbUpdatedOutlet
        _LoadProcess.NbLoadedOutlet = _NbLoadedOutlet

        # Stock
        _LoadProcess.LoadedStockVolume = _LoadedStockVolume
        _LoadProcess.LoadedStockValue = _LoadedStockValue
        _LoadProcess.TotalStockVolume = _TotalStockVolume
        _LoadProcess.TotalStockValue = _TotalStockValue

        # Sales
        _LoadProcess.LoadedSalesVolume = _LoadedSalesVolume
        _LoadProcess.LoadedSalesValue = _LoadedSalesValue
        _LoadProcess.TotalSalesVolume = _TotalSalesVolume
        _LoadProcess.TotalSalesValue = _TotalSalesValue

        _LoadProcess.NbCreatedRawData = _NbCreatedRawData
        _LoadProcess.NbRejectedRawData = _NbRejectedRawData
        _LoadProcess.NbCreatedRawDataN1 = _NbCreatedRawDataN1

        if _LoadProcess.Status != -1:
            _LoadProcess.Status = 1

        session.commit()
        print(str(_NbLoadedLine) + " " + str(_TotalSalesVolume) + ":" + str(
            _TotalSalesValue) + currency + " - Stock:" + str(_TotalStockVolume) + ":" + str(_TotalStockValue))
        print 'Fin ImportData'


def main():
    convertxls = {'folder': r'ZONE MEAI\MUMBAI', 'sheetname': 0, 'fileType': 1, 'subDir': None}

    todo = [(r'ZONE MEAI\MUMBAI', 'MUMBAI', '*.csv', 'USD', 'MUMBAI'), ]

    files_tmp = []
    files_err = []
    files_done = []

    for subdir, sourcename, filter, currency, subdirftp in todo:
        rootpathftp = Path(config["_IN_FTP_EMEA"]) / subdirftp
        rootpathftptodo = Path(config["_IN_FTP_EMEA"]) / subdirftp / 'todo'
        print(rootpathftptodo)
        rootpath = Path(config["_IN_Sales"]) / subdir

        for f in rootpathftptodo.files('*.xls*'):
            tolocal = rootpath / "xls" / f.basename()
            f.copy(tolocal)

        _ConvertExcelFiles.convertExcel(convertxls['folder'], convertxls['sheetname'], convertxls['fileType'],
                                        convertxls['subDir'])

        for f in rootpath.files(filter):
            files_tmp.append(f)

    for subdir, sourcename, filter, currency, subdirftp in todo:

        rootpath = Path(config["_IN_Sales"]) / subdir
        for f in rootpath.files(filter):
            print f

            _LoadProcess = LoadProcess()
            _LoadProcess.FileName = f
            _LoadProcess.Status = 0
            _LoadProcess.createDate = datetime.now()
            _LoadProcess.lastModified = datetime.now()
            _LoadProcess.Text = 'Error in lines : '

            session.add(_LoadProcess)
            session.commit()

            try:
                ImportData(f, _LoadProcess, sourcename, currency)
            except Exception as e:
                files_tmp.remove(f)
                files_err.append(f)
                break
            else:
                files_done.append(f)
                files_tmp.remove(f)

    if len(files_err) > 0:

        for f in rootpathftptodo.files('*.xls*'):
            toerror = rootpathftp / "error" / f.basename()
            f.move(toerror)

        for fe in files_err:
            print('err')
            newfilename = rootpathftp / "error" / 'ERR_{}_{:%Y%m%d%H%M}.csv'.format(
                fe.basename().replace('.csv', ''), datetime.now())
            fe.move(newfilename)
        for fd in files_done:
            print('done')
            newfilename = rootpathftp / "error" / 'DONE_{}_{:%Y%m%d%H%M}.csv'.format(
                fd.basename().replace('.csv', ''), datetime.now())
            fd.move(newfilename)
        for ft in files_tmp:
            print('tmp')
            newfilename = rootpathftp / "error" / 'TODO_{}_{:%Y%m%d%H%M}.csv'.format(
                ft.basename().replace('.csv', ''), datetime.now())
            ft.move(newfilename)
        for f in rootpath.files('*.csv'):
            newfilename = rootpath / "Error" / '{:%Y%m%d%H%M}_{}'.format(datetime.now(), f.basename())
            f.move(newfilename)
        raise e

    else:
        for f in files_done:
            print('done')
            newfilename = rootpath / "Loaded" / '{}_{:%Y%m%d%H%M}.csv'.format(f.basename().replace('.csv', ''),
                                                                              datetime.now())
            f.move(newfilename)
        for f in rootpathftptodo.files('*.xls*'):
            todone = rootpathftp / "done" / '{}_{:%Y%m%d%H%M}.{}'.format(f.basename().replace(f.ext, ''),
                                                                         datetime.now(), f.ext)
            f.move(todone)
        for f in rootpath.files('*.csv'):
            newfilename = rootpath / "Error" / '{:%Y%m%d%H%M}_{}'.format(datetime.now(), f.basename())
            f.move(newfilename)
        return len(files_done)


if __name__ == "__main__":
    main()
