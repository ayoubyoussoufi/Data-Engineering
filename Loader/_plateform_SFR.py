from __future__ import division
from __future__ import unicode_literals

import re
from datetime import datetime
from turbodbc import connect, make_options
import unicodecsv as csv
from DAO import *




def ImportData(filename):
    HEADER = ['SourceId',
              'SourceTitle',
              'SerieTitle',
              'Studio',
              'Distributor',
              'ProductType',
              'VODEST',
              'ContentFlavor',
              'Genre',
              'DayId',
              'CustomerPrice',
              'EnglishTitle',
              'SalesVolume',
              'CustomerCurrency',
              'MS_Series',
              'MS_Season',
              'MS_LicenseType',
              'MS_LicenseClass',
              'MS_TitleDesc',
              'MS_ExternalID1',
              'MS_ContentType',
              'UPC',
              'Type',
              'Return'
              ]
    HEADER_NAMES = [[],  # ProductSourceId
                    ["TITRE_OEUVRE"],  # SourceTitle
                    ["TITRE_OEUVRE"],  # SerieTitle
                    ["STUDIO"],  # Studio
                    ["STUDIO"],  # Distributor
                    ["GENRE"],  # ProductType
                    ["LIBELLE_TYPE_DISTRIBUTION"],  # VODEST
                    ["QUALITE"],  # ContentFlavor
                    ["GENRE"],  # Genre
                    ["SEMAINE"],  # DayId
                    ["CA_TTC"],  # CA
                    [],  # EnglishTitle
                    ["VOLUMES"],  # SalesVolume
                    [],  # CustomerCurrency
                    [],  # MS_Series
                    [],  # MS_Season
                    [],  # MS_LicenseType
                    [],  # MS_LicenseClass
                    [],  # MS_TitleDesc
                    [],  # MS_ExternalID1
                    ["CANAL"],  # MS_ContentType
                    [],  # UPC
                    ["BOUTIQUE"],  # Platform
                    [],  # ISRC
                    [],  # Country
                    []  # salesType
                    ]
    HEADER_IDX = [-2 if not x else -1 for x in HEADER_NAMES]

    plateformcache = {}
    for sname, si in session.query(Source.SourceId, Source.Name):
        plateformcache[sname] = si

    options = make_options(prefer_unicode=True,  # needed for MS SQL
                           # read_buffer_size=Rows(5)
                           )

    conn = connect(driver=config['_dbdriver'],
                   server=config['_dbserver'],
                   port=config['_dbport'],
                   database=config['_dbname'],
                   uid=config['_dbuser'],
                   pwd=config['_dbpassword'],
                   turbodbc_options=options)


    cursor = conn.cursor()

    _TotalLoadedSalesVolume = 0
    _TotalLoadedSalesValue = 0
    p = Path(filename)
    pattern = r"^(?P<weekid>20\d{2}52\d{2})_.*\.csv$"
    match = re.search(pattern, p.basename(), re.UNICODE)
    assert match, "Expected WeekId in Filename!"
    _WeekId = match.group('weekid')
    # _days = session.execute("SELECT DISTINCT DayId FROM Calendar WHERE WeekId='{}';".format(_WeekId))
    _daysId = set()
    _source_ids = set()
    plateformedistinct =set()
    with open(filename, 'rb') as f:
        reader = csv.reader(f, delimiter=str(';'), quoting=csv.QUOTE_MINIMAL, encoding='utf-8')
        ite = enumerate(reader, 1)

        # skip useless rows until header and locate current period column
        for i, row in ite:
            for i, header in enumerate(row):
                for j, headerNames in enumerate(HEADER_NAMES):
                    if header.strip().upper() in headerNames:
                        HEADER_IDX[j] = i
            if row[4].strip():
                break

        for i, header in enumerate(HEADER_NAMES):
            if HEADER_IDX[i] == -1:
                error_msg = "Field {} not find using : {} ".format(HEADER[i], "/".join(header))
                raise Exception(error_msg.encode('utf-8'))
        for i, row in ite:
            _WeekId = row[HEADER_IDX[9]].strip()[:4] + str(52) + row[HEADER_IDX[9]].strip()[4:]
            _daysId.add(_WeekId)

            _Platform = 'SFR_' + row[HEADER_IDX[22]].strip()
            plateformedistinct.add(_Platform)
            if _Platform == 'SFR_UNIVERSCIN' or _Platform == 'SFR_UV':
                _Platform = 'SFR_UNIVERS_CINE'
            elif _Platform == 'SFR_DCL' or _Platform == 'SFR_STORE::DORCELTVODSTORE' or _Platform == 'SFR_DORCEL':
                _Platform = 'SFR_DORCEL'
            elif _Platform == 'SFR_METROPOLIT' or _Platform == 'SFR_STORE::CINEMADEMANDEFTTBSTORE' or _Platform == 'SFR_MF':
                _Platform = 'SFR_CINEMA_A_LA_DEMANDE'
            elif _Platform == 'SFR_CANALVOD' or _Platform == 'SFR_STORE::CANALTVODSTORE':
                _Platform = 'SFR_CANALVOD_DST'
            elif _Platform == 'SFR_ARTE' or _Platform == 'SFR_STORE::ARTEFTTBSTORE':
                _Platform = 'SFR_ARTEVOD'
            elif _Platform == 'SFR_NEWFILMOTV' or _Platform == 'SFR_STORE::FILMOFTTBSTORE':
                _Platform = 'SFR_FILMOVOD'



            sourceid = plateformcache.get(_Platform)
            if not sourceid:
                _platform = session.query(Source).filter(Source.Name == _Platform).one_or_none()
                if _platform is None:
                    print("Error: Platform '{}' does not exist in the database. Terminating process.".format(_Platform))
                    break  # Stop execution if the platform doesn't exist
                #     print('creation platform', _Platform)
                #     _platform = Platform()
                #     _platform.Name = _Platform
                #     session.add(_platform)
                #     session.commit()
                #     sourceid = _platform.SourceId
                #
                # else:
                sourceid = _platform.SourceId
                plateformcache[_Platform] = sourceid

            _source_ids.add(sourceid)
        print(plateformedistinct)
    Dayval_query = """select  TOP 1 DayId from Calendar where WeekId IN ('{}') ORDER BY DayId """.format(','.join(map(str, _daysId)))
    print(Dayval_query)
    cursor.execute(Dayval_query)
    Dayval = cursor.fetchone()[0]
    print(Dayval)
    delete_r = """
                    DELETE FROM RawData WHERE RawdataId IN (
                    SELECT r.RawdataId FROM RawData r
                    INNER JOIN Product p ON
                    p.ProductId=r.ProductId
                    WHERE r.DayId = {} AND r.SourceId IN({}))
                    """.format(Dayval, ','.join(map(str, _source_ids)))
    print(delete_r)
    session.execute(delete_r)
    session.commit()

    batch_data = []
    with open(filename, 'rb') as f:
        reader = csv.reader(f, delimiter=str(';'), quoting=csv.QUOTE_MINIMAL, encoding='utf-8')
        ite = enumerate(reader, 1)

        # skip useless rows until header and locate current period column
        for i, row in ite:
            for i, header in enumerate(row):
                for j, headerNames in enumerate(HEADER_NAMES):
                    if header.strip().upper() in headerNames:
                        HEADER_IDX[j] = i
            if row[4].strip():
                break

        for i, header in enumerate(HEADER_NAMES):
            if HEADER_IDX[i] == -1:
                error_msg = "Field {} not find using : {} ".format(HEADER[i], "/".join(header))
                raise Exception(error_msg.encode('utf-8'))

        try:
            # Truncate the table before the loop to insert new values
            cursor.execute("TRUNCATE TABLE [tmp].[SFRVOD]")

            # Loop through the dataset and insert each row
            for i, row in ite:
                _SourceTitle = row[HEADER_IDX[1]].strip()
                _SerieTitle = row[HEADER_IDX[2]].strip()
                _Studio = row[HEADER_IDX[3]].strip()
                _Distributor = row[HEADER_IDX[4]].strip()
                _ProductType = row[HEADER_IDX[5]].strip()
                _VODEST = row[HEADER_IDX[6]].strip().replace('ACHAT(EST)','EST').replace('LOCATION(DTR)','VOD')
                _ContentFlavor = row[HEADER_IDX[7]].strip()
                _Genre = row[HEADER_IDX[8]].strip()
                _WeekId = row[HEADER_IDX[9]].strip()[:4] + str(52) + row[HEADER_IDX[9]].strip()[4:]
                _CA = float(row[HEADER_IDX[10]].strip().replace(',', '.').replace(' ', ''))
                _SalesVolume = float(row[HEADER_IDX[12]].strip().replace(',', '.').replace(' ', ''))
                _MS_ContentType = row[HEADER_IDX[20]].strip()


                _Platform = 'SFR_' + row[HEADER_IDX[22]].strip()
                if _Platform == 'SFR_UNIVERSCIN' or _Platform == 'SFR_UV':
                    _Platform = 'SFR_UNIVERS_CINE'
                elif _Platform == 'SFR_DCL' or _Platform == 'SFR_STORE::DORCELTVODSTORE' or _Platform == 'SFR_DORCEL':
                    _Platform = 'SFR_DORCEL'
                elif _Platform == 'SFR_METROPOLIT' or _Platform == 'SFR_STORE::CINEMADEMANDEFTTBSTORE' or _Platform == 'SFR_MF':
                    _Platform = 'SFR_CINEMA_A_LA_DEMANDE'
                elif _Platform == 'SFR_CANALVOD' or _Platform == 'SFR_STORE::CANALTVODSTORE':
                    _Platform = 'SFR_CANALVOD_DST'
                elif _Platform == 'SFR_ARTE' or _Platform == 'SFR_STORE::ARTEFTTBSTORE':
                    _Platform = 'SFR_ARTEVOD'
                elif _Platform == 'SFR_NEWFILMOTV' or _Platform == 'SFR_STORE::FILMOFTTBSTORE':
                    _Platform = 'SFR_FILMOVOD'



                _sourceproductUID = 'VOD_SFR_'+_Platform +_SourceTitle + _SerieTitle + _ContentFlavor + _VODEST
                _sourceproductID = 'VOD_SFR_' + _SourceTitle

                if _SalesVolume in ['', '0', None]:
                    continue
                if _SourceTitle in [None, '']:
                    continue
                if _CA in ['', '0', None]:
                    continue

                _TotalLoadedSalesVolume += _SalesVolume
                _TotalLoadedSalesValue += _CA
                # Insert each row into the table
                batch_data.append((_sourceproductUID, sourceid, _sourceproductID, _SourceTitle, _SerieTitle, _Studio,
                                   _Distributor, _ProductType, _VODEST, _ContentFlavor, _Genre, _CA,
                                   _SalesVolume, _MS_ContentType, _Platform, int(_WeekId),1))
                if len(batch_data) >= 6000:
                    cursor.executemany("""INSERT INTO [tmp].[SFRVOD]
                                                ([SourceProductUID], [SourceId], [SourceProductId], [SourceTitle], 
                                                 [SerieTitle], [Studio], [Distributor], [ProductType], [VODEST], 
                                                 [ContentFlavor], [Genre], [CA], [SalesVolume], 
                                                 [MS_ContentType], [Platform], [WeekId],[CountryId])
                                                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""", batch_data)
                    batch_data = []  # Reset batch

                    # Insert any remaining data
            if batch_data:
                cursor.executemany("""INSERT INTO [tmp].[SFRVOD]
                                        ([SourceProductUID], [SourceId], [SourceProductId], [SourceTitle], 
                                         [SerieTitle], [Studio], [Distributor], [ProductType], [VODEST], 
                                         [ContentFlavor], [Genre], [CA], [SalesVolume], 
                                         [MS_ContentType], [Platform], [WeekId],[CountryId])
                                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,?)""", batch_data)
            # Commit the transaction after the loop
            conn.commit()
            print("execution Stored Procedure")
            cursor.execute("execute [dbo].[ImportVODSFR]")
            conn.commit()
        except Exception as e:
            print("An error occurred:", e)
            conn.rollback()

        finally:
            cursor.close()
            conn.close()
        print("Total Sales Volume: " + str(_TotalLoadedSalesVolume) + " Total Sales Value: " + str(_TotalLoadedSalesValue))
        print('Import data Done SFR')
def main():
    todo = [('PLATFORM\\SFR', '*.csv'), ]

    files_tmp = []
    files_err = []
    files_done = []


    for subdir, filter in todo:

        rootpath = Path(config['_IN_Sales']) / subdir
        rootpathftp = Path(config['_IN_Sales_FTP']) / subdir
        rootpathftptodo = Path(config['_IN_Sales_FTP']) / subdir / 'todo'

        for f in rootpathftptodo.files(filter):
            tolocal = rootpath / f.basename()
            f.copy(rootpathftp / f.basename())
            f.move(tolocal)
            print(tolocal)

        for f in rootpath.files(filter):
            files_tmp.append(f)
            newfilename = rootpath / "Loaded" / '{:%Y%m%d%H%M}_{}'.format(datetime.now(), f.basename())
            f.copy(newfilename)

        #  toFTP = Path(config['_IN_Sales_FTP']) / subdir / newfilename.basename()
        #  newfilename.copy(toFTP)

    for subdir, filter in todo:
        rootpathftp = Path(config['_IN_Sales_FTP']) / subdir
        rootpathftptodo = Path(config['_IN_Sales_FTP']) / subdir / 'todo'

        rootpath = Path(config['_IN_Sales']) / subdir


        for filename in rootpath.files(filter):
            print(filename)
            try:
                ImportData(filename)
            except:
                files_tmp.remove(filename)
                files_err.append(filename)
                #sheet_TDB.cell(20, 3).value = 'ERR intégration'
                if files_err != []:
                    for fe in files_err:
                        newfilenameloaded = rootpath / "error" / '{:%Y%m%d%H%M}_{}'.format(datetime.now(),
                                                                                           fe.basename())
                        fe.copy(newfilenameloaded)
                        newfilename = rootpathftp / "error" / 'ERR_{}_{:%Y%m%d%H%M}.csv'.format(
                            fe.basename().replace('.txt', ''), datetime.now())
                        fe.move(newfilename)

                if files_tmp != []:
                    for ft in files_tmp:
                        print('tmp')
                        newfilenameloaded = rootpath / "error" / '{:%Y%m%d%H%M}_{}'.format(datetime.now(),
                                                                                           ft.basename())
                        ft.copy(newfilenameloaded)
                        newfilename = rootpathftp / "error" / 'TODO_{}_{:%Y%m%d%H%M}.csv'.format(
                            ft.basename().replace('.txt', ''), datetime.now())
                        ft.move(newfilename)
                if files_done != []:
                    for fd in files_done:
                        print('done')
                        newfilename = rootpathftp / "error" / 'DONE_{}_{:%Y%m%d%H%M}.csv'.format(
                            fd.basename().replace('.txt', ''), datetime.now())
                        fd.move(newfilename)

                for file in rootpathftp.files(filter):
                    filenameerror = rootpathftp / "error" / '{}'.format(file.basename())

                    file.move(filenameerror)

                raise

            else:
                files_done.append(filename)
                files_tmp.remove(filename)
                newfilenameloaded = rootpath / "loaded" / '{:%Y%m%d%H%M}_{}'.format(datetime.now(),
                                                                                    filename.basename())
                filename.move(newfilenameloaded)

    for f in rootpathftp.files(filter):
        newfilenameloaded = rootpathftp / "loaded" / '{:%Y%m%d%H%M}_{}'.format(datetime.now(),
                                                                               f.basename())
        f.move(newfilenameloaded)


    for f in rootpath.files(filter):
        newfilename = rootpath / "error" / '{:%Y%m%d%H%M}_{}'.format(datetime.now(), f.basename())
        f.move(newfilename)


if __name__ == "__main__":

    main()

