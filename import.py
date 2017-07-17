"""
Module to import the dwd data into the database.
"""
import os
import logging
import glob
import csv
import sys
import yaml
import MySQLdb
import progressbar


def import_data(database, cursor, table, glob_string, fields, parser):
    """
    Generic import of data
    """

    # Read files in air_temperature directory
    files = glob.glob(glob_string)

    # Create progressbar
    progress = progressbar.ProgressBar(max_value=len(files))
    progress_i = 0

    # Delete old values
    try:
        cursor.execute('TRUNCATE TABLE `{table}`'.format(table=table))
        database.commit()
    except:
        logging.error('Could not truncate table "{table}"!'.format(table=table))
        sys.exit()

    # Prepare sql statement
    sql = 'INSERT INTO `{table}`(`{fields}`) VALUES({placeholder})'.format(
        table=table,
        fields=str.join('`,`', fields),
        placeholder=str.join(',', (['%s'] * len(fields)))
    )

    for file in files:
        filename = (os.path.join(
            os.path.dirname(os.path.abspath(__file__)),
            file
        ))

        # Store rows from csv to insert in batch mode
        item_bank = []

        # Read csv values
        with open(filename) as csvfile:
            csvreader = csv.reader(csvfile, delimiter=';')
            # Skip header row
            next(csvreader, None)

            # Save formated values
            for row in csvreader:
                item_bank.append(parser(row))

        # Insert into database
        cursor.executemany(sql, item_bank)
        database.commit()

        progress_i = progress_i + 1
        progress.update(progress_i)


def parse_air_temperature(row):
    """
    Parse a air temperature csv file row.
    """
    station_id = int(row[0].strip())
    measured_at = row[1].strip()
    qn_9 = int(row[2].strip())
    tt_tu = float(row[3].strip())
    rf_tu = float(row[4].strip())

    return (
        station_id,
        measured_at[:4] + '-' +
        measured_at[4:6] + '-' +
        measured_at[6:8] + ' ' +
        measured_at[8:10] + ':00:00',
        qn_9,
        tt_tu,
        rf_tu
    )


def import_air_temperature(database, cursor):
    """
    Import air temperature hourly values.

    ftp://ftp-cdc.dwd.de/pub/CDC/observations_germany/climate/hourly/air_temperature/historical/DESCRIPTION_obsgermany_climate_hourly_tu_historical_en.pdf
    QN_9: Quality level (1-10; 10 = best)
    TT_TU: 2m air temperature in C°
    RF_TU: 2m relative humidity in %

    Example from csv:
    STATIONS_ID;MESS_DATUM;QN_9;TT_TU;RF_TU;eor
       3987;1893010101;    5; -12.3;  84.0;eor
    """
    logging.info('Importing air temperature data...')

    import_data(
        database,
        cursor,
        'air_temperature',
        'data/air_temperature/historical/produkt_tu_stunde_*.txt',
        (
            'station_id',
            'measured_at',
            'qn_9',
            'tt_tu',
            'rf_tu'
        ),
        parse_air_temperature
    )


def parse_cloudiness(row):
    """
    Parse a cloudiness csv file row.
    """
    station_id = int(row[0].strip())
    measured_at = row[1].strip()
    qn_8 = int(row[2].strip())
    v_n_i = row[3].strip()
    v_n = float(row[4].strip())

    return(
        station_id,
        measured_at[:4] + '-' +
        measured_at[4:6] + '-' +
        measured_at[6:8] + ' ' +
        measured_at[8:10] + ':00:00',
        qn_8,
        v_n_i,
        v_n
    )


def import_cloudiness(database, cursor):
    """
    Import cloudiness hourly values.

    ftp://ftp-cdc.dwd.de/pub/CDC/observations_germany/climate/hourly/cloudiness/historical/DESCRIPTION_obsgermany_climate_hourly_cloudiness_historical_en.pdf
    QN_8: Quality level (1-10; 10 = best)
    V_N_I: Index how measurement is taken (P = human Person; I = Instrument)
    V_N: Total cloud cover (-1 = not determined; 1-8 / 8)

    Example from csv:
    STATIONS_ID;MESS_DATUM;QN_8;V_N_I; V_N;eor
       1260;1949010103;    1;   P;   8;eor
    """
    logging.info('Importing cloudiness data...')

    import_data(
        database,
        cursor,
        'cloudiness',
        'data/cloudiness/historical/produkt_n_stunde_*.txt',
        (
            'station_id',
            'measured_at',
            'qn_8',
            'v_n_i',
            'v_n'
        ),
        parse_cloudiness
    )


def parse_precipitation(row):
    """
    Parse a precipitation csv file row.
    """
    station_id = int(row[0].strip())
    measured_at = row[1].strip()
    qn_8 = int(row[2].strip())
    r_1 = float(row[3].strip())
    rs_ind = int(row[4].strip())
    wrtr = int(row[5].strip())

    return(
        station_id,
        measured_at[:4] + '-' +
        measured_at[4:6] + '-' +
        measured_at[6:8] + ' ' +
        measured_at[8:10] + ':00:00',
        qn_8,
        r_1,
        rs_ind,
        wrtr
    )


def import_precipitation(database, cursor):
    """
    Import precipitation hourly values.

    ftp://ftp-cdc.dwd.de/pub/CDC/observations_germany/climate/hourly/precipitation/historical/DESCRIPTION_obsgermany_climate_hourly_precipitation_historical_en.pdf
    QN_8: Quality level (1-10; 10 = best)
    R1: Hourly precipitation height in mm
    RS_IND: 0 = No precipitation; 1 = Precipitation has fallen
    WRTR: Form of precipitation (WR-code)

    Example from csv:
    STATIONS_ID;MESS_DATUM;QN_8;  R1;RS_IND;WRTR;eor
       1219;1995090100;    1;   0.0;   0;-999;eor
    """
    logging.info('Importing precipitation data...')

    import_data(
        database,
        cursor,
        'precipitation',
        'data/precipitation/historical/produkt_rr_stunde_*.txt',
        (
            'station_id',
            'measured_at',
            'qn_8',
            'r1',
            'rs_ind',
            'wrtr'
        ),
        parse_precipitation
    )


def parse_pressure(row):
    """
    Parse a pressure csv file row.
    """
    station_id = int(row[0].strip())
    measured_at = row[1].strip()
    qn_8 = int(row[2].strip())
    p = float(row[3].strip())
    p_0 = float(row[4].strip())

    return(
        station_id,
        measured_at[:4] + '-' +
        measured_at[4:6] + '-' +
        measured_at[6:8] + ' ' +
        measured_at[8:10] + ':00:00',
        qn_8,
        p,
        p_0
    )


def import_pressure(database, cursor):
    """
    Import pressure hourly values.

    ftp://ftp-cdc.dwd.de/pub/CDC/observations_germany/climate/hourly/pressure/historical/DESCRIPTION_obsgermany_climate_hourly_pressure_historical_en.pdf
    QN_8: Quality level (1-10; 10 = best)
    P: Mean sea level pressure in hPA
    P0: Pressure at station height in hPA

    Example from csv:
    STATIONS_ID;MESS_DATUM;QN_8;   P;  P0;eor
       1260;1949010103;    1;  901.3;-999;eor
    """
    logging.info('Importing pressure data...')

    import_data(
        database,
        cursor,
        'pressure',
        'data/pressure/historical/produkt_p0_stunde_*.txt',
        (
            'station_id',
            'measured_at',
            'qn_8',
            'p',
            'p0'
        ),
        parse_pressure
    )


def parse_soil_temperature(row):
    """
    Parse a soil temperature csv file row.
    """
    station_id = int(row[0].strip())
    measured_at = row[1].strip()
    qn_2 = int(row[2].strip())
    v_te002 = float(row[3].strip())
    v_te005 = float(row[4].strip())
    v_te010 = float(row[5].strip())
    v_te020 = float(row[6].strip())
    v_te050 = float(row[7].strip())
    v_te100 = float(row[8].strip())

    return(
        station_id,
        measured_at[:4] + '-' +
        measured_at[4:6] + '-' +
        measured_at[6:8] + ' ' +
        measured_at[8:10] + ':00:00',
        qn_2,
        v_te002,
        v_te005,
        v_te010,
        v_te020,
        v_te050,
        v_te100
    )


def import_soil_temperature(database, cursor):
    """
    Import soil temperature hourly values.

    ftp://ftp-cdc.dwd.de/pub/CDC/observations_germany/climate/hourly/soil_temperature/historical/DESCRIPTION_obsgermany_climate_hourly_soil_temperature_historical_en.pdf
    QN_2: Quality level (1-10; 10 = best)
    V_TE002: Soil temperature in 2 cm depth in C°
    V_TE005: Soil temperature in 5 cm depth in C°
    V_TE010: Soil temperature in 10 cm depth in C°
    V_TE020: Soil temperature in 20 cm depth in C°
    V_TE050: Soil temperature in 50 cm depth in C°
    V_TE100: Soil temperature in 100 cm depth in C°

    Example from csv:
    STATIONS_ID;MESS_DATUM;QN_2;V_TE002;V_TE005;V_TE010;V_TE020;V_TE050;V_TE100;eor
       3404;1949010107;    5;   1.4;   0.4;  -0.2;   0.1;   1.6;-999;eor
    """
    logging.info('Importing soil_temperature data...')

    import_data(
        database,
        cursor,
        'soil_temperature',
        'data/soil_temperature/historical/produkt_eb_stunde_*.txt',
        (
            'station_id',
            'measured_at',
            'qn_2',
            'v_te002',
            'v_te005',
            'v_te010',
            'v_te020',
            'v_te050',
            'v_te100'
        ),
        parse_soil_temperature
    )


def parse_solar(row):
    """
    Parse a solar csv file row.
    """
    station_id = int(row[0].strip())
    measured_started_at = row[1].strip()
    measured_ended_at = row[8].strip()
    qn_592 = int(row[2].strip())
    atmo_lberg = float(row[3].strip())
    fd_lberg = float(row[4].strip())
    fg_lberg = float(row[5].strip())
    sd_lberg = float(row[6].strip())
    zenit = float(row[7].strip())

    return(
        station_id,
        measured_started_at[:4] + '-' +
        measured_started_at[4:6] + '-' +
        measured_started_at[6:8] + ' ' +
        measured_started_at[8:10] + ':' +
        measured_started_at[11:13] + ':00',
        measured_ended_at[:4] + '-' +
        measured_ended_at[4:6] + '-' +
        measured_ended_at[6:8] + ' ' +
        measured_ended_at[8:10] + ':' +
        measured_ended_at[11:13] + ':00',
        qn_592,
        atmo_lberg,
        fd_lberg,
        fg_lberg,
        sd_lberg,
        zenit
    )


def import_solar(database, cursor):
    """
    Import solar hourly values.

    ftp://ftp-cdc.dwd.de/pub/CDC/observations_germany/climate/hourly/solar/DESCRIPTION_obsgermany_climate_hourly_solar_en.pdf
    QN_592: Quality level (1-10; 10 = best)
    ATMO_LBERG: Hourly sum of longwave downward radiation in J/cm^2
    FD_LBERG: Hourly sum of diffuse solar radiation in J/cm^2
    FG_LBERG: Hourly sum of solar incoming radiation in J/cm^2
    SD_LBERG: Hourly sum of sunshine duration in minutes
    ZENIT: Solar zenith angle at mid of interval in degree
    MESS_DATUM_WOZ: end of interval in local true solar time

    Example from csv:
    STATIONS_ID;MESS_DATUM;QN_592;ATMO_LBERG;FD_LBERG;FG_LBERG;SD_LBERG;ZENIT;MESS_DATUM_WOZ;eor
       5419;1949010100:18;    1;   -999;    0.0;    0.0;   0;   151.47;1949010101:00;eor
    """
    logging.info('Importing solar data...')

    import_data(
        database,
        cursor,
        'solar',
        'data/solar/produkt_st_stunde_*.txt',
        (
            'station_id',
            'measured_started_at',
            'measured_ended_at',
            'qn_592',
            'atmo_lberg',
            'fd_lberg',
            'fg_lberg',
            'sd_lberg',
            'zenit',
        ),
        parse_solar
    )


def parse_sun(row):
    """
    Parse a sun csv file row.
    """
    station_id = int(row[0].strip())
    measured_at = row[1].strip()
    qn_7 = int(row[2].strip())
    sd_so = float(row[3].strip())

    return(
        station_id,
        measured_at[:4] + '-' +
        measured_at[4:6] + '-' +
        measured_at[6:8] + ' ' +
        measured_at[8:10] + ':00:00',
        qn_7,
        sd_so
    )


def import_sun(database, cursor):
    """
    Import sun hourly values.

    ftp://ftp-cdc.dwd.de/pub/CDC/observations_germany/climate/hourly/sun/historical/DESCRIPTION_obsgermany_climate_hourly_sun_historical_en.pdf
    QN_7: Quality level (1-10; 10 = best)
    SD_SO: Hourly sunshine duration in minutes

    Example from csv:
    STATIONS_ID;MESS_DATUM;QN_7;SD_SO;eor
       1580;1890010103;    5;  0.00;eor
    """
    logging.info('Importing sun data...')

    import_data(
        database,
        cursor,
        'sun',
        'data/sun/historical/produkt_sd_stunde_*.txt',
        (
            'station_id',
            'measured_at',
            'qn_7',
            'sd_so'
        ),
        parse_sun
    )


def parse_wind(row):
    """
    Parse a wind csv file row.
    """
    station_id = int(row[0].strip())
    measured_at = row[1].strip()
    qn_3 = int(row[2].strip())
    f = float(row[3].strip())
    d = int(row[4].strip())

    return(
        station_id,
        measured_at[:4] + '-' +
        measured_at[4:6] + '-' +
        measured_at[6:8] + ' ' +
        measured_at[8:10] + ':00:00',
        qn_3,
        f,
        d
    )


def import_wind(database, cursor):
    """
    Import wind hourly values.

    ftp://ftp-cdc.dwd.de/pub/CDC/observations_germany/climate/hourly/wind/historical/DESCRIPTION_obsgermany_climate_hourly_wind_historical_en.pdf
    QN_3: Quality level (1-10; 10 = best)
    F: Mean wind speed in m/s
    D: Mean wind direction in degree

    Example from csv:
    STATIONS_ID;MESS_DATUM;QN_3;   F;   D;eor
       3987;1893010100;    5;   5.4;-999;eor
    """
    logging.info('Importing wind data...')

    import_data(
        database,
        cursor,
        'wind',
        'data/wind/historical/produkt_ff_stunde_*.txt',
        (
            'station_id',
            'measured_at',
            'qn_3',
            'f',
            'd'
        ),
        parse_wind
    )


def main():
    """
    Start the import.
    """

    logging.basicConfig(level=logging.DEBUG)

    # Get the filename of the config file
    config_filename = os.path.join(
        os.path.dirname(os.path.abspath(__file__)),
        'config.yaml'
    )

    # Try to read and parse the config file
    try:
        config = yaml.load(open(config_filename, 'r'))
    except yaml.YAMLError as exc:
        logging.error('Error in configuration file: %', exc)
        return
    except OSError as exc:
        logging.error('Could not read configuration file: %', exc)
        return

    logging.debug('Start importing data into database {user}@{host}:{port}/{database}'.format(
        user=config['DB_USERNAME'],
        host=config['DB_HOST'],
        port=config['DB_PORT'],
        database=config['DB_DATABASE']
    ))

    # Connect to database
    try:
        database = MySQLdb.connect(
            host=config['DB_HOST'],
            port=config['DB_PORT'],
            user=config['DB_USERNAME'],
            passwd=config['DB_PASSWORD'],
            db=config['DB_DATABASE']
        )
    except:
        logging.error('Could not connect to database!')
        return

    logging.debug('Successfully connected to database.')

    try:
        with database.cursor() as cursor:
            import_air_temperature(database, cursor)
            import_cloudiness(database, cursor)
            import_precipitation(database, cursor)
            import_pressure(database, cursor)
            import_soil_temperature(database, cursor)
            import_solar(database, cursor)
            import_sun(database, cursor)
            import_wind(database, cursor)
    finally:
        database.close()

    logging.info('Finished importing!')


if __name__ == '__main__':
    main()
