import MySQLdb as mdb
import googlemaps as gm
from cep_credentials import cic_database, cic_user, cic_pass, cic_host
from cep_credentials import google_maps_key
from sigfox_api.api_tests import get_coverage
from time import sleep

dir = 'JARDINES DE GODOY,VILLAVICIOSA DE ODON 28670, Spain'

gmaps = gm.Client(key=key)
#loc = gmaps.geocode(dir)


def get_lat_long(cod_canalizado):
    con = mdb.connect(cic_host, cic_user, cic_pass, cic_database)
    cur = con.cursor()
    cur.execute(
        f"SELECT lat_dir, lon_dir, lat, lon FROM Canalizados_ex WHERE Codigo = '{cod_canalizado}'")
    row = cur.fetchone()
    con.close()
    if row:
        return row[2] if row[2] else row[0], row[3] if row[3] else row[1]
    else:
        return None, None


def get_theoric_signal(cod_canalizado):
    con = mdb.connect(cic_host, cic_user, cic_pass, cic_database)
    cur = con.cursor()
    cur.execute(
        f"SELECT sifox_signal_teorica FROM Canalizados_ex WHERE Codigo = '{cod_canalizado}'")
    row = cur.fetchone()
    con.close()
    if row:
        return row[0]
    else:
        return None


if __name__ == '__main__':
    sql = "select DISTINCT c.Codigo, c.Nombre, c.Localidad, c.CodPostal , ts.CCanalizado, ts.NSerial from Canalizados c," \
            " Telemedidas_SAP ts, Terminales t where t.deGasNatural =0 and if(right (t.Codigo,1)>='A',LEFT (t.Codigo,LENGTH (t.Codigo)-1) " \
            ",t.Codigo ) = ts.CCanalizado  and ts.CCanalizado = c.Codigo and (ts.BajaTemporal is null or ts.BajaTemporal =0)" \
            " order by c.Codigo ;"

    db = mdb.connect(cic_host, cic_user, cic_pass,
                     cic_database, charset='utf8')
    db2 = mdb.connect(cic_host, cic_user, cic_pass,
                      cic_database, charset='utf8')
    cursor2 = db2.cursor()
    cursor = db.cursor()
    cursor.execute(sql)
    rows = cursor.fetchall()
    for row in rows:
        t_loc = get_lat_long(row[0])
        if t_loc[0] is None:
            loc = gmaps.geocode(row[1] + ',' + row[2] + ',' + row[3])
            if not loc:
                loc = gmaps.geocode(row[1] + ',' + row[2] + ',' + row[3])
            if loc:
                print(loc)
                lat = loc[0]['geometry']['location']['lat']
                lng = loc[0]['geometry']['location']['lng']
                update = f"insert into Canalizados_ex (Codigo, lat_dir, lon_dir ) values ('{row[0]}', {lat}, {lng}) "\
                    f"on duplicate key update lat_dir = {lat}, lon_dir = {lng}"
                print(update)
                cursor2.execute(update)
                db2.commit()
                t_loc = (lat, lng)
            else:
                continue
        if t_loc[0] and not get_theoric_signal(row[0]):
            signal = get_coverage(t_loc[0], t_loc[1])
            stations = sum([1 if i != 0 else 0 for i in signal])
            print(signal)
            update = f"update Canalizados_ex set sifox_signal_teorica = {signal[0]}, sigfox_stations = {stations} where Codigo = '{row[0]}'"
            print(update)
            cursor2.execute(update)
            db2.commit()
            sleep(7)

    db.close()
