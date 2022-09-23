import calendar
from collections import Counter
from datetime import datetime, timedelta

from dateutil.relativedelta import relativedelta
from flask import Blueprint, jsonify, request
from sqlalchemy import text

from api.config import get_connection

pendapatan_bp = Blueprint('pendapatan', __name__)
engine = get_connection()


def get_default_date(tgl_awal, tgl_akhir):
    if tgl_awal == None:
        tgl_awal = datetime.today() - relativedelta(months=1)
        tgl_awal = tgl_awal.strftime('%Y-%m-%d')
    else:
        tgl_awal = datetime.strptime(tgl_awal, '%Y-%m-%d')

    if tgl_akhir == None:
        tgl_akhir = datetime.strptime(
            datetime.today().strftime('%Y-%m-%d'), '%Y-%m-%d')
    else:
        tgl_akhir = datetime.strptime(tgl_akhir, '%Y-%m-%d')
    return tgl_awal, tgl_akhir


@pendapatan_bp.route('/tren_pendapatan')
def tren_pendapatan():
    tgl_awal = request.args.get('tgl_awal')
    tahun = datetime.now().year if tgl_awal == None else int(tgl_awal[:4])
    result = engine.execute(
        text(
            f"""SELECT sbkm.TglBKM, sbkm.JmlBayar
            FROM dbo.StrukBuktiKasMasuk sbkm
            WHERE datepart(year,[TglBKM]) = {tahun-1}
            OR datepart(year,[TglBKM]) = {tahun}
            ORDER BY sbkm.TglBKM ASC;"""))

    curr_year, prev_year, predict = {}, {}, {}
    for i in range(1, 13):
        curr_year[calendar.month_name[i]] = 0
        prev_year[calendar.month_name[i]] = 0
        predict[calendar.month_name[i]] = 0

    for row in result:
        curr_m = calendar.month_name[row['TglBKM'].month]
        if row['TglBKM'].year == tahun:
            curr_year[curr_m] = curr_year[curr_m] + row['JmlBayar']
        else:
            prev_year[curr_m] = prev_year[curr_m] + row['JmlBayar']
        predict[curr_m] = predict[curr_m] + 0

    for i in range(1, 13):
        curr_m = calendar.month_name[i]
        try:
            curr_year[curr_m] = {
                    'total': curr_year[curr_m],
                    'trend_percent': ((curr_year[curr_m]-prev_year[curr_m])/prev_year[curr_m]) * 100,
                    'predict_percent': 0.0}
        except ZeroDivisionError:
            curr_year[curr_m] = {
                    'total': curr_year[curr_m],
                    'trend_percent': (curr_year[curr_m]-prev_year[curr_m]) * 100,
                    'predict_percent': 0.0}

    data = {
        "tahun": tahun,
        "total_tahun_ini": curr_year,
        "total_tahun_sebelumnya": prev_year,
        "total_prediksi": predict,
        "judul": "Tren Pendapatan",
        "label": 'Pendapatan'
    }
    return jsonify(data)


@pendapatan_bp.route('/pendapatan_instalasi')
def pendapatan_instalasi():
    tgl_awal = request.args.get('tgl_awal')
    tgl_akhir = request.args.get('tgl_akhir')
    tgl_awal, tgl_akhir = get_default_date(tgl_awal, tgl_akhir)
    result = engine.execute(
        text(
            f"""SELECT spp.TglStruk, spp.NoPendaftaran, i.NamaInstalasi , spp.TotalBiaya
           FROM rsudtasikmalaya.dbo.StrukPelayananPasien spp
           INNER JOIN rsudtasikmalaya.dbo.Ruangan r
           ON spp.KdRuanganTerakhir = r.KdRuangan
           INNER JOIN rsudtasikmalaya.dbo.Instalasi i
           ON r.KdInstalasi = i.KdInstalasi
           WHERE spp.TglStruk >= '{tgl_awal}'
           AND spp.TglStruk < '{tgl_akhir + timedelta(days=1)}'
           ORDER BY spp.TglStruk ASC;"""))
    data = []
    for row in result:
        data.append({
            "tanggal": row['TglStruk'],
            "instalasi": row['NamaInstalasi'],
            "total": row['TotalBiaya'],
            "judul": 'Pendapatan Instalasi',
            "label": 'Pendapatan'
        })
    cnt = Counter()
    for i in range(len(data)):
        cnt[data[i]['instalasi']] += data[i]['total']
    return jsonify(cnt)


@pendapatan_bp.route('/pendapatan_kelas')
def pendapatan_kelas():
    tgl_awal = request.args.get('tgl_awal')
    tgl_akhir = request.args.get('tgl_akhir')
    tgl_awal, tgl_akhir = get_default_date(tgl_awal, tgl_akhir)
    result = engine.execute(
        text(
            f"""SELECT spp.TglStruk, spp.NoPendaftaran, kp.DeskKelas, spp.TotalBiaya
           FROM dbo.StrukPelayananPasien spp
           INNER JOIN dbo.PasienDaftar pd
           ON spp.NoPendaftaran = pd.NoPendaftaran
           INNER JOIN dbo.KelasPelayanan kp
           ON pd.KdKelasAkhir = kp.KdKelas
           WHERE spp.TglStruk >= '{tgl_awal}'
           AND spp.TglStruk < '{tgl_akhir + timedelta(days=1)}'
           ORDER BY spp.TglStruk ASC;"""))
    data = []
    for row in result:
        data.append({
            "tanggal": row['TglStruk'],
            "kelas": row['DeskKelas'],
            "total": row['TotalBiaya'],
            "judul": 'Pendapatan Kelas',
            "label": 'Pendapatan'
        })
    cnt = Counter()
    for i in range(len(data)):
        cnt[data[i]['kelas']] += data[i]['total']
    return jsonify(cnt)



@pendapatan_bp.route('/pendapatan_produk')
def pendapatan_produk():
    return jsonify({'response': 'ini data pendapatan jenis produk'})


@pendapatan_bp.route('/pendapatan_cara_bayar')
def pendapatan_cara_bayar():
    tgl_awal = request.args.get('tgl_awal')
    tgl_akhir = request.args.get('tgl_akhir')
    tgl_awal, tgl_akhir = get_default_date(tgl_awal, tgl_akhir)
    result = engine.execute(
        text(
            f"""SELECT sbkm.TglBKM, cb.CaraBayar, sbkm.JmlBayar
            FROM dbo.StrukBuktiKasMasuk sbkm
            INNER JOIN dbo.CaraBayar cb
            ON sbkm.KdCaraBayar = cb.KdCaraBayar
            WHERE sbkm.TglBKM >= '{tgl_awal}'
            AND sbkm.TglBKM < '{tgl_akhir + timedelta(days=1)}'
            ORDER BY sbkm.TglBKM ASC;"""))
    data = []
    for row in result:
        data.append({
            "tanggal": row['TglBKM'],
            "cara_bayar": row['CaraBayar'],
            "total": row['JmlBayar'],
            "judul": 'Pendapatan Cara Bayar',
            "label": 'Pendapatan'
        })
    cnt = Counter()
    for i in range(len(data)):
        cnt[data[i]['cara_bayar']] += data[i]['total']
    return jsonify(cnt)

#  Counter
    # cnt = Counter()
    # for i in range(len(data)):
    #     cnt[data[i]['cara_bayar']] += 1
