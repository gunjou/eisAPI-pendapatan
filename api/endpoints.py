from collections import Counter
from datetime import datetime, timedelta

from dateutil.relativedelta import relativedelta
from flask import Blueprint, jsonify, request
from sqlalchemy import text

from api.config import MONTH_ID as month_id
from api.config import get_connection

pendapatan_bp = Blueprint('pendapatan', __name__)
engine = get_connection()


def get_default_date(tgl_awal, tgl_akhir):
    if tgl_awal == None:
        tgl_awal = datetime.today() - relativedelta(months=1)
        tgl_awal = datetime.strptime(tgl_awal.strftime('%Y-%m-%d'), '%Y-%m-%d')
    else:
        tgl_awal = datetime.strptime(tgl_awal, '%Y-%m-%d')

    if tgl_akhir == None:
        tgl_akhir = datetime.strptime(
            datetime.today().strftime('%Y-%m-%d'), '%Y-%m-%d')
    else:
        tgl_akhir = datetime.strptime(tgl_akhir, '%Y-%m-%d')
    return tgl_awal, tgl_akhir


@pendapatan_bp.route('/pendapatan/tren_pendapatan')
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

    tren = {}
    for i in range(1, 13):
        tren[month_id[i]] = {
            "tahun_ini": 0,
            "tahun_sebelumnya": 0,
            "tahun_selanjutnya": 0,
            "persentase_tren": None,
            "persentase_predict": None
        }

    for row in result:
        curr_m = month_id[row['TglBKM'].month]
        if row['TglBKM'].year == tahun:
            tren[curr_m]['tahun_ini'] = round(
                tren[curr_m]['tahun_ini'] + float(row['JmlBayar']), 2)
        else:
            tren[curr_m]['tahun_sebelumnya'] = round(
                tren[curr_m]['tahun_sebelumnya'] + float(row['JmlBayar']), 2)
        tren[curr_m]['tahun_selanjutnya'] = round(
            tren[curr_m]['tahun_selanjutnya'] + float(0), 2)

    for i in range(1, 13):
        if tren[month_id[i]]['tahun_ini'] == 0 or tren[month_id[i]]['tahun_sebelumnya'] == 0:
            tren[month_id[i]]['persentase_tren'] = None
        else:
            tren[month_id[i]]['persentase_tren'] = round(((tren[month_id[i]]['tahun_ini'] - tren[month_id[i]]['tahun_sebelumnya'])
                                                          / tren[month_id[i]]['tahun_sebelumnya']) * 100, 2)
        if tren[month_id[i]]['tahun_ini'] == 0 or tren[month_id[i]]['tahun_selanjutnya'] == 0:
            tren[month_id[i]]['persentase_predict'] = None
        else:
            tren[month_id[i]]['persentase_predict'] = round(((tren[month_id[i]]['tahun_ini'] - tren[month_id[i]]['tahun_selanjutnya'])
                                                             / tren[month_id[i]]['tahun_selanjutnya']) * 100, 2)

    data = {
        "judul": "Tren Pendapatan",
        "label": 'Pendapatan',
        "tahun": tahun,
        "tren": tren
    }
    return jsonify(data)


@pendapatan_bp.route('/pendapatan/pendapatan_instalasi')
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
        cnt[data[i]['instalasi'].lower().replace(' ', '_')] += float(data[i]['total'])

    result = {
        "judul": 'Pendapatan Instalasi',
        "label": 'Pendapatan',
        "instalasi": cnt,
        "tgl_filter": {"tgl_awal": tgl_awal, "tgl_akhir": tgl_akhir}
    }
    return jsonify(result)


@pendapatan_bp.route('/pendapatan/pendapatan_kelas')
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
        cnt[data[i]['kelas'].lower().replace(' ', '_')] += float(data[i]['total'])

    result = {
        "judul": 'Pendapatan Kelas',
        "label": 'Pendapatan',
        "kelas": cnt,
        "tgl_filter": {"tgl_awal": tgl_awal, "tgl_akhir": tgl_akhir}
    }
    return jsonify(result)


@pendapatan_bp.route('/pendapatan/pendapatan_produk')
def pendapatan_produk():
    tgl_awal = request.args.get('tgl_awal')
    tgl_akhir = request.args.get('tgl_akhir')
    tgl_awal, tgl_akhir = get_default_date(tgl_awal, tgl_akhir)
    result = engine.execute(
        text(
            f"""SELECT evprsna.TanggalPelayanan, evprsna.Tarif, jp.Deskripsi
            FROM dbo.EIS_ViewPendapatanRumahSakitNewAll evprsna
            INNER JOIN dbo.ListPelayananRS lpr 
            ON evprsna.KdPelayananRS = lpr.KdPelayananRS 
            INNER JOIN dbo.JenisPelayanan jp 
            ON lpr.KdJnsPelayanan = jp.KdJnsPelayanan 
            WHERE evprsna.TanggalPelayanan >= '{tgl_awal}'
            AND evprsna.TanggalPelayanan < '{tgl_akhir + timedelta(days=1)}'
            ORDER BY evprsna.TanggalPelayanan ASC;"""))
    data = []
    for row in result:
        data.append({
            "tanggal": row['TanggalPelayanan'],
            "jenis_pelayanan": row['Deskripsi'],
            "total": row['Tarif']
        })
    cnt = Counter()
    for i in range(len(data)):
        cnt[data[i]['jenis_pelayanan'].lower().replace(' ', '_')] += float(data[i]['total'])

    result = {
        "judul": 'Pendapatan Jenis Produk',
        "label": 'Pendapatan',
        "cara_bayar": cnt,
        "tgl_filter": {"tgl_awal": tgl_awal, "tgl_akhir": tgl_akhir}
    }
    return jsonify(result)


@pendapatan_bp.route('/pendapatan/pendapatan_cara_bayar')
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
        cnt[data[i]['cara_bayar'].lower().replace(' ', '_')] += float(data[i]['total'])

    result = {
        "judul": 'Pendapatan Cara Bayar',
        "label": 'Pendapatan',
        "cara_bayar": cnt,
        "tgl_filter": {"tgl_awal": tgl_awal, "tgl_akhir": tgl_akhir}
    }
    return jsonify(result)
