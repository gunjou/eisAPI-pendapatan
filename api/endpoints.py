from collections import Counter
from datetime import datetime, timedelta

from dateutil.relativedelta import relativedelta
from flask import Blueprint, jsonify, request

from api.config import MONTH_ID as month_id
from api.config import get_connection
from api.query import *

pendapatan_bp = Blueprint('pendapatan', __name__)
engine = get_connection()


def get_default_date(tgl_awal, tgl_akhir):
    if tgl_awal == None:
        tgl_awal = datetime.strptime((datetime.today() - relativedelta(months=1)).strftime('%Y-%m-%d'), '%Y-%m-%d')
    else:
        tgl_awal = datetime.strptime(tgl_awal, '%Y-%m-%d')

    if tgl_akhir == None:
        tgl_akhir = datetime.strptime(datetime.today().strftime('%Y-%m-%d'), '%Y-%m-%d')
    else:
        tgl_akhir = datetime.strptime(tgl_akhir, '%Y-%m-%d')
    return tgl_awal, tgl_akhir


def get_date_prev(tgl_awal, tgl_akhir):
    tgl_awal = tgl_awal - relativedelta(months=1)
    tgl_awal = tgl_awal.strftime('%Y-%m-%d')
    tgl_akhir = tgl_akhir - relativedelta(months=1)
    tgl_akhir = tgl_akhir.strftime('%Y-%m-%d')
    return tgl_awal, tgl_akhir


def count_values(data, param):
    cnt = Counter()
    for i in range(len(data)):
        cnt[data[i][param]] += float(data[i]['total'])
    return cnt


@pendapatan_bp.route('/pendapatan/tren_pendapatan')
def tren_pendapatan():
    # Date Initialization
    tgl_awal = request.args.get('tgl_awal')
    tahun = datetime.now().year if tgl_awal == None else int(tgl_awal[:4])

    # Get query result
    result = query_tren_pendapatan(tahun)

    # Initialization Trend Data
    tren = []
    for i in range(1, 13):
        tren.append({
            "month": month_id[i],
            "tahun_ini": 0,
            "tahun_sebelumnya": 0,
            "tahun_selanjutnya": 0,
            "persentase_tren": None,
            "persentase_predict": None
        })

    # Extract data income
    for row in result:
        curr_m = month_id[row['TglBKM'].month]
        for i in range(len(tren)):
            if row['TglBKM'].year == tahun and tren[i]['month'] == curr_m:
                tren[i]['tahun_ini'] = round(
                    tren[i]['tahun_ini'] + float(row['JmlBayar']), 2)
            else:
                if tren[i]['month'] == curr_m:
                    tren[i]['tahun_sebelumnya'] = round(
                        tren[i]['tahun_sebelumnya'] + float(row['JmlBayar']), 2)

            tren[i]['tahun_sebelumnya'] = round(
                    tren[i]['tahun_sebelumnya'] + float(0), 2)

    # Define trend percentage
    for i in range(len(tren)):
        if tren[i]['tahun_ini'] == 0 or tren[i]['tahun_sebelumnya'] == 0:
            tren[i]['persentase_tren'] = None
        else:
            tren[i]['persentase_tren'] = round(((tren[i]['tahun_ini'] - tren[i]['tahun_sebelumnya'])
                                                / tren[i]['tahun_sebelumnya']) * 100, 2)
        if tren[i]['tahun_ini'] == 0 or tren[i]['tahun_selanjutnya'] == 0:
            tren[i]['persentase_predict'] = None
        else:
            tren[i]['persentase_predict'] = round(((tren[i]['tahun_ini'] - tren[i]['tahun_selanjutnya'])
                                                / tren[i]['tahun_selanjutnya']) * 100, 2)

    # Define return result as a json
    data = {
        "judul": "Tren Pendapatan",
        "label": 'Pendapatan',
        "tahun": tahun,
        "data": tren
    }
    return jsonify(data)


@pendapatan_bp.route('/pendapatan/pendapatan_instalasi')
def pendapatan_instalasi():
    # Date Initialization
    tgl_awal = request.args.get('tgl_awal')
    tgl_akhir = request.args.get('tgl_akhir')
    tgl_awal, tgl_akhir = get_default_date(tgl_awal, tgl_akhir)
    tgl_awal_prev, tgl_akhir_prev = get_date_prev(tgl_awal, tgl_akhir)

    # Get query result
    result = query_pendapatan_instalasi(tgl_awal, tgl_akhir + timedelta(days=1))
    result_prev = query_pendapatan_instalasi(tgl_awal_prev, datetime.strptime(tgl_akhir_prev, '%Y-%m-%d') + timedelta(days=1))
    
    # Extract data by date (dict)
    tmp = [{"tanggal": row['TglStruk'], "instalasi": row['NamaInstalasi'], "total": row['TotalBiaya']} for row in result]
    tmp_prev = [{"tanggal": row['TglStruk'], "instalasi": row['NamaInstalasi'], "total": row['TotalBiaya']} for row in result_prev]

    # Extract data as (dataframe)
    cnts = count_values(tmp, 'instalasi')
    cnts_prev = count_values(tmp_prev, 'instalasi')
    data = [{"name": x, "value": round(y, 2)} for x, y in cnts.items()]
    data_prev = [{"name": x, "value": round(y, 2)} for x, y in cnts_prev.items()]

    # Define trend percentage
    for i in range(len(cnts)):
        percentage = None
        for j in range(len(cnts_prev)):
            if data[i]["name"] == data_prev[j]["name"]:
                percentage = (data[i]["value"] - data_prev[j]["value"]) / data[i]["value"]
            try:
                data[i]["trend"] = round(percentage, 3)
            except:
                data[i]["trend"] = percentage
        data[i]["predict"] = None

    # Define return result as a json
    result = {
        "judul": 'Pendapatan Instalasi',
        "label": 'Pendapatan',
        "data": data,
        "tgl_filter": {"tgl_awal": tgl_awal, "tgl_akhir": tgl_akhir}
    }
    return jsonify(result)


@pendapatan_bp.route('/pendapatan/pendapatan_kelas')
def pendapatan_kelas():
    # Date Initialization
    tgl_awal = request.args.get('tgl_awal')
    tgl_akhir = request.args.get('tgl_akhir')
    tgl_awal, tgl_akhir = get_default_date(tgl_awal, tgl_akhir)
    tgl_awal_prev, tgl_akhir_prev = get_date_prev(tgl_awal, tgl_akhir)

    # Get query result
    result = query_pendapatan_kelas(tgl_awal, tgl_akhir + timedelta(days=1))
    result_prev = query_pendapatan_kelas(tgl_awal_prev, datetime.strptime(tgl_akhir_prev, '%Y-%m-%d') + timedelta(days=1))

    # Extract data by date (dict)
    tmp = [{"tanggal": row['TglStruk'], "kelas": row['DeskKelas'], "total": row['TotalBiaya']} for row in result]
    tmp_prev = [{"tanggal": row['TglStruk'], "kelas": row['DeskKelas'], "total": row['TotalBiaya']} for row in result_prev]

    # Extract data as (dataframe)
    cnts = count_values(tmp, 'kelas')
    cnts_prev = count_values(tmp_prev, 'kelas')
    data = [{"name": x, "value": round(y, 2)} for x, y in cnts.items()]
    data_prev = [{"name": x, "value": round(y, 2)} for x, y in cnts_prev.items()]

    # Define trend percentage
    for i in range(len(cnts)):
        percentage = None
        for j in range(len(cnts_prev)):
            if data[i]["name"] == data_prev[j]["name"]:
                percentage = (data[i]["value"] - data_prev[j]["value"]) / data[i]["value"]
            try:
                data[i]["trend"] = round(percentage, 3)
            except:
                data[i]["trend"] = percentage
        data[i]["predict"] = None

    # Define return result as a json
    result = {
        "judul": 'Pendapatan Kelas',
        "label": 'Pendapatan',
        "data": data,
        "tgl_filter": {"tgl_awal": tgl_awal, "tgl_akhir": tgl_akhir}
    }
    return jsonify(result)


@pendapatan_bp.route('/pendapatan/pendapatan_produk')
def pendapatan_produk():
    # Date Initialization
    tgl_awal = request.args.get('tgl_awal')
    tgl_akhir = request.args.get('tgl_akhir')
    tgl_awal, tgl_akhir = get_default_date(tgl_awal, tgl_akhir)
    tgl_awal_prev, tgl_akhir_prev = get_date_prev(tgl_awal, tgl_akhir)

    # Get query result
    result = query_pendapatan_produk(tgl_awal, tgl_akhir + timedelta(days=1))
    result_prev = query_pendapatan_produk(tgl_awal_prev, datetime.strptime(tgl_akhir_prev, '%Y-%m-%d') + timedelta(days=1))

    # Extract data by date (dict)
    tmp = [{"tanggal": row['TanggalPelayanan'], "jenis_pelayanan": row['Deskripsi'], "total": row['Tarif']} for row in result]
    tmp_prev = [{"tanggal": row['TanggalPelayanan'], "jenis_pelayanan": row['Deskripsi'], "total": row['Tarif']} for row in result_prev]

    # Extract data as (dataframe)
    cnts = count_values(tmp, 'jenis_pelayanan')
    cnts_prev = count_values(tmp_prev, 'jenis_pelayanan')
    data = [{"name": x, "value": round(y, 2)} for x, y in cnts.items()]
    data_prev = [{"name": x, "value": round(y, 2)} for x, y in cnts_prev.items()]

    # Define trend percentage
    for i in range(len(cnts)):
        percentage = None
        for j in range(len(cnts_prev)):
            if data[i]["name"] == data_prev[j]["name"]:
                percentage = (data[i]["value"] - data_prev[j]["value"]) / data[i]["value"]
            try:
                data[i]["trend"] = round(percentage, 3)
            except:
                data[i]["trend"] = percentage
        data[i]["predict"] = None

    # Define return result as a json
    result = {
        "judul": 'Pendapatan Jenis Produk',
        "label": 'Pendapatan',
        "data": data,
        "tgl_filter": {"tgl_awal": tgl_awal, "tgl_akhir": tgl_akhir}
    }
    return jsonify(result)


@pendapatan_bp.route('/pendapatan/pendapatan_cara_bayar')
def pendapatan_cara_bayar():
    # Date Initialization
    tgl_awal = request.args.get('tgl_awal')
    tgl_akhir = request.args.get('tgl_akhir')
    tgl_awal, tgl_akhir = get_default_date(tgl_awal, tgl_akhir)
    tgl_awal_prev, tgl_akhir_prev = get_date_prev(tgl_awal, tgl_akhir)

    # Get query result
    result = query_pendapatan_cara_bayar(tgl_awal, tgl_akhir + timedelta(days=1))
    result_prev = query_pendapatan_cara_bayar(tgl_awal_prev, datetime.strptime(tgl_akhir_prev, '%Y-%m-%d') + timedelta(days=1))

    # Extract data by date (dict)
    tmp = [{"tanggal": row['TglBKM'], "cara_bayar": row['CaraBayar'], "total": row['JmlBayar']} for row in result]
    tmp_prev = [{"tanggal": row['TglBKM'], "cara_bayar": row['CaraBayar'], "total": row['JmlBayar']} for row in result_prev]

    # Extract data as (dataframe)
    cnts = count_values(tmp, 'cara_bayar')
    cnts_prev = count_values(tmp_prev, 'cara_bayar')
    data = [{"name": x, "value": round(y, 2)} for x, y in cnts.items()]
    data_prev = [{"name": x, "value": round(y, 2)} for x, y in cnts_prev.items()]

    # Define trend percentage
    for i in range(len(cnts)):
        percentage = None
        for j in range(len(cnts_prev)):
            if data[i]["name"] == data_prev[j]["name"]:
                percentage = (data[i]["value"] - data_prev[j]["value"]) / data[i]["value"]
            try:
                data[i]["trend"] = round(percentage, 3)
            except:
                data[i]["trend"] = percentage
        data[i]["predict"] = None

    # Define return result as a json
    result = {
        "judul": 'Pendapatan Cara Bayar',
        "label": 'Pendapatan',
        "data": data,
        "tgl_filter": {"tgl_awal": tgl_awal, "tgl_akhir": tgl_akhir}
    }
    return jsonify(result)
