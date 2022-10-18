from sqlalchemy import text

from api.config import get_connection

engine = get_connection()


def query_tren_pendapatan(tahun):
    result = engine.execute(
        text(f"""SELECT sbkm.TglBKM, sbkm.JmlBayar
            FROM dbo.StrukBuktiKasMasuk sbkm
            WHERE datepart(year,[TglBKM]) = {tahun-1}
            OR datepart(year,[TglBKM]) = {tahun}
            ORDER BY sbkm.TglBKM ASC;"""))
    return result


def query_pendapatan_instalasi(start_date, end_date):
    result = engine.execute(
        text(f"""SELECT spp.TglStruk, spp.NoPendaftaran, i.NamaInstalasi , spp.TotalBiaya
           FROM rsudtasikmalaya.dbo.StrukPelayananPasien spp
           INNER JOIN rsudtasikmalaya.dbo.Ruangan r
           ON spp.KdRuanganTerakhir = r.KdRuangan
           INNER JOIN rsudtasikmalaya.dbo.Instalasi i
           ON r.KdInstalasi = i.KdInstalasi
           WHERE spp.TglStruk >= '{start_date}'
           AND spp.TglStruk < '{end_date}'
           ORDER BY spp.TglStruk ASC;"""))
    return result


def query_pendapatan_kelas(start_date, end_date):
    result = engine.execute(
        text(f"""SELECT spp.TglStruk, spp.NoPendaftaran, kp.DeskKelas, spp.TotalBiaya
           FROM dbo.StrukPelayananPasien spp
           INNER JOIN dbo.PasienDaftar pd
           ON spp.NoPendaftaran = pd.NoPendaftaran
           INNER JOIN dbo.KelasPelayanan kp
           ON pd.KdKelasAkhir = kp.KdKelas
           WHERE spp.TglStruk >= '{start_date}'
           AND spp.TglStruk < '{end_date}'
           ORDER BY spp.TglStruk ASC;"""))
    return result


def query_pendapatan_produk(start_date, end_date):
    result = engine.execute(
        text(f"""SELECT evprsna.TanggalPelayanan, evprsna.Tarif, jp.Deskripsi
            FROM dbo.EIS_ViewPendapatanRumahSakitNewAll evprsna
            INNER JOIN dbo.ListPelayananRS lpr 
            ON evprsna.KdPelayananRS = lpr.KdPelayananRS 
            INNER JOIN dbo.JenisPelayanan jp 
            ON lpr.KdJnsPelayanan = jp.KdJnsPelayanan 
            WHERE evprsna.TanggalPelayanan >= '{start_date}'
            AND evprsna.TanggalPelayanan < '{end_date}'
            ORDER BY evprsna.TanggalPelayanan ASC;"""))
    return result


def query_pendapatan_cara_bayar(start_date, end_date):
    result = engine.execute(
        text(f"""SELECT sbkm.TglBKM, cb.CaraBayar, sbkm.JmlBayar
            FROM dbo.StrukBuktiKasMasuk sbkm
            INNER JOIN dbo.CaraBayar cb
            ON sbkm.KdCaraBayar = cb.KdCaraBayar
            WHERE sbkm.TglBKM >= '{start_date}'
            AND sbkm.TglBKM < '{end_date}'
            ORDER BY sbkm.TglBKM ASC;"""))
    return result