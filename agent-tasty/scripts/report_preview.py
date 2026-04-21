import pymssql, psycopg2, os, requests
from datetime import datetime, timezone, timedelta, date

panama_tz = timezone(timedelta(hours=-5))
today = date.today()
HANDY_TOKEN = os.getenv('HANDY_API_TOKEN')
HANDY_URL = os.getenv('HANDY_BASE_URL').rstrip('/')
headers = {'Authorization': f'Bearer {HANDY_TOKEN}'}

SKU_PRICES = {'PT00005':1.20,'PT00009':1.54,'PT00013':1.32,'PT00019':1.64,'PT00001':0.80,'PT00003':0.92,'PT00006':1.25,'PT00007':1.25,'PT00012':1.84,'PT00010':2.24,'PT00011':2.31,'PT00016':0.80,'PT00031':2.24,'PT00077':2.75}
routes = {
    'Panama Centro 01': {'lid':('8693080928453@lid',),'handy_id':74208},
    'Panama Oeste 01':  {'lid':('86123221115075@lid','6670268821675@lid'),'handy_id':74206},
    'Panama Este 01':   {'lid':('270823927636057@lid',),'handy_id':74232},
    'Colon 01':         {'lid':('172941337698403@lid',),'handy_id':74207},
    'UT Norte 01':      {'lid':('189244882571402@lid',),'handy_id':74191},
    'UT Oeste 02':      {'lid':('177979149783289@lid',),'handy_id':74202},
}

ms = pymssql.connect(server=os.getenv('MSSQL_HOST'),port=int(os.getenv('MSSQL_PORT','1433')),user=os.getenv('MSSQL_USER'),password=os.getenv('MSSQL_PASSWORD'),database=os.getenv('MSSQL_DATABASE'))
msc = ms.cursor()
msc.execute('SELECT Vendedor,SUM(Venta_Neta) FROM BI_ANALISIS_VENTAS WHERE Fecha_Documento=%s GROUP BY Vendedor',(today,))
vhoy = {r[0]:float(r[1]) for r in msc.fetchall()}
msc.execute("SELECT Vendedor,SUM(Venta_Neta) FROM BI_ANALISIS_VENTAS WHERE Mes='MAR' AND ANIO=2026 GROUP BY Vendedor")
vmes = {r[0]:float(r[1]) for r in msc.fetchall()}

pg = psycopg2.connect(os.getenv('DATABASE_URL'))
cur = pg.cursor()

print(f"REPORTE DE SUPERVISION — {today}")
print(f"{'Ruta':<22} | {'Inicio'} | {'Fin'} | {'Bot':>3} | {'Efect':>5} | {'T.Avg':>5} | {'Sug$':>7} | {'T.Prom':>7} | {'V.Hoy(BI)':>10} | {'V.Mes':>10} | {'%Meta':>6}")
print('-'*120)

for route, cfg in routes.items():
    lids = cfg['lid']
    ph = ','.join(['%s']*len(lids))

    cur.execute(f'SELECT COUNT(DISTINCT v.id) FROM salesrep_visits v WHERE v.phone_number IN ({ph}) AND DATE(v.visit_date)=%s', lids+(today,))
    bv = cur.fetchone()[0]

    cur.execute(f'SELECT i.product_code,SUM(i.suggested_order) FROM salesrep_visits v JOIN salesrep_visit_items i ON i.visit_id=v.id WHERE v.phone_number IN ({ph}) AND DATE(v.visit_date)=%s GROUP BY i.product_code', lids+(today,))
    sug = sum(float(q)*SKU_PRICES.get(c,0) for c,q in cur.fetchall())

    uid = cfg['handy_id']
    r = requests.get(f'{HANDY_URL}/api/v2/user/{uid}/salesOrder?page=1', headers=headers, timeout=10)
    tp = r.json().get('pagination',{}).get('totalPages',1)
    ot = []
    for p in [tp, tp-1]:
        if p < 1: continue
        r2 = requests.get(f'{HANDY_URL}/api/v2/user/{uid}/salesOrder?page={p}', headers=headers, timeout=10)
        for o in r2.json().get('salesOrders',[]):
            ds = o.get('mobileDateCreated','')
            if not ds: continue
            dt = datetime.fromisoformat(ds.replace('Z','+00:00')).astimezone(panama_tz)
            if dt.date() == today:
                ot.append((dt, float(o.get('totalSales',0))))
    ot.sort()

    if ot:
        ini = ot[0][0].strftime('%H:%M')
        fin = ot[-1][0].strftime('%H:%M')
        tm = int((ot[-1][0]-ot[0][0]).total_seconds()/60)
        nh = len(ot); av = tm//nh if nh else 0
        hv = sum(v for _,v in ot)
    else:
        ini = fin = '-'; nh = av = 0; hv = 0.0

    vb = vhoy.get(route, 0.0)
    vm = vmes.get(route, 0.0)
    tk = f'${hv/nh:.2f}' if nh else '-'
    mp = f'{vb/sug*100:.0f}%' if sug > 0 else '-'
    ef = f'{bv}/{nh}' if nh else '-'
    ta = f'{av}m' if av else '-'

    print(f'{route:<22} | {ini:<5} | {fin:<5} | {bv:>3} | {ef:>5} | {ta:>5} | {sug:>7.2f} | {tk:>7} | {vb:>10.2f} | {vm:>10.2f} | {mp:>6}')
