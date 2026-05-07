#!/usr/bin/env python3
from __future__ import annotations

import csv
import hashlib
import random
from collections import defaultdict
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Dict, List

SEED = 20260421
random.seed(SEED)

N_CLIENTES = 1200
N_CUENTAS = 1800
N_TRANSACCIONES = 9000
N_DISPOSITIVOS = 900
N_UBICACIONES = 160
N_COMERCIOS = 450
N_ALERTAS_OBJETIVO = 1400

ROOT = Path(__file__).resolve().parents[1]
CSV_DIR = ROOT / "csv"
CSV_DIR.mkdir(parents=True, exist_ok=True)

TODAY = datetime(2026, 4, 21, 12, 0, 0)


@dataclass
class Cliente:
    cliente_id: str
    nombre_completo: str
    documento_id: str
    fecha_nacimiento: date
    fecha_alta: date
    segmento: str
    ingresos_mensuales: float
    pep: bool
    pais_residencia: str
    riesgo_base: float
    telefonos: List[str]
    emails: List[str]
    productos_activos: List[str]
    ultima_actualizacion: datetime


@dataclass
class Cuenta:
    cuenta_id: str
    owner_cliente_id: str
    iban: str
    tipo_cuenta: str
    moneda_base: str
    fecha_apertura: date
    estado: str
    saldo_inicial: float
    saldo_actual: float
    limite_diario: float
    es_conjunta: bool
    canales_habilitados: List[str]
    pais_apertura: str
    score_riesgo: float
    ultima_actividad: datetime


@dataclass
class Dispositivo:
    dispositivo_id: str
    fingerprint: str
    tipo_dispositivo: str
    sistema_operativo: str
    app_version: str
    ip_publica: str
    imei_hash: str
    rooteado: bool
    emulador: bool
    idiomas: List[str]
    fecha_registro: date
    ultima_actividad: datetime
    confianza: float


@dataclass
class Ubicacion:
    ubicacion_id: str
    pais: str
    ciudad: str
    region: str
    codigo_postal: str
    latitud: float
    longitud: float
    zona_horaria: str
    es_frontera: bool
    nivel_riesgo_geo: int
    coordenadas_vecinas: List[str]
    fecha_actualizacion: date


@dataclass
class Comercio:
    comercio_id: str
    nombre_comercio: str
    categoria_mcc: str
    industria: str
    pais: str
    ciudad: str
    riesgo_comercio: float
    antiguedad_meses: int
    blacklist: bool
    metodos_pago_aceptados: List[str]
    fecha_alta: date
    rating_promedio: float
    url: str
    ubicacion_id: str
    settlement_cuenta_id: str


@dataclass
class TxBlueprint:
    tx_id: str
    referencia_externa: str
    fecha_hora: datetime
    fecha_valor: date
    monto: float
    moneda: str
    tipo_tx: str
    canal: str
    estado: str
    internacional: bool
    contracargo: bool
    velocity_1h: int
    etiquetas: List[str]
    score_fraude: float
    cliente_id: str
    cuenta_debito_id: str
    cuenta_credito_id: str
    dispositivo_id: str
    ubicacion_id: str
    comercio_id: str | None
    factor_autenticacion: str
    autenticacion_ok: bool
    confianza_sesion: float


def clamp(value: float, low: float, high: float) -> float:
    return max(low, min(high, value))


def iso_date(d: date) -> str:
    return d.strftime("%Y-%m-%d")


def iso_dt(dt: datetime) -> str:
    return dt.strftime("%Y-%m-%dT%H:%M:%SZ")


def join_list(values: List[str]) -> str:
    return "|".join(values)


def random_date(start: date, end: date) -> date:
    delta = (end - start).days
    return start + timedelta(days=random.randint(0, delta))


def random_datetime(start: datetime, end: datetime) -> datetime:
    delta = int((end - start).total_seconds())
    return start + timedelta(seconds=random.randint(0, delta))


def slug(text: str) -> str:
    clean = "".join(ch.lower() if ch.isalnum() else "." for ch in text)
    while ".." in clean:
        clean = clean.replace("..", ".")
    return clean.strip(".")


def weighted_choice(options: List[str], weights: List[float]) -> str:
    return random.choices(options, weights=weights, k=1)[0]


def write_csv(path: Path, header: List[str], rows: List[List[object]]) -> None:
    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(header)
        writer.writerows(rows)


def generate_ubicaciones() -> Dict[str, Ubicacion]:
    base_cities = [
        ("GT", "Guatemala", "Guatemala", 14.6349, -90.5069, "America/Guatemala", 2),
        ("GT", "Quetzaltenango", "Quetzaltenango", 14.8456, -91.5189, "America/Guatemala", 3),
        ("GT", "Escuintla", "Escuintla", 14.3050, -90.7850, "America/Guatemala", 3),
        ("SV", "San Salvador", "San Salvador", 13.6929, -89.2182, "America/El_Salvador", 4),
        ("HN", "Tegucigalpa", "Francisco Morazan", 14.0723, -87.1921, "America/Tegucigalpa", 4),
        ("CR", "San Jose", "San Jose", 9.9281, -84.0907, "America/Costa_Rica", 2),
        ("PA", "Panama City", "Panama", 8.9824, -79.5199, "America/Panama", 3),
        ("MX", "Ciudad de Mexico", "CDMX", 19.4326, -99.1332, "America/Mexico_City", 3),
        ("MX", "Monterrey", "Nuevo Leon", 25.6866, -100.3161, "America/Monterrey", 3),
        ("US", "Miami", "Florida", 25.7617, -80.1918, "America/New_York", 3),
        ("US", "Houston", "Texas", 29.7604, -95.3698, "America/Chicago", 3),
        ("CO", "Bogota", "Cundinamarca", 4.7110, -74.0721, "America/Bogota", 3),
        ("PE", "Lima", "Lima", -12.0464, -77.0428, "America/Lima", 3),
        ("EC", "Quito", "Pichincha", -0.1807, -78.4678, "America/Guayaquil", 3),
        ("AR", "Buenos Aires", "Buenos Aires", -34.6037, -58.3816, "America/Argentina/Buenos_Aires", 2),
        ("CL", "Santiago", "Santiago", -33.4489, -70.6693, "America/Santiago", 2),
        ("BR", "Sao Paulo", "Sao Paulo", -23.5505, -46.6333, "America/Sao_Paulo", 3),
        ("ES", "Madrid", "Madrid", 40.4168, -3.7038, "Europe/Madrid", 2),
        ("DE", "Berlin", "Berlin", 52.5200, 13.4050, "Europe/Berlin", 2),
        ("GB", "London", "England", 51.5074, -0.1278, "Europe/London", 2),
        ("FR", "Paris", "Ile-de-France", 48.8566, 2.3522, "Europe/Paris", 2),
        ("AE", "Dubai", "Dubai", 25.2048, 55.2708, "Asia/Dubai", 4),
        ("CN", "Shenzhen", "Guangdong", 22.5431, 114.0579, "Asia/Shanghai", 4),
        ("IN", "Mumbai", "Maharashtra", 19.0760, 72.8777, "Asia/Kolkata", 4),
    ]

    border_countries = {"GT", "MX", "SV", "HN", "PA", "US"}
    ubicaciones: Dict[str, Ubicacion] = {}

    for i in range(1, N_UBICACIONES + 1):
        base = base_cities[(i - 1) % len(base_cities)]
        pais, ciudad, region, lat, lon, tz, risk = base

        lat_j = lat + random.uniform(-0.25, 0.25)
        lon_j = lon + random.uniform(-0.25, 0.25)
        codigo_postal = f"{random.randint(1000, 99999):05d}"
        es_frontera = (pais in border_countries and random.random() < 0.23) or random.random() < 0.08
        riesgo_geo = int(clamp(risk + random.randint(-1, 2), 1, 5))

        n1 = f"{lat_j + random.uniform(-0.03, 0.03):.4f},{lon_j + random.uniform(-0.03, 0.03):.4f}"
        n2 = f"{lat_j + random.uniform(-0.03, 0.03):.4f},{lon_j + random.uniform(-0.03, 0.03):.4f}"
        ubicacion_id = f"U{i:05d}"

        ubicaciones[ubicacion_id] = Ubicacion(
            ubicacion_id=ubicacion_id,
            pais=pais,
            ciudad=ciudad,
            region=region,
            codigo_postal=codigo_postal,
            latitud=round(lat_j, 6),
            longitud=round(lon_j, 6),
            zona_horaria=tz,
            es_frontera=es_frontera,
            nivel_riesgo_geo=riesgo_geo,
            coordenadas_vecinas=[n1, n2],
            fecha_actualizacion=random_date(date(2025, 1, 1), date(2026, 4, 21)),
        )

    return ubicaciones


def generate_clientes() -> Dict[str, Cliente]:
    nombres = [
        "Ana", "Carlos", "Maria", "Jose", "Sofia", "Luis", "Fernanda", "Jorge", "Daniela", "Miguel",
        "Valeria", "Andres", "Gabriela", "Diego", "Paola", "Ricardo", "Camila", "Esteban", "Lucia", "Mario",
        "Natalia", "Juan", "Patricia", "Alberto", "Silvia", "Rafael", "Karla", "Oscar", "Monica", "Pablo",
    ]
    apellidos = [
        "Lopez", "Hernandez", "Garcia", "Martinez", "Perez", "Gonzalez", "Mendez", "Castillo", "Ramirez",
        "Reyes", "Morales", "Cruz", "Vasquez", "Ruiz", "Flores", "Diaz", "Santos", "Rojas", "Pineda", "Cifuentes",
    ]
    segmentos = ["PERSONAL", "PREMIUM", "PYME", "CORPORATIVO"]
    seg_w = [0.62, 0.20, 0.13, 0.05]
    paises = ["GT", "SV", "HN", "CR", "PA", "US", "MX"]
    pais_w = [0.78, 0.04, 0.03, 0.03, 0.03, 0.06, 0.03]
    productos_pool = [
        "AHORRO", "CORRIENTE", "TARJETA_CREDITO", "TARJETA_DEBITO", "PRESTAMO", "INVERSION", "SEGURO"
    ]

    clientes: Dict[str, Cliente] = {}

    for i in range(1, N_CLIENTES + 1):
        cliente_id = f"C{i:06d}"
        nombre = f"{random.choice(nombres)} {random.choice(nombres)} {random.choice(apellidos)} {random.choice(apellidos)}"
        documento = f"DPI-{random.randint(1000000000000, 9999999999999)}"
        nacimiento = random_date(date(1955, 1, 1), date(2005, 12, 31))
        alta = random_date(date(2018, 1, 1), date(2026, 4, 21))
        segmento = weighted_choice(segmentos, seg_w)

        if segmento == "PERSONAL":
            ingresos = random.uniform(450, 3500)
        elif segmento == "PREMIUM":
            ingresos = random.uniform(3000, 12000)
        elif segmento == "PYME":
            ingresos = random.uniform(2500, 18000)
        else:
            ingresos = random.uniform(10000, 70000)

        pep = random.random() < 0.04
        pais = weighted_choice(paises, pais_w)

        riesgo = 18.0
        if pep:
            riesgo += random.uniform(18, 32)
        if segmento in {"PREMIUM", "CORPORATIVO"}:
            riesgo += random.uniform(2, 12)
        if pais != "GT":
            riesgo += random.uniform(2, 9)
        riesgo += random.uniform(0, 28)
        riesgo = round(clamp(riesgo, 5, 98), 2)

        tel_count = 2 if random.random() < 0.28 else 1
        telefonos = [f"+502{random.randint(20000000, 79999999)}" for _ in range(tel_count)]

        email_user = slug(nombre)
        emails = [f"{email_user}.{i}@correo.com"]

        prod_count = random.randint(1, 4)
        productos = random.sample(productos_pool, k=prod_count)

        clientes[cliente_id] = Cliente(
            cliente_id=cliente_id,
            nombre_completo=nombre,
            documento_id=documento,
            fecha_nacimiento=nacimiento,
            fecha_alta=alta,
            segmento=segmento,
            ingresos_mensuales=round(ingresos, 2),
            pep=pep,
            pais_residencia=pais,
            riesgo_base=riesgo,
            telefonos=telefonos,
            emails=emails,
            productos_activos=productos,
            ultima_actualizacion=random_datetime(datetime(2026, 1, 1), TODAY),
        )

    return clientes


def generate_dispositivos() -> Dict[str, Dispositivo]:
    tipos = ["MOBILE", "LAPTOP", "TABLET"]
    tipo_w = [0.72, 0.24, 0.04]

    sistemas = {
        "MOBILE": ["Android 13", "Android 14", "iOS 17", "iOS 18"],
        "LAPTOP": ["Windows 11", "macOS 14", "Ubuntu 22.04", "Windows 10"],
        "TABLET": ["Android 13", "iPadOS 17"],
    }

    dispositivos: Dict[str, Dispositivo] = {}

    for i in range(1, N_DISPOSITIVOS + 1):
        did = f"D{i:06d}"
        dtype = weighted_choice(tipos, tipo_w)
        so = random.choice(sistemas[dtype])
        rooteado = random.random() < 0.06
        emulador = random.random() < 0.03

        base_conf = random.uniform(0.72, 0.98)
        if rooteado:
            base_conf -= random.uniform(0.18, 0.34)
        if emulador:
            base_conf -= random.uniform(0.15, 0.30)

        fp = hashlib.sha1(f"fp-{did}-{SEED}".encode()).hexdigest()
        imei_hash = hashlib.sha1(f"imei-{did}-{SEED}".encode()).hexdigest()

        dispositivos[did] = Dispositivo(
            dispositivo_id=did,
            fingerprint=f"fp_{fp[:20]}",
            tipo_dispositivo=dtype,
            sistema_operativo=so,
            app_version=f"{random.randint(2,4)}.{random.randint(0,20)}.{random.randint(0,9)}",
            ip_publica=f"{random.randint(23, 200)}.{random.randint(1, 254)}.{random.randint(1, 254)}.{random.randint(1, 254)}",
            imei_hash=f"h_{imei_hash[:22]}",
            rooteado=rooteado,
            emulador=emulador,
            idiomas=["es-GT"] + (["en-US"] if random.random() < 0.45 else []),
            fecha_registro=random_date(date(2020, 1, 1), date(2026, 3, 31)),
            ultima_actividad=random_datetime(datetime(2026, 1, 1), TODAY),
            confianza=round(clamp(base_conf, 0.10, 0.99), 2),
        )

    return dispositivos


def generate_cuentas(clientes: Dict[str, Cliente]) -> Dict[str, Cuenta]:
    cuentas: Dict[str, Cuenta] = {}

    cliente_ids = list(clientes.keys())

    owners: List[str] = []
    owners.extend(cliente_ids)  # una cuenta mínima por cliente
    owners.extend(random.choices(cliente_ids, k=N_CUENTAS - len(cliente_ids)))
    random.shuffle(owners)

    for i in range(1, N_CUENTAS + 1):
        cuenta_id = f"CU{i:06d}"
        owner = owners[i - 1]
        cli = clientes[owner]

        tipo = weighted_choice(["AHORRO", "CORRIENTE", "NOMINA", "EMPRESARIAL"], [0.46, 0.25, 0.20, 0.09])
        moneda = weighted_choice(["GTQ", "USD"], [0.72, 0.28])
        apertura_start = max(date(2018, 1, 1), cli.fecha_alta)
        apertura_end = date(2026, 4, 21)
        if apertura_start > apertura_end:
            apertura_start = apertura_end
        apertura = random_date(apertura_start, apertura_end)
        estado = weighted_choice(["ACTIVA", "BLOQUEADA", "CERRADA"], [0.90, 0.07, 0.03])

        if tipo == "EMPRESARIAL":
            saldo_ini = random.uniform(15000, 350000)
            limite = random.uniform(25000, 150000)
        elif tipo == "CORRIENTE":
            saldo_ini = random.uniform(1200, 65000)
            limite = random.uniform(5000, 60000)
        else:
            saldo_ini = random.uniform(200, 40000)
            limite = random.uniform(2500, 50000)

        es_conjunta = random.random() < 0.09
        canales_pool = ["MOBILE", "WEB", "ATM", "POS", "API"]
        canales = random.sample(canales_pool, k=random.randint(2, 4))

        score_riesgo = round(clamp(cli.riesgo_base + random.uniform(-12, 16), 1, 99), 2)

        cuentas[cuenta_id] = Cuenta(
            cuenta_id=cuenta_id,
            owner_cliente_id=owner,
            iban=f"GT82BAGT{str(i).zfill(12)}",
            tipo_cuenta=tipo,
            moneda_base=moneda,
            fecha_apertura=apertura,
            estado=estado,
            saldo_inicial=round(saldo_ini, 2),
            saldo_actual=round(saldo_ini, 2),
            limite_diario=round(limite, 2),
            es_conjunta=es_conjunta,
            canales_habilitados=canales,
            pais_apertura="GT",
            score_riesgo=score_riesgo,
            ultima_actividad=random_datetime(datetime(2026, 1, 1), TODAY),
        )

    return cuentas


def generate_client_device_relations(
    clientes: Dict[str, Cliente],
    dispositivos: Dict[str, Dispositivo],
):
    cliente_ids = list(clientes.keys())
    dispositivo_ids = list(dispositivos.keys())

    client_devices: Dict[str, List[str]] = defaultdict(list)
    device_clients: Dict[str, List[str]] = defaultdict(list)
    rel_rows: List[List[object]] = []
    usage_windows: Dict[tuple[str, str], tuple[datetime, datetime]] = {}

    random.shuffle(dispositivo_ids)

    for i, cid in enumerate(cliente_ids):
        if i < len(dispositivo_ids):
            did = dispositivo_ids[i]
        else:
            did = random.choice(dispositivo_ids)

        if did not in client_devices[cid]:
            client_devices[cid].append(did)
            device_clients[did].append(cid)

    for cid in cliente_ids:
        if random.random() < 0.27:
            did2 = random.choice(dispositivo_ids)
            if did2 not in client_devices[cid]:
                client_devices[cid].append(did2)
                device_clients[did2].append(cid)

    for cid in cliente_ids:
        for did in client_devices[cid]:
            cli = clientes[cid]
            dev = dispositivos[did]
            primer_start = datetime.combine(cli.fecha_alta, datetime.min.time())
            primer_end = TODAY - timedelta(days=15)
            if primer_start > primer_end:
                primer_start = primer_end
            primer = random_datetime(primer_start, primer_end)
            ultimo = random_datetime(primer, TODAY)
            veces = random.randint(8, 1800)
            conf_rel = round(clamp(dev.confianza + random.uniform(-0.12, 0.08), 0.05, 0.99), 2)
            activo = random.random() < 0.91

            usage_windows[(cid, did)] = (primer, ultimo)
            rel_rows.append([
                cid,
                did,
                iso_date(primer.date()),
                iso_dt(ultimo),
                veces,
                conf_rel,
                str(activo).lower(),
            ])

    return client_devices, device_clients, rel_rows, usage_windows


def generate_comercios(
    ubicaciones: Dict[str, Ubicacion],
    cuentas: Dict[str, Cuenta],
) -> Dict[str, Comercio]:
    industrias = [
        ("5411", "SUPERMERCADO", 22),
        ("5812", "RESTAURANTE", 26),
        ("5732", "ELECTRONICA", 36),
        ("6012", "SERVICIOS_FINANCIEROS", 58),
        ("5999", "E_COMMERCE", 44),
        ("7995", "APUESTAS", 82),
        ("4829", "REMITESAS", 64),
        ("4111", "TRANSPORTE", 28),
        ("4900", "SERVICIOS_BASICOS", 24),
        ("6051", "CRYPTO", 88),
    ]

    ubicacion_ids = list(ubicaciones.keys())
    cuenta_ids = list(cuentas.keys())
    comercios: Dict[str, Comercio] = {}

    for i in range(1, N_COMERCIOS + 1):
        comercio_id = f"M{i:06d}"
        mcc, industria, risk_base = random.choice(industrias)

        ub_id = random.choice(ubicacion_ids)
        ub = ubicaciones[ub_id]

        riesgo = round(clamp(risk_base + random.uniform(-10, 18), 1, 99), 2)
        blacklist = random.random() < (0.02 + (0.20 if riesgo >= 80 else 0.0))

        antig_meses = random.randint(3, 160)
        fecha_alta = TODAY.date() - timedelta(days=antig_meses * 30)

        rating = round(clamp(random.uniform(2.0, 4.9) - (riesgo / 140), 1.2, 4.9), 2)

        metodos_pool = ["TARJETA", "QR", "TRANSFERENCIA", "LINK_PAGO", "CRIPTO"]
        metodos = random.sample(metodos_pool, k=random.randint(2, 4))
        if industria == "CRYPTO" and "CRIPTO" not in metodos:
            metodos.append("CRIPTO")

        comercios[comercio_id] = Comercio(
            comercio_id=comercio_id,
            nombre_comercio=f"Comercio {industria.title()} {i:04d}",
            categoria_mcc=mcc,
            industria=industria,
            pais=ub.pais,
            ciudad=ub.ciudad,
            riesgo_comercio=riesgo,
            antiguedad_meses=antig_meses,
            blacklist=blacklist,
            metodos_pago_aceptados=metodos,
            fecha_alta=fecha_alta,
            rating_promedio=rating,
            url=f"https://comercio{i:04d}.example.com",
            ubicacion_id=ub_id,
            settlement_cuenta_id=random.choice(cuenta_ids),
        )

    return comercios


def build_transactions(
    clientes: Dict[str, Cliente],
    cuentas: Dict[str, Cuenta],
    dispositivos: Dict[str, Dispositivo],
    ubicaciones: Dict[str, Ubicacion],
    comercios: Dict[str, Comercio],
    client_devices: Dict[str, List[str]],
):
    account_ids = list(cuentas.keys())
    location_ids = list(ubicaciones.keys())
    merchant_ids = list(comercios.keys())

    tx_types = ["TRANSFERENCIA", "PAGO_COMERCIO", "RETIRO_ATM", "DEPOSITO", "PAGO_SERVICIO", "COMPRA_ONLINE"]
    tx_w = [0.36, 0.24, 0.10, 0.08, 0.10, 0.12]

    factor_pool = ["SMS_OTP", "BIOMETRIA", "TOKEN_APP", "PASSWORD", "SIN_FACTOR"]
    canal_map = {
        "TRANSFERENCIA": ["APP", "WEB", "API"],
        "PAGO_COMERCIO": ["POS", "APP", "WEB"],
        "RETIRO_ATM": ["ATM"],
        "DEPOSITO": ["SUCURSAL", "ATM", "APP"],
        "PAGO_SERVICIO": ["APP", "WEB"],
        "COMPRA_ONLINE": ["WEB", "APP"],
    }

    tx_blueprints: List[TxBlueprint] = []

    start_dt = TODAY - timedelta(days=180)

    for i in range(1, N_TRANSACCIONES + 1):
        tx_id = f"TX{i:07d}"
        referencia = f"REF-2026-{i:07d}"

        debit_account_id = random.choice(account_ids)
        debit_account = cuentas[debit_account_id]
        cliente = clientes[debit_account.owner_cliente_id]

        tipo_tx = weighted_choice(tx_types, tx_w)
        fecha_hora = random_datetime(start_dt, TODAY)
        canal = random.choice(canal_map[tipo_tx])

        if tipo_tx in {"TRANSFERENCIA", "DEPOSITO"}:
            amount = random.lognormvariate(7.4, 0.65)
        elif tipo_tx in {"COMPRA_ONLINE", "PAGO_COMERCIO", "PAGO_SERVICIO"}:
            amount = random.lognormvariate(6.6, 0.75)
        else:
            amount = random.lognormvariate(6.2, 0.55)

        amount = round(clamp(amount, 15, 25000), 2)

        if random.random() < 0.007:
            amount = round(random.uniform(25000, 180000), 2)

        dispositivo_id = random.choice(client_devices[cliente.cliente_id])
        dispositivo = dispositivos[dispositivo_id]

        local_locations = [u.ubicacion_id for u in ubicaciones.values() if u.pais == cliente.pais_residencia]
        foreign_locations = [u.ubicacion_id for u in ubicaciones.values() if u.pais != cliente.pais_residencia]

        internacional = random.random() < 0.34
        if tipo_tx == "RETIRO_ATM":
            internacional = random.random() < 0.18

        if internacional and foreign_locations:
            ubicacion_id = random.choice(foreign_locations)
        elif local_locations:
            ubicacion_id = random.choice(local_locations)
        else:
            ubicacion_id = random.choice(location_ids)

        comercio_id = None
        if tipo_tx in {"PAGO_COMERCIO", "COMPRA_ONLINE", "PAGO_SERVICIO"}:
            comercio_id = random.choice(merchant_ids)

        if comercio_id is not None:
            cuenta_credito_id = comercios[comercio_id].settlement_cuenta_id
            if cuenta_credito_id == debit_account_id:
                cuenta_credito_id = random.choice([acc for acc in account_ids if acc != debit_account_id])
        else:
            cuenta_credito_id = random.choice([acc for acc in account_ids if acc != debit_account_id])

        if internacional or random.random() < 0.22:
            moneda = "USD"
        else:
            moneda = debit_account.moneda_base

        hour = fecha_hora.hour
        night = hour < 5 or hour > 22

        merchant_risk = comercios[comercio_id].riesgo_comercio if comercio_id else 20
        device_penalty = 0.18 if (dispositivo.rooteado or dispositivo.emulador) else 0.0
        base_score = (
            0.12
            + (cliente.riesgo_base / 220)
            + (0.18 if internacional else 0.0)
            + (0.16 if amount >= 8000 else 0.0)
            + (0.10 if night else 0.0)
            + device_penalty
            + (merchant_risk / 500)
            + random.uniform(-0.08, 0.14)
        )
        score = round(clamp(base_score, 0.01, 0.99), 2)

        velocity = int(clamp(random.gauss(2 + score * 5, 1.8), 0, 15))

        estado = "APROBADA"
        if score > 0.92 and random.random() < 0.35:
            estado = "RECHAZADA"
        elif random.random() < 0.05:
            estado = "PENDIENTE"

        contracargo = comercio_id is not None and random.random() < (0.01 + score * 0.07)

        etiquetas = []
        if internacional:
            etiquetas.append("internacional")
        if amount >= 8000:
            etiquetas.append("alto_monto")
        if night:
            etiquetas.append("horario_inusual")
        if dispositivo.rooteado or dispositivo.emulador:
            etiquetas.append("dispositivo_riesgoso")
        if merchant_risk >= 80:
            etiquetas.append("comercio_riesgoso")
        if velocity >= 7:
            etiquetas.append("alta_velocidad")
        if not etiquetas:
            etiquetas.append("normal")

        factor = weighted_choice(factor_pool, [0.28, 0.22, 0.20, 0.22, 0.08])
        auth_ok = False if score > 0.9 and random.random() < 0.25 else True
        confianza_sesion = round(clamp((1.02 - score) + random.uniform(-0.12, 0.10), 0.02, 0.99), 2)

        tx_blueprints.append(
            TxBlueprint(
                tx_id=tx_id,
                referencia_externa=referencia,
                fecha_hora=fecha_hora,
                fecha_valor=fecha_hora.date(),
                monto=amount,
                moneda=moneda,
                tipo_tx=tipo_tx,
                canal=canal,
                estado=estado,
                internacional=internacional,
                contracargo=contracargo,
                velocity_1h=velocity,
                etiquetas=etiquetas,
                score_fraude=score,
                cliente_id=cliente.cliente_id,
                cuenta_debito_id=debit_account_id,
                cuenta_credito_id=cuenta_credito_id,
                dispositivo_id=dispositivo_id,
                ubicacion_id=ubicacion_id,
                comercio_id=comercio_id,
                factor_autenticacion=factor,
                autenticacion_ok=auth_ok,
                confianza_sesion=confianza_sesion,
            )
        )

    tx_blueprints.sort(key=lambda x: x.fecha_hora)

    tx_rows: List[List[object]] = []
    rel_realiza_rows: List[List[object]] = []
    rel_debita_rows: List[List[object]] = []
    rel_acredita_rows: List[List[object]] = []
    rel_origina_rows: List[List[object]] = []
    rel_destina_rows: List[List[object]] = []

    account_tx_seq: Dict[str, List[tuple[datetime, str]]] = defaultdict(list)
    tx_meta: Dict[str, Dict[str, object]] = {}

    for tx in tx_blueprints:
        deb = cuentas[tx.cuenta_debito_id]
        cre = cuentas[tx.cuenta_credito_id]

        deb_pre = deb.saldo_actual
        cre_pre = cre.saldo_actual

        if tx.estado == "APROBADA":
            deb_post = round(deb_pre - tx.monto, 2)
            cre_post = round(cre_pre + tx.monto, 2)
            deb.saldo_actual = deb_post
            cre.saldo_actual = cre_post
        else:
            deb_post = deb_pre
            cre_post = cre_pre

        deb.ultima_actividad = max(deb.ultima_actividad, tx.fecha_hora)
        cre.ultima_actividad = max(cre.ultima_actividad, tx.fecha_hora)

        if tx.score_fraude >= 0.85 or tx.contracargo:
            if "sospechosa" not in tx.etiquetas:
                tx.etiquetas.append("sospechosa")

        tx_rows.append([
            tx.tx_id,
            tx.referencia_externa,
            iso_dt(tx.fecha_hora),
            iso_date(tx.fecha_valor),
            f"{tx.monto:.2f}",
            tx.moneda,
            tx.tipo_tx,
            tx.canal,
            tx.estado,
            str(tx.internacional).lower(),
            str(tx.contracargo).lower(),
            tx.velocity_1h,
            join_list(tx.etiquetas),
            f"{tx.score_fraude:.2f}",
        ])

        rel_realiza_rows.append([
            tx.cliente_id,
            tx.tx_id,
            "ORDENANTE",
            tx.factor_autenticacion,
            str(tx.autenticacion_ok).lower(),
            dispositivos[tx.dispositivo_id].ip_publica,
            f"{tx.confianza_sesion:.2f}",
        ])

        rel_debita_rows.append([
            tx.tx_id,
            tx.cuenta_debito_id,
            f"{tx.monto:.2f}",
            tx.moneda,
            f"{deb_pre:.2f}",
            f"{deb_post:.2f}",
            iso_dt(tx.fecha_hora),
        ])

        rel_acredita_rows.append([
            tx.tx_id,
            tx.cuenta_credito_id,
            f"{tx.monto:.2f}",
            tx.moneda,
            f"{cre_pre:.2f}",
            f"{cre_post:.2f}",
            iso_dt(tx.fecha_hora),
        ])

        ub = ubicaciones[tx.ubicacion_id]
        rel_origina_rows.append([
            tx.tx_id,
            tx.ubicacion_id,
            iso_dt(tx.fecha_hora),
            random.randint(15, 350),
            weighted_choice(["GPS", "IP", "TORRE_CELULAR", "WIFI"], [0.45, 0.22, 0.15, 0.18]),
            ub.nivel_riesgo_geo,
            str(random.random() < (0.05 + (0.18 if tx.internacional else 0.0))).lower(),
        ])

        if tx.comercio_id is not None:
            rel_destina_rows.append([
                tx.tx_id,
                tx.comercio_id,
                f"TERM-{random.randint(10000, 99999)}",
                tx.canal,
                f"AUTH-{random.randint(100000, 999999)}",
                f"{round(random.uniform(0.5, 4.2), 2):.2f}",
                iso_dt(tx.fecha_hora),
            ])

        account_tx_seq[tx.cuenta_debito_id].append((tx.fecha_hora, tx.tx_id))
        tx_meta[tx.tx_id] = {
            "fecha_hora": tx.fecha_hora,
            "ip": dispositivos[tx.dispositivo_id].ip_publica,
            "ubicacion_id": tx.ubicacion_id,
            "monto": tx.monto,
            "score_fraude": tx.score_fraude,
            "cliente_id": tx.cliente_id,
            "comercio_id": tx.comercio_id,
            "internacional": tx.internacional,
            "velocity_1h": tx.velocity_1h,
            "estado": tx.estado,
        }

    return {
        "tx_rows": tx_rows,
        "rel_realiza_rows": rel_realiza_rows,
        "rel_debita_rows": rel_debita_rows,
        "rel_acredita_rows": rel_acredita_rows,
        "rel_origina_rows": rel_origina_rows,
        "rel_destina_rows": rel_destina_rows,
        "account_tx_seq": account_tx_seq,
        "tx_meta": tx_meta,
        "tx_blueprints": tx_blueprints,
    }


def generate_alertas(
    tx_blueprints: List[TxBlueprint],
    tx_meta: Dict[str, Dict[str, object]],
    comercios: Dict[str, Comercio],
):
    ordered = sorted(tx_blueprints, key=lambda t: t.score_fraude, reverse=True)

    selected: List[TxBlueprint] = []
    for tx in ordered:
        chance = 0.06 + (tx.score_fraude * 0.55)
        if tx.internacional:
            chance += 0.08
        if tx.comercio_id and comercios[tx.comercio_id].blacklist:
            chance += 0.12
        if random.random() < clamp(chance, 0.05, 0.90):
            selected.append(tx)
        if len(selected) >= N_ALERTAS_OBJETIVO:
            break

    if len(selected) < N_ALERTAS_OBJETIVO:
        extra_pool = [t for t in ordered if t not in selected]
        selected.extend(extra_pool[: N_ALERTAS_OBJETIVO - len(selected)])

    alert_rows: List[List[object]] = []
    rel_genera_rows: List[List[object]] = []
    rel_afecta_rows: List[List[object]] = []

    for i, tx in enumerate(selected, start=1):
        alerta_id = f"AL{i:07d}"

        if tx.internacional and tx.monto >= 5000:
            tipo = "ALTO_MONTO_INTERNACIONAL"
            regla = "R1_ALTO_MONTO_INTL"
        elif tx.velocity_1h >= 7:
            tipo = "VELOCIDAD_ANOMALA"
            regla = "R2_VELOCIDAD"
        elif tx.comercio_id and comercios[tx.comercio_id].blacklist:
            tipo = "COMERCIO_BLACKLIST"
            regla = "R3_COMERCIO_BLACKLIST"
        elif "dispositivo_riesgoso" in tx.etiquetas:
            tipo = "DISPOSITIVO_COMPROMETIDO"
            regla = "R4_DISPOSITIVO"
        else:
            tipo = "PATRON_SOSPECHOSO"
            regla = "R5_PATRON_RED"

        sev = int(clamp(round(tx.score_fraude * 5.3), 1, 5))
        score_alerta = round(clamp(tx.score_fraude + random.uniform(-0.04, 0.08), 0.01, 0.99), 2)

        estado = weighted_choice(["ABIERTA", "EN_INVESTIGACION", "CERRADA"], [0.48, 0.37, 0.15])
        fecha_creacion = tx.fecha_hora + timedelta(seconds=random.randint(2, 120))

        fecha_cierre = ""
        if estado == "CERRADA":
            fecha_cierre = iso_date((fecha_creacion + timedelta(days=random.randint(1, 12))).date())

        falso_positivo = random.random() < (0.18 if sev <= 2 else 0.08)

        reglas = [regla]
        if tx.internacional:
            reglas.append("R7_GEO_MISMATCH")
        if tx.monto > 8000:
            reglas.append("R8_HIGH_VALUE")

        acciones = ["monitoreo_reforzado"]
        if sev >= 4:
            acciones.extend(["bloquear_temporal", "contactar_cliente"])
        elif sev == 3:
            acciones.append("validar_identidad")

        analista = f"analista_{random.randint(1, 22):02d}"

        alert_rows.append([
            alerta_id,
            tipo,
            sev,
            f"{score_alerta:.2f}",
            estado,
            f"Alerta generada por {tipo} en transaccion {tx.tx_id}",
            iso_dt(fecha_creacion),
            fecha_cierre,
            str(falso_positivo).lower(),
            join_list(reglas),
            join_list(acciones),
            analista,
        ])

        rel_genera_rows.append([
            tx.tx_id,
            alerta_id,
            regla,
            f"{score_alerta:.2f}",
            weighted_choice(["v1.0", "v1.1", "v2.0"], [0.20, 0.55, 0.25]),
            iso_dt(fecha_creacion),
            str(sev >= 5 and random.random() < 0.65).lower(),
        ])

        rel_afecta_rows.append([
            alerta_id,
            tx_meta[tx.tx_id]["cliente_id"],
            "TITULAR",
            sev,
            iso_date(fecha_creacion.date()),
            str(estado != "ABIERTA").lower(),
            analista,
        ])

    return alert_rows, rel_genera_rows, rel_afecta_rows


def generate_comparte_dispositivo(
    device_clients: Dict[str, List[str]],
    usage_windows: Dict[tuple[str, str], tuple[datetime, datetime]],
):
    rows: List[List[object]] = []

    for did, clients in device_clients.items():
        uniq = sorted(set(clients))
        if len(uniq) < 2:
            continue

        pairs = []
        for i in range(len(uniq)):
            for j in range(i + 1, len(uniq)):
                pairs.append((uniq[i], uniq[j]))

        if len(pairs) > 20:
            pairs = random.sample(pairs, 20)

        for c1, c2 in pairs:
            w1 = usage_windows[(c1, did)]
            w2 = usage_windows[(c2, did)]
            first_dt = min(w1[0], w2[0])
            last_dt = max(w1[1], w2[1])
            coincidencias = random.randint(2, 28)
            score = round(clamp(0.28 + (coincidencias / 40) + random.uniform(-0.06, 0.20), 0.12, 0.98), 2)

            rows.append([
                c1,
                c2,
                did,
                iso_dt(first_dt),
                iso_dt(last_dt),
                coincidencias,
                f"{score:.2f}",
            ])

    return rows


def generate_vinculos_cuentas(cuentas: Dict[str, Cuenta]):
    rows: List[List[object]] = []
    account_ids = sorted(cuentas.keys())

    # Backbone de conectividad
    for i in range(len(account_ids) - 1):
        a = account_ids[i]
        b = account_ids[i + 1]
        rows.append([
            a,
            b,
            "BACKBONE_CONECTIVIDAD",
            iso_date(date(2026, 4, 21)),
            "0.01",
            "conexion_global|backbone",
            "true",
        ])

    # Vinculos sospechosos extra
    seen = {(r[0], r[1]) for r in rows}
    tipos = ["TRANSFERENCIAS_RECURRENTES", "IP_COINCIDENTE", "KYC_SIMILAR", "DISPOSITIVO_COMPARTIDO"]

    while len(rows) < (len(account_ids) - 1 + 620):
        a, b = random.sample(account_ids, 2)
        if (a, b) in seen:
            continue
        seen.add((a, b))

        tipo = random.choice(tipos)
        score = round(random.uniform(0.35, 0.97), 2)
        evidencia = [
            random.choice(["ip", "telefono", "documento", "patron_horario"]),
            random.choice(["monto_similar", "destino_comun", "frecuencia_alta"]),
        ]

        rows.append([
            a,
            b,
            tipo,
            iso_date(random_date(date(2025, 1, 1), date(2026, 4, 21))),
            f"{score:.2f}",
            join_list(evidencia),
            str(random.random() < 0.94).lower(),
        ])

    return rows


def generate_ocurre_antes(account_tx_seq: Dict[str, List[tuple[datetime, str]]], tx_meta: Dict[str, Dict[str, object]]):
    rows: List[List[object]] = []

    for _, seq in account_tx_seq.items():
        seq = sorted(seq, key=lambda x: x[0])
        for i in range(len(seq) - 1):
            t1_time, t1_id = seq[i]
            t2_time, t2_id = seq[i + 1]

            delta = int((t2_time - t1_time).total_seconds())
            if delta > 43200 and random.random() < 0.75:
                continue

            t1 = tx_meta[t1_id]
            t2 = tx_meta[t2_id]

            misma_ip = t1["ip"] == t2["ip"]
            misma_ub = t1["ubicacion_id"] == t2["ubicacion_id"]
            monto_diff = abs(float(t1["monto"]) - float(t2["monto"]))

            if delta <= 180 and monto_diff <= 50:
                patron = "SMURFING"
                score = 0.92
            elif delta <= 900:
                patron = "RAPID_FIRE"
                score = 0.78
            elif delta <= 3600 and misma_ip:
                patron = "SESION_CONTINUA"
                score = 0.62
            else:
                patron = "SECUENCIA_NORMAL"
                score = 0.30

            score = round(clamp(score + random.uniform(-0.08, 0.08), 0.05, 0.99), 2)

            rows.append([
                t1_id,
                t2_id,
                delta,
                str(misma_ip).lower(),
                str(misma_ub).lower(),
                patron,
                f"{score:.2f}",
            ])

    return rows


def write_all_csv(
    clientes: Dict[str, Cliente],
    cuentas: Dict[str, Cuenta],
    dispositivos: Dict[str, Dispositivo],
    ubicaciones: Dict[str, Ubicacion],
    comercios: Dict[str, Comercio],
    client_devices_rows: List[List[object]],
    client_devices: Dict[str, List[str]],
    tx_data: Dict[str, object],
    alert_data: tuple[List[List[object]], List[List[object]], List[List[object]]],
    comparte_rows: List[List[object]],
    vinculos_rows: List[List[object]],
    ocurre_rows: List[List[object]],
):
    # Nodos
    write_csv(
        CSV_DIR / "clientes.csv",
        [
            "cliente_id", "nombre_completo", "documento_id", "fecha_nacimiento", "fecha_alta", "segmento",
            "ingresos_mensuales", "pep", "pais_residencia", "riesgo_base", "telefonos", "emails",
            "productos_activos", "ultima_actualizacion",
        ],
        [
            [
                c.cliente_id,
                c.nombre_completo,
                c.documento_id,
                iso_date(c.fecha_nacimiento),
                iso_date(c.fecha_alta),
                c.segmento,
                f"{c.ingresos_mensuales:.2f}",
                str(c.pep).lower(),
                c.pais_residencia,
                f"{c.riesgo_base:.2f}",
                join_list(c.telefonos),
                join_list(c.emails),
                join_list(c.productos_activos),
                iso_dt(c.ultima_actualizacion),
            ]
            for c in clientes.values()
        ],
    )

    write_csv(
        CSV_DIR / "cuentas.csv",
        [
            "cuenta_id", "iban", "tipo_cuenta", "moneda_base", "fecha_apertura", "estado", "saldo_actual",
            "limite_diario", "es_conjunta", "canales_habilitados", "pais_apertura", "score_riesgo", "ultima_actividad",
        ],
        [
            [
                cu.cuenta_id,
                cu.iban,
                cu.tipo_cuenta,
                cu.moneda_base,
                iso_date(cu.fecha_apertura),
                cu.estado,
                f"{cu.saldo_actual:.2f}",
                f"{cu.limite_diario:.2f}",
                str(cu.es_conjunta).lower(),
                join_list(cu.canales_habilitados),
                cu.pais_apertura,
                f"{cu.score_riesgo:.2f}",
                iso_dt(cu.ultima_actividad),
            ]
            for cu in cuentas.values()
        ],
    )

    write_csv(
        CSV_DIR / "dispositivos.csv",
        [
            "dispositivo_id", "fingerprint", "tipo_dispositivo", "sistema_operativo", "app_version", "ip_publica",
            "imei_hash", "rooteado", "emulador", "idiomas", "fecha_registro", "ultima_actividad", "confianza",
        ],
        [
            [
                d.dispositivo_id,
                d.fingerprint,
                d.tipo_dispositivo,
                d.sistema_operativo,
                d.app_version,
                d.ip_publica,
                d.imei_hash,
                str(d.rooteado).lower(),
                str(d.emulador).lower(),
                join_list(d.idiomas),
                iso_date(d.fecha_registro),
                iso_dt(d.ultima_actividad),
                f"{d.confianza:.2f}",
            ]
            for d in dispositivos.values()
        ],
    )

    write_csv(
        CSV_DIR / "ubicaciones.csv",
        [
            "ubicacion_id", "pais", "ciudad", "region", "codigo_postal", "latitud", "longitud", "zona_horaria",
            "es_frontera", "nivel_riesgo_geo", "coordenadas_vecinas", "fecha_actualizacion",
        ],
        [
            [
                u.ubicacion_id,
                u.pais,
                u.ciudad,
                u.region,
                u.codigo_postal,
                f"{u.latitud:.6f}",
                f"{u.longitud:.6f}",
                u.zona_horaria,
                str(u.es_frontera).lower(),
                u.nivel_riesgo_geo,
                join_list(u.coordenadas_vecinas),
                iso_date(u.fecha_actualizacion),
            ]
            for u in ubicaciones.values()
        ],
    )

    write_csv(
        CSV_DIR / "comercios.csv",
        [
            "comercio_id", "nombre_comercio", "categoria_mcc", "industria", "pais", "ciudad", "riesgo_comercio",
            "antiguedad_meses", "blacklist", "metodos_pago_aceptados", "fecha_alta", "rating_promedio", "url",
        ],
        [
            [
                m.comercio_id,
                m.nombre_comercio,
                m.categoria_mcc,
                m.industria,
                m.pais,
                m.ciudad,
                f"{m.riesgo_comercio:.2f}",
                m.antiguedad_meses,
                str(m.blacklist).lower(),
                join_list(m.metodos_pago_aceptados),
                iso_date(m.fecha_alta),
                f"{m.rating_promedio:.2f}",
                m.url,
            ]
            for m in comercios.values()
        ],
    )

    write_csv(
        CSV_DIR / "transacciones.csv",
        [
            "tx_id", "referencia_externa", "fecha_hora", "fecha_valor", "monto", "moneda", "tipo_tx", "canal",
            "estado", "internacional", "contracargo", "velocity_1h", "etiquetas", "score_fraude",
        ],
        tx_data["tx_rows"],
    )

    alert_rows, _, _ = alert_data
    write_csv(
        CSV_DIR / "alertas.csv",
        [
            "alerta_id", "tipo_alerta", "severidad", "score_alerta", "estado", "descripcion", "fecha_creacion",
            "fecha_cierre", "es_falso_positivo", "reglas_disparadas", "acciones_recomendadas", "analista_asignado",
        ],
        alert_rows,
    )

    # Relaciones
    rel_posee_rows: List[List[object]] = []
    for cu in cuentas.values():
        rel_posee_rows.append([
            cu.owner_cliente_id,
            cu.cuenta_id,
            iso_date(cu.fecha_apertura),
            "PRINCIPAL",
            "100.00" if not cu.es_conjunta else "50.00",
            "ACTIVA" if cu.estado != "CERRADA" else "INACTIVA",
            weighted_choice(["APP", "SUCURSAL", "WEB"], [0.44, 0.38, 0.18]),
        ])

        if cu.es_conjunta:
            other = random.choice([cid for cid in clientes.keys() if cid != cu.owner_cliente_id])
            rel_posee_rows.append([
                other,
                cu.cuenta_id,
                iso_date(cu.fecha_apertura),
                "COTITULAR",
                "50.00",
                "ACTIVA" if cu.estado != "CERRADA" else "INACTIVA",
                weighted_choice(["APP", "SUCURSAL", "WEB"], [0.40, 0.45, 0.15]),
            ])

    write_csv(
        CSV_DIR / "rel_cliente_posee_cuenta.csv",
        [
            "cliente_id", "cuenta_id", "fecha_inicio", "tipo_titularidad", "porcentaje_propiedad",
            "estado_relacion", "origen_onboarding",
        ],
        rel_posee_rows,
    )

    write_csv(
        CSV_DIR / "rel_cliente_realiza_tx.csv",
        [
            "cliente_id", "tx_id", "rol", "factor_autenticacion", "autenticacion_ok", "ip_origen", "confianza_sesion",
        ],
        tx_data["rel_realiza_rows"],
    )

    write_csv(
        CSV_DIR / "rel_tx_debita_cuenta.csv",
        [
            "tx_id", "cuenta_id", "monto", "moneda", "saldo_previo", "saldo_posterior", "timestamp_contable",
        ],
        tx_data["rel_debita_rows"],
    )

    write_csv(
        CSV_DIR / "rel_tx_acredita_cuenta.csv",
        [
            "tx_id", "cuenta_id", "monto", "moneda", "saldo_previo", "saldo_posterior", "timestamp_contable",
        ],
        tx_data["rel_acredita_rows"],
    )

    write_csv(
        CSV_DIR / "rel_cliente_usa_dispositivo.csv",
        [
            "cliente_id", "dispositivo_id", "primer_uso", "ultimo_uso", "veces_uso", "confianza_dispositivo", "activo",
        ],
        client_devices_rows,
    )

    write_csv(
        CSV_DIR / "rel_tx_origina_ubicacion.csv",
        [
            "tx_id", "ubicacion_id", "fecha_hora", "precision_metros", "metodo_geolocalizacion", "riesgo_geo", "es_vpn",
        ],
        tx_data["rel_origina_rows"],
    )

    write_csv(
        CSV_DIR / "rel_tx_destina_comercio.csv",
        [
            "tx_id", "comercio_id", "terminal_id", "canal_pago", "autorizacion", "comision_pct", "fecha_hora",
        ],
        tx_data["rel_destina_rows"],
    )

    _, rel_genera_rows, rel_afecta_rows = alert_data

    write_csv(
        CSV_DIR / "rel_tx_genera_alerta.csv",
        [
            "tx_id", "alerta_id", "regla", "score_modelo", "version_modelo", "fecha_deteccion", "auto_bloqueo",
        ],
        rel_genera_rows,
    )

    write_csv(
        CSV_DIR / "rel_alerta_afecta_cliente.csv",
        [
            "alerta_id", "cliente_id", "rol_cliente", "criticidad", "fecha_asignacion", "investigada", "analista",
        ],
        rel_afecta_rows,
    )

    rel_comercio_ubicado_rows = [
        [
            m.comercio_id,
            m.ubicacion_id,
            iso_date(m.fecha_alta),
            "",
            "true",
            weighted_choice(["KYC_COMERCIO", "GEOCODER", "DECLARACION"], [0.50, 0.30, 0.20]),
            f"{round(random.uniform(0.72, 0.98), 2):.2f}",
        ]
        for m in comercios.values()
    ]

    write_csv(
        CSV_DIR / "rel_comercio_ubicado_en.csv",
        [
            "comercio_id", "ubicacion_id", "fecha_inicio", "fecha_fin", "es_principal", "fuente_direccion",
            "confianza_direccion",
        ],
        rel_comercio_ubicado_rows,
    )

    write_csv(
        CSV_DIR / "rel_cliente_comparte_dispositivo.csv",
        [
            "cliente_origen_id", "cliente_destino_id", "dispositivo_id_ref", "primera_coincidencia",
            "ultima_coincidencia", "coincidencias", "score_riesgo_red",
        ],
        comparte_rows,
    )

    write_csv(
        CSV_DIR / "rel_cuenta_vinculada_cuenta.csv",
        [
            "cuenta_origen_id", "cuenta_destino_id", "tipo_vinculo", "fecha_deteccion", "score_vinculo", "evidencia",
            "activo",
        ],
        vinculos_rows,
    )

    write_csv(
        CSV_DIR / "rel_tx_ocurre_antes_tx.csv",
        [
            "tx_origen_id", "tx_destino_id", "delta_segundos", "misma_ip", "misma_ubicacion", "patron", "score_patron",
        ],
        ocurre_rows,
    )


def write_summary(
    clientes: Dict[str, Cliente],
    cuentas: Dict[str, Cuenta],
    tx_data: Dict[str, object],
    dispositivos: Dict[str, Dispositivo],
    ubicaciones: Dict[str, Ubicacion],
    comercios: Dict[str, Comercio],
    alert_rows: List[List[object]],
    rel_rows: Dict[str, int],
):
    total_nodos = (
        len(clientes)
        + len(cuentas)
        + len(tx_data["tx_rows"])
        + len(dispositivos)
        + len(ubicaciones)
        + len(comercios)
        + len(alert_rows)
    )

    lines = [
        "Resumen de dataset generado",
        "===========================",
        f"Semilla: {SEED}",
        "",
        "Nodos:",
        f"- Cliente: {len(clientes)}",
        f"- Cuenta: {len(cuentas)}",
        f"- Transaccion: {len(tx_data['tx_rows'])}",
        f"- Dispositivo: {len(dispositivos)}",
        f"- Ubicacion: {len(ubicaciones)}",
        f"- Comercio: {len(comercios)}",
        f"- Alerta: {len(alert_rows)}",
        f"- TOTAL: {total_nodos}",
        "",
        "Relaciones:",
    ]

    for k, v in rel_rows.items():
        lines.append(f"- {k}: {v}")

    summary_path = ROOT / "dataset_resumen.txt"
    summary_path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def main() -> None:
    ubicaciones = generate_ubicaciones()
    clientes = generate_clientes()
    dispositivos = generate_dispositivos()
    cuentas = generate_cuentas(clientes)

    client_devices, device_clients, client_devices_rows, usage_windows = generate_client_device_relations(clientes, dispositivos)
    comercios = generate_comercios(ubicaciones, cuentas)

    tx_data = build_transactions(clientes, cuentas, dispositivos, ubicaciones, comercios, client_devices)

    alert_rows, rel_genera_rows, rel_afecta_rows = generate_alertas(
        tx_data["tx_blueprints"], tx_data["tx_meta"], comercios
    )

    comparte_rows = generate_comparte_dispositivo(device_clients, usage_windows)
    vinculos_rows = generate_vinculos_cuentas(cuentas)
    ocurre_rows = generate_ocurre_antes(tx_data["account_tx_seq"], tx_data["tx_meta"])

    write_all_csv(
        clientes=clientes,
        cuentas=cuentas,
        dispositivos=dispositivos,
        ubicaciones=ubicaciones,
        comercios=comercios,
        client_devices_rows=client_devices_rows,
        client_devices=client_devices,
        tx_data=tx_data,
        alert_data=(alert_rows, rel_genera_rows, rel_afecta_rows),
        comparte_rows=comparte_rows,
        vinculos_rows=vinculos_rows,
        ocurre_rows=ocurre_rows,
    )

    rel_counts = {
        "POSEE": sum(1 for _ in open(CSV_DIR / "rel_cliente_posee_cuenta.csv", encoding="utf-8")) - 1,
        "REALIZA": len(tx_data["rel_realiza_rows"]),
        "DEBITA": len(tx_data["rel_debita_rows"]),
        "ACREDITA": len(tx_data["rel_acredita_rows"]),
        "USA": len(client_devices_rows),
        "SE_ORIGINA_EN": len(tx_data["rel_origina_rows"]),
        "SE_DESTINA_A": len(tx_data["rel_destina_rows"]),
        "GENERA": len(rel_genera_rows),
        "AFECTA_A": len(rel_afecta_rows),
        "UBICADO_EN": len(comercios),
        "COMPARTE_DISPOSITIVO": len(comparte_rows),
        "VINCULADO_A": len(vinculos_rows),
        "OCURRE_ANTES_DE": len(ocurre_rows),
    }

    write_summary(
        clientes=clientes,
        cuentas=cuentas,
        tx_data=tx_data,
        dispositivos=dispositivos,
        ubicaciones=ubicaciones,
        comercios=comercios,
        alert_rows=alert_rows,
        rel_rows=rel_counts,
    )

    print("Dataset generado correctamente en:", CSV_DIR)
    print((ROOT / "dataset_resumen.txt").as_posix())


if __name__ == "__main__":
    main()
