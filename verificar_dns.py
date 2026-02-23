"""
Verificación de DNS y disponibilidad HTTPS de subdominios vmt.gov.py
Muestra: IP(s), tiempo de respuesta, estado HTTP.
Ejecutar: python verificar_dns.py
"""
import socket
import ssl
import time
import urllib.request
import urllib.error
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import List, Optional, Tuple

SUBDOMINIOS = [
    "agregaciones",
    "analisisdedatos",
    "api-ccm",
    "bbdd-operativa",
    "broker-operativa",
    "ciudadanos",
    "combustible",
    "drive",
    "gestion",
    "gitlab",
    "grafana",
    "infotransporte",
    "monitoreoadmin",
    "monitoreo",
    "osrm",
    "project",
    "qgis",
    "reforma",
    "registry",
    "replicatransacciones",
    "reportes",
    "ruteo",
    "sis",
    "staging-agregaciones",
    "staging-bbdd-operativa",
    "staging-broker-operativa",
    "staging-catalogos",
    "staging-gestion",
    "staging-grafana",
    "staging-monitoreoadmin",
    "staging-monitoreo",
    "staging-sis",
    "wiki",
]

TIMEOUT = 10
DOMINIO_BASE = "vmt.gov.py"


def resolver_ips(host: str) -> List[str]:
    """Resuelve todas las IP (IPv4 e IPv6) del host."""
    ips = []
    try:
        for res in socket.getaddrinfo(host, 443, socket.AF_UNSPEC, socket.SOCK_STREAM):
            familia, _, _, _, sockaddr = res
            ip = sockaddr[0]
            if ip not in ips:
                ips.append(ip)
    except socket.gaierror:
        pass
    return ips


def verificar_uno(sub: str) -> Tuple[str, bool, str, str, List[str], Optional[float]]:
    """
    Verifica DNS y HEAD HTTPS de un subdominio.
    Retorna: (subdominio, ok, mensaje_estado, detalle, lista_ips, tiempo_ms)
    """
    host = f"{sub}.{DOMINIO_BASE}"
    url = f"https://{host}/"

    # 1) Resolución DNS → IPs
    ips = resolver_ips(host)
    if not ips:
        return (sub, False, "DNS error", "No se pudo resolver", [], None)

    # 2) HEAD HTTPS (sin verificar certificado, equivalente a curl -k)
    ctx = ssl.create_default_context()
    ctx.check_hostname = False
    ctx.verify_mode = ssl.CERT_NONE
    try:
        req = urllib.request.Request(url, method="HEAD")
        t0 = time.perf_counter()
        with urllib.request.urlopen(req, timeout=TIMEOUT, context=ctx) as r:
            tiempo_ms = (time.perf_counter() - t0) * 1000
            status = r.status
            return (sub, 200 <= status < 400, f"HTTP {status}", "", ips, tiempo_ms)
    except urllib.error.HTTPError as e:
        return (sub, False, f"HTTP {e.code}", e.reason or "", ips, None)
    except urllib.error.URLError as e:
        return (sub, False, "Error URL", str(e.reason), ips, None)
    except (TimeoutError, OSError) as e:
        return (sub, False, "Timeout/Error", str(e), ips, None)


def main():
    print(f"Verificando {len(SUBDOMINIOS)} subdominios en {DOMINIO_BASE}\n")
    print("-" * 75)

    resultados_ok = []
    resultados_fallo = []

    with ThreadPoolExecutor(max_workers=8) as executor:
        futuros = {executor.submit(verificar_uno, sub): sub for sub in SUBDOMINIOS}
        for futuro in as_completed(futuros):
            sub, ok, estado, detalle, ips, tiempo_ms = futuro.result()
            url = f"https://{sub}.{DOMINIO_BASE}"
            ips_str = ", ".join(ips) if ips else "—"
            tiempo_str = f"  {tiempo_ms:.0f} ms" if tiempo_ms is not None else ""
            if ok:
                resultados_ok.append((sub, ips, tiempo_ms))
                print(f"  OK   {url}")
                print(f"        IP(s): {ips_str}  |  {estado}{tiempo_str}")
            else:
                resultados_fallo.append((sub, estado, detalle, ips))
                detalle_str = f"  ({detalle})" if detalle else ""
                print(f"  FAIL {url}  ->  {estado}{detalle_str}")
                if ips:
                    print(f"        IP(s): {ips_str}")

    print("-" * 75)
    print(f"\nResumen: {len(resultados_ok)} OK, {len(resultados_fallo)} con fallos")
    if resultados_fallo:
        print("\nCon fallos:")
        for item in resultados_fallo:
            sub, estado, detalle, ips = item[:4]
            ips_str = ", ".join(ips) if ips else "—"
            print(f"  - {sub}.{DOMINIO_BASE}  ({estado})  IP: {ips_str}")


if __name__ == "__main__":
    main()
