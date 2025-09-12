import io
import pandas as pd
import requests
from datetime import datetime, timedelta
from mage_ai.data_preparation.shared.secrets import get_secret_value

if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test

TOKEN_URL = "https://oauth.platform.intuit.com/oauth2/v1/tokens/bearer"

def _get_access_token():
    client_id = get_secret_value('qb_client_id')
    client_secret = get_secret_value('qb_client_secret')
    refresh_token = get_secret_value('qb_refresh_token')

    auth = (client_id, client_secret)
    headers = {"Accept": "application/json", "Content-Type": "application/x-www-form-urlencoded"}
    payload = {"grant_type": "refresh_token", "refresh_token": refresh_token}

    resp = requests.post(TOKEN_URL, headers=headers, data=payload, auth=auth)
    resp.raise_for_status()
    return resp.json()["access_token"]

def _fetch_qb_by_date(realm_id, access_token, query_base, base_url, minor_version, page_size=100):
    headers = {
        'Authorization': f'Bearer {access_token}',
        'Accept': 'application/json',
        'Content-Type': 'text/plain'
    }

    all_rows = []
    start_pos = 1

    while True:
        paged_query = f"{query_base} STARTPOSITION {start_pos} MAXRESULTS {page_size}"
        params = {'query': paged_query, 'minorversion': minor_version}

        resp = requests.get(f"{base_url.rstrip('/')}/v3/company/{realm_id}/query",
                            headers=headers, params=params, timeout=240)
        resp.raise_for_status()

        data = resp.json()
        rows = data.get("QueryResponse", {}).get("Customer", [])

        if not rows:
            break

        all_rows.extend(rows)
        start_pos += page_size

        if len(rows) < page_size:
            break

    return all_rows

@data_loader
def load_data_from_api(*args, **kwargs):
    realm_id = get_secret_value('qb_realm_id')
    access_token = _get_access_token()
    minor_version = 75
    base_url = 'https://sandbox-quickbooks.api.intuit.com'

    # Rangos de fecha por defecto
    fecha_inicio = kwargs.get("fecha_inicio", "2014-09-01")
    fecha_fin = kwargs.get("fecha_fin", "2025-09-01")

    start_date = datetime.fromisoformat(fecha_inicio)
    end_date = datetime.fromisoformat(fecha_fin)
    delta = timedelta(days=50)  # Tramos de 7 días

    all_data = []
    logs = []

    current_start = start_date
    while current_start < end_date:
        current_end = min(current_start + delta, end_date)

        query_base = (
            f"SELECT * FROM Customer "
            f"WHERE MetaData.LastUpdatedTime >= '{current_start.isoformat()}' "
            f"AND MetaData.LastUpdatedTime < '{current_end.isoformat()}'"
        )

        rows = _fetch_qb_by_date(realm_id, access_token, query_base, base_url, minor_version, page_size=100)
        all_data.extend(rows)

        logs.append({
            "fecha_inicio": current_start.date().isoformat(),
            "fecha_fin": current_end.date().isoformat(),
            "filas": len(rows)
        })

        current_start = current_end

    for log in logs:
        print(f"Rango {log['fecha_inicio']} a {log['fecha_fin']} -> {log['filas']} filas")

    print(f"Total Customers descargadas: {len(all_data)}")
    return all_data

@test
def test_output(output, *args) -> None:
    assert output is not None, 'The output es undefined'
    print(f"Se descargaron {len(output)} customers.")



