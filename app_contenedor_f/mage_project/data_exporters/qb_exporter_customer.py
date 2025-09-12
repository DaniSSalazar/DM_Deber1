import json
import os
from datetime import datetime, timedelta

@data_exporter
def export_invoices_to_txt(data, **kwargs) -> None:
    """
    Exporta invoices a un archivo TXT con los campos solicitados.
    data: lista de diccionarios devuelta por el loader.
    """
    # Ruta relativa, puede incluir subcarpetas
    file_path = kwargs.get("file_path", "customers.txt")
    # Crear carpeta si se pasó una subcarpeta en file_path
    folder = os.path.dirname(file_path)
    if folder:
        os.makedirs(folder, exist_ok=True)

    page_size = kwargs.get("page_size", 100)
    fecha_inicio = kwargs.get("fecha_inicio", "2014-09-01")
    fecha_fin = kwargs.get("fecha_fin", "2025-09-01")

    start_date = datetime.fromisoformat(fecha_inicio)
    end_date = datetime.fromisoformat(fecha_fin)
    delta = timedelta(days=50)

    ingested_at = datetime.utcnow().isoformat() + "Z"
    current_start = start_date
    page_number = 1

    with open(file_path, "w", encoding="utf-8") as f:
        while current_start < end_date:
            current_end = min(current_start + delta, end_date)

            # Filtramos las filas de este rango
            rows_in_range = [
                inv for inv in data
                if current_start.isoformat() <= inv.get("MetaData", {}).get("CreateTime", "") < current_end.isoformat()
            ]

            # Paginamos dentro del rango
            for i in range(0, len(rows_in_range), page_size):
                page_rows = rows_in_range[i:i+page_size]
                for row in page_rows:
                    export_row = {
                        "id": row.get("Id"),
                        "ingested_at_utc": ingested_at,
                        "extract_window_start_utc": current_start.isoformat() + "Z",
                        "extract_window_end_utc": current_end.isoformat() + "Z",
                        "page_number": page_number,
                        "page_size": page_size,
                        "request_payload": row
                    }
                    f.write(json.dumps(export_row, ensure_ascii=False) + "\n")

                page_number += 1

            current_start = current_end

    print(f"Exportación completa al archivo: {os.path.abspath(file_path)}")
