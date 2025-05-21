import pandas as pd


def transform_data(data):
    """Transforma los datos eliminando filas duplicadas y convirtiendo tipos de datos."""
    try:
        # Crear una copia explícita para evitar
        data = data.copy()

        # Eliminar filas duplicadas
        data = data.drop_duplicates()

        # Eliminar columna innecesaria
        data = data.drop(columns='transactionvouchernumber', errors='ignore')

        # Convertir tipos de datos de manera más robusta
        data['finaltrxyear'] = pd.to_numeric(data['finaltrxyear'], errors='coerce')
        data['finaltrxmonth'] = pd.to_numeric(data['finaltrxmonth'], errors='coerce')
        data['finaltrxday'] = pd.to_numeric(data['finaltrxday'], errors='coerce')

        # Manejar la columna de horas como formato de 8 dígitos
        data['finaltrxhour'] = pd.to_numeric(data['finaltrxhour'], errors='coerce')

        # Extraer componentes de tiempo del formato de 8 dígitos
        data['hour'] = data['finaltrxhour'].astype(str).str.zfill(8).str[:2].astype(float)
        data['minute'] = data['finaltrxhour'].astype(str).str.zfill(8).str[2:4].astype(float)
        data['second'] = data['finaltrxhour'].astype(str).str.zfill(8).str[4:6].astype(float)

        # Validar el rango de horas (0-23)
        data['hour'] = data['hour'].apply(
            lambda x: x if pd.notna(x) and 0 <= x < 24 else pd.NA
        )

        # Crear una columna datetime para análisis temporal
        try:
            data['datetime'] = pd.to_datetime(
                data['finaltrxyear'].astype(str) + '-' +
                data['finaltrxmonth'].astype(str) + '-' +
                data['finaltrxday'].astype(str) + ' ' +
                data['hour'].astype(str) + ':' +
                data['minute'].astype(str) + ':' +
                data['second'].astype(str),
                errors='coerce'
            )
        except Exception as e:
            print(f"Aviso: No se pudo crear la columna datetime: {e}")

        # Unificar valores en 'transactiontype'
        data['transactiontype'] = data['transactiontype'].replace({
            'No Monetaria': 'No monetaria',
            'No monetaria ': 'No monetaria',
            'Administrativa': 'Administrativa',
            'No_monetaria': 'No monetaria'
        })

        print("Datos transformados con éxito.")
        return data
    except Exception as e:
        print(f"Error al transformar datos: {e}")
        return None