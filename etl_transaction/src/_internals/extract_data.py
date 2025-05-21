import pandas as pd


def extract_data(file_path):
    """Extrae datos de un archivo CSV y los carga en un DataFrame de pandas."""
    try:
        data = pd.read_csv(file_path, index_col=0)
        print("Datos extraídos con éxito.")
        return data
    except Exception as e:
        print(f"Error al extraer datos: {e}")
        return None