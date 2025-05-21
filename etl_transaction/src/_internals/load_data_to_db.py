import pandas as pd


def load_data_to_db(data):
    """Guarda los datos en un .csv"""
    df = pd.DataFrame(data)
    df.to_csv('data\\transformed_data.csv', index=False)
    print("Datos cargados en el archivo CSV con éxito.")