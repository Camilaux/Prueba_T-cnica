"""Script para extraer, transformar y cargar datos de un archivo CSV"""

import json

from ._internals.analytic_model import analytic_model
from ._internals.detect_transaction_anomalies import detect_transaction_anomalies
from ._internals.extract_data import extract_data
from ._internals.know_your_data import know_your_data
from ._internals.load_data_to_db import load_data_to_db
from ._internals.load_model_and_encoders import load_model_and_encoders
from ._internals.transform_data import transform_data

def main():# Ruta del archivo CSV
    file_path = 'data\data.csv'
    
    # Extraer datos
    data = extract_data(file_path)
    
    if data is not None:
        # Conocer los datos
        know_your_data(data)
        
        # Transformar datos
        transformed_data = transform_data(data)
        
        if transformed_data is not None:
            print("Datos transformados:")
            print(transformed_data.head())

        # Cargar datos en el archivo CSV
        load_data_to_db(transformed_data)

        path = 'data\\transformed_data.csv'

        # Modelo analítico
        model, encoders, feature_cols = analytic_model(path)
        if model is not None:
            print("Modelo analítico construido y guardado con éxito.")
        else:
            print("Error al construir el modelo analítico.")

        # Cargar el modelo y los encoders
        model, encoders = load_model_and_encoders()
        print("Modelo y encoders cargados con éxito.")
        # Simular un lote de transacciones para la detección de anomalías
        transaction_batch = [
            {
                "channel": "NEG",
                "devicenameid": "APP",
                "transactioncode": "7900",
                "transactiontype": "No monetaria",  
                "finaltrxyear": 2024,
                "finaltrxmonth": 10,
                "finaltrxday": 15,
                "finaltrxhour": 12345678,  # Formato HHMMSSCC
                "responsecode": 500
            },
            {
                "channel": "NEG",
                "devicenameid": "APP",
                "transactioncode": "2462",
                "transactiontype": "Administrativa",  
                "finaltrxyear": 2024,
                "finaltrxmonth": 10,
                "finaltrxday": 15,
                "finaltrxhour": 87654321,  # Formato HHMMSSCC
                "responsecode": 0
            }   
        ]
        # Detectar anomalías
        alerts = detect_transaction_anomalies(model, transaction_batch, encoders)
        if alerts:
            print("Alertas generadas:")
            for alert in alerts:
                print(json.dumps(alert, indent=4))
        else:
            print("No se detectaron anomalías.")
    else:
        print("No se pudieron extraer datos del archivo CSV.")


if __name__ == "__main__":
    main()