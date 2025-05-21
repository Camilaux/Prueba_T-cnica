from .prepare_transaction_data import prepare_transaction_data


from datetime import datetime


def detect_transaction_anomalies(model, transaction_batch, encoders, threshold=0.3):
    """Detecta anomalías en un lote de transacciones usando el modelo predictivo."""
    alerts = []

    # Obtener las características que espera el modelo
    expected_features = model.feature_names_in_

    for tx in transaction_batch:
        # Preparar datos con las características esperadas por el modelo
        X = prepare_transaction_data(tx, encoders, feature_names=expected_features)

        # Predecir probabilidad de fallo 
        failure_prob = model.predict_proba(X)[0, 1]  # Probabilidad de clase 1 (fallo)

        # Si la probabilidad supera el umbral, generar alerta
        if failure_prob > threshold:
            alert = {
                "type": "TRANSACTION_RISK",
                "severity": "HIGH" if failure_prob > 0.7 else "MEDIUM",
                "timestamp": datetime.now().isoformat(),
                "details": {
                    "transaction_id": tx.get("transactioncode", "unknown"),
                    "channel": tx.get("channel", "unknown"),
                    "failure_probability": round(failure_prob, 4),
                    "reason": "Alto riesgo de fallo de transacción según modelo predictivo"
                }
            }
            alerts.append(alert)

    return alerts