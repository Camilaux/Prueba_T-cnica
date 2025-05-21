import pandas as pd


def prepare_transaction_data(transaction, encoders, feature_names=None):
    """Prepara los datos de una transacción para la predicción."""
    # Convertir a DataFrame para procesamiento
    df = pd.DataFrame([transaction])

    # Aplicar transformaciones similares a las del entrenamiento
    for col in ['channel', 'devicenameid', 'transactioncode', 'transactiontype']:
        if col in df.columns and col in encoders:
            df[f'{col}_encoded'] = encoders[col].transform(df[col].astype(str))

    # Extraer componentes temporales
    if 'finaltrxhour' in df.columns:
        df['hour'] = df['finaltrxhour'].astype(str).str.zfill(8).str[:2].astype(float)
        df['minute'] = df['finaltrxhour'].astype(str).str.zfill(8).str[2:4].astype(float)

    # Si se proporcionaron nombres específicos de características, usar solo esos
    if feature_names is not None:
        # Verificar que todas las características requeridas estén presentes
        missing_features = [feat for feat in feature_names if feat not in df.columns]
        if missing_features:
            for feat in missing_features:
                df[feat] = -999  # Valor por defecto para características faltantes
        return df[feature_names]

    # Si no, usar el enfoque anterior
    feature_cols = [col for col in df.columns if col.endswith('_encoded')]
    time_cols = ['finaltrxday', 'finaltrxmonth']
    for col in time_cols:
        if col in df.columns:
            feature_cols.append(col)

    return df[feature_cols]