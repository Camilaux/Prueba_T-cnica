import pandas as pd
import xgboost as xgb
from sklearn.metrics import accuracy_score, classification_report, confusion_matrix
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import LabelEncoder


import os
import pickle
import traceback


def analytic_model(path):
    """Modelo analítico para determinar si una transacción será exitosa o fallará"""
    try:
        print("Iniciando construcción del modelo predictivo...")
        # Cargar datos transformados
        data = pd.read_csv(path)

        # Crear variable objetivo binaria: 1=fallo (responsecode!=0), 0=éxito (responsecode=0)
        data['target'] = (data['responsecode'] != 0).astype(int)

        # Preparación de características
        # 1. Convertir variables categóricas a numéricas
        categorical_cols = ['channel', 'devicenameid', 'transactioncode', 'transactiontype']
        encoders = {}

        for col in categorical_cols:
            if col in data.columns:
                le = LabelEncoder()
                data[f'{col}_encoded'] = le.fit_transform(data[col].astype(str))
                encoders[col] = le

        # 2. Seleccionar características para el modelo
        feature_cols = [col for col in data.columns if col.endswith('_encoded')]

        # Añadir características temporales si existen
        time_cols = ['hour', 'minute', 'finaltrxday', 'finaltrxmonth']
        for col in time_cols:
            if col in data.columns:
                feature_cols.append(col)

        # Manejar valores faltantes
        data[feature_cols] = data[feature_cols].fillna(-999)

        print(f"Características seleccionadas: {feature_cols}")

        # Dividir los datos en conjuntos de entrenamiento y prueba
        X = data[feature_cols]
        y = data['target']

        X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.25, random_state=42)

        print(f"Entrenando con {X_train.shape[0]} muestras, evaluando con {X_test.shape[0]} muestras")

        # Configurar parámetros de XGBoost
        params = {
            'objective': 'binary:logistic',
            'max_depth': 4,
            'learning_rate': 0.1,
            'n_estimators': 100,
            'subsample': 0.8,
            'colsample_bytree': 0.8,
            'eval_metric': 'auc',
            'use_label_encoder': False,
        }

        # Entrenar el modelo
        model = xgb.XGBClassifier(**params)
        model.fit(X_train, y_train, eval_set=[(X_test, y_test)], verbose=1)

        # Realizar predicciones
        y_pred = model.predict(X_test)
        y_pred_proba = model.predict_proba(X_test)[:,1]

        # Evaluar el modelo
        accuracy = accuracy_score(y_test, y_pred)
        print(f"Precisión: {accuracy:.4f}")

        # Mostrar informe de clasificación
        print("\nInforme de Clasificación:")
        print(classification_report(y_test, y_pred))

        # Mostrar matriz de confusión
        conf_matrix = confusion_matrix(y_test, y_pred)
        print("\nMatriz de Confusión:")
        print(conf_matrix)

        # Importancia de características
        importance = model.feature_importances_
        feature_importance = {feature: importance[i] for i, feature in enumerate(feature_cols)}
        sorted_importance = sorted(feature_importance.items(), key=lambda x: x[1], reverse=True)

        print("\nImportancia de Características:")
        for feature, importance in sorted_importance[:10]:  # Top 10
            print(f"{feature}: {importance:.4f}")

        # Crear directorio models si no existe
        if not os.path.exists('models'):
            os.makedirs('models')

        # Guardar el modelo
        model.save_model('models/transaction_success_model.json')

        # Guardar los codificadores para uso futuro
        with open('models/encoders.pkl', 'wb') as f:
            pickle.dump(encoders, f)

        print("\nModelo guardado en 'models/transaction_success_model.json'")

        return model, encoders, feature_cols

    except Exception as e:
        print(f"Error en el modelo analítico: {e}")
        import traceback
        traceback.print_exc()
        return None