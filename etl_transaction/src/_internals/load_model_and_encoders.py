import xgboost as xgb


import pickle


def load_model_and_encoders():
    """Carga el modelo entrenado y los encoders."""
    model = xgb.XGBClassifier()
    model.load_model('models/transaction_success_model.json')

    with open('models/encoders.pkl', 'rb') as f:
        encoders = pickle.load(f)

    return model, encoders