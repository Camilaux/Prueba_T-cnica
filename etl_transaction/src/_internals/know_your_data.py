def know_your_data(data):
    """Muestra información básica sobre el DataFrame."""
    try:
        print("Información del DataFrame:")
        print(data.info())
        print("\nPrimeras filas del DataFrame:")
        print(data.head())
        print("\nDescripción estadística del DataFrame:")
        print(data.describe())
        print("\nValores nulos en el DataFrame:")
        print(data.isnull().sum())
        print("\nTipos de datos en el DataFrame:")
        print(data.dtypes)
        print("\nNúmero de filas y columnas:")
        print(data.shape)
        print("\nNombres de las columnas:")
        print(data.columns)
        print("\nValores únicos en 'transactiontype':")
        print(data['transactiontype'].unique())
    except Exception as e:
        print(f"Error al conocer los datos: {e}")