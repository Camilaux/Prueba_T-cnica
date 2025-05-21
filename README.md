# Crear entorno virtual
python -m venv .venv
En Windows: .venv\Scripts\activate

# Instalar dependencias
pip install -r requirements.txt

# 💻 Uso
Ejecución del Pipeline Completo desde la línea de comandos ```cmd```

```python -m etl_transaction```

# 🚀 Características Principales
- **Pipeline ETL completo:** Extracción, transformación y carga de datos transaccionales
- **Modelo predictivo:** Algoritmo XGBoost para predecir fallos en transacciones
- **Detección de anomalías:** Identificación de comportamientos anómalos basada en modelos estadísticos

# 📈 Flujo de Trabajo
1. **Extracción:** Lectura de datos transaccionales desde CSV
2. **Transformación:**
  - Limpieza de datos
  - Conversión de tipos
3. Extracción de componentes de fecha/hora
4. **Carga:** Almacenamiento en CSV
5. **Análisis:**
  - Entrenamiento de modelo predictivo
  - Cálculo de métricas
  - Detección de anomalías

# 🔍 Modelo Predictivo
El sistema utiliza XGBoost para predecir el éxito/fallo de transacciones:

- **Variable objetivo:** responsecode (0=éxito, cualquier otro valor=fallo)
- **Características principales:**
  - Canal de transacción
  - Tipo de dispositivo
  - Código y tipo de transacción
  - Componentes temporales (hora, día, mes)