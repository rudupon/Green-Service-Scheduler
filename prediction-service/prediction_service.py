import os
import logging
import numpy as np
import pandas as pd
import tensorflow as tf
import requests
from flask import Flask, request, jsonify
from datetime import datetime, timedelta
import time
import psutil

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger("PredictionService")

app = Flask(__name__)

SAMPLING_INTERVAL = int(os.environ.get('SAMPLING_INTERVAL_SECONDS', '5'))
FORECAST_HORIZON_MINUTES = 60  # 1 uur voorspelling
LOOKBACK_WINDOW_MINUTES = 12   # 12 minuten terugkijken
FORECAST_HORIZON = (FORECAST_HORIZON_MINUTES * 60) // SAMPLING_INTERVAL  # was: 60 * 60 // 5
LOOKBACK_WINDOW = (LOOKBACK_WINDOW_MINUTES * 60) // SAMPLING_INTERVAL   # was: 12 * 60 // 5

MODEL_TYPE = os.environ.get('MODEL_TYPE', 'baseline_model')

logger.info(f"Prediction Service gestart met:")
logger.info(f"  Sampling interval: {SAMPLING_INTERVAL}s")
logger.info(f"  Forecast horizon: {FORECAST_HORIZON} samples ({FORECAST_HORIZON_MINUTES} min)")
logger.info(f"  Lookback window: {LOOKBACK_WINDOW} samples ({LOOKBACK_WINDOW_MINUTES} min)")

model = None
model_stats = {}

def measure_resources():
    process = psutil.Process(os.getpid())
    memory_info = process.memory_info()
    
    return {
        "memory_usage_mb": memory_info.rss / (1024 * 1024),
        "cpu_percent": process.cpu_percent(interval=0.1),
        "num_threads": process.num_threads(),
        "total_system_memory_mb": psutil.virtual_memory().total / (1024 * 1024),
        "available_system_memory_mb": psutil.virtual_memory().available / (1024 * 1024),
        "system_cpu_percent": psutil.cpu_percent(interval=0.1),
        "num_cpus": psutil.cpu_count(logical=True)
    }

def load_model():
    global model, model_stats
    if MODEL_TYPE == 'baseline_model':
        model_path = os.path.join('models', 'baseline_model.tflite')
    elif MODEL_TYPE == 'dynamic_range':
        model_path = os.path.join('models', 'lstm_dynamic_range.tflite')
    elif MODEL_TYPE == 'full_int8':
        model_path = os.path.join('models', 'lstm_full_int8.tflite')
    else:
        logger.warning(f"Onbekend model type: {MODEL_TYPE}, gebruik baseline model")
        model_path = os.path.join('models', 'baseline_model.tflite')
    
    try:
        if os.path.exists(model_path):
            logger.info(f"Model gevonden op pad: {model_path}")
            before_resources = measure_resources()
            start_time = time.time()
            
            interpreter = tf.lite.Interpreter(model_path=model_path)
            interpreter.allocate_tensors()
            model = interpreter
            
            load_time = time.time() - start_time
            after_resources = measure_resources()
            
            # Verzamel model statistieken
            input_details = model.get_input_details()
            output_details = model.get_output_details()
            model_size_mb = os.path.getsize(model_path) / (1024 * 1024)
            
            model_stats = {
                "model_size_mb": model_size_mb,
                "load_time_seconds": load_time,
                "memory_increase_mb": after_resources["memory_usage_mb"] - before_resources["memory_usage_mb"],
                "input_shapes": [detail["shape"] for detail in input_details],
                "output_shapes": [detail["shape"] for detail in output_details],
                "input_types": [str(detail["dtype"]) for detail in input_details],
                "tensor_details_count": len(model.get_tensor_details())
            }
            
            logger.info(f"LSTM model succesvol geladen in {load_time:.2f}s, grootte: {model_size_mb:.2f}MB")
            return True
        else:
            logger.warning(f"Geen model gevonden op pad: {model_path}.")
            return False
    except Exception as e:
        logger.error(f"Fout bij het laden van het model: {e}")
        return False

def get_latest_data(seconds=None):
    try:
        # Forceer ALTIJD exact 720 seconden, ongeacht lokale configuratie
        required_seconds = 720  # Exact 12 minuten, hard-coded om consistentie te garanderen
        
        node_ip = os.environ.get("NODE_IP", "localhost")
        
        url = f"http://{node_ip}:30005/latest?seconds={seconds}"
        logger.info(f"Data ophalen van lokale node via NodePort: {url}")
        
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        
        data = response.json()
        logger.info(f"Data opgehaald: {data.get('samples', 0)} samples")
        
        return {
            "values": data.get("values", []),
            "timestamps": data.get("timestamps", [])
        }
    except requests.exceptions.RequestException as e:
        logger.error(f"Fout bij ophalen data van data-service: {e}")
        return None

def preprocess_data(historical_data=None):
    """
    Bereid historische data voor als invoer voor het model.
    Implementeert dezelfde feature engineering stappen als in training.
    
    Parameters:
        historical_data: Dict met 'values' en 'timestamps' lijsten, of None
        
    Returns:
        np.array: Een array van vorm (1, 144, N) met de features voor het model
    """
    if historical_data is None or 'values' not in historical_data:
        logger.info("Onvoldoende historische data, genereren van synthetische data")
        return generate_synthetic_features()
    
    logger.info(f"Feature engineering toepassen op {len(historical_data['values'])} datapunten")
    
    try:
        df = pd.DataFrame({
            'Datetime': pd.to_datetime(historical_data['timestamps']),
            'Power_Value': historical_data['values']
        })
        
        df = df.sort_values('Datetime')
        
        # 0. Tijdscomponenten toevoegen
        df['hour'] = df['Datetime'].dt.hour
        df['day'] = df['Datetime'].dt.day
        df['month'] = df['Datetime'].dt.month
        df['dayofweek'] = df['Datetime'].dt.dayofweek
        
        # 1. Tijdgerelateerde cyclische features
        df['hour_sin'] = np.sin(2 * np.pi * df['hour'] / 24)
        df['hour_cos'] = np.cos(2 * np.pi * df['hour'] / 24)
        df['dayofweek_sin'] = np.sin(2 * np.pi * df['dayofweek'] / 7)
        df['dayofweek_cos'] = np.cos(2 * np.pi * df['dayofweek'] / 7)
        
        # 2. Rolling statistics
        window_minutes = [5, 10]
        window_samples = [(w * 60) // SAMPLING_INTERVAL for w in window_minutes]
        
        for window in window_samples:
            df[f'rolling_mean_{window}'] = df['Power_Value'].rolling(window=min(window, len(df)), min_periods=1).mean()
            
            df[f'rolling_std_{window}'] = df['Power_Value'].rolling(window=min(window, len(df)), min_periods=1).std().fillna(0)
            
            df[f'rolling_min_{window}'] = df['Power_Value'].rolling(window=min(window, len(df)), min_periods=1).min()
            df[f'rolling_max_{window}'] = df['Power_Value'].rolling(window=min(window, len(df)), min_periods=1).max()
            
            min_vals = df[f'rolling_min_{window}']
            max_vals = df[f'rolling_max_{window}']
            range_vals = max_vals - min_vals
            # Vermijd deling door nul
            range_vals = range_vals.replace(0, 1)
            df[f'rolling_position_{window}'] = (df['Power_Value'] - min_vals) / range_vals
        
        # 3. Differentiële features
        # Bereken tijdsverschil in seconden
        df['time_diff'] = df['Datetime'].diff().dt.total_seconds().fillna(0)
        df['power_diff'] = df['Power_Value'].diff().fillna(0)
        # Vermijd deling door nul
        df['rate_of_change'] = np.where(df['time_diff'] > 0,
                                      df['power_diff'] / df['time_diff'],
                                      0)
        
        df['acceleration'] = df['rate_of_change'].diff().fillna(0) / df['time_diff'].replace(0, 1)
        
        # 4. Zonne-energie specifieke features
        df['is_daytime'] = ((df['hour'] >= 6) & (df['hour'] <= 20)).astype(float)
        df['is_peak_sun'] = ((df['hour'] >= 10) & (df['hour'] <= 16)).astype(float)
        df['hours_since_sunrise'] = np.where(df['hour'] >= 6,
                                           df['hour'] - 6,
                                           0)
        df['hours_until_sunset'] = np.where(df['hour'] <= 20,
                                          20 - df['hour'],
                                          0)
        
        # 5. Verwijder de kolommen die niet als features worden gebruikt
        cols_to_drop = ['Datetime', 'time_diff', 'day', 'hour', 'month', 'dayofweek']
        df_features = df.drop(columns=cols_to_drop, errors='ignore')
        
        # 6. Controleer op en vul eventuele NaN-waarden
        df_features = df_features.fillna(0)
        
        # 7. Zorg dat we het juiste aantal samples hebben (voor LSTM input)
        target_samples = LOOKBACK_WINDOW
        if len(df_features) > target_samples:
            df_features = df_features.iloc[-target_samples:].reset_index(drop=True)
        elif len(df_features) < target_samples:
            padding_rows = target_samples - len(df_features)
            padding_df = pd.DataFrame(0, index=range(padding_rows), columns=df_features.columns)
            df_features = pd.concat([padding_df, df_features], ignore_index=True)
        
        logger.info(f"Feature engineering voltooid, {len(df_features.columns)} features gegenereerd")
        
        # 8. Converteer naar numpy array geschikt voor LSTM
        model_input = df_features.values.reshape(1, target_samples, len(df_features.columns))
        
        return model_input
        
    except Exception as e:
        logger.error(f"Fout bij feature engineering: {e}")
        logger.error(f"Fallback naar synthetische features")
        return generate_synthetic_features()

def generate_synthetic_features():
    """
    Genereert synthetische features als er geen echte data beschikbaar is.
    """
    logger.info("Genereren van synthetische features")
    
    # Basispatroon (sinusgolf)
    t = np.linspace(0, 2*np.pi, LOOKBACK_WINDOW)
    power_values = 0.5 + 0.5 * np.sin(t)
    
    # DataFrame maken met synthetische tijdsdata
    base_time = datetime.now() - timedelta(minutes=LOOKBACK_WINDOW_MINUTES)
    datetimes = [base_time + timedelta(seconds=SAMPLING_INTERVAL*i) for i in range(LOOKBACK_WINDOW)]
    
    df = pd.DataFrame({
        'Datetime': datetimes,
        'Power_Value': power_values
    })
    
    # Tijdscomponenten toevoegen
    df['hour'] = df['Datetime'].dt.hour
    df['day'] = df['Datetime'].dt.day
    df['month'] = df['Datetime'].dt.month
    df['dayofweek'] = df['Datetime'].dt.dayofweek
    
    # Alle feature engineering stappen toepassen zoals hierboven
    # 1. Tijdgerelateerde cyclische features
    df['hour_sin'] = np.sin(2 * np.pi * df['hour'] / 24)
    df['hour_cos'] = np.cos(2 * np.pi * df['hour'] / 24)
    df['dayofweek_sin'] = np.sin(2 * np.pi * df['dayofweek'] / 7)
    df['dayofweek_cos'] = np.cos(2 * np.pi * df['dayofweek'] / 7)
    
    # 2. Rolling statistics
    window_minutes = [5, 10]
    window_samples = [(w * 60) // SAMPLING_INTERVAL for w in window_minutes]
    for window in window_samples:
        df[f'rolling_mean_{window}'] = df['Power_Value'].rolling(window=min(window, len(df)), min_periods=1).mean()
        df[f'rolling_std_{window}'] = df['Power_Value'].rolling(window=min(window, len(df)), min_periods=1).std().fillna(0)
        df[f'rolling_min_{window}'] = df['Power_Value'].rolling(window=min(window, len(df)), min_periods=1).min()
        df[f'rolling_max_{window}'] = df['Power_Value'].rolling(window=min(window, len(df)), min_periods=1).max()
        
        min_vals = df[f'rolling_min_{window}']
        max_vals = df[f'rolling_max_{window}']
        range_vals = max_vals - min_vals
        range_vals = range_vals.replace(0, 1)
        df[f'rolling_position_{window}'] = (df['Power_Value'] - min_vals) / range_vals
    
    # 3. Differentiële features
    df['time_diff'] = SAMPLING_INTERVAL
    df['power_diff'] = df['Power_Value'].diff().fillna(0)
    df['rate_of_change'] = df['power_diff'] / SAMPLING_INTERVAL
    df['acceleration'] = df['rate_of_change'].diff().fillna(0) / SAMPLING_INTERVAL
    
    # 4. Zonne-energie specifieke features
    df['is_daytime'] = ((df['hour'] >= 6) & (df['hour'] <= 20)).astype(float)
    df['is_peak_sun'] = ((df['hour'] >= 10) & (df['hour'] <= 16)).astype(float)
    df['hours_since_sunrise'] = np.where(df['hour'] >= 6, df['hour'] - 6, 0)
    df['hours_until_sunset'] = np.where(df['hour'] <= 20, 20 - df['hour'], 0)
    
    # 5. Verwijder de kolommen die niet als features worden gebruikt
    cols_to_drop = ['Datetime', 'time_diff', 'day', 'hour', 'month', 'dayofweek']
    df_features = df.drop(columns=cols_to_drop, errors='ignore')
    
    # 6. Controleer op en vul eventuele NaN-waarden
    df_features = df_features.fillna(0)
    
    # Converteer naar numpy array geschikt voor LSTM
    model_input = df_features.values.reshape(1, LOOKBACK_WINDOW, len(df_features.columns))
    
    logger.info(f"Synthetische features gegenereerd, vorm: {model_input.shape}")
    return model_input

def generate_synthetic_prediction():
    """Genereert een synthetische voorspelling als fallback."""
    logger.info("Genereren van een synthetische voorspelling")
    t = np.linspace(0, 2*np.pi, FORECAST_HORIZON)  # 1 uur aan 5s samples = 720 punten
    prediction = 0.5 + 0.5 * np.sin(t + time.time() % (2*np.pi))
    return prediction.tolist()

@app.route('/health', methods=['GET'])
def health_check():
    """Health check endpoint."""
    if model is None:
        load_model()    
    
    # Controleer of data-service bereikbaar is
    data_service_available = False
    try:
        response = requests.get("http://data-service:5000/health", timeout=3)
        data_service_available = response.status_code == 200
    except:
        pass
    
    model_status = model is not None
    status = "healthy" if model_status and data_service_available else "degraded" if model_status else "unhealthy"
    
    return jsonify({
        "status": status, 
        "model_loaded": model is not None,
        "data_service_available": data_service_available,
    }), 200 if status == "healthy" else 503

@app.route('/predict', methods=['POST'])
def predict():
    """
    API endpoint voor het genereren van voorspellingen.
    
    Verwacht een JSON met:
    - historical_data: Optioneel, lijst met historische metingen
    - timestamp: Huidige timestamp
    
    Retourneert een lijst met voorspellingen 1 uur vooruit
    """
    start_resources = measure_resources()
    start_time = time.time()
    prediction_metrics = {"phases": {}}
    
    data = request.json
    if not data:
        logger.warning("Geen JSON data ontvangen")
        data = {}
    
    historical_data = data.get('historical_data')
    timestamp = data.get('timestamp', time.time())
    
    # Fase 1: Data ophalen
    phase1_start = time.time()
    if historical_data is None:
        logger.info("Geen historische data meegegeven, ophalen van data-service")
        historical_data = get_latest_data(seconds=LOOKBACK_WINDOW*SAMPLING_INTERVAL)
    
    if isinstance(historical_data, list):
        historical_data = {
            "values": historical_data,
            "timestamps": [datetime.fromtimestamp(timestamp - (len(historical_data) - i - 1) * SAMPLING_INTERVAL).strftime('%Y-%m-%d %H:%M:%S') 
                         for i in range(len(historical_data))]
        }
        
    phase1_time = time.time() - phase1_start
    prediction_metrics["phases"]["data_collection"] = {
        "duration_ms": phase1_time * 1000,
        "resources_after": measure_resources()
    }
    
    # Fase 2: Preprocessing
    phase2_start = time.time()
    try:
        model_input = preprocess_data(historical_data)
        preprocessing_success = True
    except Exception as e:
        logger.error(f"Fout bij preprocessing: {e}")
        preprocessing_success = False
        model_input = None
    
    phase2_time = time.time() - phase2_start
    preprocessing_resources = measure_resources()
    prediction_metrics["phases"]["preprocessing"] = {
        "duration_ms": phase2_time * 1000,
        "resources": preprocessing_resources,
        "input_shape": model_input.shape if model_input is not None else None,
        "success": preprocessing_success
    }
    
    # Fase 3: Model inferentie
    phase3_start = time.time()
    predictions = None
    inference_success = False
    if preprocessing_success and model is not None:
        try:
            input_details = model.get_input_details()
            output_details = model.get_output_details()
            
            model_input = model_input.astype(np.float32)
            
            model.set_tensor(input_details[0]['index'], model_input)
                
            model.invoke()
            
            predictions = model.get_tensor(output_details[0]['index'])[0]
            predictions = np.clip(predictions, 0, 1).tolist()
            inference_success = True
        except Exception as e:
            logger.error(f"Inferentiefout: {e}")
            predictions = generate_synthetic_prediction()
            inference_success = False
    else:
        # Fallback als preprocessing mislukt of model niet beschikbaar
        predictions = generate_synthetic_prediction()
        inference_success = False
    
    phase3_time = time.time() - phase3_start
    inference_resources = measure_resources()
    prediction_metrics["phases"]["inference"] = {
        "duration_ms": phase3_time * 1000,
        "resources": inference_resources,
        "success": inference_success,
        "fallback_used": not (preprocessing_success and model is not None)
    }
    
    # Totale metriek berekening voor logging
    total_time = time.time() - start_time
    end_resources = measure_resources()
    
    memory_delta = end_resources["memory_usage_mb"] - start_resources["memory_usage_mb"]
    
    # Bouw metrics voor logs
    metrics = {
        "total_duration_ms": total_time * 1000,
        "phases": {
            "data_collection_ms": phase1_time * 1000,
            "preprocessing_ms": phase2_time * 1000,
            "inference_ms": phase3_time * 1000,
        },
        "memory": {
            "start_mb": start_resources["memory_usage_mb"],
            "end_mb": end_resources["memory_usage_mb"],
            "delta_mb": memory_delta,
            "peak_during_inference_mb": inference_resources["memory_usage_mb"]
        },
        "cpu": {
            "preprocessing_percent": prediction_metrics["phases"]["preprocessing"]["resources"]["cpu_percent"],
            "inference_percent": inference_resources["cpu_percent"],
            "num_cores": end_resources["num_cpus"]
        },
        "model": model_stats if model is not None else {"status": "not_loaded"},
        "system": {
            "node_name": os.environ.get("NODE_NAME", "unknown"),
            "total_memory_mb": end_resources["total_system_memory_mb"],
            "available_memory_mb": end_resources["available_system_memory_mb"],
            "system_cpu_percent": end_resources["system_cpu_percent"],
        }
    }
    
    logger.info(f"Voorspelling gemaakt in {metrics['total_duration_ms']:.2f}ms (preprocesseren: {metrics['phases']['preprocessing_ms']:.2f}ms, inferentie: {metrics['phases']['inference_ms']:.2f}ms)")
    
    # Gedetailleerde resource metrieken loggen
    logger.info(f"Memory metrics: start={metrics['memory']['start_mb']:.2f}MB, "
                f"end={metrics['memory']['end_mb']:.2f}MB, "
                f"delta={metrics['memory']['delta_mb']:.2f}MB, "
                f"peak={metrics['memory']['peak_during_inference_mb']:.2f}MB")
    
    logger.info(f"CPU metrics: preprocessing={metrics['cpu']['preprocessing_percent']:.2f}%, "
                f"inference={metrics['cpu']['inference_percent']:.2f}%, "
                f"cores={metrics['cpu']['num_cores']}")
    
    logger.info(f"System metrics: node={metrics['system']['node_name']}, "
                f"total memory={metrics['system']['total_memory_mb']:.2f}MB, "
                f"available memory={metrics['system']['available_memory_mb']:.2f}MB, "
                f"system CPU={metrics['system']['system_cpu_percent']:.2f}%")
    
    # Log model informatie indien beschikbaar
    if model is not None and 'model_size_mb' in metrics['model']:
        logger.info(f"Model metrics: size={metrics['model']['model_size_mb']:.2f}MB, "
                    f"load time={metrics['model']['load_time_seconds']:.2f}s, "
                    f"memory increase={metrics['model']['memory_increase_mb']:.2f}MB")
    
    return jsonify({"prediction": predictions})

if __name__ == '__main__':
    load_model()
    
    port = int(os.environ.get('FLASK_PORT', 8000))
    logger.info(f"Prediction Service wordt gestart...")
    app.run(host='0.0.0.0', port=port)
