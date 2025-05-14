import os
import json
import logging
import numpy as np
import tensorflow as tf
from flask import Flask, request, jsonify
from datetime import datetime, timedelta
import time

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger("PredictionService")

app = Flask(__name__)

model = None

def load_model():
    global model
    model_path = os.path.join('models', 'lstm_model.tflite')
    
    try:
        if os.path.exists(model_path):
            logger.info(f"Model gevonden op pad: {model_path}")
            interpreter = tf.lite.Interpreter(model_path=model_path)
            interpreter.allocate_tensors()
            model = interpreter  # Store the interpreter as your model
            logger.info("LSTM model succesvol geladen")
            return True
        else:
            logger.warning(f"Geen model gevonden op pad: {model_path}.")
            return False
    except Exception as e:
        logger.error(f"Fout bij het laden van het model: {e}")
        return False

def preprocess_data(historical_data=None):
    if historical_data is None or len(historical_data) < 144:
        logger.info("Geen historische data ontvangen, genereren van synthetische data")
        t = np.linspace(0, 2*np.pi, 144)
        synthetic_data = 0.5 + 0.5 * np.sin(t)
        model_input = synthetic_data.reshape(1, 144, 1)  # Vorm: (1, 144, 1)
    else:
        # Gebruik de laatste 144 waarden (12 minuten aan 5s samples)
        logger.info(f"Preprocessing van {len(historical_data)} datapunten")
        recent_data = historical_data[-144:]
        model_input = np.array(recent_data).reshape(1, 144, 1)  # Vorm: (1, 144, 1)
    
    return model_input

def generate_prediction(historical_data=None):
    """
    Genereer een voorspelling voor de komende uur met 5-seconden granulariteit.
    """
    if model is None:
        success = load_model()
        if not success:
            logger.error("Kon model niet laden, fallback naar synthetische voorspelling")
            return generate_synthetic_prediction()
    
    try:
        model_input = preprocess_data(historical_data)
        
        # Get input and output details from the interpreter
        input_details = model.get_input_details()
        output_details = model.get_output_details()
        
        # Ensure the input is in the correct format for TFLite
        # You might need to adjust this depending on your model's expected input
        model_input = model_input.astype(np.float32)
        
        # Set the tensor data
        model.set_tensor(input_details[0]['index'], model_input)
        
        # Run the inference
        model.invoke()
        
        # Get the prediction output
        prediction = model.get_tensor(output_details[0]['index'])[0]
        
        prediction = np.clip(prediction, 0, 1)
        
        logger.info(f"Voorspelling gegenereerd met {len(prediction)} datapunten")
        return prediction.tolist()
    except Exception as e:
        logger.error(f"Fout bij het genereren van voorspelling: {e}")
        return generate_synthetic_prediction()

def generate_synthetic_prediction():
    logger.info("Genereren van een synthetische voorspelling")
    t = np.linspace(0, 2*np.pi, 720)  # 1 uur aan 5s samples = 720 punten
    prediction = 0.5 + 0.5 * np.sin(t + time.time() % (2*np.pi))
    return prediction.tolist()

@app.route('/health', methods=['GET'])
def health_check():
    if model is None:
        load_model()    
    status = "healthy" if model is not None else "unhealthy"
    return jsonify({"status": status, "timestamp": datetime.now()}), 200 if status == "healthy" else 503

@app.route('/predict', methods=['POST'])
def predict():
    """
    API endpoint voor het genereren van voorspellingen.
    
    Verwacht een JSON met:
    - historical_data: Optioneel, lijst met historische metingen
    - timestamp: Huidige timestamp
    
    Retourneert een lijst met 720 voorspellingen (1 uur aan 5s intervallen)
    """
    # Start tijd meten voor performance logging
    start_time = time.time()
    
    data = request.json
    if not data:
        logger.warning("Geen JSON data ontvangen")
        data = {}
    
    historical_data = data.get('historical_data')
    timestamp = data.get('timestamp', time.time())
    
    # Genereer de voorspelling
    prediction = generate_prediction(historical_data)
    
    # Bouw response
    response = {
        "prediction": prediction,
        "timestamp": timestamp,
        "prediction_time": datetime.now().isoformat(),
        "prediction_duration_ms": (time.time() - start_time) * 1000
    }
    
    logger.info(f"Voorspelling gemaakt in {response['prediction_duration_ms']:.2f}ms")
    return jsonify(response)

if __name__ == '__main__':
    load_model()
    
    port = int(os.environ.get('FLASK_PORT', 8000))
    logger.info(f"Prediction Service wordt gestart...")
    app.run(host='0.0.0.0', port=port)