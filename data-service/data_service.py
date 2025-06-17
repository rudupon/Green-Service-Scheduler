import os
import pandas as pd
from flask import Flask, request, jsonify
from datetime import datetime, timedelta
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger("DataService")

app = Flask(__name__)

SAMPLING_INTERVAL = int(os.environ.get('SAMPLING_INTERVAL_SECONDS', '5'))

# Dynamisch aantal samples voor 12 minuten berekenen
LOOKBACK_WINDOW_MINUTES = 12
LOOKBACK_WINDOW_SAMPLES = int((LOOKBACK_WINDOW_MINUTES * 60) / SAMPLING_INTERVAL)

logger.info(f"Data Service gestart met sampling interval: {SAMPLING_INTERVAL}s")
logger.info(f"Lookback window: {LOOKBACK_WINDOW_MINUTES} minuten = {LOOKBACK_WINDOW_SAMPLES} samples")

# Globale variabele voor dataset
df = None
DATA_FILE = os.environ.get('DATA_FILE', '/app/data/solar_data.csv')

def load_data():
    """Laad het CSV-bestand bij het opstarten"""
    global df
    try:
        logger.info(f"CSV-bestand laden: {DATA_FILE}")
        df = pd.read_csv(DATA_FILE)
        df['Datetime'] = pd.to_datetime(df['Datetime'])
        df.sort_values('Datetime', inplace=True)
        logger.info(f"CSV-bestand geladen met {len(df)} rijen")
        return True
    except Exception as e:
        logger.error(f"Fout bij laden van CSV-bestand: {e}")
        return False

@app.route('/health', methods=['GET'])
def health_check():
    """Endpoint voor gezondheidscheck"""
    if df is None:
        status = "unhealthy"
    else:
        status = "healthy"
    return jsonify({"status": status, "timestamp": datetime.now().isoformat()}), 200 if status == "healthy" else 503

@app.route('/latest', methods=['GET'])
def get_latest_metrics():
    """
    Endpoint om de laatste metingen op te vragen.
    """
    if df is None:
        return jsonify({"error": "Dataset niet geladen"}), 500
    
    # Diagnostische logging behouden
    node_name = os.environ.get("NODE_NAME", "ONBEKEND")
    logger.info("=================== DIAGNOSE ===================")
    logger.info(f"Node naam: {node_name}")
    logger.info(f"Sampling interval uit omgevingsvariabele: {os.environ.get('SAMPLING_INTERVAL_SECONDS', 'NIET GEZET')}")
    logger.info(f"SAMPLING_INTERVAL variabele in code: {SAMPLING_INTERVAL}")
    logger.info(f"Aantal rijen in dataset: {len(df)}")
    logger.info(f"Dataset tijdsrange: {df['Datetime'].min()} - {df['Datetime'].max()}")
    logger.info("===============================================")
    
    # Aantal seconden dat we moeten teruggeven (standaard 12 minuten)
    seconds = int(request.args.get('seconds', 720))
    
    # Bereken het verwachte aantal samples op basis van interval
    expected_samples = seconds // SAMPLING_INTERVAL
    
    logger.info(f"Aanvraag voor {seconds} seconden met interval {SAMPLING_INTERVAL}s, verwacht {expected_samples} samples")
    
    # Bepaal de simulatietijd
    current_time_str = request.args.get('current_time')
    if current_time_str:
        try:
            current_time = pd.to_datetime(current_time_str)
        except:
            current_time = datetime.now()
    else:
        current_time = datetime.now()
    
    # Dataset tijdsbereik
    min_date = df['Datetime'].min()
    max_date = df['Datetime'].max()
    dataset_span = (max_date - min_date).total_seconds()
    
    # Adapteer indien buiten bereik (cyclische mapping)
    if current_time < min_date or current_time > max_date:
        logger.info(f"Huidige tijd {current_time} valt buiten dataset bereik, cyclische mapping toepassen")
        
        if current_time > max_date:
            seconds_past_end = (current_time - max_date).total_seconds()
        else:
            seconds_past_end = (current_time - min_date).total_seconds() - dataset_span
            
        offset_in_dataset = seconds_past_end % dataset_span
        sim_current_time = min_date + timedelta(seconds=offset_in_dataset)
        
        logger.info(f"Huidige tijd {current_time} wordt omgezet naar gesimuleerde tijd {sim_current_time}")
        current_time = sim_current_time
    
    # Genereer tijdstippen waarop we samples willen hebben
    timestamps = []
    for i in range(expected_samples):
        # Begin met huidige tijd en ga terug in stappen van SAMPLING_INTERVAL
        sample_time = current_time - timedelta(seconds=(i * SAMPLING_INTERVAL))
        timestamps.append(sample_time)
    
    timestamps = sorted(timestamps)  # Sorteer van vroegst naar laatst
    
    # Vind voor elk tijdstip het dichtstbijzijnde datapunt in onze dataset
    result_values = []
    result_timestamps = []
    
    for target_time in timestamps:
        # Vind het dichtstbijzijnde tijdstip in onze dataset
        closest_idx = df['Datetime'].sub(target_time).abs().idxmin()
        closest_row = df.iloc[closest_idx]
        
        # Zorg voor native Python types (niet numpy types)
        result_values.append(float(closest_row['Power_Value']))
        result_timestamps.append(closest_row['Datetime'])
    
    # Log het resultaat
    logger.info(f"RESULTAAT: {len(result_values)} samples gegenereerd met interval {SAMPLING_INTERVAL}s")
    
    # Maak de response
    response = {
        "values": result_values,
        "timestamps": [ts.strftime('%Y-%m-%d %H:%M:%S') for ts in result_timestamps],
        "node_name": node_name,
        "samples": len(result_values),
        "expected_samples": expected_samples,
        "sampling_interval": SAMPLING_INTERVAL
    }
    
    return jsonify(response)

if __name__ == '__main__':
    load_data()
    port = int(os.environ.get("FLASK_PORT", 5000))
    app.run(host="0.0.0.0", port=port)