import os
import pandas as pd
from flask import Flask, request, jsonify
from datetime import datetime, timedelta
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger("DataService")

app = Flask(__name__)

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
    Endpoint om de laatste metingen op te vragen met een flexibele tijdsduur.
    Ondersteunt simulatie door data "cyclisch" te maken over seizoenen heen.
    
    Query parameters:
    - seconds: Aantal seconden om terug te kijken (standaard 720 = 12 minuten)
    - current_time: Optioneel, huidige tijd voor simulatie (standaard: echte huidige tijd)
    
    Returns:
    - JSON met waarden en timestamps
    """
    if df is None:
        return jsonify({"error": "Dataset niet geladen"}), 500
    
    # Parameters uit query string
    seconds = int(request.args.get('seconds', 720))
    
    # Bepaal de simulatietijd
    current_time_str = request.args.get('current_time')
    if current_time_str:
        try:
            current_time = pd.to_datetime(current_time_str)
        except:
            current_time = datetime.now()
    else:
        # Gebruik de echte huidige tijd
        current_time = datetime.now()
    
    # Dataset tijdsbereik
    min_date = df['Datetime'].min()
    max_date = df['Datetime'].max()
    dataset_span = (max_date - min_date).total_seconds()
    
    # Adapteer indien buiten bereik (bijvoorbeeld in oktober)
    if current_time < min_date or current_time > max_date:
        logger.info(f"Huidige tijd {current_time} valt buiten dataset bereik, cyclische mapping toepassen")
        
        # Bereken hoeveel tijd we voorbij het einde of voor het begin zijn
        if current_time > max_date:
            seconds_past_end = (current_time - max_date).total_seconds()
        else:
            seconds_past_end = (current_time - min_date).total_seconds() - dataset_span
            
        # Modulo berekening om cyclisch binnen het dataset bereik te komen
        offset_in_dataset = seconds_past_end % dataset_span
        
        # Nieuwe gesimuleerde 'huidige tijd' binnen het dataset bereik
        sim_current_time = min_date + timedelta(seconds=offset_in_dataset)
        
        logger.info(f"Huidige tijd {current_time} wordt omgezet naar gesimuleerde tijd {sim_current_time}")
        current_time = sim_current_time
    
    # Bereken het startmoment
    start_time = current_time - timedelta(seconds=seconds)
    
    # Probleem: start_time kan vóór min_date liggen
    # Oplossing: split de query indien nodig in twee delen
    results = []
    
    if start_time < min_date:
        # Deel 1: Vanaf min_date tot current_time
        filtered_data1 = df[(df['Datetime'] >= min_date) & (df['Datetime'] <= current_time)]
        
        # Deel 2: Van het einde van de dataset, de resterende tijd
        remaining_seconds = (min_date - start_time).total_seconds()
        end_part_start = max_date - timedelta(seconds=remaining_seconds)
        filtered_data2 = df[df['Datetime'] >= end_part_start]
        
        # Combineer (eerst deel 2, dan deel 1 voor chronologische volgorde)
        results = pd.concat([filtered_data2, filtered_data1])
    else:
        # Normale situatie: alles binnen het dataset bereik
        results = df[(df['Datetime'] >= start_time) & (df['Datetime'] <= current_time)]
    
    if results.empty:
        logger.warning(f"Geen data gevonden voor periode {start_time} tot {current_time}")
        return jsonify({"error": "Geen data gevonden voor opgegeven periode"}), 404
    
    # Structureer de data als een lijst met waarden
    response = {
        "values": results['Power_Value'].tolist(),
        "timestamps": results['Datetime'].dt.strftime('%Y-%m-%d %H:%M:%S').tolist(),
        "start_time": start_time.isoformat(),
        "current_time": current_time.isoformat(),
        "real_time": datetime.now().isoformat(),
        "samples": len(results)
    }
    
    logger.info(f"Laatste metingen opgehaald: {len(results)} samples")
    return jsonify(response)

if __name__ == '__main__':
    load_data()
    port = int(os.environ.get("FLASK_PORT", 5000))
    app.run(host="0.0.0.0", port=port)