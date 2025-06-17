import os
from flask import Flask, request, jsonify
import logging
import numpy as np
from datetime import datetime, timedelta

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger("SchedulerService")

app = Flask(__name__)

SAMPLING_INTERVAL = int(os.environ.get('SAMPLING_INTERVAL_SECONDS', '5'))
logger.info(f"Scheduler Service gestart met sampling interval: {SAMPLING_INTERVAL}s")

@app.route("/health", methods=["GET"])
def health_check():
    return jsonify({"status": "healthy", "timestamp": datetime.now().isoformat()}), 200

@app.route("/schedule", methods=["POST"])
def schedule_task():
    data = request.json
    if not data:
        return jsonify({"error": "Geen JSON data ontvangen"}), 400
    
    task_params = data.get("task_params")
    energy_prediction = data.get("energy_prediction")
    
    if not task_params or not energy_prediction:
        return jsonify({"error": "Missende task_params of energy_prediction"}), 400
    
    try:
        # Valideer energy_prediction
        if not isinstance(energy_prediction, list) or len(energy_prediction) == 0:
            return jsonify({"error": "energy_prediction moet een niet-lege lijst zijn"}), 400
            
        optimal_time, score = find_optimal_execution_time(task_params, energy_prediction)
        return jsonify({
            "optimal_time": optimal_time,
            "score": score
        }), 200
    except Exception as e:
        logger.error(f"Fout bij bepalen van optimale uitvoeringstijd: {e}")
        return jsonify({"error": str(e)}), 500

def find_optimal_execution_time(task_params, energy_prediction):
    """
    Algoritme dat het optimale uitvoeringsmoment bepaalt.
    
    Parameters:
    - task_params: Dict met taakparameters (energy_requirement, priority, max_delay, duration)
    - energy_prediction: List met voorspelde energiebeschikbaarheid (1 uur vooruit, dynamische granulariteit)
    
    Returns:
    - ISO-geformatteerde string met optimale uitvoeringstijd
    """
    # Haal parameters op met betere validatie
    energy_requirement = float(task_params.get('energy_requirement', 1.0))
    priority = int(task_params.get('priority', 1))
    max_delay_seconds = int(task_params.get('max_delay', 3600))  # standaard max 1 uur uitstel
    duration_seconds = max(int(task_params.get('duration', 100)), SAMPLING_INTERVAL)  # minimum 1 sample interval
    
    # Bereken hoeveel datapunten nodig zijn voor de taakduur
    duration_samples = max(1, duration_seconds // SAMPLING_INTERVAL)  # Minimum 1 sample
    energy_array = np.array(energy_prediction)
    
    # Controleer of we genoeg voorspellingen hebben
    if len(energy_array) < duration_samples:
        raise ValueError(f"Niet genoeg energie-voorspellingen voor taakduur van {duration_seconds} seconden")
    
    # Max aantal stappen dat we vooruit kunnen kijken (beperkt door max_delay en voorspellingslengte)
    max_steps = max(1, min(max_delay_seconds // SAMPLING_INTERVAL, len(energy_array) - duration_samples))
    
    # Bereken de optimale startindex door rekening te houden met energy_requirement
    best_score = -float('inf')
    best_start_index = 0
    
    for start_idx in range(max_steps):
        window = energy_array[start_idx:start_idx + duration_samples]
        
        # Controleer of de energie op elk moment boven de vereiste drempel ligt
        if np.min(window) < energy_requirement:
            continue  # Skip dit tijdsvenster als er niet genoeg energie is op enig moment
        
        # Bereken scores op een beter gebalanceerde manier
        avg_energy_ratio = np.mean(window) / energy_requirement  # Hoe hoger boven minimum, hoe beter
        delay_factor = 1.0 - (start_idx * SAMPLING_INTERVAL / max_delay_seconds)  # 1.0 bij start, aflopend naar 0.0
        
        # Bepaal gewichten op basis van prioriteit (1-10)
        priority_normalized = priority / 10.0  # Normaliseren naar 0.1-1.0
        energy_weight = 1.0 - priority_normalized  # Lager bij hoge prioriteit
        delay_weight = priority_normalized        # Hoger bij hoge prioriteit
        
        # Gewogen score (0-1 range voor beide componenten)
        score = (energy_weight * avg_energy_ratio) + (delay_weight * delay_factor)
        
        if score > best_score:
            best_score = score
            best_start_index = start_idx
    
    # Als geen geschikte tijd is gevonden (alle tijden onder energy_requirement)
    if best_score == -float('inf'):
        logger.warning("Geen optimale tijd gevonden die voldoet aan energy_requirement")
        # Val terug op beste optie, zelfs onder energy_requirement
        best_start_index, best_score = find_best_fallback(energy_array, duration_samples, max_steps, energy_requirement, priority)
    
    # Bereken optimale tijd (huidige tijd + beste start in seconden)
    optimal_seconds = best_start_index * SAMPLING_INTERVAL
    optimal_time = datetime.now() + timedelta(seconds=optimal_seconds)
    
    logger.info(f"Optimale tijd voor taak: {optimal_time.isoformat()}, score: {best_score:.4f}")
    
    return optimal_time.isoformat(), best_score

def find_best_fallback(energy_array, duration_samples, max_steps, energy_requirement, priority):
    """
    Vind de beste fallback optie wanneer geen enkel tijdvenster aan de energy_requirement voldoet.
    """
    best_start_index = 0
    best_score = -float('inf')
    
    for start_idx in range(max_steps):
        window = energy_array[start_idx:start_idx + duration_samples]
        # Bereken hoeveel % van energy_requirement we gemiddeld halen
        energy_ratio = np.mean(window) / energy_requirement
        
        # Dezelfde prioriteitslogica toepassen als in de hoofdfunctie
        priority_normalized = priority / 10.0  # Normaliseren naar 0.1-1.0
        energy_weight = 1.0 - priority_normalized  # Lager bij hoge prioriteit
        delay_factor = 1.0 - (start_idx * SAMPLING_INTERVAL / max_steps)  # 1.0 bij start, aflopend naar 0.0
        delay_weight = priority_normalized  # Hoger bij hoge prioriteit
        
        # Consistente score berekening met de hoofdfunctie
        score = (energy_weight * energy_ratio) + (delay_weight * delay_factor)
        
        if score > best_score:
            best_score = score
            best_start_index = start_idx
    
    return best_start_index, best_score

if __name__ == "__main__":
    logger.info("Scheduler Service wordt gestart...")
    port = int(os.environ.get("FLASK_PORT", 8001))
    app.run(host="0.0.0.0", port=port)
