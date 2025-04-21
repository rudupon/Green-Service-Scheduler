import os
from flask import Flask, request, jsonify
import logging
import numpy as np
from datetime import datetime, timedelta

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger("SchedulerService")

app = Flask(__name__)

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
        return jsonify({"error", "Missende task_params of energy_prediction"}), 400

    try:
        optimal_time = find_optimal_execution_time(task_params, energy_prediction)
        return jsonify({"optimal_time": optimal_time}), 200
    except Exception as e:
        logger.error(f"Fout bij bepalen van optimale uitvoeringstijd: {e}")
        return jsonify({"error": str(e)}), 500

def find_optimal_execution_time(task_params, energy_prediction):
    """
    Algoritme dat het optimale uitvoeringsmoment bepaalt.
    
    Parameters:
    - task_params: Dict met taakparameters (energy_requirement, priority, max_delay, duration)
    - energy_prediction: List met voorspelde energiebeschikbaarheid (1 uur vooruit, 5 seconden granulariteit)
    
    Returns:
    - ISO-geformatteerde string met optimale uitvoeringstijd
    """
    energy_requirement = float(task_params.get('energy_requirement', 1.0))
    priority = int(task_params.get('priority', 1))
    max_delay_seconds = int(task_params.get('max_delay', 3600))  # standaard max 1 uur uitstel
    duration_seconds = int(task_params.get('duration', 100))     # standaard 100 seconden durende taak
    
    # Bereken hoeveel datapunten nodig zijn voor de taakduur (bij 5s granulariteit)
    duration_samples = duration_seconds // 5
    energy_array = np.array(energy_prediction)
    
    # Bereken de optimale startindex door te kijken naar gemiddelde energie over taakduur
    best_score = -float('inf')
    best_start_index = 0
    
    # Max aantal stappen dat we vooruit kunnen kijken (beperkt door max_delay en voorspellingslengte)
    max_steps = min(max_delay_seconds // 5, len(energy_array) - duration_samples)
    
    for start_idx in range(max_steps):
        window = energy_array[start_idx:start_idx + duration_samples]
        
        # Bereken score gebaseerd op gemiddelde energie, prioriteit en wachttijd
        # Hogere prioriteit → meer belang aan direct uitvoeren (minder uitstel)
        avg_energy = np.mean(window)
        delay_penalty = (start_idx * 5) / max_delay_seconds  # Genormaliseerde vertraging (0-1)
        
        # Score is een combinatie van energiebeschikbaarheid en urgentie
        # Bij hoge prioriteit weegt delay_penalty zwaarder
        priority_factor = priority / 10  # Normaliseer prioriteit (prioriteit is 1-10)
        score = avg_energy - (delay_penalty * priority_factor)
        
        if score > best_score:
            best_score = score
            best_start_index = start_idx
    
    # Bereken optimale tijd (huidige tijd + beste start in seconden)
    optimal_seconds = best_start_index * 5
    optimal_time = datetime.now() + timedelta(seconds=optimal_seconds)
    
    logger.info(f"Optimale tijd voor taak: {optimal_time.isoformat()}, score: {best_score:.4f}")
    
    return optimal_time.isoformat()

if __name__ == "__main__":
    logger.info("Scheduler Service wordt gestart...")
    port = int(os.environ.get("FLASK_PORT", 8001))
    app.run(host="0.0.0.0", port=port)