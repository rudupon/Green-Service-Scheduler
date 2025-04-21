import requests
import logging
import time

logger = logging.getLogger("ServiceClients")

class PredictionServiceClient:
    def __init__(self, host="localhost", port=8000, max_retries=3):
        self.base_url = f"http://{host}:{port}"
        self.max_retries = max_retries
        self.available = False
        self.check_service_available()
    
    def check_service_available(self):
        for attempt in range(self.max_retries):
            try:
                response = requests.get(f"{self.base_url}/health")
                if response.status_code == 200:
                    self.available = True
                    logger.info("Prediction service is bereikbaar")
                    return True
            except requests.exceptions.RequestException:
                logger.warning(f"Prediction service niet bereikbaar - poging {attempt + 1}/{self.max_retries}")
                time.sleep(5)
        
        self.available = False
        logger.error("Prediction service is niet beschikbaar na meerdere pogingen")
        return False
    
    def predict_energy(self, historical_data=None):
        if not self.available:
            logger.warning("Prediction service niet beschikbaar, gebruik fallback voorspelling")
            return self._fallback_prediction()
        
        try:
            response = requests.post(f"{self.base_url}/predict", json={
                "historical_data": historical_data,
                "timestamp": time.time()
            })
            response.raise_for_status()
            
            data = response.json()
            return data.get("prediction", [])
        except requests.exceptions.RequestException as e:
            logger.error(f"Fout bij ophalen voorspelling: {e}")
            return self._fallback_prediction()
    
    def _fallback_prediction(self):
        import numpy as np
        # Genereer een basislijn voor 1 uur (720 samples per 5 seconden)
        timestamps = np.arange(720)
        predictions = np.sin(timestamps * np.pi / 360) * 0.5 + 0.5
        return predictions.tolist()

class SchedulerServiceClient:
    def __init__(self, host="localhost", port=8001, max_retries=3):
        self.base_url = f"http://{host}:{port}"
        self.max_retries = max_retries
        self.available = False
        self.check_service_available()
    
    def check_service_available(self):
        for attempt in range(self.max_retries):
            try:
                response = requests.get(f"{self.base_url}/health")
                if response.status_code == 200:
                    self.available = True
                    logger.info("Scheduler service is bereikbaar")
                    return True
            except requests.exceptions.RequestException:
                logger.warning(f"Scheduler service niet bereikbaar - poging {attempt + 1}/{self.max_retries}")
                time.sleep(5)
        
        self.available = False
        logger.error("Scheduler service is niet beschikbaar na meerdere pogingen")
        return False
    
    def find_optimal_time(self, task_params, energy_prediction):
        if not self.available:
            logger.warning("Scheduler service niet beschikbaar, gebruik fallback scheduling")
            return self._fallback_scheduling(task_params)
        
        try:
            response = requests.post(f"{self.base_url}/schedule", json={
                "task_params": task_params,
                "energy_prediction": energy_prediction
            })
            response.raise_for_status()
            
            data = response.json()
            return data.get("optimal_time")
        except requests.exceptions.RequestException as e:
            logger.error(f"Fout bij ophalen optimale tijd: {e}")
            return self._fallback_scheduling(task_params)
    
    def _fallback_scheduling(self, task_params):
        from datetime import datetime, timedelta
        max_delay_seconds = task_params.get("max_delay", 3600)
        # Eenvoudige strategie: plan op 70% van max delay
        optimal_seconds = min(max_delay_seconds, int(max_delay_seconds * 0.7))
        optimal_time = datetime.now() + timedelta(seconds=optimal_seconds)
        return optimal_time.isoformat()