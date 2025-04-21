import os
import time
import logging
import requests
import json
from kubernetes import client, config, watch
from datetime import datetime, timedelta

# Configureer logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("green-scheduler")

# Definieer constanten
SCHEDULER_NAME = "green-scheduler"
ENERGY_THRESHOLD = 0.6  # 60% van maximale groene energie om als "groen" te worden beschouwd
NODE_ENERGY_ENDPOINTS = {
    "home": "http://edge-node-home:8080/energy-prediction",
    "tom": "http://edge-node-tom:8080/energy-prediction",
    "ktn": "http://edge-node-ktn:8080/energy-prediction"
}

def get_energy_prediction(node_name):
    """Haal energievoorspelling op van een edge node."""
    try:
        if node_name not in NODE_ENERGY_ENDPOINTS:
            logger.warning(f"Geen energie-endpoint geconfigureerd voor node {node_name}")
            return None

        url = NODE_ENERGY_ENDPOINTS[node_name]
        response = requests.get(url, timeout=5)
        if response.status_code == 200:
            prediction = response.json()
            logger.info(f"Energievoorspelling voor {node_name}: {prediction}")
            return prediction
        else:
            logger.error(f"Fout bij ophalen energievoorspelling voor {node_name}: {response.status_code}")
            return None
    except Exception as e:
        logger.error(f"Uitzondering bij ophalen energievoorspelling: {e}")
        return None

def find_best_green_energy_node():
    """Vind de node met de beste groene energie voorspelling."""
    best_node = None
    highest_score = 0
    
    for node_name in NODE_ENERGY_ENDPOINTS.keys():
        prediction = get_energy_prediction(node_name)
        if prediction and 'energy_score' in prediction:
            if prediction['energy_score'] > highest_score:
                highest_score = prediction['energy_score']
                best_node = node_name
    
    logger.info(f"Beste node: {best_node} met score {highest_score}")
    return best_node, highest_score

def is_task_critical(priority):
    """Bepaal of een taak kritisch is op basis van prioriteitslabel."""
    return priority.lower() == 'critical'

def is_enough_green_energy(energy_score):
    """Controleer of er genoeg groene energie is."""
    return energy_score >= ENERGY_THRESHOLD

def schedule_pod(pod):
    """Schedule een pod op basis van energievoorspellingen en metadata."""
    try:
        # Lees node selector voor edge locatie
        if not pod.spec.node_selector or 'edge-location' not in pod.spec.node_selector:
            logger.error(f"Pod {pod.metadata.name} heeft geen edge-location in nodeSelector")
            return False
        
        node_name = pod.spec.node_selector['edge-location']
        logger.info(f"Pod {pod.metadata.name} is gebonden aan node {node_name}")
        
        # Lees labels en annotaties
        labels = pod.metadata.labels or {}
        annotations = pod.metadata.annotations or {}
        
        # Haal parameters uit labels en annotaties
        priority = labels.get('priority', 'normal')
        energy_requirement = labels.get('energy-requirement', 'medium')
        max_delay_seconds = int(annotations.get('max-delay', '3600'))
        min_green_energy = float(annotations.get('min-green-energy', '0.5'))
        
        logger.info(f"Taakparameters: priority={priority}, energy_requirement={energy_requirement}, "
                   f"max_delay={max_delay_seconds}s, min_green_energy={min_green_energy}")
        
        # Haal energievoorspelling op voor deze node
        prediction = get_energy_prediction(node_name)
        if not prediction:
            logger.warning(f"Geen energievoorspelling beschikbaar voor {node_name}, direct schedulen")
            return schedule_now(pod, node_name)
        
        # Bepaal of taak kritisch is
        if is_task_critical(priority):
            logger.info(f"Kritieke taak {pod.metadata.name} direct schedulen op {node_name}")
            return schedule_now(pod, node_name)
            
        # Vind optimale uitvoeringstijd
        best_time_index, energy_score = find_optimal_execution_time(
            prediction, 
            energy_requirement,
            max_delay_seconds
        )
        
        logger.info(f"Beste uitvoeringstijd voor {pod.metadata.name}: "
                   f"index {best_time_index} ({best_time_index*5}s), score {energy_score}")
        
        # Controleer of er voldoende groene energie is op het beste moment
        if energy_score >= min_green_energy:
            if best_time_index == 0:
                # Direct uitvoeren als nu het beste moment is
                logger.info(f"Nu is het optimale moment voor {pod.metadata.name}, direct schedulen")
                return schedule_now(pod, node_name)
            else:
                # Uitstellen tot het optimale moment
                logger.info(f"Uitstellen van {pod.metadata.name} tot optimaal moment "
                           f"(over {best_time_index*5} seconden)")
                return delay_pod_execution(pod, node_name, best_time_index * 5)
        else:
            # Onvoldoende groene energie beschikbaar, zelfs op beste moment
            logger.warning(f"Onvoldoende groene energie voor {pod.metadata.name}, "
                          f"beste score {energy_score} < minimaal vereiste {min_green_energy}")
            
            if max_delay_seconds > 0:
                # Als uitstel is toegestaan, in wachtrij plaatsen voor latere controle
                logger.info(f"Taak {pod.metadata.name} in wachtrij plaatsen voor later")
                return add_to_delay_queue(pod)
            else:
                # Anders toch uitvoeren
                logger.info(f"Geen uitstel mogelijk voor {pod.metadata.name}, toch uitvoeren")
                return schedule_now(pod, node_name)
    
    except Exception as e:
        logger.error(f"Fout bij scheduling van pod {pod.metadata.name}: {e}")
        return False

# Global variable voor uitgestelde taken
delayed_tasks = {}

def delay_pod_execution(pod, node_name, delay_seconds):
    """Stel de uitvoering van een pod uit met een timer."""
    try:
        pod_key = f"{pod.metadata.namespace}/{pod.metadata.name}"
        execution_time = time.time() + delay_seconds
        
        delayed_tasks[pod_key] = {
            'pod': pod,
            'node': node_name,
            'execution_time': execution_time
        }
        
        logger.info(f"Taak {pod_key} ingepland voor uitvoering om {time.ctime(execution_time)}")
        return True
    except Exception as e:
        logger.error(f"Fout bij uitstellen van pod {pod.metadata.name}: {e}")
        return False

def schedule_now(pod, node_name):
    """Wijs pod direct toe aan een node."""
    try:
        # Maak een binding object
        target = client.V1ObjectReference(
            api_version="v1",
            kind="Node",
            name=node_name
        )
        
        meta = client.V1ObjectMeta(name=pod.metadata.name)
        body = client.V1Binding(
            metadata=meta,
            target=target
        )
        
        # Binding API call
        api = client.CoreV1Api()
        api.create_namespaced_binding(
            namespace=pod.metadata.namespace,
            body=body
        )
        
        logger.info(f"Pod {pod.metadata.name} succesvol gepland op {node_name}")
    except Exception as e:
        logger.error(f"Fout bij binding pod aan node: {e}")

def schedule_later(pod):
    """Markeer pod om later gepland te worden."""
    # In een echte implementatie zou je dit kunnen doen met:
    # 1. Een wachtlijst in een database
    # 2. Opnieuw in de wachtrij plaatsen met een timed controller
    # 3. Kubernetes CronJob voor periodieke planning
    
    # Voor deze demo houden we een eenvoudig proces aan:
    # Schedule alsnog, maar log de intentie om later te plannen
    best_node, _ = find_best_green_energy_node()
    schedule_now(pod, best_node)
    logger.info(f"Pod {pod.metadata.name} zou idealiter later gepland worden, maar wordt nu toch uitgevoerd op {best_node}")

def find_optimal_execution_time(prediction, energy_requirement, max_delay_seconds):
    """Vind het optimale tijdstip voor uitvoering binnen de voorspellingsperiode."""
    # Zet energievereiste om naar een numerieke waarde
    energy_req_map = {
        'low': 0.3,
        'medium': 0.6,
        'high': 0.9
    }
    required_energy = energy_req_map.get(energy_requirement.lower(), 0.6)
    
    # Bepaal max uitstel in voorspellingsstappen (5 seconden per stap)
    max_delay_steps = min(max_delay_seconds // 5, len(prediction['forecasts']) - 1)
    
    # Zoek naar het moment met hoogste energie binnen toegestane vertraging
    best_index = 0
    best_score = 0
    
    for i in range(max_delay_steps + 1):  # +1 om huidige tijdstip ook mee te nemen
        # Haal energiescore uit voorspelling
        current_score = prediction['forecasts'][i]
        
        # Weeg energiescore af tegen de wachttijd (kleine penalty voor wachten)
        # Dit zorgt ervoor dat we alleen uitstellen als het energievoordeel significant is
        wait_penalty = i * 0.005  # 0.5% penalty per 5 seconden wachten
        adjusted_score = current_score - wait_penalty
        
        if adjusted_score > best_score:
            best_score = adjusted_score
            best_index = i
    
    return best_index, prediction['forecasts'][best_index]

def main():
    logger.info("Green Energy Scheduler gestart!")
    
    try:
        # Voor in-cluster executie
        config.load_incluster_config()
    except:
        # Voor lokaal testen
        config.load_kube_config()
    
    # Creëer een Kubernetes API client
    v1 = client.CoreV1Api()
    scheduler_name = os.environ.get("SCHEDULER_NAME", SCHEDULER_NAME)
    
    w = watch.Watch()
    
    # Blijf luisteren naar nieuwe pods
    while True:
        try:
            # Kijk naar pods die door onze scheduler gepland moeten worden
            for event in w.stream(v1.list_pod_for_all_namespaces, timeout_seconds=60):
                pod = event['object']
                
                # Controleer of deze pod door onze scheduler moet worden gepland
                if pod.spec.scheduler_name == scheduler_name and not pod.spec.node_name:
                    logger.info(f"Nieuwe pod gevonden die gepland moet worden: {pod.metadata.name} in namespace {pod.metadata.namespace}")
                    schedule_pod(pod)
        
        except Exception as e:
            logger.error(f"Uitzondering in hoofdlus: {e}")
            time.sleep(5)

if __name__ == "__main__":
    main()