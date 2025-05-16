import os
import time
import logging
import threading
import json
from datetime import datetime, timedelta
from kubernetes import client, config, watch
from kubernetes.client.rest import ApiException
from service_clients import PredictionServiceClient, SchedulerServiceClient
from flask import Flask, request, jsonify
import requests

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger("NodeController")

class NodeController:
    def __init__(self):
        config.load_incluster_config()
        self.core_api = client.CoreV1Api()
        self.batch_api = client.BatchV1Api()
        
        # Huidige node naam ophalen uit environment
        self.node_name = os.environ.get("NODE_NAME")
        if not self.node_name:
            raise ValueError("NODE_NAME environment variable moet ingesteld zijn")
        
        self.namespace = os.environ.get("NAMESPACE", "edge-computing")
        self.controller_port = int(os.environ.get("CONTROLLER_PORT", "8002"))
        
        logger.info(f"Node Controller gestart op node: {self.node_name}")
        
        self.task_queue = []
        self.timer_threads = {}
        
        self.prediction_client = PredictionServiceClient()
        self.scheduler_client = SchedulerServiceClient()

        # Flask app voor HTTP server
        self.app = Flask(__name__)
        self._setup_routes()

    def _setup_routes(self):
        """Configureer de Flask routes"""
        
        @self.app.route('/health', methods=['GET'])
        def health_check():
            return jsonify({
                "status": "healthy", 
                "node": self.node_name
            }), 200
        
        @self.app.route('/api/prediction', methods=['POST'])
        def handle_prediction_request():
            data = request.json
            if not data:
                return jsonify({"error": "Geen data ontvangen"}), 400
            
            # Haal taakparameters op
            task_params = {
                "energy_requirement": float(data.get("energy_requirement", 1.0)),
                "priority": int(data.get("priority", 1)),
                "max_delay": int(data.get("max_delay", 3600)),
                "duration": int(data.get("duration", 100)),
            }
            
            try:
                # Maak voorspelling met lokale prediction service
                energy_prediction = self.prediction_client.predict_energy()
                optimal_time_str, score = self.scheduler_client.find_optimal_time(task_params, energy_prediction)
                
                return jsonify({
                    "prediction": energy_prediction,
                    "optimal_time": optimal_time_str,
                    "score": score,
                    "node": self.node_name
                })
            except Exception as e:
                logger.error(f"Fout bij maken voorspelling: {e}")
                return jsonify({"error": str(e)}), 500
    
    def _start_flask_server(self):
        """Start de Flask server in een aparte thread"""
        logger.info(f"Flask server wordt gestart op poort {self.controller_port}")
        self.app.run(host='0.0.0.0', port=self.controller_port)
    
    def start(self):
        logger.info("Controller wordt gestart")
        
        threading.Thread(target=self.check_services_availability, daemon=True).start()
        
        threading.Thread(target=self.check_jobs_periodically, daemon=True).start()
        
        threading.Thread(target=self._start_flask_server, daemon=True).start()
        
        # Blijf draaien
        try:
            while True:
                time.sleep(10)
        except KeyboardInterrupt:
            logger.info("Controller wordt gestopt")

    
    def check_services_availability(self):
        while True:
            if not self.prediction_client.available:
                self.prediction_client.check_service_available()
            if not self.scheduler_client.available:
                self.scheduler_client.check_service_available()
            time.sleep(60)  # Check elke minuut
    
    def check_jobs_periodically(self):
        while True:
            try:
                self.check_for_new_tasks()
            except Exception as e:
                logger.error(f"Fout bij controleren van nieuwe taken: {e}")
            
            time.sleep(5)
    
    def check_for_new_tasks(self):
        logger.info(f"Controleren op nieuwe taken voor node {self.node_name}")
    
        jobs = self.batch_api.list_namespaced_job(
            namespace="edge-computing",
            label_selector=f"type=green-task,status=pending-scheduling,target-node={self.node_name}"
        )
        
        for job in jobs.items:
            try:
                self.process_task_job(job)
            except Exception as e:
                logger.error(f"Fout bij verwerken van Job {job.metadata.name}: {e}")
    
    def process_task_job(self, job):
        name = job.metadata.name
        namespace = job.metadata.namespace
        logger.info(f"Verwerken van taak Job: {name} in namespace {namespace}")
        
        # Haal parameters uit annotaties
        annotations = job.metadata.annotations if job.metadata.annotations else {}
        
        task_params = {
            "name": name,
            "namespace": namespace,
            "energy_requirement": float(annotations.get("energy-requirement", "1.0")),
            "priority": int(annotations.get("priority", "1")),
            "max_delay": int(annotations.get("max-delay", "3600")),
            "duration": int(annotations.get("duration", "100")),
            "created_at": datetime.now().isoformat(),
            "can_migrate": job.metadata.labels.get("can-migrate", "false").lower() == "true"
        }
        
        # Markeer job als 'in behandeling'
        self.mark_job_as_processing(name, namespace)
        
        # Plan de taak in
        self.schedule_task(task_params, job)
    
    def mark_job_as_processing(self, name, namespace):
        try:
            job = self.batch_api.read_namespaced_job(name, namespace)
            
            if not job.metadata.labels:
                job.metadata.labels = {}
            job.metadata.labels["status"] = "processing"
            
            self.batch_api.patch_namespaced_job(
                name=name,
                namespace=namespace,
                body={"metadata": {"labels": job.metadata.labels}}
            )
            logger.info(f"Job {name} in namespace {namespace} gemarkeerd als in verwerking")
        except ApiException as e:
            logger.error(f"Fout bij markeren Job als in verwerking: {e}")
    
    def schedule_task(self, task_params, original_job=None):
        logger.info(f"Planning van taak: {task_params['name']}")
        self.task_queue.append(task_params)
        
        # Controleer of de taak kan migreren
        if task_params.get('can_migrate', False):
            best_node, optimal_time_str = self.find_best_node_for_task(task_params)
        else:
            # Als migratie niet is toegestaan, gebruik alleen deze node
            energy_prediction = self.prediction_client.predict_energy()
            optimal_time_str, _ = self.scheduler_client.find_optimal_time(task_params, energy_prediction)
            best_node = self.node_name
        
        try:
            optimal_time = datetime.fromisoformat(optimal_time_str)
        except ValueError:
            logger.error(f"Ongeldige tijdformaat ontvangen: {optimal_time_str}")
            optimal_time = datetime.now() + timedelta(seconds=60)  # Fallback naar 1 minuut in de toekomst
        
        # Bereken vertraging tot optimaal moment
        now = datetime.now()
        if optimal_time > now:
            delay_seconds = (optimal_time - now).total_seconds()
        else:
            delay_seconds = 0
        
        logger.info(f"Taak {task_params['name']} ingepland voor uitvoering over {delay_seconds} seconden op node {best_node}")
        
        # Start timer voor taakuitvoering
        timer_thread = threading.Timer(delay_seconds, self.execute_task, args=[task_params, best_node, original_job])
        timer_thread.daemon = True
        timer_thread.start()
        
        self.timer_threads[task_params['name']] = timer_thread

    def find_best_node_for_task(self, task_params):
        """
        Zoekt de beste node voor een taak door voorspellingen van alle nodes te vergelijken.
        
        Returns:
            tuple: (beste_node_naam, optimaal_tijdstip)
        """
        logger.info(f"Zoeken naar beste node voor taak: {task_params['name']}")
    
        # Haal alle nodes op
        try:
            nodes = self.get_cluster_nodes()
        except Exception as e:
            logger.error(f"Fout bij ophalen cluster nodes: {e}")
            nodes = [self.node_name]  # Fallback naar alleen huidige node
        
        # Initialiseer met huidige node (lokaal)
        best_node = self.node_name
        
        # Lokale voorspelling ophalen
        local_prediction = self.prediction_client.predict_energy()
        
        # Optimale tijd op deze node bepalen
        local_time_str, local_score = self.scheduler_client.find_optimal_time(task_params, local_prediction)
        
        # Converteer string naar datetime object
        try:
            best_time = datetime.fromisoformat(local_time_str)
        except ValueError:
            logger.error(f"Ongeldige tijdformaat ontvangen: {local_time_str}")
            best_time = datetime.now() + timedelta(seconds=60)  # Fallback
        
        # Voor gemakkelijke logging
        best_time_str = local_time_str
        best_score = local_score
        
        # Voor elke andere node, voorspellingen opvragen via HTTP
        for node in nodes:
            if node == self.node_name:
                continue  # Skip huidige node, die hebben we al
            
            try:
                # Voorspelling opvragen van andere node via HTTP
                node_prediction, node_time_str, node_score = self.get_prediction_from_node(node, task_params)
                
                # Converteer naar datetime-object voor vergelijking
                try:
                    node_time = datetime.fromisoformat(node_time_str)
                except ValueError:
                    logger.error(f"Ongeldige tijdformaat ontvangen van node {node}: {node_time_str}")
                    continue  # Skip deze node bij ongeldige tijd
                
                logger.info(f"Voorspelling van node {node}: tijd={node_time_str}, score={node_score}")
                
                # Vergelijk scores om de beste node te bepalen
                if node_score > best_score:
                    best_node = node
                    best_time = node_time
                    best_time_str = node_time_str
                    best_score = node_score
            except Exception as e:
                logger.error(f"Fout bij opvragen voorspelling van node {node}: {e}")
        
        logger.info(f"Beste node voor taak {task_params['name']}: {best_node} (starttijd: {best_time_str}, score: {best_score})")
        return best_node, best_time_str

    def get_cluster_nodes(self):
        """Haalt alle worker nodes in het cluster op"""
        try:
            nodes_list = self.core_api.list_node()
            
            # Filter nodes (optioneel: bijvoorbeeld alleen nodes met bepaalde labels)
            nodes = []
            for node in nodes_list.items:
                # Skip control-plane nodes
                if 'node-role.kubernetes.io/control-plane' not in node.metadata.labels:
                    nodes.append(node.metadata.name)
                    
            return nodes
        except ApiException as e:
            logger.error(f"Fout bij ophalen cluster nodes: {e}")
            # Fallback: alleen de huidige node
            return [self.node_name]

    def get_prediction_from_node(self, node_name, task_params):
        """
        Vraagt een voorspelling op van een specifieke node via HTTP.
        """
        logger.info(f"Voorspelling opvragen van node {node_name}")
        
        try:
            # Vind de pod die op deze node draait
            pods = self.core_api.list_namespaced_pod(
                namespace=self.namespace,
                label_selector="app=edge-node-services",
                field_selector=f"spec.nodeName={node_name}"
            )
            
            if not pods.items:
                logger.error(f"Geen pod gevonden op node {node_name}")
                raise ValueError(f"Geen pod gevonden op node {node_name}")
            
            pod_ip = pods.items[0].status.pod_ip
            
            # Directe verbinding naar pod IP
            service_url = f"http://{pod_ip}:{self.controller_port}/api/prediction"
            
            logger.info(f"HTTP-verzoek naar {service_url}")
            
            # Stuur HTTP-verzoek
            response = requests.post(service_url, json=task_params, timeout=30)
            response.raise_for_status()
            
            # Verwerk antwoord
            data = response.json()
            logger.info(f"Voorspelling ontvangen van node {node_name}")
            
            return data.get('prediction', []), data.get('optimal_time', ''), float(data.get('score', 0))
        except requests.exceptions.RequestException as e:
            logger.error(f"Fout bij opvragen voorspelling van node {node_name}: {e}")
            raise TimeoutError(f"Geen antwoord van node {node_name}: {str(e)}")
    
    def execute_task(self, task_params, target_node, original_job=None):
        name = task_params["name"]
        namespace = task_params["namespace"]
        logger.info(f"Uitvoeren van taak: {name} op node: {target_node}")
        
        try:
            # Verwijder taak uit queue
            self.task_queue = [t for t in self.task_queue if t["name"] != name]
            if name in self.timer_threads:
                del self.timer_threads[name]
            
            # Bij een originele job, pas deze aan om uitvoering te starten
            if original_job:
                self.activate_job(original_job, target_node)
            else:
                # Fallback naar het oude systeem als er geen originele job is
                job = self.create_job_object(task_params, target_node)
                self.batch_api.create_namespaced_job(
                    namespace=namespace,
                    body=job
                )
            
            logger.info(f"Job geactiveerd voor taak {name} in namespace {namespace} op node {target_node}")
        except ApiException as e:
            logger.error(f"Fout bij activeren van Job voor taak {name}: {e}")
    
    def activate_job(self, job, target_node):
        name = job.metadata.name
        namespace = job.metadata.namespace
        
        try:
            # Verwijder 'suspend: true' om uitvoering te starten
            job_patch = {
                "spec": {
                    "suspend": False
                },
                "metadata": {
                    "labels": {
                        "status": "scheduled"
                    }
                }
            }
            
            # Als de target_node verschilt van de oorspronkelijke, pas node selector aan
            if 'target-node' in job.metadata.labels and job.metadata.labels['target-node'] != target_node:
                if not 'template' in job_patch['spec']:
                    job_patch['spec']['template'] = {}
                if not 'spec' in job_patch['spec']['template']:
                    job_patch['spec']['template']['spec'] = {}
                
                job_patch['spec']['template']['spec']['nodeSelector'] = {
                    "kubernetes.io/hostname": target_node
                }
            
            # Patch de job
            self.batch_api.patch_namespaced_job(
                name=name,
                namespace=namespace,
                body=job_patch
            )
        except ApiException as e:
            logger.error(f"Fout bij activeren van Job {name}: {e}")

    def create_job_object(self, task_params, target_node):
        job_name = f"{task_params['name']}-{int(time.time())}"
    
        container = client.V1Container(
            name="task-container",
            image="gitlab.stud.atlantis.ugent.be:5050/rdupon/mp/dummy-task:latest",
            env=[
                client.V1EnvVar(name="TASK_NAME", value=task_params['name']),
                client.V1EnvVar(name="ENERGY_REQUIREMENT", value=str(task_params['energy_requirement'])),
                client.V1EnvVar(name="PRIORITY", value=str(task_params['priority'])),
                client.V1EnvVar(name="MAX_DELAY", value=str(task_params['max_delay'])),
                client.V1EnvVar(name="DURATION", value=str(task_params['duration']))
            ]
        )
        
        template = client.V1PodTemplateSpec(
            metadata=client.V1ObjectMeta(labels={"job-name": job_name}),
            spec=client.V1PodSpec(
                restart_policy="Never",
                containers=[container],
                node_selector={"kubernetes.io/hostname": target_node},
                image_pull_secrets=[client.V1LocalObjectReference(name="gitlab-ugent-registry")]
            )
        )
        
        spec = client.V1JobSpec(
            template=template,
            backoff_limit=0
        )
        
        job = client.V1Job(
            api_version="batch/v1",
            kind="Job",
            metadata=client.V1ObjectMeta(name=job_name),
            spec=spec
        )
        
        return job


def main():
    controller = NodeController()
    controller.start()


if __name__ == "__main__":
    main()