import os
import time
import logging
import threading
import json
from datetime import datetime, timedelta
from kubernetes import client, config, watch
from kubernetes.client.rest import ApiException
from service_clients import PredictionServiceClient, SchedulerServiceClient

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
        
        logger.info(f"Node Controller gestart op node: {self.node_name}")
        
        self.task_queue = []
        self.timer_threads = {}
        
        self.prediction_client = PredictionServiceClient()
        self.scheduler_client = SchedulerServiceClient()
    
    def start(self):
        logger.info("Controller wordt gestart")
        
        threading.Thread(target=self.check_services_availability, daemon=True).start()
        
        threading.Thread(target=self.check_configmaps_periodically, daemon=True).start()
        
        threading.Thread(target=self.check_prediction_requests_periodically, daemon=True).start()
        
        # Blijf draaien
        try:
            while True:
                time.sleep(10)
        except KeyboardInterrupt:
            logger.info("Controller wordt gestopt")

    def check_prediction_requests_periodically(self):
        while True:
            try:
                self.check_for_prediction_requests()
            except Exception as e:
                logger.error(f"Fout bij controleren van prediction requests: {e}")
            
            time.sleep(5)  # Elke 5 seconden controleren
    
    def check_services_availability(self):
        while True:
            if not self.prediction_client.available:
                self.prediction_client.check_service_available()
            if not self.scheduler_client.available:
                self.scheduler_client.check_service_available()
            time.sleep(60)  # Check elke minuut
    
    def check_configmaps_periodically(self):
        while True:
            try:
                self.check_for_new_tasks()
            except Exception as e:
                logger.error(f"Fout bij controleren van nieuwe taken: {e}")
            
            time.sleep(5)
    
    def check_for_new_tasks(self):
        logger.info(f"Controleren op nieuwe taken voor node {self.node_name}")
    
        # Haal ConfigMaps op met label selector voor deze node, maar sluit voorspellingsverzoeken expliciet uit
        config_maps = self.core_api.list_namespaced_config_map(
            namespace="edge-computing",
            label_selector=f"target-node={self.node_name},processed=false,request-type!=prediction"
        )
        
        for config_map in config_maps.items:
            try:
                self.process_task_configmap(config_map)
            except Exception as e:
                logger.error(f"Fout bij verwerken van ConfigMap {config_map.metadata.name}: {e}")
    
    def process_task_configmap(self, config_map):
        name = config_map.metadata.name
        namespace = config_map.metadata.namespace
        logger.info(f"Verwerken van taak ConfigMap: {name} in namespace {namespace}")
        
        # Haal taakparameters op
        task_params = {
            "name": name,
            "namespace": namespace,
            "energy_requirement": float(config_map.data.get("energy_requirement", "1.0")),
            "priority": int(config_map.data.get("priority", "1")),
            "max_delay": int(config_map.data.get("max_delay", "3600")),
            "duration": int(config_map.data.get("duration", "100")),
            "created_at": datetime.now().isoformat(),
            "can_migrate": config_map.data.get("can_migrate", "false").lower() == "true"
        }
        self.mark_configmap_as_processed(name, namespace)
        self.schedule_task(task_params)
    
    def mark_configmap_as_processed(self, name, namespace):
        try:
            config_map = self.core_api.read_namespaced_config_map(name, namespace)
            
            if not config_map.metadata.labels:
                config_map.metadata.labels = {}
            config_map.metadata.labels["processed"] = "true"
            
            self.core_api.patch_namespaced_config_map(
                name=name,
                namespace=namespace,
                body=config_map
            )
            logger.info(f"ConfigMap {name} in namespace {namespace} gemarkeerd als verwerkt")
        except ApiException as e:
            logger.error(f"Fout bij markeren ConfigMap als verwerkt: {e}")
    
    def schedule_task(self, task_params):
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
        timer_thread = threading.Timer(delay_seconds, self.execute_task, args=[task_params, best_node])
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
        
        # Looptijd voor gemakkelijke logging
        best_time_str = local_time_str
        
        # Voor elke andere node, voorspellingen opvragen via Control Plane
        for node in nodes:
            if node == self.node_name:
                continue  # Skip huidige node, die hebben we al
            
            try:
                # Voorspelling opvragen van andere node
                node_prediction, node_time_str, node_score = self.get_prediction_from_node(node, task_params)
                
                # Converteer naar datetime-object voor vergelijking
                try:
                    node_time = datetime.fromisoformat(node_time_str)
                except ValueError:
                    logger.error(f"Ongeldige tijdformaat ontvangen van node {node}: {node_time_str}")
                    continue  # Skip deze node bij ongeldige tijd
                
                logger.info(f"Voorspelling van node {node}: tijd={node_time_str}, score={node_score}")
                
                # Vergelijk op basis van absolute tijd (vroegste tijd wint)
                if node_time < best_time:
                    best_node = node
                    best_time = node_time
                    best_time_str = node_time_str
            except Exception as e:
                logger.error(f"Fout bij opvragen voorspelling van node {node}: {e}")
        
        logger.info(f"Beste node voor taak {task_params['name']}: {best_node} (starttijd: {best_time_str})")
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
        Vraagt een voorspelling op van een specifieke node via het Control Plane.
        Deze functie maakt een tijdelijke ConfigMap aan om de vraag te stellen,
        en wacht vervolgens op een antwoord via een andere ConfigMap.
        
        Returns:
            tuple: (voorspelling, optimale_tijd, score)
        """
        logger.info(f"Voorspelling opvragen van node {node_name}")
    
        task_short_name = task_params['name'][:8].rstrip('-')  # Eerste 8 karakters
        node_short_name = node_name.split('-')[-1][:8].rstrip('-')  # Laatste component, eerste 8 tekens
        timestamp = str(int(time.time()) % 10000)  # Laatste 4 cijfers van timestamp
        
        # Kortere, unieke namen genereren
        request_name = f"req-{task_short_name}-{node_short_name}-{timestamp}"
        response_name = f"resp-{task_short_name}-{node_short_name}-{timestamp}"
        
        # Controleer dat de naam geldig is (max 63 tekens)
        if len(response_name) > 63:
            logger.warning(f"Response naam te lang ({len(response_name)} tekens), zal worden ingekort")
            response_name = response_name[:63]
        
        # ConfigMap aanmaken voor vraag
        request_cm = {
            'apiVersion': 'v1',
            'kind': 'ConfigMap',
            'metadata': {
                'name': request_name,
                'namespace': task_params['namespace'],
                'labels': {
                    'target-node': node_name,
                    'request-type': 'prediction',
                    'response-name': response_name,
                    'processed': 'false'
                }
            },
            'data': {
                'energy_requirement': str(task_params['energy_requirement']),
                'priority': str(task_params['priority']),
                'max_delay': str(task_params['max_delay']),
                'duration': str(task_params['duration']),
            }
        }
        
        # Vraag ConfigMap aanmaken
        try:
            self.core_api.create_namespaced_config_map(
                namespace=task_params['namespace'],
                body=request_cm
            )
        except ApiException as e:
            logger.error(f"Fout bij aanmaken prediction request ConfigMap: {e}")
            raise
        
        # Wacht op antwoord ConfigMap (met timeout)
        timeout = 30  # seconden
        start_time = time.time()
        
        while time.time() - start_time < timeout:
            try:
                # Check of response ConfigMap bestaat
                response_cm = self.core_api.read_namespaced_config_map(
                    name=response_name,
                    namespace=task_params['namespace']
                )
                
                # Parse response
                prediction = json.loads(response_cm.data.get('prediction', '[]'))
                optimal_time = response_cm.data.get('optimal_time', '')
                score = float(response_cm.data.get('score', '0'))
                
                # Opruimen
                try:
                    self.core_api.delete_namespaced_config_map(
                        name=response_name,
                        namespace=task_params['namespace']
                    )
                except:
                    logger.warning(f"Kon response ConfigMap {response_name} niet verwijderen")
                
                return prediction, optimal_time, score
            except ApiException as e:
                if e.status != 404:  # Anders dan Not Found
                    logger.error(f"Fout bij controleren response ConfigMap: {e}")
                    raise
                
                # Wacht even en probeer opnieuw
                time.sleep(1)
        
        # Timeout bereikt
        logger.warning(f"Timeout bij wachten op voorspelling van node {node_name}")
        raise TimeoutError(f"Geen antwoord van node {node_name} binnen {timeout} seconden")
    
    def execute_task(self, task_params, target_node):
        name = task_params["name"]
        namespace = task_params["namespace"]
        logger.info(f"Uitvoeren van taak: {name} op node: {target_node}")
        
        try:
            # Verwijder taak uit queue
            self.task_queue = [t for t in self.task_queue if t["name"] != name]
            if name in self.timer_threads:
                del self.timer_threads[name]
            
            job = self.create_job_object(task_params, target_node)
            
            self.batch_api.create_namespaced_job(
                namespace=namespace,
                body=job
            )
            
            logger.info(f"Job aangemaakt voor taak {name} in namespace {namespace} op node {target_node}")
        except ApiException as e:
            logger.error(f"Fout bij aanmaken van Job voor taak {name}: {e}")
    
    def create_job_object(self, task_params, target_node):
        job_name = f"{task_params['name']}-{int(time.time())}"
        
        container = client.V1Container(
            name="task-container",
            image="gitlab.stud.atlantis.ugent.be:5050/rdupon/mp/dummy-task:latest",
            env=[
                client.V1EnvVar(name="TASK_PARAMS", value=json.dumps(task_params))
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

    def check_for_prediction_requests(self):
        """Controleert op verzoeken van andere nodes voor voorspellingen"""
        logger.info(f"Controleren op voorspellingsverzoeken voor node {self.node_name}")
        
        config_maps = self.core_api.list_namespaced_config_map(
            namespace="edge-computing",
            label_selector=f"target-node={self.node_name},request-type=prediction,processed=false"
        )
        
        for config_map in config_maps.items:
            try:
                self.process_prediction_request(config_map)
            except Exception as e:
                logger.error(f"Fout bij verwerken van voorspellingsverzoek {config_map.metadata.name}: {e}")

    def process_prediction_request(self, config_map):
        """Verwerkt een voorspellingsverzoek van een andere node"""
        logger.info(f"Verwerken van voorspellingsverzoek: {config_map.metadata.name}")
        
        response_name = config_map.metadata.labels.get('response-name')
        if not response_name:
            logger.error(f"Geen response-name in labels van ConfigMap {config_map.metadata.name}")
            return
        
        # Controleer dat de respons-naam niet te lang is
        if len(response_name) > 63:
            logger.warning(f"Response naam te lang ({len(response_name)} tekens), zal worden ingekort")
            response_name = response_name[:63]
        
        # Taakparameters ophalen
        task_params = {
            "energy_requirement": float(config_map.data.get("energy_requirement", "1.0")),
            "priority": int(config_map.data.get("priority", "1")),
            "max_delay": int(config_map.data.get("max_delay", "3600")),
            "duration": int(config_map.data.get("duration", "100")),
        }
        
        # Markeer het verzoek als verwerkt - LET OP: We gebruiken read_namespaced_config_map om de meest recente versie te krijgen
        try:
            latest_config_map = self.core_api.read_namespaced_config_map(
                name=config_map.metadata.name,
                namespace=config_map.metadata.namespace
            )
            
            if not latest_config_map.metadata.labels:
                latest_config_map.metadata.labels = {}
            latest_config_map.metadata.labels['processed'] = 'true'
            
            self.core_api.patch_namespaced_config_map(
                name=config_map.metadata.name,
                namespace=config_map.metadata.namespace,
                body=latest_config_map
            )
        except ApiException as e:
            logger.error(f"Fout bij markeren van voorspellingsverzoek als verwerkt: {e}")
        
        # Voorspelling maken
        energy_prediction = self.prediction_client.predict_energy()
        optimal_time_str, score = self.scheduler_client.find_optimal_time(task_params, energy_prediction)
        
        # Antwoord ConfigMap aanmaken
        response_cm = {
            'apiVersion': 'v1',
            'kind': 'ConfigMap',
            'metadata': {
                'name': response_name,
                'namespace': config_map.metadata.namespace,
            },
            'data': {
                'prediction': json.dumps(energy_prediction),
                'optimal_time': optimal_time_str,
                'score': str(score)
            }
        }
        
        try:
            self.core_api.create_namespaced_config_map(
                namespace=config_map.metadata.namespace,
                body=response_cm
            )
            logger.info(f"Antwoord ConfigMap {response_name} aangemaakt")
        except ApiException as e:
            logger.error(f"Fout bij aanmaken van antwoord ConfigMap: {e}")


def main():
    controller = NodeController()
    controller.start()


if __name__ == "__main__":
    main()