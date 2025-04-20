import os
import time
import logging
import threading
import json
from datetime import datetime, timedelta
from kubernetes import client, config, watch
from kubernetes.client.rest import ApiException


logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(messages)s')
logger = logging.getLogger("NodeController")

class NodeController:
    def __init__(self):
        config.load_incluster_config()
        self.core_api = client.CoreV1Api()
        self.batch_api = client.BatchV1Api()

        self.node_name = os.environ.get("NODE_NAME")
        if not self.node_name:
            raise ValueError("NODE_NAME environment variable niet ingesteld")
        
        logger.info(f"Node Controller gestart op node: {self.node_name}")

        self.task_queue = []
        self.timer_threads = {}

        self.lstm_model = self.initialize_lstm_model()
    
    def initialize_lstm_model(self):
        logger.info("LSTM model wordt geïnitialiseerd")
        return MockLSTMModel()

    def start(self):
        logger.info("Controller wordt gestart")

        threading.Thread(target=self.check_configmaps_periodically, daemon=True).start()

        try:
            # blijf runnen
            while True:
                time.sleep(10)
        except KeyboardInterrupt:
            logger.info("Controller wordt gestopt")

    def check_configmaps_periodically(self):
        while True:
            try:
                self.check_for_new_tasks()
            except Exception as e:
                logger.error(f"Fout bij controleren van nieuwe taken: {e}")
            
            # check om de 5 seconden
            time.sleep(5)
    
    def check_for_new_tasks(self):
        logger.info(f"Controleren op nieuwe taken voor node {self.node_name}")
        config_maps = self.core_api.list_config_map_for_all_namespaces(
            label_selector = f"target-node={self.node_name},processed=false"
        )

        for config_map in config_maps.items:
            try:
                self.process_task_configmap(config_map)
            except Exception as e:
                logger.error(f"Fout bij verwerken van CoonfigMap {config_map.metadata.name}: {e}")

    def process_task_configmap(self, config_map):
        name = config_map.metadata.name
        namespace = config_map.metadata.namespace
        logger.info(f"Verwerken van taak ConfigMap: {name} in namespace {namespace}")

        task_params = {
            "name": name,
            "namespace": namespace,
            "energy_requirement": float(config_map.data.get("energy_requirement", "1.0")),
            "priority": int(config_map.data.get("priority", "1")),
            "max_delay": int(config_map.data.get("max_delay", "3600")),
            "duration": int(config_map.data.get("duration", "100")),
            "image": string(config_map.data.get("image", "task-image"))
            "created_at": datetime.now()
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
                name = name,
                namespace = namespace,
                body = config_map
            )
            logger.info(f"ConfigMap {name} in namespace {namespace} gemarkeerd als verwerkt")
        except ApiException as e:
            logger.error(f"Fout bij markeren ConfigMap als verwerkt: {e}")

    def schedule_task(self, task_params):
        logger.info(f"Planning van taak: {task_params["name"]}")
        self.task_queue.append(task_params)

        optimal_time = self.determine_optimal_time(task_params)

        now = datetime.now()
        if optimal_time > now:
            delay_seconds = (optimal_time - now).total_seconds()
        else:
            delay_seconds = 0

        logger.info(f"Taak {task_params["name"]} ingepland voor uitvoering over {delay_seconds} seconden")

        timer_thread = threading.Timer(delay_seconds, self.execute_task, args=[task_params])
        timer_thread.daemon = True
        timer_thread.start()

        self.timer_threads[task_params["name"]] = timer_thread

    def determine_optimal_time(self, task_params)
        energy_prediction = self.lstm_model.predict_energy()

        max_delay_seconds = task_params["max_delay"]
        energy_requirement = task_params["energy_requirement"]

        start_time = datetime.now()
        end_time = start_time + timedelta(seconds=max_delay_seconds)


        # MOCK
        optimal_seconds = min(max_delay_seconds, int(max_delay_seconds * 0.7))
        optimal_time = start_time + timedelta(seconds=optimal_seconds)

        logger.info(f"Optimaal tijdstip voor taak {task_params["name"]}: {optimal_time}")

    def execute_task(self, task_params):
        name = task_params["name"]
        namespace = task_params["namespace"]
        logger.info(f"Uitvoeren van taak: {name}")

        try:
            # TODO: dit is omslachtig
            self.task_queue = [to for t in self.task_queue if t["name"] != name]
            if name in self.timer_threads:
                del self.timer_threads[name]

            job = self.create_job_object(task_params)

            self.batch_api.create_namespaced_job(
                namespace = namespace,
                body = job
            )

            logger.info(f"Job aangemaakt voor taak {name} in namespace {namespace}")
        except ApiException as e:
            logger.error(f"Fout bij aanmaken job voor taak {name}: {e}")

    def create_job_object(self, task_params):
        job_name = f"{task_params["name"]}-{int(time.time())}"

        container = client.V1Container(
            name = "task-container",
            image= task_params["image"],
            env = [client.V1EnvVar(name = "TASK_PARAMS", value = json.dumps(task_params))]
        )

        template = client.V1PodTemplateSpec(
            metadata=client.V1ObjectMeta(labels={"job-name": job_name}),
            spec=client.V1PodSpec(
                restart_policy="Never",
                containers=[container],
                node_selector={"kubernetes.io/hostname": self.node_name}
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

class MockLSTMModel:
    """Mock LSTM model voor testen"""
    def predict_energy(self):
        """Mock voorspelling van energie voor het komende uur"""
        # Genereer mock data: 720 waarden (1 uur met 5s granulariteit)
        # In werkelijkheid zou dit de echte voorspelling van je LSTM model zijn
        import numpy as np
        
        # Genereer een basislijn met sinusgolf om dag/nacht te simuleren
        timestamps = np.arange(720)
        predictions = np.sin(timestamps * np.pi / 360) * 0.5 + 0.5  # Waarden tussen 0 en 1
        
        # Voeg wat ruis toe
        predictions += np.random.normal(0, 0.05, 720)
        predictions = np.clip(predictions, 0, 1)  # Houd waarden tussen 0 en 1
        
        return predictions


def main():
    """Main functie om controller te starten"""
    controller = NodeController()
    controller.start()


if __name__ == "__main__":
    main()