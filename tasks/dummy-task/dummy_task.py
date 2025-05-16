import os
import time
import logging
import psutil
import numpy as np
from datetime import datetime

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger("DummyTask")

def perform_cpu_intensive_work(duration, intensity=0.8, report_interval=5):
    """
    Voert CPU-intensief werk uit voor de opgegeven duur.
    
    Parameters:
    - duration: Duur in seconden
    - intensity: Fractie van beschikbare CPU capaciteit te gebruiken (0.0 tot 1.0)
    - report_interval: Interval in seconden om voortgang te rapporteren
    """
    start_time = time.time()
    end_time = start_time + duration
    
    logger.info(f"Start CPU-intensieve taak voor {duration} seconden met intensiteit {intensity}")
    
    last_report_time = start_time
    while time.time() < end_time:
        # Voer wat rekenwerk uit om CPU te belasten
        for _ in range(int(10000 * intensity)):
            _ = np.random.random((100, 100)) @ np.random.random((100, 100))
        
        # Korte pauze om CPU niet volledig te blokkeren
        time.sleep(0.01 * (1 - intensity))
        
        # Rapporteer voortgang periodiek
        current_time = time.time()
        if current_time - last_report_time >= report_interval:
            elapsed = current_time - start_time
            remaining = end_time - current_time
            cpu_percent = psutil.cpu_percent()
            memory_percent = psutil.virtual_memory().percent
            
            logger.info(f"Voortgang: {elapsed:.1f}s verstreken, {remaining:.1f}s resterend, CPU: {cpu_percent}%, Geheugen: {memory_percent}%")
            last_report_time = current_time

def main():
    logger.info("Dummy taak gestart")
    
    # Haal parameters op als individuele omgevingsvariabelen
    task_name = os.environ.get("TASK_NAME", "onbekende-taak")
    task_duration = int(os.environ.get("DURATION", "60"))
    task_energy = float(os.environ.get("ENERGY_REQUIREMENT", "1.0"))
    task_priority = int(os.environ.get("PRIORITY", "1"))
    max_delay = int(os.environ.get("MAX_DELAY", "3600"))
    
    logger.info(f"Taak {task_name} wordt uitgevoerd:")
    logger.info(f"  Prioriteit: {task_priority}")
    logger.info(f"  Energie-eis: {task_energy}")
    logger.info(f"  Geplande duur: {task_duration} seconden")
    logger.info(f"  Maximale vertraging: {max_delay} seconden")
    logger.info(f"  Starttijd: {datetime.now().isoformat()}")
    
    # Vertaal energie-eis naar CPU-intensiteit (1.0 betekent max energie, dus max CPU)
    cpu_intensity = min(1.0, task_energy)
    
    perform_cpu_intensive_work(task_duration, intensity=cpu_intensity)
    logger.info(f"Taak {task_name} voltooid op {datetime.now().isoformat()}")

if __name__ == "__main__":
    main()