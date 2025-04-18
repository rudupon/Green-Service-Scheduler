#!/bin/bash
echo "$(date +"%Y-%m-%d %H:%M:%S") - Taak gestart op node: $HOSTNAME"
echo "Taak type: $TASK_TYPE, Energie vereiste: $ENERGY_REQUIREMENT"
echo "Prioriteit: $PRIORITY"

DURATION=${TASK_DURATION:-30}
CPU_LOAD=${CPU_LOAD:-50}

echo "Taak zal nu $DURATION seconden draaien met CPU belasting van ongeveer $CPU_LOAD%..."

START_TIME=$(date +%s)
END_TIME=$((START_TIME + DURATION))

WORK_TIME=$(echo "scale=3; $CPU_LOAD/100" | bc)
SLEEP_TIME=$(echo "scale=3; 1-$WORK_TIME" | bc)

while [ $(date +%s) -lt $END_TIME ]; do
    START_WORK=$(date +%s.%N)
    while true; do
        for i in {1..1000}; do
            echo "scale=10; a($i) * s($i) / c($i)" | bc -l >/dev/null 2>&1
        done
        
        CURRENT=$(date +%s.%N)
        ELAPSED=$(echo "$CURRENT - $START_WORK" | bc)
        if (( $(echo "$ELAPSED > $WORK_TIME" | bc -l) )); then
            break
        fi
    done
    
    if (( $(echo "$SLEEP_TIME > 0" | bc -l) )); then
        sleep $SLEEP_TIME
    fi
    
    PROGRESS=$(echo "scale=2; ($(date +%s) - $START_TIME) * 100 / $DURATION" | bc)
    echo "Voortgang: $PROGRESS% voltooid"
done

echo "$(date +"%Y-%m-%d %H:%M:%S") - Taak voltooid op node: $HOSTNAME"