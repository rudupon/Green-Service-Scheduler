const express = require('express');
const path = require('path');
const cors = require('cors');
const k8s = require('@kubernetes/client-node');
const bodyParser = require('body-parser');

const app = express();
const PORT = process.env.PORT || 3000;

// Middleware
app.use(cors());
app.use(bodyParser.json());
app.use(express.static(path.join(__dirname, 'public')));

// Kubernetes configuratie
const kc = new k8s.KubeConfig();
kc.loadFromDefault(); // Laadt de configuratie binnen het cluster

const k8sApi = kc.makeApiClient(k8s.CoreV1Api);
const namespace = process.env.NAMESPACE || 'edge-computing';

// Routes
app.get('/api/nodes', async (req, res) => {
    try {
        const response = await k8sApi.listNode();
        const nodes = response.body.items.map(node => ({
            name: node.metadata.name,
            ready: node.status.conditions.find(c => c.type === 'Ready')?.status === 'True',
            labels: node.metadata.labels
        }));
        res.json(nodes);
    } catch (error) {
        console.error('Fout bij ophalen nodes:', error);
        res.status(500).json({ message: 'Fout bij ophalen nodes' });
    }
});

app.get('/api/tasks', async (req, res) => {
    try {
        const batchApi = kc.makeApiClient(k8s.BatchV1Api);
        
        const response = await batchApi.listNamespacedJob(
            namespace,
            undefined, // pretty
            undefined, // allowWatchBookmarks
            undefined, // _continue
            undefined, // fieldSelector
            'type=green-task' // labelSelector
        );
        
        const tasks = response.body.items.map(job => {
            return {
                name: job.metadata.name,
                node: job.metadata.labels['target-node'] || 'onbekend',
                status: job.metadata.labels['status'] || 'Wachtend',
                created_at: job.metadata.creationTimestamp
            };
        });
        
        res.json(tasks);
    } catch (error) {
        console.error('Fout bij ophalen taken:', error);
        res.status(500).json({ message: 'Fout bij ophalen taken' });
    }
});

app.post('/api/tasks', async (req, res) => {
    try {
        const { name, node, energy_requirement, priority, max_delay, duration, can_migrate } = req.body;
        
        // Validatie
        if (!name || !node || !energy_requirement || !priority || !max_delay || !duration) {
            return res.status(400).json({ message: 'Alle velden zijn verplicht' });
        }
        
        // Creëer een job definitie
        const job = {
            apiVersion: 'batch/v1',
            kind: 'Job',
            metadata: {
                name: name,
                namespace: namespace,
                labels: {
                    'type': 'green-task',
                    'status': 'pending-scheduling',
                    'target-node': node,
                    'can-migrate': can_migrate ? 'true' : 'false'
                },
                annotations: {
                    'energy-requirement': energy_requirement.toString(),
                    'priority': priority.toString(),
                    'max-delay': max_delay.toString(),
                    'duration': duration.toString()
                }
            },
            spec: {
                suspend: true,
                template: {
                    spec: {
                        containers: [{
                            name: 'task-container',
                            image: 'gitlab.stud.atlantis.ugent.be:5050/rdupon/mp/dummy-task:latest',
                            env: [
                                { name: 'ENERGY_REQUIREMENT', value: energy_requirement.toString() },
                                { name: 'PRIORITY', value: priority.toString() },
                                { name: 'MAX_DELAY', value: max_delay.toString() },
                                { name: 'DURATION', value: duration.toString() }
                            ]
                        }],
                        restartPolicy: 'Never'
                    }
                },
                backoffLimit: 0
            }
        };
        
        // Maak de batch/v1 API client
        const batchApi = kc.makeApiClient(k8s.BatchV1Api);
        
        // Maak de job
        await batchApi.createNamespacedJob(namespace, job);
        
        res.status(201).json({ 
            message: 'Taak succesvol aangemaakt',
            name,
            node,
            can_migrate
        });
    } catch (error) {
        console.error('Fout bij aanmaken taak:', error);
        res.status(500).json({ message: `Fout bij aanmaken taak: ${error.message}` });
    }
});

// SPA route handler - stuur index.html voor alle niet-API routes
app.get('*', (req, res) => {
    res.sendFile(path.join(__dirname, 'public', 'index.html'));
});

// Start server
app.listen(PORT, () => {
    console.log(`Server draait op poort ${PORT}`);
});