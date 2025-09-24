
# How to Run the System

### 1. Clone the Repository
Run the following command to clone the repository:

```bash
git clone https://github.com/poliukhovych/inform-systems-lab.git
cd inform-systems-lab
```

### 2. Start All Services

Start all services by running:

```bash
docker-compose up --build -d
```

* `--build` forces a rebuild of the `auth-service` and `data-generator` images with the latest code.
* `-d` runs the containers in detached mode (in the background).

## Accessing the Services

Once the system is running, you can access the following web interfaces:

* **Grafana**: [http://localhost:3000](http://localhost:3000)
* **Prometheus**: [http://localhost:9090](http://localhost:9090)

### Grafana Configuration

Grafana is configured automatically using provisioning. It automatically imports the Prometheus data source and all dashboards from the `./grafana/provisioning/` folder, so you don't need to add them manually.

## Stopping the System

To stop and remove all containers, run the following command:

```bash
docker-compose down
```
