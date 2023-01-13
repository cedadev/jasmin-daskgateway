# Using Dask in Lotus from the JASMIN Notebooks Service.
Using dask in Lotus requires connecting to a proxy service, called dask-gateway, which will create Lotus jobs on your behalf and proxy communication between your dask client and the dask clusters running in Lotus.


## Creating a dask cluster.
To create and connect to a dask cluster, you can use the following python snippet. Note that this will only work on the JASMIN Notebook service.
```python
import dask_gateway

# Create a connection to dask-gateway.
gw = dask_gateway.Gateway("https://dask-gateway.jasmin.ac.uk", auth="jupyterhub")

# Inspect and change the options if required before creating your cluster.
options = gw.cluster_options()
options.worker_cores = 2

# Create a dask cluster, or, if one already exists, connect to it.
# This stage creates the scheduler job in SLURM, so may take some time.
# While your job queues.
clusters = gw.list_clusters()
if not clusters:
    cluster = gw.new_cluster(options, shutdown_on_close=False)
else:
    cluster = gw.connect(clusters[0].name)

# Create at least one worker, and allow your cluster to scale to three.
cluster.adapt(minimum=1, maximum=3)

# Get a dask client.
client = cluster.get_client()
```

When you are done, and whish to release your cluster:
```python
cluster.shutdown()
```

## Other information and FAQs.
### Accessing the dask dashboard.
Currently the dask dashboard is not accessible from a browser outside the JASMIN firewall. If you're browser fails to load the dashboard link returned, please use our [interactive login service](https://help.jasmin.ac.uk/article/4810-graphical-linux-desktop-access-using-nx) to run a browser inside the firewall to view your dashboard.
