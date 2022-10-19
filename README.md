# Using Dask on in Lotus from the JASMIN Notebooks Service.
Using dask in Lotus requires connecting to a proxy service, called dask-gateway, which will create Lotus jobs on your behalf and proxy communication between your dask client and the dask clusters running in Lotus.


## Creating a dask cluster.
To create and connect to a dask cluster, you can use the following python snippet:
```python
import dask_gateway

# Create a connection to dask-gateway.
gw = dask_gateway.Gateway("https://dask-gateway.jasmin.ac.uk", auth="jupyterhub")

# Create a dask cluster, or, if one already exists, connect to it.
# This stage creates the scheduler job in SLURM, so may take some time.
# While your job queues.
clusters = gw.list_clusters()
if not clusters:
    cluster = gw.new_cluster(shutdown_on_close=False)
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
