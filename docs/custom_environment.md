## Using a custom environment with dask-gateway on jasmin.

By defaultm the jasmin notebook service and dask gateway use the latest version of the [jaspy](https://help.jasmin.ac.uk/article/4729-jaspy-envs#jaspy-envs) software environment. However, often users would like to use their own software environments.

### Understanding the problem.
When dask gateway greates a dask cluster for a user, it runs a setup command to activate a conda environment or python venv.
To have dask use you packages, you need to create a custom environment which you can pass to dask-gateway to activate.

However, for techical reasons, it is not currently possible to use the same virtual environment in both the notebook service and on jasmin. So you will need to make two envirnments, one for your notebook to use and one for dask to use. **It is VERY important that these environments have the same packages installed in them, and that the packages are exactly the same version in both environments.** If you do not keep packages and versions in-sync you can expect many confusing errors.

### Creating a virtual environment for dask.
* Login to jasmin.
* Activate jaspy `module load jaspy`.
* Create your environment in the normal way, for example `python -m venv name-of-environment`.
* Activate the environment `source name-of-environment/bin/activate`.
* Install dask and dask gateway and dependencies `pip install dask-gateway dask lz4`. Without this step your environment will not work with dask.

### Creating a virtual environment for the notebook service.
* Follow the instructions [here](https://gist.github.com/amanning9/02cfbb7717c6b37731d424703e386562) to create a virtual environment.
* Install dask and dask gateway and dependencies `pip install dask-gateway dask lz4`. Without this step your environment will not work with dask.


### Putting it all together
* Set your notebook virtual environment as the kernel for the notebook in question as shown in the instructions linked above.
* Set `options.worker_setup` to a command which will activate your dask virtual environment. For example `options.worker_setup = "source /home/users/example/name-of-environment/bin/activate`. See `examples/activate-venv.ipynb` for an example of how this works in practice.
* If you have an existing dask cluster, close it and ensure all lotus jobs are stopped before recreating it using the new environment.
