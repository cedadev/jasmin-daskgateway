It is possible to use the dask gateway service from the command line on the JASMIN sci machines.

At the current time, it is still necessary to use the notebook service to generate an api token to allow you to connect to the gateway server.

## Setup
1. Make a dask configuration folder in your home directory `mkdir -p ~/.config/dask`
1. Create a configuration file for dask-gateway `touch ~/.config/dask/gateway.yaml`
1. Change the permissions on the file so that only you can read it `chmod 600 ~/.config/dask/gateway.yaml`.
1. Head to https://notebooks.jasmin.ac.uk/hub/token , put a note in the box to remind yourself what this token is for, press the big orange button then copy then token.
1. Paste the following snippet into `~/.config/dask/gateway.yaml`, the part in brackets with the API token you just copied.
```yaml
gateway:
  address: https://dask-gateway.jasmin.ac.uk
  auth:
    type: jupyterhub
    kwargs:
      api_token: <replaceWithYourSecretAPIToken>
```
1. Youre done. You can now use dask gateway from the command line. Please see this folder for example useage in a python script!

Note: It is very important that your API token is not shared between users and remains secret. With it, another user can submit dask jobs to lotus as you, and they could exploit this to see anything in your jasmin account.
