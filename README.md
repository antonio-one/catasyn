##Catasyn

A simple data catalogue synchronisation service.

For this to run make sure [datcat](https://github.com/antonio-one/datcat) is running in a container.

The microservice scheduler is WIP. Use the below to run the synchronisation manually for now.

```bash
python -m catasyn.entrypoints.scheduler
```


