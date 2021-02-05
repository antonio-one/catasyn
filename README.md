##Catasyn

A simple data catalogue synchronisation service. 

For this to run make sure [datcat](https://github.com/antonio-one/datcat) is running in a container.

```bash
source .env

docker rmi "$(docker images --filter dangling=true -q)" 2> /dev/null
docker container prune --force 2> /dev/null

rm dist/catasyn* 2> /dev/null

poetry build --format wheel
docker build --tag catasyn .

CONTAINER_HOSTNAME="catasyn_"$(uuidgen | awk -F- '{print $1}')
echo "CONTAINER_HOSTNAME=${CONTAINER_HOSTNAME}"

CONTAINER_ID=$(
docker run --hostname "${CONTAINER_HOSTNAME}" \
  --name catasyn \
  --env-file .env \
  --publish 50001:"${CATASYN_PORT}" \
  --detach catasyn
  )

echo "[STOP COMMAND COPIED TO CLIPBOARD] docker stop ${CONTAINER_ID}"
echo "docker stop ${CONTAINER_ID}" | pbcopy

#create some internal networking for the containers to see each other
#TODO: find a better way to orchestrate this
docker network create sidiousnet 2>/dev/null
docker network connect sidiousnet datcat 2>/dev/null
docker network connect sidiousnet catasyn 2>/dev/null

docker exec -it "${CONTAINER_ID}" /bin/bash

#check what the scheduler is doing
tail -f ${SCHEDULER_LOG_FILENAME}
```