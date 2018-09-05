# HaBolt usage example

## Build

Create image :

`docker-compose build --force-rm image`

## Run node 1

`docker-compose up -d node1`

## Run node2 & node3

`docker-compose up -d node2 node3`


## Check logs

`docker-compose logs -f`