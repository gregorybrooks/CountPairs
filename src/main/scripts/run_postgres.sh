#!/bin/bash
set -v
docker run -d --name postgres-container -e TZ=UTC -p 30432:5432 -e POSTGRES_PASSWORD=postgres -v /mnt/scratch/glbrooks/postgres_pgdata:/var/lib/postgresql/data -v /mnt/scratch/glbrooks/negin/data:/data ubuntu/postgres
docker network connect postgres-network postgres-container
