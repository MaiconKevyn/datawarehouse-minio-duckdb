# datawarehouse-minio-duckdb



`docker-compose run --rm --no-deps python bash -c "\
  apt-get update && \
  apt-get install -y git && \
  pip install -r /scripts/requirements.txt && \
  python /scripts/extract_load.py\
"`

### rebuild the image
`docker-compose build python
`

### run the image
`docker-compose up -d minio`

### exec the script
`docker-compose run --rm --no-deps python python query_parquet.py
`

### build the container
`docker-compose up -d
`
### enter in bash python to execute the script

`docker-compose exec python bash
`

### list buckets

`python list_bucket.py
`