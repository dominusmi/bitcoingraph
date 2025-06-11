#!/bin/bash

# Check if the path is provided
if [ -z "$1" ]; then
  echo "Error: No path provided. Please provide a path as an argument."
  exit 1
fi

path="$1"

# You can add further actions using the provided path here
echo "Path provided: $path"

chmod +x scripts/bcgraph-compute-entities
chmod +x scripts/bcgraph-pk-to-addresses

./scripts/bcgraph-compute-entities -i $path
./scripts/bcgraph-pk-to-addresses -i $path

cd scripts/merge-entities && cargo build --release && cd ../..
./scripts/merge-entities/target/release/merge_entities $path/rel_entity_address.csv $path/rel_entity_address_merged.csv
