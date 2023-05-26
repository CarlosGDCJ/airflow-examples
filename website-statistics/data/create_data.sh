docker build -f ../eventsim/Dockerfile.eventsim -t eventsim ../eventsim

start_time=$(date +"%Y-%m-%dT%H:%M:%S" -d "-60 days")
end_time=$(date +"%Y-%m-%dT%H:%M:%S" -d "+60 days")
docker run --rm -v "${PWD}:/opt/eventsim/generated_data" --name eventsim-run eventsim \
"-c examples/example-config.json --nouseAvro -n 200 --start-time ${start_time} \
--end-time ${end_time} --growth-rate 0.8 --userid 1000000 generated_data"