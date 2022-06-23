from varys import varys_producer, varys_consumer

host = "147.188.173.154"
port = 5672
username = "roz"
password = "siXdySi5Jn"
log_name = "varys_test"
log_file = "/rds/homes/w/wilkisaj/projects/climb-mpx/ware/roz/test_logfile.log"

consumer = varys_consumer(log_name, log_file, True, username, password, host, port, queue="inbound.triplets", routing_key="default", blocking=True, durable=True)

consumer.run()

