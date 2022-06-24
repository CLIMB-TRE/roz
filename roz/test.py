from varys import varys_producer, varys_consumer

host = "147.188.173.154"
port = 5672
username = "roz"
password = "siXdySi5Jn"
log_name = "varys_test"
log_file = "/home/sam/onedrive/bioinformatics/climb-pathogens/roz/test_logfile.log"

consumer = varys_consumer(username, password, "inbound.triplets", False, host, port, log_file)

consumer.run()

