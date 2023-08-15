# from varys import varys_producer, varys_consumer

# host = "147.188.173.154"
# port = 5672
# username = "roz"
# password = "siXdySi5Jn"
# log_name = "varys_test"
# log_file = "/home/sam/onedrive/bioinformatics/climb-pathogens/roz/test_logfile.log"

# consumer = varys_consumer(username, password, "inbound.triplets", False, host, port, log_file)

# consumer.run()


# def parse_fname(fname, fname_layout):
#     fname_split = fname.split(".")
#     spec_split = fname_layout.split(".")

#     return {field: content for field, content in zip(spec_split, fname_split)}


# test = parse_fname(
#     "mscapetest.sample_test1.run_test1.csv", "project.sample_id.run_name.ftype"
# )

# print(test)

import varys
import time

varys_client = varys.varys(
    profile="roz",
    logfile="/home/ubuntu/varys_test.log",
    log_level="DEBUG",
    config_path="/home/ubuntu/varys_profiles.json",
)

# varys_client.send({"hello": "world"}, "nonsense", queue_suffix="nonsense")
# varys_client.send({"hello": "world"}, "nonsense_2", queue_suffix="nonsense_2")

varys_client.receive("nonsense", queue_suffix="nonsense", block=False)
# varys_client.receive_batch("nonsense_2", queue_suffix="nonsense_2")

print(varys_client.get_channels())
varys_client.close()
