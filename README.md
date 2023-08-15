# roz
CLIMB-pathogens S3 ingest solution and officious bureaucrat.


# TODO
* Add SSL support before the users get access
* Fix add credentials func in s3_config

# Stuff I need to remember

* push-endpoint not push_endpoint
* verify-ssl should be false on ceph side

* For testing these opts should be off but in prod they will need to be because local certs
* #ssl_options.verify     = verify_peer
* #ssl_options.fail_if_no_peer_cert = true

* if it fails for no reason when you createtopic make sure https is on
* 