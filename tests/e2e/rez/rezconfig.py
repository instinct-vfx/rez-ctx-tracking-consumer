# rez configuration for the end-to-end test. The context tracking part is the
# README's recommended setup, so the test also checks the documented config.
packages_path = ["/packages"]

context_tracking_host = "rabbitmq:5672"
context_tracking_amqp = {
    "userid": "rez",
    "password": "rez",
    "connect_timeout": 10,
    "exchange_name": "rez.context",
    "exchange_routing_key": "REZ.CONTEXT",
    "message_delivery_mode": 2,
}
# Expanded by rez from the environment; dropped by rez if SHOW is unset.
context_tracking_extra_fields = {"show": "${SHOW}"}
