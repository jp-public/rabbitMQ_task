# Task on asynchronous messaging with RabbitMQ

Aalto University CS-E4190 - Cloud Software and Systems

Smart shopping application using Python and RabbitMQ(Pika)


Using test scripts:

to start worker: `python3 run_worker.py --id "<worker-id>" --queue "<worker-id>_queue" -w "1"`

to start customer app: `python3 run_customer_app.py -c "<customer-id>"`

generate shopping events: `python3 produce_shopping_event.py -e "pick up" -c "<customer-id>" -t 5 and python3 produce_shopping_event.py -e "purchase" -c "<customer-id>" -t 10`

