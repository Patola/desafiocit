## Broker settings.
broker_url = 'amqp://guest:guest@127.0.0.1:5672//'

# serializer
celery_task_serializer = 'pickle'
#CELERY_TASK_SERIALIZER = 'pickle'

# List of modules to import when the Celery worker starts.
# imports = ('myapp.tasks',)
#imports = ('desafiocit', )
imports = ( 'desafiocit', )

broker_heartbeat=0

## Using the database to store task state and results.
result_backend = 'db+sqlite:///results.db'

task_default_delivery_mode = "transient"

task_annotations = {'tasks.add': {'rate_limit': '10/s'}}
