# rabbitmq_eval

#Setup

Install Python 3.8 (although 3.9 may also work)
Install OTP: https://www.erlang.org/downloads
Install RabbitMQ Server: https://www.rabbitmq.com/install-windows.html
(Optional) Install SQL Lite DB Viewer: https://sqlitebrowser.org/blog/version-3-12-1-released/

Open a windows command prompt.

Navigate to the project folder.

#Create and launch the python virtual environment for each client (producer, consumer, grapher)
`python -m venv run_env`
`run_env\Scripts\activate`

#Now you can install libraries without affecting your global python environment
`python -m pip install --upgrade pip`
`pip install pika`
`pip install matplotlib`

#Run each client in their own shell (starting in reverse order)
`python grapher.py`
`python consumer.py`
`python producer.py`