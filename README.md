# jms-mq

Local run:

<code>
docker run --env LICENSE=accept --env MQ_QMGR_NAME=QM1 \
           --publish 1414:1414 \
           --publish 9443:9443 \
           --detach \
           ibmcom/mq
</code>

Entrar a la instancia del container:

<code>
docker ps
docker exec -t -i (id) /bin/bash
</code>

Luego:

<code>
setmqaut -t qmgr -p admin +connect
setmqaut -n DEV.QUEUE.1 -m QM1 -t queue -p admin +all
setmqaut -t qmgr -p admin +all

ERR-LOGs: ./mnt/mqm/data/qmgrs/QM1/errors/AMQERR01.LOG
</code>

TODO:

1) Pack JAR and run on Windows (cmd;.NET)
2) Write some basic log before log4j on application.java?