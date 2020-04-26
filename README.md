# jms-mq

Local run:

docker run --env LICENSE=accept --env MQ_QMGR_NAME=QM1 \
           --publish 1414:1414 \
           --publish 9443:9443 \
           --detach \
           ibmcom/mq

Entrar a la instancia del docker y:

setmqaut -t qmgr -p admin +connect
setmqaut -n DEV.QUEUE.1 -m QM1 -t queue -p admin +all
setmqaut -t qmgr -p admin +all


ERR-LOG: ./mnt/mqm/data/qmgrs/QM1/errors/AMQERR01.LOG

TODO:

1) Write some basic log before log4j on application.java
2) (Dynamic path) Store of output file received
3) Pack JAR and run on Windows (cmd;.NET)