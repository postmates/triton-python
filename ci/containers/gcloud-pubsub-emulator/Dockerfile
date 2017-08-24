# Emulator used by testing pipeline.  Available at: quay.io/postmates/gcloud-pubsub-emulator.

FROM google/cloud-sdk

ENV DATA_DIR "/tmp/gcloud/pubsub"
ENV HOST_PORT 0.0.0.0:8042

RUN mkdir -p ${DATA_DIR}

CMD gcloud beta emulators pubsub start --host-port=$HOST_PORT --verbosity=error --data-dir=$DATA_DIR 2> /dev/null
