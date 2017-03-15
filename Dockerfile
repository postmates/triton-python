FROM python:2-alpine

# TODO: Use all the wheel build artifacts (will need musl builds first)

COPY . /triton
WORKDIR /triton
RUN apk add --no-cache postgresql-dev snappy-dev build-base zeromq libpq snappy \
    && pip install pystatsd-2.0.1-py2-none-any.whl \
    && pip install -r cfg/requirements.txt \
    && apk del postgresql-dev snappy-dev build-base \
    && python setup.py install \
    && rm -rf /root/.cache pystatsd-2.0.1-py2-none-any.whl

CMD python /triton/bin/tritond
