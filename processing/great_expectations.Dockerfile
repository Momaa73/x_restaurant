FROM python:3.10

WORKDIR /workspace
RUN pip install --no-cache-dir --force-reinstall great_expectations

ENV PATH="/usr/local/bin:$PATH"
ENTRYPOINT ["/bin/sh"] 