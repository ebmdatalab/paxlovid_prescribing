FROM ebmdatalab/datalab-jupyter:python3.8.1-2328e31e7391a127fe7184dcce38d581a17b1fa5

# Set up jupyter environment
ENV MAIN_PATH=/home/app/notebook

# Set path to BQ service account credentials
ENV EBMDATALAB_BQ_CREDENTIALS_PATH=/tmp/bq-service-account.json

# Install pip requirements
COPY requirements.txt /tmp/
# Hack until this is fixed https://github.com/jazzband/pip-tools/issues/823
RUN chmod 644 /tmp/requirements.txt
RUN pip install --requirement /tmp/requirements.txt

EXPOSE 8888

# This is a custom ipython kernel that allows us to manipulate
# `sys.path` in a consistent way between normal and pytest-with-nbval
# invocations
COPY config/kernel.json /tmp/kernel_with_custom_path/kernel.json
RUN jupyter kernelspec install /tmp/kernel_with_custom_path/ --user --name="python3"

CMD cd ${MAIN_PATH} && PYTHONPATH=${MAIN_PATH} jupyter lab --config=config/jupyter_notebook_config.py

# Copy BQ service account credentials into container
# We work around the credentials not existing in CI with the glob
COPY bq-service-account.jso[n] /tmp/
