# Use the latest version of Ubuntu as the base image
FROM ubuntu:18.04

# Refresh cache and install packages
RUN DEBIAN_FRONTEND=noninteractive apt-get update && \
    apt-get install -y --no-install-recommends git wget

# Install Python 3.8 instead of the default 3.6
RUN DEBIAN_FRONTEND=noninteractive apt-get install -y software-properties-common
RUN add-apt-repository ppa:deadsnakes/ppa
RUN DEBIAN_FRONTEND=noninteractive apt-get update
RUN DEBIAN_FRONTEND=noninteractive apt-get -y install python3.8
RUN update-alternatives --install /usr/bin/python3 python3 /usr/bin/python3.8 1

## Install pip
RUN DEBIAN_FRONTEND=noninteractive apt-get install -y python3.8-distutils
RUN wget https://bootstrap.pypa.io/get-pip.py
RUN python3 get-pip.py
RUN pip3 install --upgrade pip setuptools wheel

# Set symbolic links to use python3.8 with python and pip
RUN ln -s /usr/bin/python3.8 /usr/bin/python && \
    ln -s /usr/bin/pip3 /usr/bin/pip

# Install OpenJDK 8
RUN apt-get update && apt-get install -y openjdk-8-jdk

# Set environment variables
ENV JAVA_HOME /usr/lib/jvm/java-8-openjdk-amd64
ENV SPARK_HOME /opt/spark
ENV PATH $PATH:$SPARK_HOME/bin

# Download and install Spark
RUN apt-get install -y wget && \
    wget -qO- https://archive.apache.org/dist/spark/spark-3.2.1/spark-3.2.1-bin-hadoop3.2.tgz | tar zx -C /opt && \
    mv /opt/spark-3.2.1-bin-hadoop3.2 /opt/spark

# Download PostgreSQL JDBC driver
RUN wget -O /opt/spark/jars/postgresql-connector.jar https://jdbc.postgresql.org/download/postgresql-42.2.23.jar

# Install AWS CLI with noninteractive mode
RUN DEBIAN_FRONTEND=noninteractive apt-get install -y awscli

# Configure AWS CLI with credentials
RUN aws configure set aws_access_key_id <aws_access_key_id>
RUN aws configure set aws_secret_access_key <aws_secret_access_key_id>
RUN aws configure set default.region <aws_default_region> 

# Copy Directory to install 
COPY . /app 

# Define working directory
WORKDIR /app

# Install python libraries mentioned in requirements.txt file
RUN pip3 install -r requirements.txt && \
    pip3 freeze > requirements_versions.txt

# Define default command to run the PySpark script
# Run the script when the container starts
CMD ["/bin/bash", "/app/project/run_jobs.sh"]