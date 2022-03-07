# Based image
FROM python:3.7
# Install requirements
COPY ./requirements.txt /source/requirements.txt
RUN  pip install --upgrade pip
RUN pip install -r /source/requirements.txt

# Copy your code
COPY . /source/

WORKDIR /source/sweetviz
RUN pip install -e .


WORKDIR /source/EDA_app_with_Streamlit_Components

# Add rights to run the Python code
EXPOSE 8080
# RUN chmod +x /source/EDA_app_with_Streamlit_Components/start.sh
CMD streamlit run --server.maxUploadSize=5000 --server.port 8080 --server.enableCORS false app.py
