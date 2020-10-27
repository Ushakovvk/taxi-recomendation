FROM tiangolo/uvicorn-gunicorn-fastapi:python3.8
# COPY ./app /app
COPY requirements.txt .
RUN pip install torch==1.5.1+cpu torchvision==0.6.1+cpu -f https://download.pytorch.org/whl/torch_stable.html
RUN pip install --no-cache-dir -r requirements.txt
