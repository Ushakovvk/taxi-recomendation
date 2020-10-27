# Data Handling
import pickle
from pydantic import BaseModel

import torch
import numpy as np

# Server
import uvicorn
from fastapi import FastAPI

app = FastAPI()

# Initialize files
q_values = pickle.load(open('qlearning_model/qvalues.pkl', 'rb'))

simple_features = ['location', 'hour', 'weekday']
neural_features = ['location', 'hour', 'weekday']

network = torch.load('./deep_qlearning_model/torch-network', map_location=torch.device('cpu'))
network.eval()
transform = pickle.load(open('./deep_qlearning_model/sklearn-transformer', 'rb'))


def get_action(state):
    if state not in q_values.keys():
        return 95  # TODO подумать
    else:
        return max(q_values[state], key=q_values[state].get)


def get_action_deep(state):
    states = [state]
    if transform:
        states = transform.transform(states)
    states = torch.tensor(states, dtype=torch.float32)
    q_values = network(states).detach().to('cpu').numpy()
    action = np.argmax(q_values)
    return int(action) + 1


class SimpleData(BaseModel):
    location: int
    hour: int
    weekday: int


class NeuralData(BaseModel):
    location: int
    hour: int
    weekday: int


@app.post("/predict_qlearning")
def predict(data: SimpleData):
    # Extract data in correct order
    data_dict = data.dict()
    state = tuple([data_dict[feature] for feature in simple_features])
    # Create and return prediction
    action = get_action(state)
    return {"action": action}


@app.post("/predict_deep")
def predict(data: NeuralData):
    # Extract data in correct order
    data_dict = data.dict()
    state = tuple([data_dict[feature] for feature in neural_features])
    # Create and return prediction
    action = get_action_deep(state)
    return {"action": action}
