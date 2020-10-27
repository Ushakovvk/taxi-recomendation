import requests
import numpy as np


class Driver:
    """
    Класс воспроизводящий действия водителя такси
    """

    def __init__(self, env, client, label=None, state=None, money=0, duration=0, test=False):
        """
        Инициализация водителя в окружении env
        """
        self.env = env
        self.client = client
        self.label = label
        if state is None:
            state = env.get_random_state()
        self.state = state
        self.money = money
        self.duration = duration
        self.total_duration = 0
        self.test = test

    def request_action(self) -> int:
        """
        Запрос действия у рекомендательной системы
        :return: action
        """
        return self.client.get_best_action(self.state)

    def step(self):
        """
        Водитель выполняет действие
        """
        if self.test:
            action = 7
        else:
            action = self.request_action()
        next_state, reward, duration = self.env.get_next_state(self.state, action)
        self.state = next_state
        self.money += reward
        self.duration += duration
        self.total_duration += duration

        if self.duration > 6 * 60:
            self.reset()

    def reset(self):
        self.state = self.env.get_random_state()
        self.duration = 0

    def get_info(self):
        return {'state': self.state, 'money': self.money, 'duration': self.duration}


class World:
    """
    Окружение такси
    """

    def __init__(self, env_dict):
        """
        Инициализация окружения
        :param env_dict: словарь содержащий состояния, действия и вознаграждения
        """
        self.env = env_dict
        self.state_count = len(self.env.keys())

    def get_next_state(self, state: tuple, action: int) -> tuple:
        """Метод возвращает новое состояние среды, вознаграждение и продолжительность
        в зависимости от состояния и действия
        :param state: состояние среды - tuple
        :param action: действие (1:265) - int
        :return: next_state, reward, duration
        """
        reward = 0
        duration = 0
        next_state = self.get_random_state()

        if state in self.env.keys():
            if action in self.env[state].keys():
                x = list(map(list, zip(*self.env[state][action])))
                if x:
                    i = np.random.choice(len(x[2]))
                    next_state = x[0][i]
                    reward = x[1][i]
                    duration = x[2][i]

        return next_state, reward, duration

    def get_random_state(self) -> tuple:
        """Метод возвращает случайное состояние среды"""
        i = np.random.choice(self.state_count)
        key = list(self.env.keys())[i]
        return key


class ClientApp:
    """
    Клиент рекомендательной системы
    """

    def __init__(self, url='http://backend:8000/predict'):
        self.url = url
        # TODO: загружать список признаков из файла
        self.features = ['location', 'hour', 'weekday']

    def get_best_action(self, state: tuple) -> int:
        """
        Метод запрашивает у сервера лучшее действие в данном состоянии
        :param state: состояние среды - tuple
        :return: action
        """
        state_dict = {self.features[i]: x for i, x in enumerate(state)}
        r = requests.post(self.url, json=state_dict)
        result = r.json()
        return result['action']
