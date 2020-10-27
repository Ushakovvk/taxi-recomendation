from collections import defaultdict
import numpy as np

from numpy.random import default_rng
rg = default_rng(12345)


class QLearningAgent:
    def __init__(self, alpha, discount, possible_actions):
        """
        Q-Learning Agent
        based on https://inst.eecs.berkeley.edu/~cs188/sp19/projects.html
        Instance variables you have access to
          - self.alpha (learning rate)
          - self.discount (discount rate aka gamma)

        Functions you should use
          - self.get_legal_actions(state) {state, hashable -> list of actions, each is hashable}
            which returns legal actions for a state
          - self.get_qvalue(state,action)
            which returns Q(state,action)
          - self.set_qvalue(state,action,value)
            which sets Q(state,action) := value
        !!!Important!!!
        Note: please avoid using self._qValues directly. 
            There's a special self.get_qvalue/set_qvalue for that.
        """

        self.possible_actions = possible_actions
        self._qvalues = defaultdict(lambda: defaultdict(lambda: 0))
        self.alpha = alpha
        self.discount = discount


    def get_qvalue(self, state, action):
        """ Returns Q(state,action) """
        return self._qvalues[state][action]


    def set_qvalue(self, state, action, value):
        """ Sets the Qvalue for [state,action] to the given value """
        self._qvalues[state][action] = value


    def get_value(self, state):
        """
        Compute your agent's estimate of V(s) using current q-values
        V(s) = max_over_action Q(state,action) over possible actions.
        Note: please take into account that q-values can be negative.
        """

        q_values = [self.get_qvalue(state, action) for action in self.possible_actions]
        return max(q_values)


    def update(self, state, action, reward, next_state):
        """
        You should do your Q-Value update here:
           Q(s,a) := (1 - alpha) * Q(s,a) + alpha * (r + gamma * V(s'))
        """
        # agent parameters
        gamma = self.discount
        learning_rate = self.alpha
        value = self.get_value(next_state)
        q_value = (1.0 - learning_rate) * self.get_qvalue(state, action) + learning_rate * (reward + gamma * value)

        self.set_qvalue(state, action, q_value)


    def get_best_action(self, state):
        """
        Compute the best action to take in a state (using current q-values). 
        """

        q_values = {action: self.get_qvalue(state, action) for action in self.possible_actions}

        return max(q_values, key=q_values.get)

