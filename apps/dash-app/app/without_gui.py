from objects import Driver, World, ClientApp
import pickle

env_dict = pickle.load(open('env_dict.pkl', 'rb'))
env = World(env_dict)

client_app = ClientApp('http://192.168.0.105/predict')

driver = Driver(env, client_app, test=True)

for _ in range(100):
    driver.step()
    print(driver.get_info())
