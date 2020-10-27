import dash
import dash_core_components as dcc
import dash_html_components as html
import dash_bootstrap_components as dbc
from dash.dependencies import Input, Output, State
import plotly.graph_objs as go
from dash_bootstrap_components import NavbarSimple
import geopandas as gpd
import pandas as pd
import json
import os
from objects import Driver, World, ClientApp
import pickle
import random
import uuid

# Загрузка данных
path_to_shape = 'shape/'

env_dict = pickle.load(open('env_dict.pkl', 'rb'))
env = World(env_dict)

test = False
client_app_qlearning = ClientApp('http://backend:8000/predict_qlearning')
client_app_deep = ClientApp('http://backend:8000/predict_deep')

label_color = {'simple': 'green', 'q-learning': 'blue', 'deep q-learning': 'red'}

update_time = 1 * 1000
all_drivers = dict()

# with open('mapbox_token', 'r') as f:
    # mapbox_token = f.read()

taxi_zone = gpd.read_file(os.path.join(path_to_shape, 'taxi_zones.shp')).to_crs("EPSG:4326")
taxi_zone = taxi_zone.set_index('LocationID')
taxi_zone['lon_center'] = taxi_zone.geometry.centroid.x
taxi_zone['lat_center'] = taxi_zone.geometry.centroid.y

taxi_zone_json = json.loads(taxi_zone[['geometry']].to_json())


def loc_to_coordinates(location: int):
    if location in taxi_zone.index:
        s = 0.005
        lon = taxi_zone.loc[location, 'lon_center'] + random.uniform(-s, s)
        lat = taxi_zone.loc[location, 'lat_center'] + random.uniform(-s, s)
    else:
        lat = 40.7142700
        lon = -74.0059700
    return lat, lon


loc_info = pd.read_csv('loc_info.csv')

map_fig = go.Figure()
# map_fig.add_trace(go.Choroplethmapbox(geojson=taxi_zone_json, locations=loc_info['PULocationID'], z=loc_info['count'],
# marker_opacity=0.4))
map_fig.update_layout(mapbox={'style': "open-street-map", 'zoom': 10,
                              'center': go.layout.mapbox.Center(lat=40.7142700, lon=-74.0059700),
                              # 'accesstoken': mapbox_token,
                              }, margin={'l': 0, 'r': 0, 't': 0, 'b': 0}, height=600)

chart_fig = go.Figure()

navbar_container: NavbarSimple = dbc.NavbarSimple([
    dbc.NavItem(dbc.NavLink("Сервис", active=True, href="#")),
    dbc.NavItem(dbc.NavLink("Контакты", href="#"))],
    brand="Выбор квартиры", brand_href="#", color="primary")


def create_drivers(n_simple=20, n_qlearning=20, n_deep=20):
    init_state = env.get_random_state()
    drivers = []
    drivers += [Driver(env, ClientApp(), label='simple', state=init_state, test=True) for _ in range(n_simple)]
    drivers += [Driver(env, client_app_qlearning, label='q-learning', state=init_state, test=test) for _ in range(n_qlearning)]
    drivers += [Driver(env, client_app_deep, label='deep q-learning', state=init_state, test=test) for _ in range(n_deep)]
    return drivers


def generate_map(session_id=None):
    fig = go.Figure(map_fig)
    if session_id:
        drivers = all_drivers[session_id]
        labels = [driver.label for driver in drivers]
        lat_list, lon_list = [], []
        for driver in drivers:
            lat, lon = loc_to_coordinates(driver.get_info()['state'][0])
            lat_list.append(lat)
            lon_list.append(lon)
        marker = go.scattermapbox.Marker(size=10, color=[label_color[x] for x in labels])
        fig.add_trace(go.Scattermapbox(lat=lat_list, lon=lon_list, marker=marker))
    return fig


def generate_chart(session_id=None):
    fig = go.Figure(map_fig)
    if session_id:
        drivers = all_drivers[session_id]
        labels = [driver.label for driver in drivers]
        moneys = [driver.money for driver in drivers]
        durations = [driver.total_duration for driver in drivers]

        data = pd.DataFrame({'label': labels, 'money': moneys, 'duration': durations})
        data['score'] = data['money'] / data['duration'] * 60
        group = data.groupby('label', as_index=False).mean()
        fig = go.Figure([go.Bar(x=group['label'], y=group['score'], marker_color=[label_color[x] for x in group['label']])])
        fig.update_layout(height=500, yaxis=dict(range=[0, 50], title='Средний доход в час, USD',))
    return fig


maps = [dbc.Alert("Карта", color="primary"), dcc.Graph(figure=generate_map(), id='map')]
chart = [dbc.Alert("Доходы", color="primary"), dcc.Graph(figure=generate_chart(), id='chart')]

body = dbc.Container([
    dbc.Jumbotron([html.H4('Заголовок'),
                   html.P('Описание'),
                   ]),
    dbc.Row([dbc.Col(maps), dbc.Col(chart, width=5)]),
                    ])


def serve_layout():

    drivers = create_drivers()
    session_id = str(uuid.uuid4())

    session_block = html.Div(session_id, id='session-id', style={'display': 'show'})
    all_drivers[session_id] = drivers
    return html.Div([session_block, navbar_container,
                     body,
                     dcc.Interval(id='interval-component', interval=update_time, n_intervals=0)
                     ])


app = dash.Dash(__name__, external_stylesheets=[dbc.themes.JOURNAL])
# app.config.suppress_callback_exceptions = True
app.title = 'Зеленое Такси'
app.layout = serve_layout
# the Flask app
server = app.server


@app.callback([Output('map', 'figure'),
               Output('chart', 'figure')],
              [Input('interval-component', 'n_intervals')],
              [State('session-id', 'children')])
def update_map(_, session_id):
    if session_id not in all_drivers:
        all_drivers[session_id] = create_drivers()
    drivers = all_drivers[session_id]
    [driver.step() for driver in drivers]
    return generate_map(session_id), generate_chart(session_id)


if __name__ == '__main__':
    app.run_server(debug=False)
