""" Import all needed packages"""
import dash
from dash import dcc, html, Input, Output
import dash_bootstrap_components as dbc
from dash.dependencies import Input, Output
import pandas as pd
import plotly.express as px
from sqlalchemy import create_engine, text
from logger_config import logger  # Import the logger file
import os

# Database Configuration
DB_HOST = "postgres"  # Docker service name for postgreSql
DB_PORT = 5432
DB_NAME = "my_database"

DB_USER = os.getenv("DB_USER")  # Default to `db_user` instead the superuser
DB_PASSWORD = os.getenv("DB_PASSWORD")


# Creating database connection with ssl encryption
db_url = f'postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}?sslmode=require'
engine = create_engine(db_url)


# Initialize Dash app with Bootstrap predesigned styles
app = dash.Dash(__name__, external_stylesheets=[dbc.themes.BOOTSTRAP])

logger.info("Starting Dash visualization app...")


# Define X and Y axis options
x_axis_options = [
    "transactionID", "transType", "amount", "nameOrig", "oldbalanceOrg", "newbalanceOrig",
    "nameDest", "oldbalanceDest", "newbalanceDest", "isFraud", "isFlaggedFraud", "timestamp"
]
y_axis_options = ["transactionID", "transType", "amount", "isFraud", "isFlaggedFraud"]



'''settings the Navigation bar which includes the navbar container, 
   the navbarbrand and radio buttons for chosing which graph to display 
'''
navbar = dbc.Navbar(
    dbc.Container([
        dbc.NavbarBrand("Fraud Detection Dashboard", className="ms-2"),
        
        dbc.Row([
            dbc.Col(
                dcc.RadioItems(
                    id='graph-selector',
                    options=[
                        {'label': 'Fraud Transactions', 'value': 'fraud_transactions'},
                        {'label': 'Fraud by Transaction Type', 'value': 'fraud_by_type'}
                    ],
                    value='fraud_transactions',  # Default graph selected
                    inline=True,
                    className="text-white",
                    style={'display': 'flex', 'gap': '15px'}
                ),
                width="auto"
            ),
        ], align="center", className="ms-auto"),

    ], fluid=True),
    color="dark",
    dark=True,
    className="mb-4"
)



"""The general Layout of the Dash App
   containing the navbar, dropdown controls, the graph title and the graph
"""

app.layout = dbc.Container([
    navbar,  # The navbar which contains the radio buttons.

    dbc.Row([
        dbc.Col([
            dbc.Card([
                dbc.CardBody([
                    html.Div([
                        # Dropdown controls (Only visible for 'Fraud Transactions')
                        html.Div([
                            html.Label("Select X-axis:", className="dropdown-label"),
                            dcc.Dropdown(
                                id='x-axis-dropdown',
                                options=[{'label': col, 'value': col} for col in x_axis_options],
                                value='transactionID', className="mb-3"
                            ),

                            html.Label("Select Y-axis:", className="dropdown-label"),
                            dcc.Dropdown(
                                id='y-axis-dropdown',
                                options=[{'label': col, 'value': col} for col in y_axis_options],
                                value='isFraud', className="mb-3"
                            ),
                        ], id="dropdown-container", className="controls"),  # Hide/Show this dynamically based on selected graph
                        

                        # Graph Display Area (display one graph at a time, updates dynamically)
                        html.Div([
                            html.H3("Fraud Transactions", id="graph-title", style={'color': '#010103', 'text-align': 'center'}),
                            dcc.Graph(id='fraud-graph', style={'flex-grow': 1, 'width':'750px', 'height': '500px' }),
                        ], className="graph-container")

                    ], className="container")
                ])
            ], className="shadow-lg p-3 mb-5 bg-white rounded")
        ], width=12)  
    ]),  
], fluid=True,
   className='dashboard-container',
   style={'background-color': '#ffffff', 'border-color': '#010103'})



"""a callback and the function that updates the graph and title based on the selected graph
   output- for the graph,title and the dropdown option
   input- based on the selected graph via the radiobuttons, the x and y axis based on 
   what is selected from the dropdown options
"""

@app.callback(
    [Output('fraud-graph', 'figure'),
    Output('dropdown-container', 'style'),# show/hide dropdowns
    Output('graph-title', 'children')],  #Update title dynamically base on selected graph
    [Input('graph-selector', 'value'),
     Input('x-axis-dropdown', 'value'),
     Input('y-axis-dropdown', 'value')],
    prevent_initial_call=False
)

def update_graph(selected_graph, selected_x, selected_y):
    try:
        #Set the title dynamically based on the selected graph
        title = "Fraud Transactions" if selected_graph == "fraud_transactions" else "Fraudulent Transactions by Type"

        # Show dropdowns only when 'Fraud Transactions' is selected
        dropdown_style = {'display': 'block'} if selected_graph == 'fraud_transactions' else {'display': 'none'}

        #Select the appropriate query based on graph
        query = text("SELECT * FROM transformed_data") if selected_graph == 'fraud_transactions' else text("SELECT * FROM fraud_by_type")
        df = pd.read_sql(query, con=engine)
        
        if df.empty:
            return {'data': [], 'layout': {'title': 'No data available'}}, dropdown_style, title

        if selected_graph == 'fraud_transactions':
            if selected_x not in df.columns or selected_y not in df.columns:
                return {'data': [], 'layout': {'title': 'Invalid column selection'}}, dropdown_style, title

            fig = px.scatter(df, x= selected_x, y=selected_y, title=f"{selected_x} vs {selected_y}") 
            fig.update_layout(xaxis_title=selected_x, yaxis_title=selected_y)

        else:  # Fraud by Type graph
            fig = px.bar(df, x='transType', y='fraudulent_transactions', title='Fraud by Type', color_discrete_sequence=["#1E90FF"])

        return fig, dropdown_style, title

    except Exception as e:
        logger.error(e)
        return {'data': [], 'layout': {'title': 'Error retrieving data'}}, {'display': 'block'}


# Run the app
'''The app is ment to run 0n http//0000/8050
   but it also runs on localhost//8050 and 127.0.0.1/8050
'''
if __name__ == '__main__':
    app.run(debug=True, host="0.0.0.0", port=8050)  

