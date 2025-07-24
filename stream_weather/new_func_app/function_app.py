import logging  # Import logging for information and error messages
import azure.functions as func  # Import Azure Functions SDK
import requests  # Import requests for HTTP API calls
import json  # Import json for encoding/decoding JSON data
from azure.eventhub import EventHubProducerClient, EventData  # Import Event Hub client and event data
from azure.identity import DefaultAzureCredential  # Import default Azure credential for authentication
from azure.keyvault.secrets import SecretClient  # Import Key Vault client for secret management

app = func.FunctionApp()  # Instantiate the Azure Function App

@app.schedule(
    schedule="*/30 * * * * *",  # Run every 30 seconds
    arg_name="myTimer",
    run_on_startup=True,
    use_monitor=False
)
def weather_api_function_using_VS(myTimer: func.TimerRequest) -> None:
    if myTimer.past_due:  # Check if timer is past due
        logging.info('The timer is past due!')  # Log past due event

    logging.info('Python timer trigger function executed.')  # Log function execution

    EVENT_HUB_NAME = "weather-event-hub"  # Event Hub name
    EVENT_HUB_NAMESPACE = "weather-event-namespace.servicebus.windows.net"  # Event Hub namespace

    credential = DefaultAzureCredential()  # Use managed identity for authentication

    producer = EventHubProducerClient(
        fully_qualified_namespace=EVENT_HUB_NAMESPACE,  # Set Event Hub namespace
        eventhub_name=EVENT_HUB_NAME,  # Set Event Hub name
        credential=credential  # Set authentication credential
    )

    def send_event(event):  # Send event data to Event Hub
        event_data_batch = producer.create_batch()  # Create a batch for events
        event_data_batch.add(EventData(json.dumps(event)))  # Add event data to batch
        producer.send_batch(event_data_batch)  # Send batch to Event Hub

    def handle_response(response):  # Handle API response
        if response.status_code == 200:  # If response is successful
            return response.json()  # Return JSON data
        else:
            return f"Error: {response.status_code}, {response.text}"  # Return error message

    def get_current_weather(base_url, api_key, location):  # Fetch current weather and air quality
        current_weather_url = f"{base_url}/current.json"  # API endpoint for current weather
        params = {
            'key': api_key,  # API key
            'q': location,  # Location
            "aqi": 'yes'  # Request air quality data
        }
        response = requests.get(current_weather_url, params=params)  # Make API call
        return handle_response(response)  # Return handled response

    def get_forecast_weather(base_url, api_key, location, days):  # Fetch weather forecast
        forecast_url = f"{base_url}/forecast.json"  # API endpoint for forecast
        params = {
            "key": api_key,  # API key
            "q": location,  # Location
            "days": days,  # Number of forecast days
        }
        response = requests.get(forecast_url, params=params)  # Make API call
        return handle_response(response)  # Return handled response

    def get_alerts(base_url, api_key, location):  # Fetch weather alerts
        alerts_url = f"{base_url}/alerts.json"  # API endpoint for alerts
        params = {
            'key': api_key,  # API key
            'q': location,  # Location
            "alerts": 'yes'  # Request alerts
        }
        response = requests.get(alerts_url, params=params)  # Make API call
        return handle_response(response)  # Return handled response

    def flatten_data(current_weather, forecast_weather, alerts):  # Merge and flatten weather data
        location_data = current_weather.get("location", {})  # Extract location info
        current = current_weather.get("current", {})  # Extract current weather info
        condition = current.get("condition", {})  # Extract weather condition
        air_quality = current.get("air_quality", {})  # Extract air quality info
        forecast = forecast_weather.get("forecast", {}).get("forecastday", [])  # Extract forecast days
        alert_list = alerts.get("alerts", {}).get("alert", [])  # Extract alerts

        flattened_data = {
            'name': location_data.get('name'),  # City name
            'region': location_data.get('region'),  # Region name
            'country': location_data.get('country'),  # Country name
            'lat': location_data.get('lat'),  # Latitude
            'lon': location_data.get('lon'),  # Longitude
            'localtime': location_data.get('localtime'),  # Local time
            'temp_c': current.get('temp_c'),  # Temperature (Celsius)
            'is_day': current.get('is_day'),  # Day/night indicator
            'condition_text': condition.get('text'),  # Weather condition text
            'condition_icon': condition.get('icon'),  # Weather condition icon
            'wind_kph': current.get('wind_kph'),  # Wind speed (kph)
            'wind_degree': current.get('wind_degree'),  # Wind direction (degrees)
            'wind_dir': current.get('wind_dir'),  # Wind direction
            'pressure_in': current.get('pressure_in'),  # Pressure (inches)
            'precip_in': current.get('precip_in'),  # Precipitation (inches)
            'humidity': current.get('humidity'),  # Humidity (%)
            'cloud': current.get('cloud'),  # Cloud cover (%)
            'feelslike_c': current.get('feelslike_c'),  # Feels like temperature (Celsius)
            'uv': current.get('uv'),  # UV index
            'air_quality': {  # Air quality metrics
                'co': air_quality.get('co'),
                'no2': air_quality.get('no2'),
                'o3': air_quality.get('o3'),
                'so2': air_quality.get('so2'),
                'pm2_5': air_quality.get('pm2_5'),
                'pm10': air_quality.get('pm10'),
                'us-epa-index': air_quality.get('us-epa-index'),
                'gb-defra-index': air_quality.get('gb-defra-index')
            },
            'alerts': [  # List of alerts
                {
                    'headline': alert.get('headline'),  # Alert headline
                    'severity': alert.get('severity'),  # Alert severity
                    'description': alert.get('desc'),  # Alert description
                    'instruction': alert.get('instruction')  # Alert instructions
                }
                for alert in alert_list
            ],
            'forecast': [  # List of forecast days
                {
                    'date': day.get('date'),  # Forecast date
                    'maxtemp_c': day.get('day', {}).get('maxtemp_c'),  # Max temperature
                    'mintemp_c': day.get('day', {}).get('mintemp_c'),  # Min temperature
                    'condition': day.get('day', {}).get('condition', {}).get('text')  # Forecast condition
                }
                for day in forecast
            ]
        }
        return flattened_data  # Return the merged data

    def get_secret_from_keyvault(vault_url, secret_name):  # Fetch secret from Azure Key Vault
        credential = DefaultAzureCredential()  # Use Managed Identity for authentication
        secret_client = SecretClient(vault_url=vault_url, credential=credential)  # Create Key Vault client
        retrieved_secret = secret_client.get_secret(secret_name)  # Get the secret value
        return retrieved_secret.value  # Return the secret

    def fetch_weather_data():  # Main program logic
        base_url = "http://api.weatherapi.com/v1/"  # Base URL for weather API
        location = "Queretaro"  # Location for weather data

        VAULT_URL = "https://weather-kv.vault.azure.net/"  # Azure Key Vault URL
        API_KEY_SECRET_NAME = "weather-api-key"  # Name of the secret containing API key
        weatherapikey = get_secret_from_keyvault(VAULT_URL, API_KEY_SECRET_NAME)  # Fetch API key

        current_weather = get_current_weather(base_url, weatherapikey, location)  # Get current weather
        forecast_weather = get_forecast_weather(base_url, weatherapikey, location, 3)  # Get forecast for 3 days
        alerts = get_alerts(base_url, weatherapikey, location)  # Get weather alerts

        merged_data = flatten_data(current_weather, forecast_weather, alerts)  # Merge all data

        send_event(merged_data)  # Send merged data to Event Hub

    fetch_weather_data()  # Call the main program to execute the workflow
