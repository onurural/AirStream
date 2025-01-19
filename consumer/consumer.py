from kafka import KafkaConsumer
from kafka import KafkaProducer
import json
import joblib
import numpy as np
import pandas as pd
from sklearn.preprocessing import StandardScaler
from tensorflow.keras.models import load_model
from datetime import datetime, timezone, timedelta
import clickhouse_connect


producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda x: json.dumps(x).encode('utf-8')
)

# Connect to ClickHouse
client = clickhouse_connect.get_client(
    host='localhost',   # Use 'localhost' instead of the container name
    port=8123,          # Port exposed by the container
    username='default', # Default ClickHouse username
    password='',        # Default ClickHouse password (blank)
    database='airstream_db'  # Database name
)

# Load the saved machine learning model (e.g., a Random Forest or LSTM model)
model = load_model('/Users/onurural/onur/AirStream/consumer/trained_aqi_model-4.keras')
scaler = joblib.load('/Users/onurural/onur/AirStream/consumer/scaler-3.pkl')
# Initialize Kafka consumer
consumer = KafkaConsumer('raw-data-topic-2',
                         bootstrap_servers=['localhost:9092'],
                         auto_offset_reset='earliest',
                         enable_auto_commit=True,
                         value_deserializer=lambda x: json.loads(x.decode('utf-8')))


def preprocess_data(data_string):
    # Split the data_string by newlines to process each JSON object individually
    data_points = []
    for line in data_string.strip().splitlines():
        data_points.append(json.loads(line))

    
    # Parse each JSON line and create a list of dictionaries
    records = []
    for data in data_points:
        
        # Convert 'dt' to a timezone-aware datetime and extract additional features
        dt = datetime.fromtimestamp(data['dt'], tz=timezone.utc)
        data['dt'] = dt  # Update dt to datetime format
        data['hour'] = dt.hour
        data['day_of_week'] = dt.weekday()
        data['month'] = dt.month
        data['day_of_year'] = dt.timetuple().tm_yday
        
        # Append the transformed data point to the records list
        records.append(data)
    
    # Convert the list of dictionaries to a DataFrame
    data_df = pd.DataFrame(records)
    
    # Drop columns not used as input for the ML model (e.g., 'aqi' which is the target)
    data_df = data_df.drop(columns=['aqi'])
    
    # Final preprocessing step: Remove 'dt' column if not needed for the model
    features_df = data_df.drop(columns=['dt'])
    
    # Define the correct feature order expected by the ML model
    feature_order = [
        "components.co", "components.no", "components.no2", "components.o3", "components.so2",
        "components.pm2_5", "components.pm10", "components.nh3", "temp", "dew_point", 
        "feels_like", "pressure", "humidity", "wind_speed", "wind_deg", "clouds_all",
        "hour", "day_of_week", "month", "day_of_year"
    ]
    
    # Rename columns in `features_df` to match the model's expected naming convention
    rename_mapping = {
        'co': 'components.co',
        'no': 'components.no',
        'no2': 'components.no2',
        'o3': 'components.o3',
        'so2': 'components.so2',
        'pm2_5': 'components.pm2_5',
        'pm10': 'components.pm10',
        'nh3': 'components.nh3'
    }

    features_df = features_df.rename(columns=rename_mapping)
    
    # Reorder columns to match the expected model input order
    features_df = features_df[feature_order]

    standardized_features = scaler.transform(features_df)
    
    # Convert standardized features to a DataFrame (optional)
    standardized_features_df = pd.DataFrame(standardized_features, columns=feature_order)

    processed_data = standardized_features_df.values  # Ensure it's a NumPy array
    processed_data = processed_data.reshape(1, 72, 20)  # Reshape to (1, 72, 20)
    
    
    return processed_data

def add_timestamps_to_predictions(predictions, starting_timestamp, interval=timedelta(hours=1)):
    # Generate timestamps for predictions
    timestamps = [starting_timestamp + i * interval for i in range(len(predictions[0]))]

    # Combine predictions with timestamps in a DataFrame
    predictions_df = pd.DataFrame({
        'timestamp': timestamps,
        'predicted_aqi': predictions[0]
    })

    return predictions_df


# Predict AQI based on real-time data
for message in consumer:
    real_time_data = message.value
    print('Message from producer: ', real_time_data)
    
    processed_data = preprocess_data(real_time_data)

    print(processed_data)

    # Make prediction
    prediction = model.predict(processed_data) 

    predictions_df = add_timestamps_to_predictions(prediction, starting_timestamp=datetime.now())
    # real_time_json = real_time_df.to_json(orient='records', lines=True)
    prediction_json = predictions_df.to_json(orient='records', lines=True)


    print(f"Predicted AQI: {(prediction_json)} ")

    for json_line in prediction_json.splitlines():
        data = json.loads(json_line)
        predicted_aqi = data['predicted_aqi']
        timestamp_ms = data['timestamp']
        timestamp_datetime = datetime.fromtimestamp(timestamp_ms / 1000)  # Ensure correct datetime format
        
        row = [(timestamp_datetime, predicted_aqi)]
        # Insert into ClickHouse
        client.insert("aqi_predictions", row)
        # producer.send('predictions-topic', value=json_line)
        # print(f"Sent message: {timestamp}, {predicted_aqi}")
        
    producer.flush()
    # print(f"Prediction sent to 'predictions-topic': {prediction_json}")
