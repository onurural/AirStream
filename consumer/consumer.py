from kafka import KafkaConsumer
from kafka import KafkaProducer
import json
import joblib
import numpy as np
import pandas as pd
from sklearn.preprocessing import StandardScaler
from tensorflow.keras.models import load_model


producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda x: json.dumps(x).encode('utf-8')
)

# Load the saved machine learning model (e.g., a Random Forest or LSTM model)
model = load_model('lstm_aqi_model.keras')
scaler = joblib.load('scaler.pkl')
# Initialize Kafka consumer
consumer = KafkaConsumer('raw-data-topic-1',
                         bootstrap_servers=['localhost:9092'],
                         auto_offset_reset='earliest',
                         enable_auto_commit=True,
                         value_deserializer=lambda x: json.loads(x.decode('utf-8')))

# Preprocessing function (if scaling or transformation is needed)
def preprocess_data(data):
    # Create a numpy array with the required features

    features = np.array([[
        data['co'],
        data['no'],
        data['no2'],
        data['o3'],
        data['so2'],
        data['pm2_5'],
        data['pm10'],
        data['nh3'],
        data['temperature'],
        data['dew_point'],
        data['feels_like'],
        data['temp_min'],
        data['temp_max'],
        data['pressure'],
        data['humidity'],
        data['wind_speed'],
        data['wind_deg'],
        data['clouds_all'],
        data['hour'],
        data['day_of_week'],
        data['month']
        
    ]])

    # Apply scaling (assuming you used a scaler during training)
    scaled_features = scaler.transform(features)  # Use the scaler from your training pipeline
    return scaled_features

# Predict AQI based on real-time data
for message in consumer:
    real_time_data = message.value

    print(real_time_data)
    
    # Preprocess data
    processed_data = preprocess_data(real_time_data)
    
    # Make prediction
    prediction = model.predict(processed_data)

    # keys = ['co', 'no', 'no2', 'o3', 'so2', 'pm2_5', 'pm10', 'nh3', 'temperature', 'dew_point', 'feels_like', 'temp_min', 'temp_max', 'pressure', 'humidity', 'wind_speed', 'wind_deg', 'clouds_all', 'datetime', 'hour', 'day_of_week', 'month']
    # feature_dict = dict(zip(keys, processed_data))
    # Print the prediction or send it to another Kafka topic
    aqi_categories = ['Good', 'Moderate', 'Unhealthy', 'Unhealthy for Sensitive Groups', 'Very Unhealthy']
    predicted_category = aqi_categories[int(prediction[0])]

    
    print(f"Predicted AQI: {(prediction[0])} ")
    print(f"AQI: {(real_time_data)} ")


    real_time_data['predicted_aqi'] = float(prediction[0][0])

    producer.send('predictions-topic', value=real_time_data)
    producer.flush()
    print(f"Prediction sent to 'predictions-topic': {real_time_data}")
