from flask import Flask, request, jsonify
import joblib
import numpy as np

# Initialize the Flask app
app = Flask(__name__)

# Load the trained XGBoost model
model = joblib.load('xgboost_aqi_model.pkl')

# Route for predicting AQI category
@app.route('/predict', methods=['POST'])
def predict():
    data = request.json  # Get the data from the POST request
    
    # Ensure the necessary fields are provided
    if 'features' not in data:
        return jsonify({'error': 'Missing features data'}), 400
    
    features = np.array(data['features']).reshape(1, -1)  # Reshape for a single prediction
    
    # Make a prediction using the loaded model
    prediction = model.predict(features)
    
    # Define AQI categories based on your encoding
    aqi_categories = ['Good', 'Moderate', 'Unhealthy', 'Unhealthy for Sensitive Groups', 'Very Unhealthy']
    predicted_category = aqi_categories[int(prediction[0])]
    
    # Return the prediction as a JSON response
    return jsonify({'predicted_aqi_category': predicted_category})

if __name__ == '__main__':
    app.run(debug=True)
