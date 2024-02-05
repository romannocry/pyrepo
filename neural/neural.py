import numpy as np
import time
import csv

# Sigmoid activation function
def sigmoid(x):
    return 1 / (1 + np.exp(-x))

# Derivative of sigmoid function
def sigmoid_derivative(x):
    return x * (1 - x)

# Function to predict using the trained neural network
def predict(input_value):
    hidden_layer_input = np.dot(input_value, weights_input_output) + bias_output
    prediction = sigmoid(hidden_layer_input)
    return prediction.flatten()

# Set the seed for reproducibility (optional)
np.random.seed(42)

# Generate a random array of shape (200, 3) with values 0 or 1
random_arrays_x_train = np.random.randint(2, size=(100, 3))

# Generate a random array of shape (200, 3) with values 0 or 1
random_arrays_y_train = np.random.randint(2, size=(100, 1))

# Specify the CSV file name
csv_file_name_x = 'x.csv'

# Write the array to the CSV file
with open(csv_file_name_x, 'w', newline='') as csvfile:
    csv_writer = csv.writer(csvfile)
    # Write the header
    csv_writer.writerow(['male/female', 'young/old', 'paris/marseille'])
    # Write the data
    csv_writer.writerows(random_arrays_x_train)
    print(f"CSV file '{csv_file_name_x}' has been created.")

# Specify the CSV file name
csv_file_name_y = 'y.csv'

# Write the array to the CSV file
with open(csv_file_name_y, 'w', newline='') as csvfile:
    csv_writer = csv.writer(csvfile)
    # Write the header
    csv_writer.writerow(['psychopath'])
    # Write the data
    csv_writer.writerows(random_arrays_y_train)
    print(f"CSV file '{csv_file_name_y}' has been created.")

# Generate training data
X_train = random_arrays_x_train#np.array([[0], [1], [2], [3], [4], [5]])
y_train = random_arrays_y_train#np.array([[0], [1], [1], [1], [1], [1]])

# Function to initialize weights and biases
def initialize_parameters(input_size, output_size):
    np.random.seed(42)
    weights = np.random.randn(input_size, output_size)
    biases = np.zeros((1, output_size))
    return weights, biases

# Function to perform forward pass
def forward_pass(inputs, weights, biases):
    hidden_layer_input = np.dot(inputs, weights) + biases
    hidden_layer_output = sigmoid(hidden_layer_input)
    return hidden_layer_output

# Function to train the neural network
def train_neural_network(inputs, outputs, learning_rate, epochs):
    input_size = inputs.shape[1]
    output_size = outputs.shape[1]

    weights, biases = initialize_parameters(input_size, output_size)

    for epoch in range(epochs):
        # Forward pass
        hidden_layer_output = forward_pass(inputs, weights, biases)

        # Calculate the error
        error = outputs - hidden_layer_output

        # Backpropagation
        output_error = error * sigmoid_derivative(hidden_layer_output)
        weights += inputs.T.dot(output_error) * learning_rate
        biases += np.sum(output_error, axis=0, keepdims=True) * learning_rate

    return weights, biases

# Example usage
inputs = random_arrays_x_train
outputs = random_arrays_y_train

learning_rate = 0.1
epochs = 100000

trained_weights, trained_biases = train_neural_network(inputs, outputs, learning_rate, epochs)

# Test the trained neural network
test_input = np.array([[0, 1, 1]])
predicted_output = forward_pass(test_input, trained_weights, trained_biases)

print(f"Predicted Output: {predicted_output[0][0]}")