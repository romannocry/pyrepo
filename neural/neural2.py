import numpy as np

# Sigmoid activation function
def sigmoid(x):
    return 1 / (1 + np.exp(-x))

# Derivative of sigmoid function
def sigmoid_derivative(x):
    return x * (1 - x)

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



# Load data from CSV file into a NumPy array
x = np.genfromtxt('x.csv', delimiter=',')
x2 = x.reshape(-1,1)
y = np.genfromtxt('y.csv', delimiter=',')
# Reshape the 1D array to a 2D array with one column
y2 = y.reshape(-1, 1)

#print(x)
# Example usage
#inputs = np.array(x)
#outputs = np.array(y2)
inputs = np.array([[1],[2],[3],[4],[5]])
outputs = np.array([[0.1], [0.2], [0.3], [0.4],[0.5]])

learning_rate = 0.01
epochs = 100000

trained_weights, trained_biases = train_neural_network(inputs, outputs, learning_rate, epochs)

# Test the trained neural network
test_input = np.array([[8]])
predicted_output = forward_pass(test_input, trained_weights, trained_biases)

print(f"Predicted Output: {predicted_output[0][0]}")