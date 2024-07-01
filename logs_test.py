import json
import random
import string
import subprocess


def generate_random_string_big(size_in_mb):
    """
    Generates a random string of a specific size in megabytes.

    :param size_in_mb: The desired size of the string in megabytes.
    :return: A random string of the specified size.
    """
    # There are 1 million bytes in a megabyte.
    num_chars = size_in_mb * 10**4

    # Generate a random string of the specified number of characters.
    # Note: This is a simple and not the most efficient way to do it for large sizes.
    random_string = "".join(
        random.choices(string.ascii_letters + string.digits, k=num_chars)
    )

    return random_string


def generate_random_string(length=20):
    """Generate a random string of fixed length."""
    letters = string.ascii_letters + string.digits
    return "".join(random.choice(letters) for i in range(length))


def generate_data():
    """Generate the JSON data with a random string for the body."""
    return {
        "resource_logs": [
            {
                "resource": {
                    "attributes": [
                        {
                            "key": "service.name",
                            "value": {"stringValue": "test-with-curl"},
                        },
                        {
                            "key": "deployment.environment",
                            "value": {"stringValue": "test"},
                        },
                        {"key": "host.name", "value": {"stringValue": "my-test-host"}},
                    ]
                },
                "scope_logs": [
                    {
                        "scope": {"name": "manual-test"},
                        "log_records": [
                            {
                                "name": "test",
                                "severity_text": "INFO",
                                "body": {"stringValue": generate_random_string_big(2)},
                            }
                        ],
                    }
                ],
            }
        ]
    }


def send_data(json_data):
    """Send the generated JSON data to the specified URL."""
    # Define the URL
    url = "http://localhost:4318/v1/logs"

    # Prepare the curl command
    curl_command = (
        f"curl -XPOST {url} -H 'Content-Type: application/json' --data-binary @-"
    )

    # Execute the curl command with the JSON data
    process = subprocess.Popen(curl_command, shell=True, stdin=subprocess.PIPE)
    process.communicate(input=json_data.encode())

    return process.returncode


# Send 10 messages with different random strings in the body
success_count = 0
for _ in range(1000):
    data = generate_data()
    json_data = json.dumps(data)
    result = send_data(json_data)
    if result == 0:
        success_count += 1
        print("Data sent successfully.")
    else:
        print("Failed to send data.")

print(f"Successfully sent {success_count} out of 10 messages.")